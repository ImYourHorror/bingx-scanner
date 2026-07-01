#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
BingX TradFi real-time scanner — точные % в реалтайме (главный экран «Числа»),
фейд-стратегия вынесена в отдельную вкладку «Стратегия» (черновик).

Самодостаточно: ТОЛЬКО стандартная библиотека Python. Никаких pip, $0.
Реал-тайм цена — WebSocket BingX (свой мини-клиент на сокетах, gzip, Ping→Pong).

ДАННЫЕ / ИСТОЧНИКИ (всё проверено против живых API 2026-06-30):
  • Универс TradFi берётся ИЗ API (не хардкод). Класс актива закодирован в префиксе символа:
        NCSK… = акции/ETF («Serenity»), NCFX… = форекс, NCCO… = товары, NCSI… = индексы.
    Отдельного поля-категории в API НЕТ. «Serenity» — не официальное имя; это NCSK-семейство
    (BingX TradFi stock perps, запуск 02.11.2025).
  • Live-цена: WS wss://open-api-swap.bingx.com/swap-market, подписка <SYM>@lastPrice → data.c.
        (open-api-ws.bingx.com/market — это СПОТ, перп-символы там не работают.)
  • Спред/премиум: REST /openApi/swap/v2/quote/premiumIndex (bulk = все сразу одним запросом)
        → indexPrice. Премиум% = (lastPrice_ws − indexPrice) / indexPrice. Покрывает ВСЁ, real-time.
  • Внешний кросс-чек: Pyth Hermes (hermes.pyth.network), Equity.US.<T>/USD, keyless.
        vsPyth% = (lastPrice_ws − pyth) / pyth, где Pyth покрывает (≈107/153 акций); иначе «—».
  • Ref-close (референс гэпа, дневной): Yahoo chart. Гэп% = (lastPrice_ws − close) / close.
        Регион/таймзона/сессия — из meta Yahoo (DST-корректно через zoneinfo).

КОМИССИЯ (перепроверено): 0-fee на TradFi — это ВРЕМЕННОЕ ПРОМО (~13.04→31.07.2026), НЕ постоянно.
  Catch: только реферальные юзеры; ЛЮБОЙ включённый API-ключ лишает льготы ДАЖE для ручных ордеров;
  funding платится всегда. Через API/бота реально списывается СТАНДАРТ: maker 0.02% / taker 0.05%
  (это и есть feeRate из API). UI «0 Fees» — только ручная торговля реферала без API-ключа.
  ⇒ для бота костовый потолок НИКУДА не делся; «0-fee» снимает его только для ручной торговли до 31.07.2026.

Запуск:  python bingx_gap_scanner.py   → открой http://127.0.0.1:8787
"""

import os, re, sys, json, time, gzip, base64, socket, ssl, struct, sqlite3, threading, webbrowser, urllib.request, urllib.error
from urllib.parse import urlencode, urlparse, parse_qs, quote
from datetime import datetime, timedelta, timezone, time as dtime
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

# Windows-консоль часто cp1251/cp866 → принудительно UTF-8, чтобы print с кириллицей/«→» не падал.
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

try:
    from zoneinfo import ZoneInfo
    _HAVE_TZ = True
except Exception:
    _HAVE_TZ = False

# ----------------------------- CONFIG -----------------------------
BINGX        = "https://open-api.bingx.com"
WS_HOST      = "open-api-swap.bingx.com"
WS_PATH      = "/swap-market"
# Бинд/порт/автооткрытие браузера — из окружения (для VPS/systemd за reverse-proxy).
# По умолчанию слушаем ТОЛЬКО loopback (безопасно за Caddy); для Tailscale выстави GAP_HOST=0.0.0.0.
HOST         = os.environ.get("GAP_HOST", "127.0.0.1")
PORT         = int(os.environ.get("GAP_PORT", "8787"))
OPEN_BROWSER = os.environ.get("GAP_OPEN_BROWSER", "1") == "1"

SUBS_PER_WS       = 80      # эмпирически на 1 конн акается ~100 подписок → берём меньше, ДВА конна
PREMIUM_REFRESH   = 5       # сек, bulk premiumIndex (один запрос на всё)
PYTH_REFRESH      = 6       # сек, батч Pyth latest
CLOSE_REFRESH     = 1800    # сек, дневной close (кэш на день всё равно)
SNAPSHOT_REFRESH  = 1       # сек, сборка снапшота из кэшей (без сети)
BROWSER_REFRESH   = 1       # сек, опрос /data браузером

FINNHUB_KEY = os.environ.get("FINNHUB_API_KEY", "").strip()  # необяз. фолбэк close, ключ только из env

MSK = ZoneInfo("Europe/Moscow") if _HAVE_TZ else timezone(timedelta(hours=3))

# Теги для вкладки «Стратегия» (НЕ фильтр универса — универс целиком из API).
CORE   = {"RKLB","CBRS","MRVL","SNDK","INTC","RIVN","AVGO","MU"}
AVOID  = {"RDDT","PLTR","ARM","ORCL","LLY","BMNR","MSFT","CRCL","COST","AMZN"}
REGIME = "QQQ"

# Префикс символа → тип актива
FAMILY = {"NCSK": "stock", "NCSI": "index", "NCCO": "commodity", "NCFX": "forex"}
FAM_RU = {"stock": "Акции", "index": "Индексы", "commodity": "Товары", "forex": "Форекс"}
FAM_ORDER = ["stock", "index", "commodity", "forex"]

# Регион → (IANA tz, окно входа local (h,m), RTH (open h,m)-(close h,m))
# Окна по ТЗ: US 16:00 ET; HK 09:30; JP/KR 09:00; TW 09:00; EU 09:00; IN 09:15; BR 10:00 (local open).
REGIONS = {
    "US": ("America/New_York", (16, 0), ((9, 30), (16, 0))),
    "HK": ("Asia/Hong_Kong",   ( 9,30), ((9, 30), (16, 0))),
    "JP": ("Asia/Tokyo",       ( 9, 0), ((9,  0), (15, 0))),
    "KR": ("Asia/Seoul",       ( 9, 0), ((9,  0), (15,30))),
    "TW": ("Asia/Taipei",      ( 9, 0), ((9,  0), (13,30))),
    "EU": ("Europe/Berlin",    ( 9, 0), ((9,  0), (17,30))),
    "IN": ("Asia/Kolkata",     ( 9,15), ((9, 15), (15,30))),
    "BR": ("America/Sao_Paulo",(10, 0), ((10, 0), (17, 0))),
}
ENTRY_WIN_MIN = 10  # ширина окна входа, ±мин (фактически [t, t+10])
TZ_TO_REGION = {
    "America/New_York": "US", "Asia/Hong_Kong": "HK", "Asia/Tokyo": "JP",
    "Asia/Seoul": "KR", "Asia/Taipei": "TW", "Asia/Kolkata": "IN",
    "America/Sao_Paulo": "BR",
}

# Фиксапы тикера BingX → символ Pyth (Equity.US.<X>/USD)
PYTH_FIX = {"BRKB": "BRK.B", "IBMR": "IBM", "NETFLIX": "NFLX"}
# Фиксапы тикера BingX → реальный US-тикер для Finnhub (close pc). Иностранные без US-листинга
# (SAMSUNG, SKHYNIX) сюда НЕ кладём → Finnhub не вернёт pc → гэп прочерк (так и надо).
FINNHUB_FIX = {"BRKB": "BRK.B", "IBMR": "IBM", "NETFLIX": "NFLX", "TSMU": "TSM"}

# Универс: оставляем только акции/ETF (NCSK). QQQ — внутри NCSK (датчик режима).
# Форекс (NCFX), товары (NCCO) и индексы (NCSI) выкинуты.
ALLOWED_FAMILIES = {"stock"}

# Источник клоуз-референса (переключаемо: Hermes-Pyth станет платным с 31.07.2026,
# поэтому Finnhub — основной надёжный клоуз; параметр на будущее).
CLOSE_SOURCE      = os.environ.get("GAP_CLOSE_SRC", "finnhub")
FINNHUB_MIN_GAP   = 1.1     # сек между вызовами Finnhub (free 60/мин) → ~55/мин
BASIS_SUSPECT_PCT = 5.0     # |live−Pyth|/Pyth выше → строка «данные сомнительны», гэп не торговый
OI_REFRESH        = 20      # сек паузы между полными циклами опроса OI
LOG_INTERVAL      = 60      # сек, как часто писать снапшот в sqlite
DB_PATH = os.environ.get("GAP_DB_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "signals.db")


# ----------------------------- HTTP helpers -----------------------------
def _http_json(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 gap-scanner"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))

def _http_text(url, timeout=15):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 gap-scanner"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")

def _bingx(path, params=None):
    url = BINGX + path + (("?" + urlencode(params)) if params else "")
    j = _http_json(url)
    if isinstance(j, dict) and j.get("code") not in (0, None):
        raise RuntimeError(f"BingX code {j.get('code')}: {j.get('msg')}")
    return j.get("data") if isinstance(j, dict) else j


# ----------------------------- universe (всё из API) -----------------------------
def family_of(sym):
    m = re.match(r"^(NC[A-Z]{2})", sym)
    return FAMILY.get(m.group(1)) if m else None  # None = крипта/прочее → не TradFi

def base_ticker(sym):
    m = re.match(r"^NC[A-Z]{2}(.+?)2USD", sym)
    if m:
        return m.group(1)
    m = re.match(r"^NCFX(.+?)-USDT$", sym)  # форекс: NCFXEUR2USD уже покрыт выше; запас
    return m.group(1) if m else sym

def fetch_universe():
    """→ list[dict(symbol, display, fam, base, taker, maker)] для всех TradFi (NC*)."""
    out = []
    for c in (_bingx("/openApi/swap/v2/quote/contracts") or []):
        if not isinstance(c, dict):
            continue
        sym = c.get("symbol", "")
        fam = family_of(sym)
        if not fam or fam not in ALLOWED_FAMILIES:   # только NCSK-акции/ETF (вкл. QQQ)
            continue
        disp = c.get("displayName") or sym
        b = base_ticker(sym)
        tick = b if fam == "stock" else re.sub(r"-USDT$", "", disp)  # для не-акций имя из displayName
        out.append({
            "symbol": sym, "display": disp, "fam": fam, "base": b, "tick": tick,
            "taker": c.get("takerFeeRate"), "maker": c.get("makerFeeRate"),
        })
    return out


# ----------------------------- WS price feed (stdlib) -----------------------------
def _ws_connect(host, path, port=443, timeout=10):
    raw = socket.create_connection((host, port), timeout=timeout)
    s = ssl.create_default_context().wrap_socket(raw, server_hostname=host)
    s.settimeout(timeout)
    key = base64.b64encode(os.urandom(16)).decode()
    s.send((f"GET {path} HTTP/1.1\r\nHost: {host}\r\nUpgrade: websocket\r\n"
            f"Connection: Upgrade\r\nSec-WebSocket-Key: {key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n\r\n").encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        ch = s.recv(4096)
        if not ch:
            break
        resp += ch
    ok = b" 101 " in resp.split(b"\r\n", 1)[0]
    return s, ok

def _ws_send(s, data, opcode=0x1):
    if isinstance(data, str):
        data = data.encode()
    n = len(data); mask = os.urandom(4); hdr = bytes([0x80 | opcode])
    if n < 126:
        hdr += bytes([0x80 | n])
    elif n < 65536:
        hdr += bytes([0x80 | 126]) + struct.pack(">H", n)
    else:
        hdr += bytes([0x80 | 127]) + struct.pack(">Q", n)
    s.send(hdr + mask + bytes(b ^ mask[i % 4] for i, b in enumerate(data)))

def _ws_recv(s):
    def rd(n):
        buf = b""
        while len(buf) < n:
            ch = s.recv(n - len(buf))
            if not ch:
                raise ConnectionError("ws closed")
            buf += ch
        return buf
    b0, b1 = rd(2)
    op = b0 & 0x0f; masked = b1 & 0x80; ln = b1 & 0x7f
    if ln == 126:
        ln = struct.unpack(">H", rd(2))[0]
    elif ln == 127:
        ln = struct.unpack(">Q", rd(8))[0]
    mask = rd(4) if masked else None
    pl = rd(ln) if ln else b""
    if mask:
        pl = bytes(b ^ mask[i % 4] for i, b in enumerate(pl))
    return op, pl

def _ws_decode(pl):
    try:
        return gzip.decompress(pl).decode("utf-8", "replace")
    except Exception:
        try:
            return pl.decode("utf-8", "replace")
        except Exception:
            return ""

class PriceFeed:
    """Несколько ws-соединений (≤190 подписок каждое) держат свежий lastPrice."""
    def __init__(self, symbols):
        self.symbols = symbols
        self.prices = {}            # sym -> (price float, epoch_ms)
        self.lock = threading.Lock()
        self.conns_up = 0
        self.series = None          # Series (буфер для графика), ставится в main

    def start(self):
        for i in range(0, len(self.symbols), SUBS_PER_WS):
            chunk = self.symbols[i:i + SUBS_PER_WS]
            threading.Thread(target=self._loop, args=(chunk,), daemon=True).start()

    def get(self, sym):
        with self.lock:
            return self.prices.get(sym)

    def status(self):
        with self.lock:
            return self.conns_up, len(self.prices)

    def _loop(self, chunk):
        while True:
            try:
                self._session(chunk)
            except Exception:
                pass
            time.sleep(2)  # backoff + reconnect

    def _session(self, chunk):
        s, ok = _ws_connect(WS_HOST, WS_PATH)
        if not ok:
            s.close(); return
        for sym in chunk:
            _ws_send(s, json.dumps({"id": sym, "reqType": "sub",
                                    "dataType": f"{sym}@lastPrice"}))
            time.sleep(0.04)                   # троттлинг: иначе часть подписок не акается
        with self.lock:
            self.conns_up += 1
        s.settimeout(30)
        try:
            while True:
                op, pl = _ws_recv(s)
                if op == 0x8:
                    break
                if op == 0x9:                      # ws-ping → ws-pong
                    _ws_send(s, pl, 0xA); continue
                txt = _ws_decode(pl)
                if not txt:
                    continue
                if txt.strip() == "Ping":          # heartbeat BingX
                    _ws_send(s, "Pong"); continue
                try:
                    d = json.loads(txt).get("data")
                except Exception:
                    continue
                if isinstance(d, dict) and d.get("e") == "lastPriceUpdate":
                    c = d.get("c"); sym = d.get("s")
                    if sym and c is not None:
                        px = float(c)
                        with self.lock:
                            self.prices[sym] = (px, d.get("E"))
                        if self.series is not None:
                            self.series.add_bingx(sym, px, (d.get("E") or time.time() * 1000) / 1000.0)
        finally:
            with self.lock:
                self.conns_up -= 1
            try: s.close()
            except Exception: pass


# ----------------------------- premiumIndex (spread) -----------------------------
class PremiumFeed:
    def __init__(self):
        self.data = {}              # sym -> dict(indexPrice, markPrice, lastFundingRate, ...)
        self.lock = threading.Lock()
    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
    def get(self, sym):
        with self.lock:
            return self.data.get(sym)
    def _loop(self):
        while True:
            try:
                arr = _bingx("/openApi/swap/v2/quote/premiumIndex")  # bulk
                if isinstance(arr, list):
                    m = {d["symbol"]: d for d in arr if isinstance(d, dict) and d.get("symbol")}
                    with self.lock:
                        self.data = m
            except Exception:
                pass
            time.sleep(PREMIUM_REFRESH)


# ----------------------------- Pyth Hermes (external cross-check) -----------------------------
HERMES = "https://hermes.pyth.network"

class PythFeed:
    def __init__(self, bases):
        self.bases = bases          # список базовых тикеров акций
        self.id2base = {}           # feedID -> base
        self.base2id = {}           # base -> feedID
        self.px = {}                # base -> (price float, publish_time int)
        self.lock = threading.Lock()
        self.covered = 0

    def start(self):
        try:
            self._load_catalog()
        except Exception:
            pass
        threading.Thread(target=self._loop, daemon=True).start()

    def _load_catalog(self):
        cat = _http_json(HERMES + "/v2/price_feeds?asset_type=equity", timeout=30)
        usmap = {}
        for f in cat:
            a = f.get("attributes", {}) or {}
            desc = (a.get("description") or "").upper()
            if "DEPRECATED" in desc:                  # .PRE/.POST/.ON — пропускаем
                continue
            sym = a.get("symbol", "")
            country = (a.get("country") or "").upper()
            qcur = (a.get("quote_currency") or a.get("quoteCurrency") or "").upper()
            base_attr = a.get("base")
            tk = None
            m = re.match(r"^Equity\.US\.(.+?)/USD$", sym)
            if m:
                tk = m.group(1)
            elif country == "US" and qcur == "USD" and base_attr:   # шире, чем регэксп
                tk = base_attr
            if tk:
                usmap.setdefault(tk, f["id"])
        for b in self.bases:
            key = PYTH_FIX.get(b, b)
            fid = usmap.get(key)
            if fid:
                self.base2id[b] = fid
                self.id2base[fid] = b
        self.covered = len(self.base2id)

    def get(self, base):
        with self.lock:
            return self.px.get(base)

    def coverage(self):
        return self.covered, len(self.bases)

    def _loop(self):
        ids = list(self.id2base.keys())
        while True:
            try:
                for i in range(0, len(ids), 50):          # батчами по 50
                    chunk = ids[i:i + 50]
                    q = "&".join("ids[]=" + x for x in chunk)
                    j = _http_json(HERMES + "/v2/updates/price/latest?" + q, timeout=20)
                    upd = {}
                    for p in j.get("parsed", []):
                        fid = p.get("id"); pr = p.get("price", {})
                        base = self.id2base.get(fid) or self.id2base.get((fid or "").lower())
                        if not base:
                            continue
                        try:
                            val = int(pr["price"]) * (10 ** int(pr["expo"]))
                            pt = int(pr.get("publish_time") or 0)
                        except Exception:
                            continue
                        if val > 0 and pt > 0:
                            upd[base] = (val, pt)
                    if upd:
                        with self.lock:
                            self.px.update(upd)
            except Exception:
                pass
            time.sleep(PYTH_REFRESH)


# ----------------------------- 10-мин буфер на символ (для графика) -----------------------------
SERIES_MAXAGE = 600   # сек = 10 мин

def fetch_klines_1m(sym, limit=12):
    arr = _bingx("/openApi/swap/v3/quote/klines", {"symbol": sym, "interval": "1m", "limit": limit})
    out = []
    for b in (arr or []):
        try:
            t = int(b["time"]) / 1000.0 if isinstance(b, dict) else int(b[0]) / 1000.0
            c = float(b["close"]) if isinstance(b, dict) else float(b[4])
            out.append((t, c))
        except Exception:
            continue
    return sorted(out)

class Series:
    """Постоянный буфер последних ~10 мин (ts_sec, price) на каждый символ.
       BingX-линия: сид из 1m-klines при старте + добивка из WS. Base-линия: Pyth (real-time)."""
    def __init__(self, symbols):
        self.bx   = {s: deque() for s in symbols}
        self.base = {s: deque() for s in symbols}
        self.lock = threading.Lock()
        self._symbols = list(symbols)
        self.seeded = 0

    def start(self):
        threading.Thread(target=self._seed_loop, daemon=True).start()

    def _trim(self, dq, now):
        cut = now - SERIES_MAXAGE
        while dq and dq[0][0] < cut:
            dq.popleft()
        while len(dq) > 1500:
            dq.popleft()

    def add_bingx(self, sym, px, ts_sec):
        dq = self.bx.get(sym)
        if dq is None:
            return
        with self.lock:
            dq.append((ts_sec, px)); self._trim(dq, ts_sec)

    def add_base(self, sym, px, ts_sec):
        dq = self.base.get(sym)
        if dq is None:
            return
        with self.lock:
            if dq and dq[-1][1] == px and ts_sec - dq[-1][0] < 4:
                return
            dq.append((ts_sec, px)); self._trim(dq, ts_sec)

    def get(self, sym):
        with self.lock:
            return list(self.bx.get(sym, [])), list(self.base.get(sym, []))

    def _seed_loop(self):
        for sym in self._symbols:                      # один проход при старте
            try:
                pts = fetch_klines_1m(sym)
                dq = self.bx.get(sym)
                if dq is not None and pts:
                    with self.lock:
                        have = {round(t) for t, _ in dq}
                        merged = [(t, p) for t, p in pts if round(t) not in have] + list(dq)
                        merged.sort()
                        dq.clear(); dq.extend(merged)
                        self.seeded += 1
            except Exception:
                pass
            time.sleep(0.08)

# ----------------------------- timezone helper -----------------------------
def _et_naive_tz(name):
    if _HAVE_TZ:
        try:
            return ZoneInfo(name)
        except Exception:
            pass
    return timezone(timedelta(hours=0))

class RateLimited(Exception):
    pass

def us_ticker(base):
    """Базовый тикер перпа → реальный US-тикер для Finnhub."""
    return FINNHUB_FIX.get(base, base)

def finnhub_pc(ticker):
    """Вчерашний RTH-клоуз (pc) c Finnhub. None если нет данных; RateLimited на 429."""
    if not FINNHUB_KEY:
        return None
    url = f"https://finnhub.io/api/v1/quote?symbol={quote(ticker)}&token={FINNHUB_KEY}"
    req = urllib.request.Request(url, headers={"User-Agent": "gap-scanner"})
    try:
        with urllib.request.urlopen(req, timeout=12) as r:
            j = json.loads(r.read().decode("utf-8"))
    except urllib.error.HTTPError as e:
        if e.code == 429:
            raise RateLimited()
        return None
    except Exception:
        return None
    try:
        pc = float(j.get("pc"))
    except Exception:
        return None
    return pc if pc and pc > 0 else None

class RefData:
    """Дневной previous-close (RTH) из Finnhub — ОСНОВНОЙ клоуз-референс по US-тикерам.
    pc меняется раз в день → тянем последовательно с задержкой (free-лимит 60/мин,
    ~150 тикеров за ~3 мин), кэшируем на торговый день US, обновляем при смене даты ET.
    Yahoo больше не используется (Finnhub надёжнее и не отдаёт местную валюту по US-ADR)."""
    def __init__(self, instruments):
        self.inst = [it for it in instruments if it["fam"] == "stock"]
        self.ref = {}               # base -> dict(close, src, region, cur, us, day)
        self.lock = threading.Lock()
    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
    def get(self, base):
        with self.lock:
            return self.ref.get(base)
    def _ref_marker(self):
        # Референс-клоуз должен быть ПОСЛЕДНИМ ЗАВЕРШЁННЫМ RTH-закрытием US.
        # Маркер меняется при смене даты ET (00:00 ET = 07:00 МСК, задолго до премаркета
        # 15:30–16:30 МСК → к премаркету pc свежий = вчерашний клоуз) И после 17:00 ET
        # (закрытие+сеттлмент → pc перевернулся на сегодняшний клоуз для вечера/овернайта).
        now = datetime.now(_et_naive_tz("America/New_York"))
        phase = "post" if (now.weekday() < 5 and (now.hour, now.minute) >= (17, 0)) else "pre"
        return now.strftime("%Y-%m-%d") + phase
    def _loop(self):
        while True:
            marker = self._ref_marker()
            for it in self.inst:
                b = it["base"]
                cur = self.ref.get(b)
                if cur and cur.get("day") == marker:  # уже тянули в этой фазе (вкл. «нет данных»)
                    continue
                ust = us_ticker(b)
                pc = None
                for _ in range(3):                    # backoff на 429
                    try:
                        pc = finnhub_pc(ust); break
                    except RateLimited:
                        time.sleep(5)
                    except Exception:
                        break
                with self.lock:
                    self.ref[b] = {"close": pc, "src": ("finnhub" if pc else None),
                                   "region": "US", "cur": ("USD" if pc else None),
                                   "us": ust, "day": marker}
                time.sleep(FINNHUB_MIN_GAP)           # бережём free-лимит 60/мин
            time.sleep(CLOSE_REFRESH)                 # ждём смены фазы/даты ET


# ----------------------------- region / session / DST window -----------------------------
def _region_now(region):
    tzname = REGIONS.get(region, REGIONS["US"])[0]
    return datetime.now(_et_naive_tz(tzname))

def session_state(region):
    """('RTH'|'pre'|'after'|'closed'|'24/5'|'—', in_entry_window: bool)"""
    if region in ("FX", "COMM"):
        return ("24/5", False)
    cfg = REGIONS.get(region)
    if not cfg:
        return ("—", False)
    tzname, win, rth = cfg
    now = datetime.now(_et_naive_tz(tzname))
    if now.weekday() >= 5:
        return ("выходной", False)
    cur = now.hour * 60 + now.minute
    o = rth[0][0] * 60 + rth[0][1]; c = rth[1][0] * 60 + rth[1][1]
    wmin = win[0] * 60 + win[1]
    in_win = (wmin <= cur <= wmin + ENTRY_WIN_MIN)
    if cur < o:
        st = "пре"
    elif cur < c:
        st = "RTH"
    else:
        st = "afterh"
    return (st, in_win)

def window_msk_str(region):
    """Окно входа региона (local) → строка по МСК (DST-корректно)."""
    cfg = REGIONS.get(region)
    if not cfg:
        return None
    tzname, win, _ = cfg
    tz = _et_naive_tz(tzname)
    now_local = datetime.now(tz)
    wl = now_local.replace(hour=win[0], minute=win[1], second=0, microsecond=0)
    return wl.astimezone(MSK).strftime("%H:%M")


# ----------------------------- open interest (BingX REST) -----------------------------
class OIFeed:
    """Открытый интерес по символам. Bulk-эндпоинта нет → опрашиваем по одному с паузой.
    OI меняется медленно, полный цикл ~минута — ок."""
    def __init__(self, symbols):
        self.symbols = symbols
        self.oi = {}                # sym -> float
        self.lock = threading.Lock()
    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
    def get(self, sym):
        with self.lock:
            return self.oi.get(sym)
    def _loop(self):
        logged = 0
        while True:
            for sym in self.symbols:
                try:
                    d = _bingx("/openApi/swap/v2/quote/openInterest", {"symbol": sym})
                    if logged < 3:                    # сырой ответ в лог для сверки единиц
                        print(f"OI raw [{sym}]: {json.dumps(d, ensure_ascii=False)}")
                        logged += 1
                    v = d.get("openInterest") if isinstance(d, dict) else (
                        d[0].get("openInterest") if isinstance(d, list) and d else None)
                    if v is not None:
                        with self.lock:
                            self.oi[sym] = float(v)
                except Exception:
                    pass
                time.sleep(0.25)
            time.sleep(OI_REFRESH)


# ----------------------------- signal log (sqlite3, stdlib) -----------------------------
class SignalLog:
    """Тихий лог снапшотов в SQLite — датасет под будущий форвард-тест гэпов."""
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.conn = None
        self.ok = False
    def start(self):
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self.conn = sqlite3.connect(self.path, check_same_thread=False)
            self.conn.execute("""CREATE TABLE IF NOT EXISTS signals(
                ts INTEGER, ticker TEXT, gap REAL, live_price REAL, close_ref REAL,
                basis REAL, funding REAL, oi REAL, qqq_regime REAL, session TEXT)""")
            self.conn.commit()
            self.ok = True
        except Exception as e:
            self.ok = False
            print(f"!! SignalLog отключён (не пишется {self.path}): {e}")
    def write(self, ts, rows, qqq):
        if not self.ok:
            return
        try:
            recs = []
            for r in rows:
                if r.get("gap") is None:              # пишем только торговые гэпы
                    continue
                fr = (r.get("funding") or {}).get("rate")
                recs.append((ts, r["ticker"], r["gap"], r["live"], r["close"],
                             r.get("basis"), fr, r.get("oi"), qqq, r["session"]))
            if recs:
                with self.lock:
                    self.conn.executemany("INSERT INTO signals VALUES(?,?,?,?,?,?,?,?,?,?)", recs)
                    self.conn.commit()
        except Exception:
            pass


# ----------------------------- snapshot builder -----------------------------
STATE = {"updated": None, "rows": [], "strategy": {}, "regime": None,
         "ws": "—", "pyth": "—", "note": "", "now_msk": None, "windows": {}}
LOCK = threading.Lock()

# глобалы для /series (ставятся в main)
SERIES = None          # Series

def _strategy_label(base, gap):
    """Черновик-бакеты. Знак гэпа всегда виден у вызывающего."""
    a = abs(gap)
    tag = "core" if base in CORE else ("avoid" if base in AVOID else "extra")
    if a < 1:
        return "noise", "шум <1%", tag
    if a >= 2:
        return "skip", "скип >2% (убегает)", tag
    if gap > 0:   # перп ВЫШЕ close → фейд-шорт
        if tag == "core":
            return "fade_short", "✅ фейд-шорт", tag
        return "short_weak", "1–2% шорт, не ядро", tag
    return "long_weak", "1–2% лонг — слабая нога", tag   # перп НИЖЕ close

def build_snapshot(inst, pf, prem, pyth, ref, oif):
    rows = []
    regime = None
    buckets = {"fade_short": [], "short_weak": [], "long_weak": [], "skip": [], "noise": []}
    for it in inst:
        sym, fam, base = it["symbol"], it["fam"], it["base"]
        pv = pf.get(sym)
        live = pv[0] if pv else None
        if live is None:                    # нет live-цены BingX → торговать нельзя → скрываем
            continue
        # spread / premium из BingX
        pm = prem.get(sym)
        premium = None
        if pm and live is not None:
            try:
                ip = float(pm.get("indexPrice"))
                if ip:
                    premium = (live - ip) / ip * 100
            except Exception:
                premium = None
        # funding (перп, платится всегда) — из premiumIndex
        funding = None
        if pm:
            try:
                fr = pm.get("lastFundingRate")
                funding = {"rate": float(fr) * 100 if fr not in (None, "") else None,
                           "ih": pm.get("fundingIntervalHours")}
            except Exception:
                funding = None
        # Pyth = real-time «реальная цена базы» (кросс-чек) + добивка base-линии графика
        vp = None; base_px = None
        pp = pyth.get(base)
        if pp and pp[0]:
            base_px = pp[0]
            if live is not None:
                vp = (live - base_px) / base_px * 100
            if SERIES is not None:
                SERIES.add_base(sym, base_px, time.time())
        basis = vp                                    # (live − база)/база, %
        # close-референс (Finnhub pc — основной) и регион
        rd = ref.get(base)
        close = rd["close"] if rd else None
        close_src = rd.get("src") if rd else None
        region = (rd or {}).get("region", "US")
        # гэп + надёжность (1c): лучше прочерк, чем мусор
        gap = None; gap_raw = None; suspect = False; reason = None
        if close and live is not None and close != 0:
            gap_raw = (live - close) / close * 100
            if basis is not None and abs(basis) > BASIS_SUSPECT_PCT:
                suspect = True; reason = f"перп оторван от Pyth {basis:+.1f}%"
            elif abs(gap_raw) > 25:
                suspect = True; reason = "аномалия >25%"
            else:
                gap = gap_raw                         # торговый гэп
        elif live is not None:
            reason = "нет надёжного клоуза (Finnhub)"
        oi = oif.get(sym) if oif else None
        st, in_win = session_state(region)
        row = {
            "ticker": it["tick"], "symbol": it["display"], "api": sym, "fam": fam,
            "live": live, "close": close, "close_src": close_src,
            "gap": gap, "gap_raw": gap_raw, "suspect": suspect, "reason": reason,
            "premium": premium, "pyth": vp, "basis": basis, "funding": funding, "oi": oi,
            "taker": it["taker"], "region": region, "session": st, "in_win": in_win,
            "tag": ("core" if base in CORE else ("avoid" if base in AVOID else "")),
            "is_regime": (base == REGIME),
        }
        rows.append(row)
        if base == REGIME:
            regime = row
        # стратегия: только акции с гэпом
        if fam == "stock" and gap is not None:
            bucket, label, tag = _strategy_label(base, gap)
            item = {"ticker": base, "api": sym, "gap": gap,
                    "label": label, "tag": tag, "region": region, "session": st, "in_win": in_win}
            if bucket == "short_weak":
                buckets["short_weak"].append(item)
            else:
                buckets.setdefault(bucket, []).append(item)

    # сортировки
    fam_rank = {f: i for i, f in enumerate(FAM_ORDER)}
    def absk(x, key):
        v = x.get(key)
        return -abs(v) if v is not None else 1e9
    rows.sort(key=lambda r: (fam_rank.get(r["fam"], 9), r["gap"] is None and r["premium"] is None,
                             absk(r, "gap") if r["gap"] is not None else absk(r, "premium")))
    for k in buckets:
        buckets[k].sort(key=lambda x: -abs(x["gap"]))

    cw, cp = pf.status()
    cov_c, cov_t = pyth.coverage()
    now_msk = datetime.now(MSK)
    windows = {r: window_msk_str(r) for r in REGIONS}
    us_st, _ = session_state("US")
    us_session = {"пре": "премаркет", "RTH": "RTH (торги)", "afterh": "afterhours",
                  "выходной": "выходной"}.get(us_st, us_st)
    with LOCK:
        STATE.update({
            "updated": now_msk.strftime("%H:%M:%S МСК"),
            "now_msk": now_msk.strftime("%H:%M:%S"),
            "rows": rows, "regime": regime,
            "strategy": {"note": "ЧЕРНОВИК — пороги не оттестированы", "buckets": buckets},
            "ws": f"{cw} конн · {cp} цен",
            "pyth": f"{cov_c}/{cov_t} акций",
            "windows": windows, "us_session": us_session,
            "note": "",
        })

def updater(inst, pf, prem, pyth, ref, oif, siglog):
    last_log = 0.0
    while True:
        try:
            build_snapshot(inst, pf, prem, pyth, ref, oif)
            now = time.time()
            if siglog and now - last_log >= LOG_INTERVAL:
                with LOCK:
                    rows = list(STATE.get("rows", []))
                    rg = STATE.get("regime")
                siglog.write(int(now), rows, (rg.get("gap") if rg else None))
                last_log = now
        except Exception as e:
            with LOCK:
                STATE["note"] = f"updater error: {e}"
        time.sleep(SNAPSHOT_REFRESH)


# ----------------------------- HTML -----------------------------
HTML = r"""<!doctype html><html lang="ru"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>BingX · TradFi real-time</title>
<style>
:root{--bg:#0d1117;--panel:#161b22;--line:#262d36;--txt:#e6edf3;--mut:#7d8590;
--go:#2ea043;--skip:#f85149;--warn:#d29922;--blue:#388bfd;--up:#3fb950;--dn:#f85149}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--txt);
font:13px/1.45 ui-monospace,SFMono-Regular,Menlo,Consolas,monospace}
.wrap{max-width:1180px;margin:0 auto;padding:16px 14px 48px}
h1{font-size:16px;margin:0 0 2px;letter-spacing:.3px}
.sub{color:var(--mut);font-size:12px;margin-bottom:10px}
.tabs{display:flex;gap:6px;margin:10px 0 12px}
.tab{cursor:pointer;font-size:13px;padding:6px 14px;border:1px solid var(--line);
border-radius:6px;color:var(--mut);background:var(--panel)}
.tab.on{color:#0d1117;background:var(--txt);font-weight:700;border-color:var(--txt)}
.bar{display:flex;gap:8px;align-items:center;flex-wrap:wrap;margin-bottom:10px;color:var(--mut);font-size:11px}
.pill{padding:3px 8px;border:1px solid var(--line);border-radius:4px}
.pill.win{color:#0d1117;background:var(--go);border-color:var(--go);font-weight:700}
.regime{font-size:12px;color:var(--mut);padding:8px 10px;background:var(--panel);
border:1px solid var(--line);border-radius:6px;margin-bottom:12px}
table{width:100%;border-collapse:collapse;background:var(--panel);
border:1px solid var(--line);border-radius:8px;overflow:hidden}
th,td{text-align:right;padding:6px 9px;border-bottom:1px solid var(--line);white-space:nowrap}
th{color:var(--mut);font-weight:600;font-size:10px;text-transform:uppercase;letter-spacing:.4px}
td.l,th.l{text-align:left}
tr:last-child td{border-bottom:none}
tr.grp td{background:#11161d;color:var(--mut);font-weight:700;text-transform:uppercase;
font-size:10px;letter-spacing:.5px}
tr.win{background:rgba(46,160,67,.10)}
.up{color:var(--up)}.dn{color:var(--dn)}.mut{color:var(--mut)}
.tag{font-size:9px;padding:1px 5px;border-radius:3px;margin-left:6px;vertical-align:middle}
.tag.core{background:rgba(46,160,67,.18);color:var(--go)}
.tag.avoid{background:rgba(248,81,73,.16);color:var(--skip)}
.dot{display:inline-block;width:7px;height:7px;border-radius:50%;margin-right:6px;vertical-align:middle}
.dot.stock{background:var(--go)}.dot.index{background:var(--blue)}
.dot.commodity{background:var(--warn)}.dot.forex{background:#9b7cf6}
th[data-key]{cursor:pointer;user-select:none}th[data-key]:hover{color:var(--txt)}
th .ar{color:var(--blue)}
.bxico{vertical-align:middle;margin-right:3px}
tbody tr{cursor:pointer}tbody tr:hover{background:rgba(56,139,253,.08)}
tr.sel{background:rgba(56,139,253,.16)!important;box-shadow:inset 2px 0 0 var(--blue)}
.fz{cursor:pointer;padding:3px 8px;border:1px solid var(--line);border-radius:4px;background:var(--panel);color:var(--mut)}
.fz.on{color:#0d1117;background:var(--blue);border-color:var(--blue);font-weight:700}
.bk{margin:0 0 14px}.bk h3{font-size:12px;margin:0 0 6px;letter-spacing:.3px}
.bk.fade h3{color:var(--go)}.bk.skip h3{color:var(--skip)}.bk.noise h3{color:var(--mut)}
.bk.long h3,.bk.short h3{color:var(--warn)}
.row{display:flex;justify-content:space-between;padding:4px 8px;border:1px solid var(--line);
border-radius:5px;margin-bottom:4px;background:var(--panel)}
.row.win{border-color:var(--go)}
.legend{color:var(--mut);font-size:11px;margin-top:12px;line-height:1.7}
.faq{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px 16px;line-height:1.65}
.faq h3{font-size:13px;margin:14px 0 4px;color:var(--txt)}.faq h3:first-child{margin-top:0}
.faq p{margin:4px 0;color:var(--mut)}.faq b{color:var(--txt)}
.faq code{background:#11161d;padding:1px 5px;border-radius:3px;color:#9cc7ff}
#chartpanel{margin-top:12px;background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:10px 12px}
.chead{display:flex;align-items:center;gap:10px;margin-bottom:6px}
.chead .cx{margin-left:auto;cursor:pointer;color:var(--mut);font-size:14px}
.chead .cx:hover{color:var(--skip)}
#csvg{width:100%;height:220px;display:block}
.cleg{font-size:11px;color:var(--mut);margin-top:4px}
.cleg .lbx{color:var(--blue)}.cleg .lbs{color:var(--warn)}
.tvrow{margin-top:8px}.tvbtn{cursor:pointer;font-size:11px;color:var(--blue);border:1px solid var(--line);
padding:3px 8px;border-radius:4px}
#tvwrap{margin-top:8px}
tr.chartrow td{padding:0;background:#0e1320}
.cbox{padding:10px 12px}
#cchart{height:240px;width:100%}
td.susp{color:var(--mut);text-decoration:underline dotted;cursor:help}
.qhelp{cursor:help;color:var(--blue);font-size:11px;margin-left:4px}
#bxlink{color:var(--blue);text-decoration:none;font-size:11px;margin-left:8px}
#bxlink:hover{text-decoration:underline}
.tvattr{color:var(--mut);font-size:10px;margin-left:10px}.tvattr a{color:var(--mut)}
.tvbtn{color:var(--go)!important;border-color:var(--go)!important}
.tvchart{color:var(--go);text-decoration:none;font-size:11px;margin-left:8px;border:1px solid var(--go);padding:3px 8px;border-radius:4px}
.tvchart:hover{background:rgba(46,160,67,.12)}
#buckets .srow{cursor:pointer}#buckets .srow:hover{background:rgba(56,139,253,.10)}
#buckets .srow.sel{box-shadow:inset 2px 0 0 var(--blue)}
.sess{padding:3px 8px;border-radius:4px;border:1px solid var(--line);font-weight:700}
.sess.rth{color:#0d1117;background:var(--go);border-color:var(--go)}
.sess.pre{color:#0d1117;background:var(--warn);border-color:var(--warn)}
.sess.ah{color:var(--mut)}
.draft{display:inline-block;background:var(--warn);color:#0d1117;font-weight:700;
font-size:10px;padding:2px 7px;border-radius:4px;margin-left:8px}
.hide{display:none}
</style></head><body><div class="wrap">
<h1>BingX · TradFi real-time</h1>
<div class="sub">live = WebSocket lastPrice · гэп = (live − Finnhub close)/close · vs Pyth = базис к реальной цене · клик по строке → график · обновлено <span id="upd">—</span></div>
<div class="bar">
  <span class="pill">ws: <span id="ws">—</span></span>
  <span class="pill">pyth: <span id="pyth">—</span></span>
  <span class="sess ah" id="sesspill">US: —</span>
  <span class="pill">US: закрытие 23:00 · вход ~16:00 · открытие 16:30 МСК <span class="qhelp" title="Гэп считается от вчерашнего закрытия US RTH (23:00 МСК). Вход в премаркет ~16:00–16:10 МСК, рынок открывается 16:30 МСК.">(?)</span></span>
  <span class="fz" id="freezebtn">❄ заморозить порядок</span>
</div>
<div class="tabs">
  <span class="tab on" id="t-num" onclick="show('num')">Акции</span>
  <span class="tab" id="t-strat" onclick="show('strat')">Стратегия</span>
  <span class="tab" id="t-faq" onclick="show('faq')">FAQ</span>
</div>

<div id="num">
<table><thead><tr>
<th class="l" data-key="ticker">Тикер<span class="ar"></span></th>
<th data-key="live"><svg class="bxico" width="13" height="13" viewBox="0 0 150 150"><defs><linearGradient id="bxLogo" x1="17.68" y1="116.45" x2="132.14" y2="32.11" gradientUnits="userSpaceOnUse"><stop offset="0" stop-color="#2a54ff"/><stop offset=".52" stop-color="#2143cb"/><stop offset="1" stop-color="#2a54ff"/></linearGradient></defs><path fill="url(#bxLogo)" d="M140.2,22.33c-25.18-.09-49.79,10.83-66.63,29.47-6.06,6.27-10.1,13.95-14.96,21.06-11.64,15.93-29.81,25.14-49.5,25.13h0v28.65h0c25.17,.1,49.78-10.86,66.63-29.5,6.03-6.27,10.13-13.94,14.96-21.06,11.64-15.91,29.81-25.12,49.5-25.11V22.33h0Z"/><path fill="#2a54ff" d="M140.2,97.99c-19.68,0-37.86-9.2-49.5-25.11-4.81-7.12-8.92-14.78-14.94-21.06C58.95,33.18,34.3,22.24,9.13,22.35h0v28.65h0c21.8-.11,42.05,11.62,53.01,30.46,3.22,5.62,7.06,10.9,11.45,15.74,16.83,18.63,41.46,29.59,66.63,29.5l-.02-28.7h0Z"/></svg>Live BingX<span class="ar"></span></th>
<th data-key="close">Ref close<span class="ar"></span></th>
<th data-key="gap">Гэп%<span class="ar"></span></th>
<th data-key="pyth">vs Pyth%<span class="ar"></span></th>
<th data-key="oi">OI $<span class="ar"></span></th>
<th data-key="funding">Funding<span class="ar"></span></th>
</tr></thead><tbody id="tb"></tbody></table>

<div class="legend">
только US-акции/ETF (NCSK, вкл. QQQ); строки без live-цены BingX скрыты (торговать нельзя) · клик по заголовку — сортировка (числовые — числом) ·
<b>«заморозить порядок»</b> работает и в «Акции», и в «Стратегия» · <b>клик по строке → график (lightweight-charts)</b> в обеих вкладках ·
<b>OI $</b> = открытый интерес в USD-нотионале (потолок ~$1M/инструмент) · гэп «—» = нет клоуза/данные сомнительны (наведи)
</div>
</div>

<div id="strat" class="hide">
<div class="regime" id="regime">QQQ —</div>
<div id="buckets"></div>
<div class="legend">Бакеты строятся по знаковому гэпу акций: ✅ фейд-шорт = перп выше close на 1–2% И тикер из ядра ·
&gt;2% скип (со знаком) · &lt;1% шум · перп ниже close 1–2% = лонг (слабая нога).
Теги: <span class="tag core">ядро</span> <span class="tag avoid">мусор</span>. Это ЧЕРНОВИК.</div>
</div>

<div id="faq" class="hide"><div class="faq">
<h3>Что считает каждая колонка</h3>
<p><b>Live BingX</b> — последняя цена перпа из WebSocket-потока (<code>@lastPrice</code>), обновляется в реалтайме. Иконка слева = источник цены (BingX); задел под другие биржи.</p>
<p><b>Ref close</b> — вчерашний RTH-клоуз базового актива (Finnhub <code>pc</code> — основной источник). Якорь для гэпа, обновляется раз в день после закрытия US.</p>
<p><b>Гэп% = (BingX − close) / close</b> — насколько перп ушёл от вчерашнего закрытия. «—» если нет надёжного Finnhub-клоуза, ИЛИ базис перп-vs-Pyth аномальный (&gt;5%) либо гэп &gt;25% — тогда строка «данные сомнительны» (наведи на «—»: причина + сырой %). Лучше прочерк, чем мусор.</p>
<p><b>vs Pyth% = (BingX − Pyth) / Pyth</b> — кросс-чек против независимого оракула Pyth (реальная цена акции, real-time). Только где Pyth покрывает (≈109/153), иначе «—». Большое расхождение = перп оторвался от рынка. (Спред-премиум к индекс-цене BingX из таблицы убран.)</p>
<p><b>OI $</b> — открытый интерес перпа в USD-нотионале (BingX <code>/openInterest</code>). У этих сток-перпов он с потолком ~$1M на инструмент, поэтому значения жмутся к ~$0.9–1.0M. <code>k/M</code> = тысячи/миллионы $. Пишется в sqlite-лог.</p>
<p><b>Funding</b> — ставка финансирования за период (напр. <code>+0.0100%/8h</code>). Платится ВСЕГДА — часть реального коста удержания. «+» лонги платят шортам. (Стандартный taker 0.05% — постоянная величина, из таблицы убрана.)</p>
<p><b>Сессия US</b> — состояние рынка (премаркет / RTH / afterhours / выходной) вынесено одним индикатором в шапку; меняется в течение дня и важно для окна входа.</p>
<h3>Вкладка «Стратегия» (черновик)</h3>
<p>Раскладывает акции по знаковому гэпу: <b>✅ фейд-шорт</b> = перп выше закрытия на 1–2% И тикер из «ядра» (играем на возврат вниз); <b>шорт 1–2% не ядро</b> и <b>лонг 1–2%</b> (слабая нога) — пограничные; <b>скип &gt;2%</b> — гэп убегает; <b>шум &lt;1%</b> — мелочь. Пороги НЕ оттестированы — это черновик.</p>
<h3>Почему по части тикеров клоуз/гэп скрыт</h3>
<p>Перп номинирован в USD. Для иностранных акций без US-листинга (Samsung, SK Hynix) Finnhub не отдаёт <code>pc</code> → клоуза нет → гэп «—». Остаётся спред (внутри-инструментный) и базис vs Pyth, где Pyth покрывает. С 31.07.2026 публичный Hermes-Pyth станет платным — поэтому Finnhub оставлен ОСНОВНЫМ клоуз-источником, а источник переключаем параметром.</p>
<h3>Про «0-fee» (важно)</h3>
<p>«0 Fees» в интерфейсе BingX — это <b>временное промо</b> (≈до 31.07.2026), а не постоянная фича, и с catch'ем: только реферальные юзеры и только <b>ручная</b> торговля. <b>Любой включённый API-ключ лишает льготы — даже для ручных ордеров.</b> Значит через API/бота списывается стандарт 0.02%/0.05% + funding. Для автоматизации костовый потолок остаётся.</p>
<h3>Про график и TradingView</h3>
<p>Клик по строке разворачивает график-аккордеон (TradingView <b>lightweight-charts</b>, Apache 2.0, вендорится локально): сплошная линия <b>BingX</b> (перп, WS) против <b>реальной базы</b> (Pyth real-time, иначе Yahoo <i>delayed</i>) — базис вживую. Буфер 10 мин/символ на сервере (сид из 1m-свечей + WS), real-time через <code>series.update</code>. У TradingView нет data-API, поэтому их линию не тянем; готовый виджет TV — отдельной кнопкой для сверки. Атрибуция TradingView обязательна по лицензии.</p>
</div></div>

</div>
<script src="/static/lightweight-charts.standalone.production.js"></script>
<script>
const REFRESH=__REFRESH__;
const f2=(x,d=2)=>x==null?'—':Number(x).toFixed(d);
const sgn=(x,d=2)=>x==null?'—':(x>0?'+':'')+Number(x).toFixed(d);
const cls=x=>x==null?'mut':(x>0?'up':(x<0?'dn':''));
const fmtOi=v=>v==null?'—':'$'+(Math.abs(v)>=1e6?(v/1e6).toFixed(2)+'M':Math.abs(v)>=1e3?Math.round(v/1e3)+'k':''+Math.round(v));
let LAST=null, SEL=null, SELTICK=null, SELSRC=null, TVON=false, LASTORDER='';
let SORT={key:null,dir:0}, FROZEN=false, FROZEN_ORDER=null;
let CHART=null,BXS=null,BASES=null,CHARTBOX=null,CHARTROW=null,lastBxT=0,lastBaseT=0;
const rowEls=new Map();
const NYSE=new Set(['GS','JPM','MS','JNJ','XOM','COP','OXY','SLB','LNG','PFE','LLY','MCD','NKE','WMT','GE','F','IBM','LMT','MGM','MP','NU','RACE','BB','GLW','HPQ','CCL','CRCL','ORCL','GME','SPCE','UNH','SNAP','BRKB','DELL']);
const tvExch=tk=>NYSE.has((tk||'').toUpperCase())?'NYSE':'NASDAQ';

function show(t){['num','strat','faq'].forEach(x=>{
  document.getElementById(x).className=(x==t?'':'hide');
  document.getElementById('t-'+x).className='tab'+(x==t?' on':'');});}

/* ---- sort / order ---- */
function val(r,k){switch(k){
  case 'ticker':return r.ticker||'';
  case 'live':return r.live;case 'close':return r.close;case 'gap':return r.gap;
  case 'pyth':return r.pyth;case 'oi':return r.oi;
  case 'funding':return r.funding?r.funding.rate:null;}return null;}
function defcmp(a,b){const sa=a.gap!=null?Math.abs(a.gap):(a.pyth!=null?Math.abs(a.pyth):-1);
  const sb=b.gap!=null?Math.abs(b.gap):(b.pyth!=null?Math.abs(b.pyth):-1);return sb-sa;}
function cmp(a,b,k,dir){const va=val(a,k),vb=val(b,k);
  const na=(va==null||va===''),nb=(vb==null||vb==='');
  if(na&&nb)return defcmp(a,b);if(na)return 1;if(nb)return -1;
  let c=(typeof va==='string')?va.localeCompare(vb):va-vb;return c*dir||defcmp(a,b);}
function sortedApis(rows){
  return rows.slice().sort((a,b)=>SORT.key?cmp(a,b,SORT.key,SORT.dir):defcmp(a,b)).map(r=>r.api);}
function orderRows(rows){
  if(FROZEN&&FROZEN_ORDER){const idx={};FROZEN_ORDER.forEach((id,i)=>idx[id]=i);
    return rows.slice().sort((a,b)=>((idx[a.api]??9999)-(idx[b.api]??9999))).map(r=>r.api);}
  return sortedApis(rows);}
function updateArrows(){document.querySelectorAll('th[data-key]').forEach(th=>{
  th.querySelector('.ar').textContent=(SORT.key==th.dataset.key)?(SORT.dir>0?' ▲':' ▼'):'';});}
function sortBy(k){
  if(SORT.key!==k)SORT={key:k,dir:1};else if(SORT.dir===1)SORT.dir=-1;else SORT={key:null,dir:0};
  if(FROZEN&&LAST)FROZEN_ORDER=sortedApis(LAST.rows);
  updateArrows();LASTORDER='';if(LAST){renderNum(LAST);renderStrat(LAST);}}
function toggleFreeze(){const b=document.getElementById('freezebtn');
  if(!FROZEN){FROZEN_ORDER=(LAST?orderRows(LAST.rows):[]);FROZEN=true;b.textContent='❄ порядок заморожен';b.classList.add('on');}
  else{FROZEN=false;FROZEN_ORDER=null;b.textContent='❄ заморозить порядок';b.classList.remove('on');}
  LASTORDER='';if(LAST){renderNum(LAST);renderStrat(LAST);}}

/* ---- таблица «Акции»: строка 1 раз, ячейки обновляем ПО МЕСТУ ---- */
function makeRow(x){const tr=document.createElement('tr');
  tr.dataset.api=x.api;tr.dataset.tick=x.ticker;
  const tag=x.tag?`<span class="tag ${x.tag}">${x.tag=='core'?'ядро':'мусор'}</span>`:'';
  tr.innerHTML=`<td class="l"><span class="dot ${x.fam}"></span>${x.ticker}${x.is_regime?' ★':''}${tag}</td>`+
    `<td></td><td class="mut"></td><td></td><td></td><td class="mut"></td><td class="mut"></td>`;
  return {tr};}
function updateRow(o,x){const ch=o.tr.children;
  o.tr.className=(x.in_win?'win ':'')+(x.api==SEL?'sel':'');
  ch[1].textContent=f2(x.live);ch[2].textContent=f2(x.close);
  const g=ch[3];
  if(x.gap!=null){g.textContent=sgn(x.gap);g.className=cls(x.gap);g.title='';}
  else if(x.gap_raw!=null){g.textContent='—';g.className='susp';g.title=(x.reason||'данные сомнительны')+' · сырой '+sgn(x.gap_raw)+'%';}
  else{g.textContent='—';g.className='mut';g.title=x.reason||'';}
  ch[4].textContent=sgn(x.pyth,3);ch[4].className=cls(x.pyth);
  ch[5].textContent=fmtOi(x.oi);
  ch[6].textContent=(x.funding&&x.funding.rate!=null)?sgn(x.funding.rate,4)+'%/'+(x.funding.ih||'?')+'h':'—';}
function renderNum(s){const tb=document.getElementById('tb');const seen=new Set();
  for(const x of s.rows){seen.add(x.api);let o=rowEls.get(x.api);
    if(!o){o=makeRow(x);rowEls.set(x.api,o);tb.appendChild(o.tr);}updateRow(o,x);}
  for(const[api,o]of rowEls){if(!seen.has(api)){o.tr.remove();rowEls.delete(api);}}
  const ordered=orderRows(s.rows);const key=ordered.join(',');
  const pinned=(SEL&&SELSRC==='num');            // график открыт в таблице → не пересортировываем
  if(key!==LASTORDER&&!pinned){LASTORDER=key;
    const frag=document.createDocumentFragment();
    for(const api of ordered){const o=rowEls.get(api);if(o)frag.appendChild(o.tr);}
    tb.appendChild(frag);}
  if(SEL&&SELSRC==='num')positionChart();}

/* ---- вкладка «Стратегия»: тоже кликабельно + заморозка ---- */
function renderStrat(s){const rg=s.regime,el=document.getElementById('regime');
  const q='<span class="qhelp" title="QQQ = ETF на Nasdaq-100, датчик режима всего рынка. Сильный гэп QQQ = трендовый день; фейдить отдельные растущие токены ПРОТИВ общего тренда рискованно.">(?)</span>';
  if(rg&&rg.gap!=null){const big=Math.abs(rg.gap)>=1;
    el.innerHTML=`QQQ ${sgn(rg.gap)}% ${q} — `+(big?'<b style="color:var(--skip)">трендовый день, фейд-шорт рискован</b>':'спокойно');
  }else el.innerHTML='QQQ — '+q;
  const wrap=document.getElementById('buckets');
  if(CHARTBOX&&wrap.contains(CHARTBOX))CHARTBOX.remove();   // спасаем график от rebuild
  wrap.innerHTML='';
  const defs=[['fade_short','fade','✅ Фейд-шорт (ядро)'],['short_weak','short','Шорт 1–2% (не ядро)'],
    ['long_weak','long','Лонг 1–2% (слабая нога)'],['skip','skip','Скип >2%'],['noise','noise','Шум <1%']];
  const b=(s.strategy&&s.strategy.buckets)||{};
  const fidx={};if(FROZEN&&FROZEN_ORDER)FROZEN_ORDER.forEach((id,i)=>fidx[id]=i);
  for(const [key,c,title] of defs){let items=(b[key]||[]).slice();
    if(FROZEN&&FROZEN_ORDER)items.sort((x,y)=>((fidx[x.api]??9999)-(fidx[y.api]??9999)));
    const div=document.createElement('div');div.className='bk '+c;
    let h=`<h3>${title} <span class="mut">(${items.length})</span></h3>`;
    for(const it of items){h+=`<div class="srow${it.in_win?' win':''}${it.api==SEL?' sel':''}" data-api="${it.api}" data-tick="${it.ticker}"><span>${it.ticker}`+
      (it.tag=='core'?' <span class="tag core">ядро</span>':(it.tag=='avoid'?' <span class="tag avoid">мусор</span>':''))+
      `</span><span><span class="${cls(it.gap)}">${sgn(it.gap)}%</span></span></div>`;}
    div.innerHTML=h;wrap.appendChild(div);}
  if(SEL&&SELSRC==='strat')positionChart();}

/* ---- график: lightweight-charts, аккордеон в ОБЕИХ вкладках ---- */
function selectRow(api,tick,source){
  if(SEL===api){closeChart();return;}
  closeChart();
  SEL=api;SELTICK=tick;SELSRC=source;TVON=false;
  fillChartBox(tick,api);positionChart();initChart();refreshChart(true);}
function fillChartBox(tick,api){
  if(!CHARTBOX){CHARTBOX=document.createElement('div');CHARTBOX.className='cbox';}
  const ex=tvExch(tick);
  CHARTBOX.innerHTML=
    `<div class="chead"><b>${tick} · ${api}</b>`+
    `<a class="mut" style="margin-left:8px" target="_blank" rel="noopener" href="https://bingx.com/en/perpetual/${api}">BingX ↗</a>`+
    `<span id="cbasis" class="mut"></span><span class="cx" onclick="closeChart()">✕</span></div>`+
    `<div id="cchart"></div>`+
    `<div class="cleg"><span style="color:#2f81f7">━ BingX перп</span> &nbsp; <span style="color:#d29922">━ <span id="lbase">база</span></span>`+
    `<span class="tvattr">графики: <a href="https://www.tradingview.com" target="_blank" rel="noopener">TradingView</a> Lightweight Charts™</span></div>`+
    `<div class="tvrow"><span class="tvbtn" id="tvbtn" onclick="toggleTV()">показать TradingView</span>`+
    `<a class="tvchart" target="_blank" rel="noopener" href="https://www.tradingview.com/chart/?symbol=${ex}:${encodeURIComponent(tick)}">график на TradingView ↗</a></div>`+
    `<div id="tvwrap"></div>`;}
function ensureChartRow(){if(!CHARTROW){CHARTROW=document.createElement('tr');CHARTROW.className='chartrow';
    CHARTROW.innerHTML='<td colspan="7"></td>';}return CHARTROW;}
function positionChart(){if(!SEL||!CHARTBOX)return;
  if(SELSRC==='num'){const o=rowEls.get(SEL);if(!o)return;ensureChartRow();
    if(CHARTROW.firstElementChild.firstChild!==CHARTBOX)CHARTROW.firstElementChild.appendChild(CHARTBOX);
    if(o.tr.nextSibling!==CHARTROW)o.tr.parentNode.insertBefore(CHARTROW,o.tr.nextSibling);}
  else{const a=document.querySelector('#buckets .srow[data-api="'+SEL+'"]');
    if(a&&a.nextSibling!==CHARTBOX)a.after(CHARTBOX);}}
function initChart(){const el=document.getElementById('cchart');if(!el||!window.LightweightCharts)return;
  el.innerHTML='';
  CHART=LightweightCharts.createChart(el,{autoSize:true,
    layout:{background:{color:'#0e1320'},textColor:'#7d8590',fontSize:11,attributionLogo:true},
    grid:{vertLines:{color:'#21262d'},horzLines:{color:'#21262d'}},
    rightPriceScale:{visible:true,borderColor:'#30363d'},
    timeScale:{visible:true,timeVisible:true,secondsVisible:true,borderColor:'#30363d',
      tickMarkFormatter:t=>new Date(t*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit'})},
    localization:{timeFormatter:t=>new Date(t*1000).toLocaleTimeString()},crosshair:{mode:0}});
  BXS=CHART.addLineSeries({color:'#2f81f7',lineWidth:2,lastValueVisible:true,priceLineVisible:true});
  BASES=CHART.addLineSeries({color:'#d29922',lineWidth:2,lastValueVisible:true,priceLineVisible:false});
  lastBxT=0;lastBaseT=0;}
function toSeries(arr){const out=[];let lt=-1;
  for(const p of arr){const t=Math.floor(p[0]);
    if(t===lt)out[out.length-1]={time:t,value:p[1]};else{out.push({time:t,value:p[1]});lt=t;}}return out;}
async function refreshChart(initial){if(!SEL||!CHART)return;let d;
  try{d=await(await fetch('/series?symbol='+encodeURIComponent(SEL),{cache:'no-store'})).json();}catch(e){return;}
  const bx=toSeries(d.bingx||[]),base=toSeries(d.base||[]);
  if(initial){if(bx.length)BXS.setData(bx);BASES.setData(base);CHART.timeScale().fitContent();
    lastBxT=bx.length?bx[bx.length-1].time:0;lastBaseT=base.length?base[base.length-1].time:0;}
  else{for(const pt of bx)if(pt.time>=lastBxT){BXS.update(pt);lastBxT=pt.time;}
       for(const pt of base)if(pt.time>=lastBaseT){BASES.update(pt);lastBaseT=pt.time;}}
  const bl=bx.length?bx[bx.length-1].value:null,ba=base.length?base[base.length-1].value:null;
  const basis=(bl!=null&&ba)?((bl-ba)/ba*100):null;
  const cb=document.getElementById('cbasis');if(cb)cb.innerHTML=bl!=null?
    ('BingX '+bl.toFixed(2)+(ba?(' · база '+ba.toFixed(2)+' (Pyth) · базис <b class="'+cls(basis)+'">'+sgn(basis,3)+'%</b>'):' · базы нет')):'ждём тики…';
  const lb=document.getElementById('lbase');if(lb)lb.textContent=(d.base_src=='pyth')?'база: Pyth (real-time)':'база: нет данных';}
function closeChart(){if(CHART){try{CHART.remove();}catch(e){}CHART=null;}
  if(CHARTBOX&&CHARTBOX.parentNode)CHARTBOX.remove();
  if(CHARTROW&&CHARTROW.parentNode)CHARTROW.remove();
  SEL=null;SELSRC=null;LASTORDER='';if(LAST)renderNum(LAST);}
function toggleTV(){TVON=!TVON;const wrap=document.getElementById('tvwrap');const btn=document.getElementById('tvbtn');
  if(!TVON){wrap.innerHTML='';btn.textContent='показать TradingView';return;}
  btn.textContent='скрыть TradingView';
  wrap.innerHTML='<div class="tradingview-widget-container"><div class="tradingview-widget-container__widget"></div></div>';
  const sc=document.createElement('script');
  sc.src='https://s3.tradingview.com/external-embedding/embed-widget-symbol-overview.js';sc.async=true;
  sc.text=JSON.stringify({symbols:[[SELTICK]],chartOnly:false,width:'100%',height:300,colorTheme:'dark',isTransparent:true,locale:'ru'});
  wrap.querySelector('.tradingview-widget-container').appendChild(sc);}

/* ---- poll ---- */
async function tick(){try{const s=await(await fetch('/data',{cache:'no-store'})).json();LAST=s;
  document.getElementById('upd').textContent=s.updated||'—';
  document.getElementById('ws').textContent=s.ws||'—';
  document.getElementById('pyth').textContent=s.pyth||'—';
  const sp=document.getElementById('sesspill'),us=s.us_session||'—';
  sp.textContent='US: '+us;
  sp.className='sess '+(us.indexOf('RTH')>=0?'rth':(us.indexOf('премаркет')>=0?'pre':'ah'));
  renderNum(s);renderStrat(s);if(SEL)refreshChart(false);
  }catch(e){document.getElementById('upd').textContent='нет связи с локальным сервером';}}

document.querySelectorAll('th[data-key]').forEach(th=>th.addEventListener('click',()=>sortBy(th.dataset.key)));
document.getElementById('tb').addEventListener('click',e=>{
  if(e.target.closest('.chartrow'))return;
  const tr=e.target.closest('tr[data-api]');if(tr)selectRow(tr.dataset.api,tr.dataset.tick,'num');});
document.getElementById('buckets').addEventListener('click',e=>{
  if(e.target.closest('.cbox'))return;
  const r=e.target.closest('.srow[data-api]');if(r)selectRow(r.dataset.api,r.dataset.tick,'strat');});
document.getElementById('freezebtn').addEventListener('click',toggleFreeze);
tick();setInterval(tick,REFRESH*1000);
</script></body></html>"""


# ----------------------------- server -----------------------------
class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass
    def _json(self, obj):
        body = json.dumps(obj, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.startswith("/data"):
            with LOCK:
                body = json.dumps(STATE, ensure_ascii=False).encode("utf-8")
            self.send_response(200)
            self.send_header("Content-Type", "application/json; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        elif self.path.startswith("/series"):
            sym = (parse_qs(urlparse(self.path).query).get("symbol") or [""])[0]
            bx, base = (SERIES.get(sym) if SERIES else ([], []))
            # база = ТОЛЬКО Pyth (real-time). Нет Pyth → базовой линии нет (Yahoo убран совсем).
            src = "pyth" if base else "none"
            self._json({"symbol": sym, "bingx": bx, "base": base,
                        "base_src": src, "base_delayed": False})
        elif self.path.startswith("/static/"):
            name = os.path.basename(urlparse(self.path).path)   # basename => без обхода путей
            fp = os.path.join(os.path.dirname(os.path.abspath(__file__)), "static", name)
            if os.path.isfile(fp) and name.endswith((".js", ".css", ".svg")):
                ct = ("application/javascript" if name.endswith(".js")
                      else "text/css" if name.endswith(".css") else "image/svg+xml")
                with open(fp, "rb") as f:
                    body = f.read()
                self.send_response(200)
                self.send_header("Content-Type", ct + "; charset=utf-8")
                self.send_header("Cache-Control", "public, max-age=86400")
                self.end_headers()
                self.wfile.write(body)
            else:
                self.send_response(404); self.end_headers()
        else:
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.end_headers()
            self.wfile.write(HTML.replace("__REFRESH__", str(BROWSER_REFRESH)).encode("utf-8"))


def main():
    global PORT
    print("Тяну универс TradFi из BingX contracts…")
    try:
        inst = fetch_universe()
    except Exception as e:
        print(f"!! contracts недоступны: {e}")
        return
    fams = {}
    for it in inst:
        fams[it["fam"]] = fams.get(it["fam"], 0) + 1
    print(f"TradFi всего: {len(inst)} → " + ", ".join(f"{FAM_RU[k]}={fams.get(k,0)}" for k in FAM_ORDER))
    serenity = [it for it in inst if it["fam"] == "stock"]
    print(f"«Serenity» (NCSK-акции/ETF): {len(serenity)}. Категория в API НЕ размечена — определяем по префиксу NCSK.")
    # fee реальность
    print("Комиссия: API feeRate taker=0.05%/maker=0.02% (стандарт). 0-fee = ПРОМО до 31.07.2026,")
    print("          только ручная торговля реферала БЕЗ API-ключа; через API/бота — 0.02/0.05 + funding.")
    print("          ⇒ для бота костовый потолок остаётся; '0-fee' его НЕ снимает.")

    symbols = [it["symbol"] for it in inst]
    bases   = [it["base"] for it in inst if it["fam"] == "stock"]

    global SERIES
    SERIES = Series(symbols)

    pf   = PriceFeed(symbols)
    pf.series = SERIES
    prem = PremiumFeed()
    pyth = PythFeed(bases)
    ref  = RefData(inst)
    oif  = OIFeed(symbols)
    siglog = SignalLog(DB_PATH)
    pf.start(); prem.start(); pyth.start(); ref.start(); oif.start(); SERIES.start(); siglog.start()
    print(f"WS: подписка на {len(symbols)} символов ({(len(symbols)+SUBS_PER_WS-1)//SUBS_PER_WS} соединений).")
    print(f"График: буфер 10 мин/символ — сид из 1m-klines (фоном) + добивка из WS; линия базы = Pyth.")
    if FINNHUB_KEY:
        print(f"Close-референс: Finnhub pc (основной), {len(bases)} тикеров последовательно (~3 мин).")
    else:
        print("!! FINNHUB_API_KEY не задан — клоуза не будет, гэпы пойдут прочерком. Задай env-ключ.")
    print(f"SQLite-лог сигналов: {DB_PATH if siglog.ok else '(отключён)'}")
    time.sleep(2.0)  # дать ws/premium наполниться
    cov_c, cov_t = pyth.coverage()
    print(f"Pyth Hermes: покрытие акций {cov_c}/{cov_t} (Equity.US.*). Спред-премиум — у всех инструментов.")

    threading.Thread(target=updater, args=(inst, pf, prem, pyth, ref, oif, siglog), daemon=True).start()

    srv = None
    for p in range(PORT, PORT + 10):
        try:
            srv = ThreadingHTTPServer((HOST, p), Handler); PORT = p; break
        except OSError:
            continue
    if not srv:
        print("Нет свободного порта."); return
    disp = HOST if HOST not in ("0.0.0.0", "") else "<IP-сервера>"
    url = f"http://{disp}:{PORT}"
    print(f"\nГотово. Слушаю {HOST}:{PORT} → {url}   (Ctrl+C — стоп)\n")
    if OPEN_BROWSER:
        try: webbrowser.open(url)
        except Exception: pass
    try:
        srv.serve_forever()
    except KeyboardInterrupt:
        print("\nСтоп.")


if __name__ == "__main__":
    main()
