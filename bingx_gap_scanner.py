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
  • Андерлайн ГРАФИКА (базовая линия + базис): TradingView screener (тот же, что и клоуз), delayed ~15м.
        В таблице отдельной колонки нет (в премаркет = клоуз, дублировала бы гэп). Pyth за флагом USE_PYTH (выкл).
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
CLOSE_SOURCE      = os.environ.get("GAP_CLOSE_SRC", "tv")   # tv (осн.) | finnhub — переключаемо
# Андерлайн графика (базовая линия + базис) = TradingView screener (delayed ~15м).
# Pyth оставлен в коде целиком, но по умолчанию ВЫКЛЮЧЕН. Вернуть Pyth: env USE_PYTH=true.
USE_PYTH          = os.environ.get("USE_PYTH", "false").strip().lower() in ("1", "true", "yes", "on")
TW_REFRESH        = 20      # сек между поллами текущей (delayed 15м) цены TradingView-андерлайна
FINNHUB_MIN_GAP   = 1.1     # сек между вызовами Finnhub (free 60/мин) → ~55/мин
BASIS_SUSPECT_PCT = 5.0     # |live−Pyth|/Pyth выше → строка «данные сомнительны», гэп не торговый
OI_REFRESH        = 20      # сек паузы между полными циклами опроса OI
LOG_INTERVAL      = 60      # сек, как часто писать снапшот в sqlite
DB_PATH = os.environ.get("GAP_DB_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "signals.db")

# ----------------------------- Батч 2.5: outcome-джоб (форвард-лог) -----------------------------
# Времена снапшотов outcome (UTC). ВАЛИДНЫ ПРИ EDT (лето США, ~март - начало ноября):
#   вход 13:00 UTC = 16:00 МСК (премаркет), выход 13:35 UTC = 16:35 МСК (RTH открылась 13:30 UTC).
# При EST (зима, с ~1 ноября 2026) RTH-открытие сместится на 14:30 UTC - менять ЗДЕСЬ
# (вход (14,0), выход (14,35)), по коду времена НЕ хардкодить.
OUTCOME_ENTRY_UTC = (13, 0)
OUTCOME_EXIT_UTC  = (13, 35)
OUTCOME_LIVE_MAX_AGE = 300   # сек: макс возраст WS-тика для live-снапшота; протухшее добирает bf_5m
FEE_ERA_SWITCH = "2026-08-01"  # fee_era: '0fee' по 2026-07-31 включительно (промо BingX), 'fees' дальше
# Праздники NYSE, 2-е полугодие 2026 (сверено с календарём NYSE):
#   2026-07-03 Пт  - Independence Day (4 июля = суббота, выходной наблюдается в пятницу)
#   2026-09-07 Пн  - Labor Day
#   2026-11-26 Чт  - Thanksgiving  (27.11 - раннее закрытие, но рынок ОТКРЫТ - не праздник)
#   2026-12-25 Пт  - Christmas    (24.12 - раннее закрытие, рынок открыт)
# Перпы BingX торгуют и в праздник, поэтому наличие свечей НЕ признак рыночного дня.
# Гейты рыночности: live-путь = Finnhub market-status (основной); recon для вчерашней даты =
# свежесть Finnhub QQQ `t`; recon для старых дат = будни минус этот список.
US_HOLIDAYS_2026H2 = {"2026-07-03", "2026-09-07", "2026-11-26", "2026-12-25"}
RTH_OPEN_UTC  = (13, 30)     # RTH-открытие US при EDT (зимой 14:30) - для t-гейта recon
RTH_CLOSE_UTC = (20, 0)      # RTH-закрытие US при EDT (зимой 21:00) - начало окна свежести t-гейта
RECON_DEPTH_DATES = 10       # recon чинит пропуски/NULL за последние N дат лога (15m у BingX ~10 дней)
RECON_HOUR_UTC   = 1         # ночной прогон recon ~01:30 UTC (US-клоуз 20:00 UTC давно прошёл)
QQQ_REG_BAND = 0.20          # мёртвая зона режима QQQ, как REG_BAND в gap_cells_1m.py: "рос" = gap > +0.2
# Группа "Лонг 1-2% (слабая нога)" во вкладке Стратегия: лонг-фейд down-гэпа 1-2%.
# ОТКЛЮЧЕНА 02.07.2026: in-sample слабый, walk-forward не проходил. Код сохранён в запас
# (бакет long_weak считается всегда, флаг гасит только рендер). Вернуть = True.
SHOW_WEAK_LEG_GROUP = False

# ----------------------------- Батч 2.5: коллектор 1m klines -----------------------------
# BingX хранит 1m ~сутки - копим свою историю в отдельной базе.
KL1M_DB_PATH = os.environ.get("GAP_KL1M_PATH") or os.path.join(
    os.path.dirname(os.path.abspath(__file__)), "data", "klines_1m.db")
KL1M_INTERVAL = 3600         # сек между полными проходами коллектора
KL1M_START_DELAY = 35 * 60   # сек: ПЕРВЫЙ прогон коллектора через ~35 мин после старта -
                             # развести со стартовым recon-backfill (вместе = сотни запросов,
                             # ровно тот объём, на котором словили бан 01.07)
# Общий предохранитель + rate-limiter ВСЕХ REST-klines запросов процесса (recon + коллектор + сид графика).
# Факты бана 01.07.2026: сам ТЕМП не банил (0.15с между запросами, ~90 успешных подряд - ок;
# ранее ~460 запросов с шагом 0.2с - ок). Банят ОШИБКИ: >10 ответов 109415 (оконный запрос по
# символу без свечей в окне) за 900 сек -> код 109429 = IP-бан ~900с на весь public REST.
KLINES_MIN_GAP   = 0.5       # сек между ЛЮБЫМИ klines-запросами (x2+ запас от темпа 0.15с при бане)
KLINES_BREAKER_N = 3         # N ПОДРЯД 429/109429/109415 -> стоп klines до следующего планового цикла

def fee_era_for(date_iso):
    """'0fee' по 2026-07-31 включительно, 'fees' с FEE_ERA_SWITCH (ISO-строки сравнимы лексикографически)."""
    return "0fee" if date_iso < FEE_ERA_SWITCH else "fees"


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
            "status": c.get("status"),   # 1 = торгуется, 25 = пауза (klines по паузному -> ошибка 109415)
        })
    return out

def fetch_paused_symbols():
    """Свежие ПАУЗНЫЕ перпы (status=25) из /contracts: любой klines-запрос по ним отвечает
    ошибкой 109415 ("is pause currently"), а 10 ошибок за 15 мин = IP-бан 109429. Поэтому
    recon и коллектор фильтруют их ДО запросов. Ошибка выборки -> пустое множество (не гейтим)."""
    try:
        return {c["symbol"] for c in (_bingx("/openApi/swap/v2/quote/contracts") or [])
                if isinstance(c, dict) and c.get("status") == 25}
    except Exception:
        return set()


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
    KLGATE.wait()   # общий темп всех klines-запросов процесса (сид графика тоже)
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

def fetch_klines_win(sym, interval, start_ms=None, end_ms=None, limit=1000):
    """Свечи BingX (тот же v3-путь, что fetch_klines_1m) с опц. окном startTime/endTime (мс).
    -> dict{open_time_ms: (o, h, l, c, volume)}. Корректность на выборке по КЛЮЧУ (точное время свечи).
    ⚠ ГРАБЛИ (проверено 01.07.2026): запрос С ОКНОМ по символу, у которого в окне НЕТ свечей
    (неликвид), возвращает ОШИБКУ 109415, а 10 ошибок за 15 мин = IP-бан всего API (код 109429,
    ~900с). Поэтому recon и коллектор зовут БЕЗ окна (см. fetch_klines_1000) - пустота без окна
    это code 0 + data=[], безопасно."""
    params = {"symbol": sym, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    arr = _bingx("/openApi/swap/v3/quote/klines", params)
    out = {}
    for b in (arr or []):
        try:
            if isinstance(b, dict):
                t = int(b["time"]); o = float(b["open"]); h = float(b["high"])
                l = float(b["low"]); c = float(b["close"]); v = float(b.get("volume") or 0)
            else:
                t = int(b[0]); o = float(b[1]); h = float(b[2]); l = float(b[3]); c = float(b[4]); v = float(b[5])
            if o > 0:
                out[t] = (o, h, l, c, v)
        except Exception:
            continue
    return out

class _KlinesGate:
    """Общий rate-limiter + circuit breaker ВСЕХ REST-klines запросов процесса (recon, коллектор,
    сид графика). wait() держит темп не чаще 1 запроса в KLINES_MIN_GAP; fail() считает ПОДРЯД
    идущие rate-limit/бан-ответы и после KLINES_BREAKER_N взводит предохранитель - все klines-
    вызовы возвращают пусто до reset() в начале следующего планового цикла. Сквозь бан не долбим."""
    def __init__(self):
        self.lock = threading.Lock()
        self.last = 0.0
        self.fails = 0
        self.tripped = False
    def wait(self):
        while True:
            with self.lock:
                now = time.time()
                need = KLINES_MIN_GAP - (now - self.last)
                if need <= 0:
                    self.last = now
                    return
            time.sleep(min(need, KLINES_MIN_GAP))
    def ok(self):
        with self.lock:
            self.fails = 0
    def fail(self, why=""):
        with self.lock:
            self.fails += 1
            if self.fails >= KLINES_BREAKER_N and not self.tripped:
                self.tripped = True
                print(f"[klines] ПРЕДОХРАНИТЕЛЬ: {self.fails} подряд rate-limit/бан ({why}) - "
                      f"стоп REST-klines до следующего планового цикла")
    def is_tripped(self):
        with self.lock:
            return self.tripped
    def reset(self, who=""):
        with self.lock:
            if self.tripped:
                print(f"[klines] предохранитель сброшен ({who})")
            self.tripped = False
            self.fails = 0

KLGATE = _KlinesGate()

def fetch_klines_1000(sym, interval, end_ms=None):
    """Безопасная выборка свечей: limit=1000, по умолчанию БЕЗ временнОго окна (грабли 109415 -
    см. fetch_klines_win). end_ms - ТОЛЬКО для добора старшего хвоста по символу, у которого
    свечи до end_ms ТОЧНО есть (первая страница пришла полной) - тогда окно безопасно.
    Идёт через общий KLGATE (темп + предохранитель); одна попытка, без долбёжки сквозь бан."""
    if KLGATE.is_tripped():
        return {}
    KLGATE.wait()
    try:
        k = fetch_klines_win(sym, interval, None, end_ms, 1000)
        KLGATE.ok()
        return k
    except Exception as e:
        s = str(e)
        if "109429" in s or "109415" in s or "429" in s:
            KLGATE.fail(s[:60])
        return {}

class Series:
    """Постоянный буфер последних ~10 мин (ts_sec, price) на каждый символ.
       BingX-линия: сид из 1m-klines при старте + добивка из WS. Base-линия: TradingView (delayed ~15м)."""
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

def finnhub_market_open():
    """Гейт рыночности для LIVE-пути outcome (основной): Finnhub /stock/market-status?exchange=US.
    Зовётся в 13:35 UTC = 9:35 ET - на торговый день RTH уже ОТКРЫТ (isOpen=true), на праздник false.
    -> True | False | None (ключа нет / недоступно; тогда live не пишем - добьёт ночной recon)."""
    if not FINNHUB_KEY:
        return None
    try:
        url = f"https://finnhub.io/api/v1/stock/market-status?exchange=US&token={FINNHUB_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "gap-scanner"})
        with urllib.request.urlopen(req, timeout=12) as r:
            j = json.loads(r.read().decode("utf-8"))
        if isinstance(j, dict) and "isOpen" in j:
            return bool(j.get("isOpen"))
    except Exception:
        pass
    return None

def finnhub_qqq_last_ts():
    """Метка времени `t` последнего апдейта котировки QQQ (unix сек) с Finnhub, либо None.
    Для recon-гейта ВЧЕРАШНЕЙ даты: t >= D 13:30 UTC => в D была RTH-сессия.
    Валидно ТОЛЬКО пока recon бежит в окне (D 20:00 UTC .. D+1 13:30 UTC) - до следующей сессии."""
    if not FINNHUB_KEY:
        return None
    try:
        url = f"https://finnhub.io/api/v1/quote?symbol=QQQ&token={FINNHUB_KEY}"
        req = urllib.request.Request(url, headers={"User-Agent": "gap-scanner"})
        with urllib.request.urlopen(req, timeout=12) as r:
            j = json.loads(r.read().decode("utf-8"))
        t = int(j.get("t") or 0)
        return t if t > 0 else None
    except Exception:
        return None

# Биржу для TV-символа НЕ гадаем: берём готовое "s" ("EXCHANGE:SYMBOL") из ответа screener
# (RefData._fetch_tv → tvsym в строке). Клоуз/гэп биржу тоже не гадают — те же данные screener.

def tv_scan(tv_syms, columns):
    """POST на TradingView screener (keyless, stdlib). → JSON {data:[{s,d},...]}."""
    body = json.dumps({"symbols": {"tickers": tv_syms, "query": {"types": []}},
                       "columns": columns}).encode()
    req = urllib.request.Request("https://scanner.tradingview.com/america/scan", data=body,
        headers={"Content-Type": "application/json", "User-Agent": "Mozilla/5.0 gap-scanner"})
    with urllib.request.urlopen(req, timeout=20) as r:
        return json.loads(r.read().decode("utf-8"))

class RefData:
    """Реф-клоуз = ПОСЛЕДНЕЕ ЗАВЕРШЁННОЕ RTH-закрытие US.
    ОСНОВНОЙ источник — TradingView screener (bulk, keyless): prev_close = close − change_abs
    (в премаркете 15:30–16:30 МСК = вчерашний клоуз). ФОЛБЭК/КРОСС-ЧЕК — Finnhub pc.
    Кэш на маркер (дата + pre/post 16:30 ET). GAP_CLOSE_SRC=tv|finnhub — переключаемо."""
    def __init__(self, instruments):
        self.inst = [it for it in instruments if it["fam"] == "stock"]
        self.ref = {}               # base -> dict(close, src, tv, fh, mismatch, region, cur, us, day)
        self.lock = threading.Lock()
        self._marker = None
    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
    def get(self, base):
        with self.lock:
            return self.ref.get(base)
    def _now_et(self):
        return datetime.now(_et_naive_tz("America/New_York"))
    def _ref_marker(self):
        now = self._now_et()
        phase = "post" if (now.weekday() < 5 and (now.hour, now.minute) >= (16, 30)) else "pre"
        return now.strftime("%Y-%m-%d") + phase
    def _ref_from(self, close, change_abs):
        """close/change_abs c TV → последнее ЗАВЕРШЁННОЕ RTH-закрытие (сессионно-зависимо).
        Регулярка ИДЁТ (9:30–16:00 ET, будни): TV close = ЖИВАЯ цена → завершённый = вчерашний
          = close − change_abs. Регулярка НЕ идёт (премаркет / afterhours / выходной): TV close =
          последний settled RTH-клоуз → берём его напрямую (в премаркете это вчерашний клоуз)."""
        if not isinstance(close, (int, float)):
            return None
        now = self._now_et()
        hm = (now.hour, now.minute)
        rth_live = (now.weekday() < 5 and (9, 30) <= hm < (16, 0))
        if rth_live and isinstance(change_abs, (int, float)):
            return close - change_abs
        return close
    def _combine(self, tvc, fh):
        if CLOSE_SOURCE == "finnhub":
            if fh: return fh, "finnhub"
            if tvc: return tvc, "tv"
        else:
            if tvc: return tvc, "tv"
            if fh: return fh, "finnhub"
        return None, None
    def _fetch_tv(self):
        """→ {base: {"ref": клоуз, "prev": вчерашний close−change_abs, "tvsym": "EXCHANGE:SYMBOL" из ответа}}.
        Биржу не гадаем: спрашиваем NASDAQ/NYSE/AMEX/CBOE/BATS сразу, берём попавшую."""
        out, s2b, syms = {}, {}, []
        for it in self.inst:
            t = us_ticker(it["base"])
            for ex in ("NASDAQ", "NYSE", "AMEX", "CBOE", "BATS"):
                s = f"{ex}:{t}"; s2b[s] = it["base"]; syms.append(s)
        for i in range(0, len(syms), 250):
            try:
                j = tv_scan(syms[i:i+250], ["close", "change_abs"])
                for row in (j.get("data") or []):
                    b = s2b.get(row.get("s")); d = row.get("d") or []
                    if b and b not in out and len(d) >= 2 and isinstance(d[0], (int, float)):
                        ref = self._ref_from(d[0], d[1])
                        prev = (d[0] - d[1]) if isinstance(d[1], (int, float)) else None
                        if ref:
                            out[b] = {"ref": ref, "prev": prev, "tvsym": row.get("s")}
            except Exception:
                pass
        return out
    def _loop(self):
        while True:
            marker = self._ref_marker()
            if marker != self._marker:
                try:
                    tv = self._fetch_tv()             # 1) TV bulk — быстро, всем сразу
                except Exception:
                    tv = {}
                with self.lock:
                    for it in self.inst:
                        b = it["base"]; td = tv.get(b) or {}
                        tvc = td.get("ref"); tvp = td.get("prev")
                        fh = self.ref.get(b, {}).get("fh")
                        close, src = self._combine(tvc, fh)
                        # кросс-чек: вчерашний клоуз TV (close−change_abs) vs Finnhub pc — как с как
                        self.ref[b] = {"close": close, "src": src, "tv": tvc, "tvprev": tvp, "fh": fh,
                                       "tvsym": td.get("tvsym"),
                                       "mismatch": bool(tvp and fh and abs(tvp-fh)/fh > 0.02),
                                       "region": "US", "cur": ("USD" if close else None),
                                       "us": us_ticker(b), "day": marker}
                self._marker = marker
                if FINNHUB_KEY:                        # 2) Finnhub — медленно, кросс-чек + фолбэк
                    for it in self.inst:
                        b = it["base"]
                        fh = None
                        for _ in range(3):
                            try: fh = finnhub_pc(us_ticker(b)); break
                            except RateLimited: time.sleep(5)
                            except Exception: break
                        with self.lock:
                            r = self.ref.get(b, {}); tvc = r.get("tv"); tvp = r.get("tvprev")
                            close, src = self._combine(tvc, fh)
                            r.update({"fh": fh, "close": close, "src": src,
                                      "mismatch": bool(tvp and fh and abs(tvp-fh)/fh > 0.02),
                                      "cur": ("USD" if close else None)})
                            self.ref[b] = r
                        time.sleep(FINNHUB_MIN_GAP)
            time.sleep(30)                             # проверяем смену маркера


# ----------------------------- TradingView underlying (delayed ~15м) -----------------------------
class TWFeed:
    """Текущая (delayed ~15м) цена акции с ТОГО ЖЕ TradingView screener, что и клоуз — АНДЕРЛАЙН
    вместо Pyth. get(base) -> (price, poll_ts). Нет US/ADR-листинга (иностранные) → None."""
    def __init__(self, instruments):
        self.inst = [it for it in instruments if it["fam"] == "stock"]
        self.px = {}                    # base -> (price float, poll_ts)
        self.lock = threading.Lock()
        self.covered = 0
        self._s2b, self._syms = {}, []  # биржу не гадаем: NASDAQ/NYSE/AMEX/CBOE/BATS сразу, берём попавшую
        for it in self.inst:
            t = us_ticker(it["base"])
            for ex in ("NASDAQ", "NYSE", "AMEX", "CBOE", "BATS"):
                s = f"{ex}:{t}"; self._s2b[s] = it["base"]; self._syms.append(s)
    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
    def get(self, base):
        with self.lock:
            return self.px.get(base)
    def coverage(self):
        with self.lock:
            return len(self.px), len(self.inst)
    def _loop(self):
        while True:
            got = {}
            for i in range(0, len(self._syms), 250):
                try:
                    j = tv_scan(self._syms[i:i+250], ["close"])
                    for row in (j.get("data") or []):
                        b = self._s2b.get(row.get("s")); d = row.get("d") or []
                        if b and b not in got and d and isinstance(d[0], (int, float)) and d[0] > 0:
                            got[b] = float(d[0])
                except Exception:
                    pass
            if got:
                now = time.time()
                with self.lock:
                    for b, p in got.items():
                        self.px[b] = (p, now)
                    self.covered = len(self.px)
            time.sleep(TW_REFRESH)


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


# ----------------------------- Батч 2.5: outcomes (форвард-исходы сетапа) -----------------------------
def _uts_utc(date_iso, hh, mm):
    """UTC unix-сек для date_iso + чч:мм (даты ISO, время из конфиг-констант)."""
    d = datetime.fromisoformat(date_iso)
    return int(d.replace(hour=hh, minute=mm, second=0, microsecond=0, tzinfo=timezone.utc).timestamp())

def _is_market_day(date_iso, now_ts):
    """Был ли в date_iso рыночный день US. Будни минус US_HOLIDAYS_2026H2 (конфиг);
    для ВЧЕРАШНЕЙ даты (recon в окне D RTH-close .. D+1 RTH-open) дополнительно t-гейт:
    Finnhub QQQ `t` >= D RTH-open => сессия в D была. Для старых дат t-гейт ЛОЖНОПОЛОЖИТЕЛЕН
    (t от любой поздней сессии) - там только календарь. Свечи BingX гейтом НЕ являются (перпы 24/7)."""
    d = datetime.fromisoformat(date_iso)
    if d.weekday() >= 5:
        return False
    if date_iso in US_HOLIDAYS_2026H2:
        return False
    w0 = _uts_utc(date_iso, *RTH_CLOSE_UTC)
    w1 = _uts_utc(date_iso, *RTH_OPEN_UTC) + 86400   # D+1 RTH-open
    if w0 < now_ts <= w1:
        t = finnhub_qqq_last_ts()
        if t is not None:
            return t >= _uts_utc(date_iso, *RTH_OPEN_UTC)
    return True

class OutcomeStore:
    """Таблица outcomes в ТОМ ЖЕ data/signals.db (join к signals по date+ticker).
    Схему signals НЕ трогаем - только новая таблица. Запись идемпотентна:
    существующая строка обновляется ТОЛЬКО если её ret_long_pct IS NULL (дочинка NULL-строк).
    outcome_src: 'live' (WS-снапшоты) | 'bf_5m' | 'bf_15m_proxy' (ночной добор из klines) |
    'no_data' (свечи есть, меток нет - терминально) | 'paused' (перп приостановлен - терминально)."""
    def __init__(self, path):
        self.path = path
        self.lock = threading.Lock()
        self.conn = None
        self.ok = False
    def start(self):
        try:
            os.makedirs(os.path.dirname(self.path), exist_ok=True)
            self.conn = sqlite3.connect(self.path, check_same_thread=False)
            self.conn.execute("""CREATE TABLE IF NOT EXISTS outcomes(
                date TEXT, ticker TEXT, entry_price REAL, exit_price REAL,
                ret_long_pct REAL, outcome_src TEXT, fee_era TEXT, qqq_gap_entry REAL,
                PRIMARY KEY(date, ticker))""")
            self.conn.commit()
            self.ok = True
        except Exception as e:
            print(f"!! OutcomeStore отключён (не пишется {self.path}): {e}")
    def write(self, date_iso, ticker, entry, exit_, src, qqq_gap):
        if not self.ok:
            return False
        ret = ((exit_ - entry) / entry * 100.0) if (entry and exit_) else None
        try:
            with self.lock:
                self.conn.execute("""INSERT INTO outcomes VALUES(?,?,?,?,?,?,?,?)
                    ON CONFLICT(date,ticker) DO UPDATE SET
                        entry_price=excluded.entry_price, exit_price=excluded.exit_price,
                        ret_long_pct=excluded.ret_long_pct, outcome_src=excluded.outcome_src,
                        fee_era=excluded.fee_era, qqq_gap_entry=excluded.qqq_gap_entry
                    WHERE outcomes.ret_long_pct IS NULL""",
                    (date_iso, ticker, entry, exit_, ret, src, fee_era_for(date_iso), qqq_gap))
                self.conn.commit()
            return True
        except Exception:
            return False
    def _q(self, sql, args=()):
        with self.lock:
            return self.conn.execute(sql, args).fetchall()
    def log_dates(self, n):
        """Последние N дат, за которые есть строки в signals (свежие первыми)."""
        if not self.ok: return []
        return [r[0] for r in self._q(
            "SELECT DISTINCT date(ts,'unixepoch') AS d FROM signals ORDER BY d DESC LIMIT ?", (n,))]
    def log_tickers(self, date_iso):
        if not self.ok: return []
        return [r[0] for r in self._q(
            "SELECT DISTINCT ticker FROM signals WHERE date(ts,'unixepoch')=?", (date_iso,))]
    def resolved_tickers(self, date_iso):
        """Тикеры, по которым за дату УЖЕ есть терминальный результат: посчитанный ret ИЛИ
        'no_data' (свечи по символу есть, нужных меток нет - ре-фетч бессмыслен, ретенция
        только уменьшается; иначе мёртвый хвост копит API-ошибки каждую ночь -> бан)."""
        if not self.ok: return set()
        return {r[0] for r in self._q(
            """SELECT ticker FROM outcomes WHERE date=?
               AND (ret_long_pct IS NOT NULL OR outcome_src IN ('no_data','paused'))""",
            (date_iso,))}
    # Границы премаркет-окна для QQQ-референса из signals: маркер кэша клоуза переворачивается
    # в 00:00 ET (=04:00 UTC при EDT) -> с этого момента и до RTH-открытия close_ref = ВЧЕРАШНИЙ
    # завершённый клоуз (верный реф гэпа). Вечерние строки (afterhours) держат клоуз ТОГО ЖЕ дня -
    # брать их НЕЛЬЗЯ (баг ловился на 30.06, когда лог начался только вечером).
    _PREMKT_FROM_UTC = (4, 0)
    def qqq_close_ref(self, date_iso, target_ts):
        """close_ref QQQ из ПРЕМАРКЕТНЫХ строк signals за дату (ближайшая к target_ts)."""
        if not self.ok: return None
        lo = _uts_utc(date_iso, *self._PREMKT_FROM_UTC)
        hi = _uts_utc(date_iso, *RTH_OPEN_UTC)
        r = self._q("""SELECT close_ref FROM signals WHERE ticker='QQQ' AND close_ref IS NOT NULL
                       AND ts BETWEEN ? AND ? ORDER BY ABS(ts-?) LIMIT 1""", (lo, hi, target_ts))
        return r[0][0] if r else None
    def qqq_gap_signal(self, date_iso, target_ts):
        """Фолбэк qqq_gap_entry: qqq_regime (гэп QQQ) из ПРЕМАРКЕТНЫХ строк signals за дату.
        Нет премаркетных строк - None (честнее, чем вечернее значение против не того клоуза)."""
        if not self.ok: return None
        lo = _uts_utc(date_iso, *self._PREMKT_FROM_UTC)
        hi = _uts_utc(date_iso, *RTH_OPEN_UTC)
        r = self._q("""SELECT qqq_regime FROM signals WHERE qqq_regime IS NOT NULL
                       AND ts BETWEEN ? AND ? ORDER BY ABS(ts-?) LIMIT 1""", (lo, hi, target_ts))
        return r[0][0] if r else None

class OutcomeTracker:
    """Live-путь outcome: снапшоты WS-кэша (буфер Series) в OUTCOME_ENTRY_UTC / OUTCOME_EXIT_UTC,
    запись outcome_src='live'. Берём только свежие тики (<= OUTCOME_LIVE_MAX_AGE) - протухший
    неликвид честнее добрать klines'ами. Гейт рыночности live-пути (ОСНОВНОЙ) - Finnhub
    market-status в момент выхода (13:35 UTC = 9:35 ET: на торговый день рынок уже открыт).
    Рестарт между входом и выходом теряет снапшот - добирает ночной recon (bf_5m)."""
    def __init__(self, inst, series, ref, store):
        self.stocks = [(it["tick"], it["symbol"]) for it in inst if it["fam"] == "stock"]
        self.series = series
        self.ref = ref
        self.store = store
        self.pending = None
    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
    def _px_at(self, sym, ts):
        bx, _ = self.series.get(sym)
        best = None
        for t, p in bx:
            if t <= ts + 1:
                best = (t, p)
            else:
                break
        if best and ts - best[0] <= OUTCOME_LIVE_MAX_AGE:
            return best[1]
        return None
    def _snap(self, ts):
        out = {}
        for tick, sym in self.stocks:
            p = self._px_at(sym, ts)
            if p:
                out[tick] = p
        return out
    def _qqq_gap(self, snap):
        px = snap.get(REGIME)
        rd = self.ref.get(REGIME)
        close = rd.get("close") if rd else None
        if px and close:
            return (px - close) / close * 100.0
        return None
    def _loop(self):
        done_entry = done_exit = None
        while True:
            try:
                now = int(time.time())
                nd = datetime.fromtimestamp(now, timezone.utc)
                d = nd.strftime("%Y-%m-%d")
                weekday_ok = nd.weekday() < 5 and d not in US_HOLIDAYS_2026H2
                te = _uts_utc(d, *OUTCOME_ENTRY_UTC)
                tx = _uts_utc(d, *OUTCOME_EXIT_UTC)
                if weekday_ok and done_entry != d and te <= now < te + 60:
                    snap = self._snap(te)
                    self.pending = {"date": d, "entries": snap, "qqq": self._qqq_gap(snap)}
                    done_entry = d
                    q = self.pending["qqq"]
                    qtxt = f"{q:+.2f}%" if q is not None else "н/д"
                    print(f"[outcome] {d}: entry-снапшот {len(snap)} тикеров, QQQ-гэп={qtxt}")
                if weekday_ok and done_exit != d and tx <= now < tx + 60:
                    done_exit = d
                    p = self.pending if (self.pending and self.pending["date"] == d) else None
                    self.pending = None
                    if not p or not p["entries"]:
                        print(f"[outcome] {d}: нет entry-снапшота (рестарт между 13:00 и 13:35?) - добьёт ночной recon")
                    else:
                        gate = finnhub_market_open()
                        if gate is True:
                            ex = self._snap(tx)
                            n = 0
                            for tick, epx in p["entries"].items():
                                xpx = ex.get(tick)
                                if xpx and self.store.write(d, tick, epx, xpx, "live", p["qqq"]):
                                    n += 1
                            print(f"[outcome] {d}: записано live {n} исходов (entry 13:00 / exit 13:35 UTC)")
                        elif gate is False:
                            print(f"[outcome] {d}: market-status ЗАКРЫТ (праздник US) - outcome не пишем")
                        else:
                            print(f"[outcome] {d}: market-status недоступен - live не пишем, добьёт ночной recon")
            except Exception as e:
                print(f"[outcome] ошибка цикла: {e}")
            time.sleep(5)

def recon_outcomes(store, inst, label="recon"):
    """Сверочный джоб (старт сервиса + ночью): добирает пропуски и чинит NULL-строки outcomes
    за последние RECON_DEPTH_DATES дат лога signals. Источники:
      bf_5m         - open 5m-свечей entry/exit (обе метки на 5m-сетке; BingX 5m ~3.5 дня);
      bf_15m_proxy  - open 15m-свечи entry + open свечи entry+30мин как прокси выхода (15m ~10 дней).
    Свечи тянем БЕЗ временнОго окна (fetch_klines_1000, см. грабли 109415/109429) и КЭШИРУЕМ
    на символ в рамках прогона: 1-2 запроса на символ на ВСЕ даты сразу.
    Исходы отсутствия данных РАЗЛИЧАЕМ (ничего не выдумываем):
      'no_data'  - свечи по символу есть, но нужных меток нет -> терминальная NULL-строка,
                   БОЛЬШЕ НЕ ре-фетчится (ретенция только уменьшается; вечный ре-фетч мёртвого
                   хвоста копил API-ошибки и вёл к бану - грабли 01.07);
      skip_err   - выборка пустая (ошибка/бан/мёртвый символ) -> НЕ пишем, retry следующим прогоном."""
    if not store.ok:
        return
    KLGATE.reset("recon")                        # новый плановый цикл - сбросить предохранитель
    tick2sym = {it["tick"]: it["symbol"] for it in inst if it["fam"] == "stock"}
    paused = fetch_paused_symbols()              # свежие статусы: паузные не дёргаем (109415 -> бан)
    now = int(time.time())
    # 1) отобрать даты и need-списки
    plan = []                                    # [(d, need:list, te, tx)]
    for d in store.log_dates(RECON_DEPTH_DATES):
        tx = _uts_utc(d, *OUTCOME_EXIT_UTC)
        if now < tx + 300:
            print(f"[{label}] {d}: окно выхода ещё не закрыто - пропуск")
            continue
        if not _is_market_day(d, now):
            print(f"[{label}] {d}: нерыночный день US (выходной/праздник) - outcome не пишем")
            continue
        done = store.resolved_tickers(d)
        need = [t for t in store.log_tickers(d) if t not in done]
        if need:
            plan.append((d, need, _uts_utc(d, *OUTCOME_ENTRY_UTC), tx))
    if not plan:
        print(f"[{label}] добирать нечего")
        return
    # 2) кэш свечей: symbol -> {"5m": dict|None, "15m": dict|None} (None = ещё не тянули)
    kcache = {}
    def candles(sym, interval):
        c = kcache.setdefault(sym, {})
        if interval not in c:
            c[interval] = fetch_klines_1000(sym, interval)   # темп/предохранитель внутри (KLGATE)
        return c[interval]
    # 3) QQQ-гэп на вход per дата: open QQQ-свечи entry vs close_ref QQQ из signals за дату
    qsym = tick2sym.get(REGIME)
    qqq_gap_d = {}
    for d, _need, te, tx in plan:
        qgap = None
        qclose = store.qqq_close_ref(d, te)
        if qsym and qclose:
            qk = candles(qsym, "5m")
            qopen = qk[te * 1000][0] if te * 1000 in qk else None
            if qopen is None:
                qk = candles(qsym, "15m")
                qopen = qk[te * 1000][0] if te * 1000 in qk else None
            if qopen:
                qgap = (qopen - qclose) / qclose * 100.0
        if qgap is None:
            qgap = store.qqq_gap_signal(d, te)       # фолбэк: qqq_regime (гэп QQQ) из лога
        qqq_gap_d[d] = qgap
    # 4) добор по датам из кэша
    for d, need, te, tx in plan:
        te_ms, tx_ms = te * 1000, tx * 1000
        tx15_ms = (te + 1800) * 1000                 # 15m-прокси выхода: entry + 30 мин
        stats = {"bf_5m": 0, "bf_15m_proxy": 0, "no_data": 0, "paused": 0, "skip_err": 0}
        nodata = []
        for t in need:
            if KLGATE.is_tripped():                  # предохранитель: остаток добьёт следующий прогон
                print(f"[{label}] {d}: предохранитель klines - прерываю, остаток добьёт следующий прогон")
                return
            sym = tick2sym.get(t)
            if sym and sym in paused:                # паузный перп: терминально, ни одного запроса
                store.write(d, t, None, None, "paused", qqq_gap_d.get(d))
                stats["paused"] += 1
                continue
            entry = exit_ = src = None
            k5 = k15 = {}
            if sym:
                k5 = candles(sym, "5m")
                if te_ms in k5 and tx_ms in k5:
                    entry, exit_, src = k5[te_ms][0], k5[tx_ms][0], "bf_5m"
                else:
                    k15 = candles(sym, "15m")
                    if te_ms in k15 and tx15_ms in k15:
                        entry, exit_, src = k15[te_ms][0], k15[tx15_ms][0], "bf_15m_proxy"
                    elif k5 or k15:
                        src = "no_data"              # свечи по символу ЕСТЬ, нужных меток нет - терминально
            if src in ("bf_5m", "bf_15m_proxy"):
                store.write(d, t, entry, exit_, src, qqq_gap_d.get(d))
                stats[src] += 1
            elif src == "no_data":
                store.write(d, t, None, None, "no_data", qqq_gap_d.get(d))
                stats["no_data"] += 1
                nodata.append(t)
            else:
                # обе выборки пустые = ошибка/бан/мёртвый символ -> НЕ пишем (retry следующим прогоном)
                stats["skip_err"] += 1
        msg = (f"[{label}] {d}: bf_5m={stats['bf_5m']} bf_15m_proxy={stats['bf_15m_proxy']} "
               f"no_data={stats['no_data']} paused={stats['paused']} skip_err={stats['skip_err']}")
        if nodata:
            msg += " | нет свечей на метках (терминально): " + ",".join(nodata[:20]) + ("..." if len(nodata) > 20 else "")
        print(msg)

class OutcomeRecon:
    """Планировщик recon_outcomes: один прогон при старте (через ~60с) + ночью в RECON_HOUR_UTC:30."""
    def __init__(self, inst, store):
        self.inst = inst
        self.store = store
    def start(self):
        threading.Thread(target=self._loop, daemon=True).start()
    def _loop(self):
        time.sleep(60)
        try:
            recon_outcomes(self.store, self.inst, "recon-start")
        except Exception as e:
            print(f"[recon-start] ошибка: {e}")
        while True:
            now = datetime.fromtimestamp(time.time(), timezone.utc)
            nxt = now.replace(hour=RECON_HOUR_UTC, minute=30, second=0, microsecond=0)
            if nxt <= now:
                nxt += timedelta(days=1)
            time.sleep(max(60.0, (nxt - now).total_seconds()))
            try:
                recon_outcomes(self.store, self.inst, "recon-night")
            except Exception as e:
                print(f"[recon-night] ошибка: {e}")

# ----------------------------- Батч 2.5: коллектор 1m klines -----------------------------
class KlinesCollector:
    """Копим СВОЮ историю 1m (BingX хранит ~сутки; ретро недоступна). Раз в KL1M_INTERVAL проходим
    весь stock-универс (вкл. QQQ): v3 klines limit=1000 БЕЗ временнОго окна (~16.7ч покрытия,
    сильно больше часового шага - потерь нет). Оконная пагинация startTime/endTime ОТКЛЮЧЕНА
    сознательно: оконный запрос по неликвиду без свечей в окне даёт ошибку 109415, 10 ошибок за
    15 мин = IP-бан API 109429 (см. fetch_klines_win). Троттлинг + backoff на 429/бан,
    дедуп по PRIMARY KEY(symbol, open_time) + INSERT OR IGNORE. Отдельная база KL1M_DB_PATH."""
    def __init__(self, symbols):
        self.symbols = list(symbols)
        self.lock = threading.Lock()
        self.conn = None
        self.ok = False
        self.last_stats = {}
    def start(self):
        try:
            os.makedirs(os.path.dirname(KL1M_DB_PATH), exist_ok=True)
            self.conn = sqlite3.connect(KL1M_DB_PATH, check_same_thread=False)
            self.conn.execute("""CREATE TABLE IF NOT EXISTS klines(
                symbol TEXT, open_time INTEGER, o REAL, h REAL, l REAL, c REAL, volume REAL,
                PRIMARY KEY(symbol, open_time))""")
            self.conn.commit()
            self.ok = True
        except Exception as e:
            print(f"!! KlinesCollector отключён (не пишется {KL1M_DB_PATH}): {e}")
            return
        threading.Thread(target=self._loop, daemon=True).start()
    def _insert(self, sym, k):
        rows = [(sym, t, v[0], v[1], v[2], v[3], v[4]) for t, v in k.items()]
        with self.lock:
            before = self.conn.execute("SELECT COUNT(*) FROM klines WHERE symbol=?", (sym,)).fetchone()[0]
            self.conn.executemany("INSERT OR IGNORE INTO klines VALUES(?,?,?,?,?,?,?)", rows)
            self.conn.commit()
            after = self.conn.execute("SELECT COUNT(*) FROM klines WHERE symbol=?", (sym,)).fetchone()[0]
        return before, after - before
    def _pull(self, sym):
        """Безопасный запрос limit=1000 без окна (через KLGATE); INSERT OR IGNORE. Если по символу
        это ПЕРВЫЕ данные и страница пришла полной (1000 = ~16.7ч из ~24ч ретенции) - добираем
        старший хвост ОДНИМ endTime-запросом: окно тут безопасно, свечи до oldest точно есть
        (ошибка 109415 бывает только когда свечей в окне нет). -> сколько НОВЫХ строк."""
        k = fetch_klines_1000(sym, "1m")
        if not k:
            return 0
        before, added = self._insert(sym, k)
        if before == 0 and len(k) >= 1000 and not KLGATE.is_tripped():
            k2 = fetch_klines_1000(sym, "1m", end_ms=min(k.keys()) - 1)   # добор до края ретенции
            if k2:
                _, add2 = self._insert(sym, k2)
                added += add2
        return added
    def _loop(self):
        print(f"[kl1m] первый прогон через {KL1M_START_DELAY//60} мин (развожу со стартовым recon)")
        time.sleep(KL1M_START_DELAY)
        while True:
            t0 = time.time()
            KLGATE.reset("kl1m")                     # новый плановый цикл
            paused = fetch_paused_symbols()          # свежие статусы: паузные скипаем (109415 -> бан)
            with_data = empty = skipped = added = 0
            for sym in self.symbols:
                if KLGATE.is_tripped():
                    print("[kl1m] предохранитель klines - прогон прерван, продолжу в следующем цикле")
                    break
                if sym in paused:
                    skipped += 1
                    continue
                try:
                    a = self._pull(sym)
                except Exception:
                    a = 0
                if a > 0:
                    with_data += 1
                    added += a
                else:
                    empty += 1
            try:
                size = os.path.getsize(KL1M_DB_PATH)
            except Exception:
                size = 0
            with self.lock:
                total = self.conn.execute("SELECT COUNT(*) FROM klines").fetchone()[0]
            self.last_stats = {"with_data": with_data, "empty": empty, "paused": skipped,
                               "added": added, "total": total, "db_mb": round(size / 1e6, 1),
                               "took_s": round(time.time() - t0, 1)}
            print(f"[kl1m] проход: +{added} новых строк | {with_data} символов с данными / {empty} без / "
                  f"{skipped} паузных скипнуто | всего {total} строк, база {self.last_stats['db_mb']} МБ, "
                  f"{self.last_stats['took_s']}с")
            time.sleep(max(60.0, KL1M_INTERVAL - (time.time() - t0)))


# ----------------------------- snapshot builder -----------------------------
STATE = {"updated": None, "rows": [], "strategy": {}, "regime": None,
         "ws": "—", "tw": "—", "note": "", "now_msk": None, "windows": {}}
LOCK = threading.Lock()

# глобалы для /series (ставятся в main)
SERIES = None          # Series

def _strategy_label(base, gap):
    """Черновик-бакеты. Знак гэпа всегда виден у вызывающего."""
    a = abs(gap)
    if a < 1:
        return "noise", "шум <1%"
    if gap >= 5:   # up-гэп >= +5%: континуация-лонг (gap-and-go), вход 16:00, стоп 2% обязателен
        return "cont_long", "континуация-лонг >5% (gap-and-go)"
    if a >= 2:     # 2-5% обеих сторон + down >5% (без режима) - скип
        return "skip", "скип 2-5% / down >5%"
    if gap > 0:   # перп ВЫШЕ close -> фейд-шорт (up-гэп 1-2%, единый бакет)
        return "fade_short", "фейд-шорт 1-2%"
    return "long_weak", "лонг 1-2% (слабая нога)"   # перп НИЖЕ close; рендер за флагом SHOW_WEAK_LEG_GROUP

def build_snapshot(inst, pf, prem, pyth, tw, ref, oif):
    rows = []
    regime = None
    buckets = {"fade_short": [], "cont_long": [], "long_weak": [], "skip": [], "noise": []}
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
        # Андерлайн графика/базиса = TradingView (delayed ~15м): ТОЛЬКО базовая линия графика
        # (в таблице отдельной колонки нет — в премаркет она равна клоузу и дублировала бы ГЭП%).
        tp = tw.get(base) if tw else None
        if tp and tp[0] and SERIES is not None:
            SERIES.add_base(sym, tp[0], time.time())
        # Pyth — внутренний real-time кросс-чек басиса для suspect-гейта; за флагом USE_PYTH.
        # Выключен (по умолчанию) → basis=None → гейт по басису неактивен (гэп >25% всё равно ловится).
        basis = None
        pp = pyth.get(base) if pyth else None
        if pp and pp[0] and live is not None:
            basis = (live - pp[0]) / pp[0] * 100
        # close-референс (TradingView — основной, Finnhub — фолбэк/кросс-чек) и регион
        rd = ref.get(base)
        close = rd["close"] if rd else None
        close_src = rd.get("src") if rd else None
        close_mismatch = bool(rd and rd.get("mismatch"))   # TV vs Finnhub расходятся >2%
        region = (rd or {}).get("region", "US")
        # гэп + надёжность: лучше прочерк, чем мусор
        gap = None; gap_raw = None; suspect = False; reason = None
        if close and live is not None and close != 0:
            gap_raw = (live - close) / close * 100
            if basis is not None and abs(basis) > BASIS_SUSPECT_PCT:
                suspect = True; reason = f"перп оторван от Pyth {basis:+.1f}%"
            elif abs(gap_raw) > 25:
                suspect = True; reason = "аномалия >25%"
            else:
                gap = gap_raw                         # торговый гэп
                if close_mismatch:
                    reason = f"клоуз расходится: TV-вчера {rd.get('tvprev')} vs Finnhub {rd.get('fh')}"
        elif live is not None:
            reason = "нет надёжного клоуза (TV/Finnhub)"
        oi = oif.get(sym) if oif else None
        st, in_win = session_state(region)
        row = {
            "ticker": it["tick"], "symbol": it["display"], "api": sym, "fam": fam,
            "live": live, "close": close, "close_src": close_src, "tvsym": (rd or {}).get("tvsym"), "close_mismatch": close_mismatch,
            "gap": gap, "gap_raw": gap_raw, "suspect": suspect, "reason": reason,
            "premium": premium, "basis": basis, "funding": funding, "oi": oi,
            "taker": it["taker"], "region": region, "session": st, "in_win": in_win,
            "is_regime": (base == REGIME),
        }
        rows.append(row)
        if base == REGIME:
            regime = row
        # стратегия: только акции с гэпом
        if fam == "stock" and gap is not None:
            bucket, label = _strategy_label(base, gap)
            item = {"ticker": base, "api": sym, "gap": gap, "mismatch": close_mismatch,
                    "label": label, "region": region, "session": st, "in_win": in_win}
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
    cov_c, cov_t = tw.coverage()
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
            "tw": f"{cov_c}/{cov_t} акций",
            "windows": windows, "us_session": us_session,
            "note": "",
        })

def updater(inst, pf, prem, pyth, tw, ref, oif, siglog):
    last_log = 0.0
    while True:
        try:
            build_snapshot(inst, pf, prem, pyth, tw, ref, oif)
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
.search{padding:3px 9px;border:1px solid var(--line);border-radius:4px;background:var(--panel);color:var(--txt);font-size:12px;min-width:130px;outline:none}
.search:focus{border-color:var(--blue)}
.search::placeholder{color:var(--mut)}
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
.tvchart{color:var(--warn);text-decoration:none;font-size:11px;margin-left:8px;border:1px solid var(--warn);padding:3px 8px;border-radius:4px}
.tvchart:hover{background:rgba(210,153,34,.14)}
#buckets .bk{margin:0 0 14px}#buckets .bk h3{font-size:12px;margin:0 0 7px;letter-spacing:.3px}
#buckets .srow{display:flex;justify-content:space-between;align-items:center;padding:7px 11px;
border:1px solid var(--line);border-radius:6px;margin-bottom:5px;background:var(--panel);cursor:pointer}
#buckets .srow:hover{background:rgba(56,139,253,.10)}
#buckets .srow.sel{box-shadow:inset 3px 0 0 var(--blue)}
#buckets .srow.win{border-color:var(--go)}#buckets .srow.warn{border-color:var(--warn)}
#buckets .srow .l{display:flex;align-items:center;gap:8px}#buckets .srow .g{font-weight:700}
td.warn{color:var(--warn);cursor:help}
.sess{padding:3px 8px;border-radius:4px;border:1px solid var(--line);font-weight:700}
.sess.rth{color:#0d1117;background:var(--go);border-color:var(--go)}
.sess.pre{color:#0d1117;background:var(--warn);border-color:var(--warn)}
.sess.ah{color:var(--mut)}
.draft{display:inline-block;background:var(--warn);color:#0d1117;font-weight:700;
font-size:10px;padding:2px 7px;border-radius:4px;margin-left:8px}
.hide{display:none}
</style></head><body><div class="wrap">
<h1>BingX · TradFi real-time</h1>
<div class="sub">live = WebSocket lastPrice · ГЭП% (TW) = (live − вчерашний TW-клоуз)/клоуз · клик по строке → график · обновлено <span id="upd">—</span></div>
<div class="bar">
  <span class="pill">ws: <span id="ws">—</span></span>
  <span class="pill">TW: <span id="tw">—</span></span>
  <span class="sess ah" id="sesspill">US: —</span>
  <span class="pill"><span id="cellc" class="mut">Cell C: —</span> <span class="qhelp" title="Cell C: шорт-континуация down-гэпа 2-5% при QQQ-гэпе вверх (band +0.2% как в движке клеток). In-sample PF 2.97, N=44. Форвардом НЕ подтверждено.">(?)</span></span>
  <span class="pill">US: закрытие 23:00 · вход ~16:00 · открытие 16:30 МСК <span class="qhelp" title="Гэп считается от вчерашнего закрытия US RTH (23:00 МСК). Вход в премаркет ~16:00-16:10 МСК, рынок открывается 16:30 МСК.">(?)</span></span>
  <input id="search" class="search" type="search" placeholder="поиск тикера" autocomplete="off" spellcheck="false" oninput="onSearch(this.value)" onkeydown="if(event.key=='Escape'){this.value='';onSearch('');}">
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
<th data-key="gap" title="BingX vs вчерашний TW-клоуз (REF CLOSE); стабилен при любой сессии"><svg class="bxico" width="17" height="9" viewBox="74 156 353 183"><path fill="#2962ff" d="M 74.268 158.688 L 216.747 158.711 L 217.506 338.161 L 145.466 337.657 L 145.445 229.983 L 74.2 229.871 L 74.268 158.688 Z"/><circle fill="#2962ff" cx="270.59" cy="192.178" r="35.956"/><polygon fill="#2962ff" points="343.41 158.808 426.417 158.896 351.232 336.856 269.104 336.679 342.837 158.951"/></svg>ГЭП% (TW)<span class="ar"></span></th>
<th data-key="funding">Funding<span class="ar"></span></th>
</tr></thead><tbody id="tb"></tbody></table>

<div class="legend">
только US-акции/ETF (NCSK, вкл. QQQ); строки без live-цены BingX скрыты (торговать нельзя) · клик по заголовку - сортировка (числовые - числом) ·
<b>«заморозить порядок»</b> работает и в «Акции», и в «Стратегия» · <b>клик по строке → график (lightweight-charts)</b> в обеих вкладках ·
<b>ГЭП% (TW)</b> = BingX vs вчерашний TW-клоуз (стабилен при любой сессии) · гэп «—» = нет клоуза/данные сомнительны (наведи) · OI убран из таблицы, пишется в лог · на графике базовая линия = внутридневная TW (задержка ~15м)
</div>
</div>

<div id="strat" class="hide">
<div class="regime" id="regime">QQQ -</div>
<div id="buckets"></div>
<div class="legend">Бакеты по знаковому гэпу: ✅ фейд-шорт = up 1-2% · 🚀 континуация-лонг = up &gt;=5% (вход 16:00, стоп 2% обязателен, чище от &gt;7%; спекулятивно) ·
скип = 2-5% обеих сторон и down &gt;5% без режима · шум &lt;1%. Режим QQQ: band +-0.2% (рос / падал / нейтр), как в движке клеток. Это ЧЕРНОВИК.</div>
</div>

<div id="faq" class="hide"><div class="faq">
<h3>Что считает каждая колонка</h3>
<p><b>Live BingX</b> - последняя цена перпа из WebSocket-потока (<code>@lastPrice</code>), обновляется в реалтайме. Иконка слева = источник цены (BingX); задел под другие биржи.</p>
<p><b>Ref close</b> - последнее завершённое RTH-закрытие US. ОСНОВНОЙ источник - TradingView screener (<code>close − change_abs</code>, keyless, ~15-мин задержка), ФОЛБЭК/кросс-чек - Finnhub <code>pc</code>. В премаркете (15:30-16:30 МСК) = вчерашний клоуз. Если TV и Finnhub расходятся &gt;2% - строка помечена «клоуз расходится» (наведи на «⚠»). Обновляется раз в день (маркер pre/post 16:30 ET).</p>
<p><b>ГЭП% (TW) = (BingX − вчерашний TW-клоуз) / клоуз</b> - насколько перп ушёл от вчерашнего RTH-закрытия US (клоуз = REF CLOSE из TradingView screener). Стабилен при любой сессии (всегда к вчерашнему клоузу). «—» если нет надёжного клоуза, либо гэп &gt;25% - тогда строка «данные сомнительны» (наведи на «—»: причина + сырой %). Лучше прочерк, чем мусор.</p>
<p><b>Внутридневная TW (только график)</b> - на графике базовая линия и базис к перпу считаются от ТЕКУЩЕЙ цены TradingView (задержка ~15м; тот же screener, что и клоуз). Отдельной колонки в таблице нет: в премаркет текущая TW = вчерашнему клоузу и дублировала бы ГЭП%. Нет US/ADR-листинга → базы нет. (Pyth-андерлайн отключён флагом <code>USE_PYTH</code>, код на месте.)</p>
<p><b>OI</b> - открытый интерес перпа (USD-нотионал, потолок ~$1M/инструмент). Колонка из таблицы убрана (из-за капа между инструментами почти не различает), но <b>продолжает писаться в sqlite-лог</b> для ресёрча.</p>
<p><b>Funding</b> - ставка финансирования за период (напр. <code>+0.0100%/8h</code>). Платится ВСЕГДА - часть реального коста удержания. «+» лонги платят шортам. (Стандартный taker 0.05% - постоянная величина, из таблицы убрана.)</p>
<p><b>Сессия US</b> - состояние рынка (премаркет / RTH / afterhours / выходной) вынесено одним индикатором в шапку; меняется в течение дня и важно для окна входа.</p>
<h3>Вкладка «Стратегия» (черновик)</h3>
<p>Раскладывает акции по знаковому гэпу: <b>✅ фейд-шорт</b> = перп выше закрытия на 1-2% (играем на возврат вниз); <b>🚀 континуация-лонг</b> = up-гэп &gt;=5% (gap-and-go: вход 16:00, стоп 2% ОБЯЗАТЕЛЕН, чище от &gt;7%; walk-forward НЕ пройден - спекулятивный сетап); <b>скип</b> = 2-5% обеих сторон и down &gt;5% без режима; <b>шум &lt;1%</b> мелочь. Группа "лонг 1-2% (слабая нога)" отключена 02.07.2026 (walk-forward не прошла), код сохранён за флагом. Пороги in-sample, это черновик.</p>
<h3>Почему по части тикеров клоуз/гэп скрыт</h3>
<p>Перп номинирован в USD. Для иностранных акций без US-листинга (Samsung, SK Hynix) Finnhub не отдаёт <code>pc</code> → клоуза нет → гэп «—». Остаётся спред (внутри-инструментный) и базис vs Pyth, где Pyth покрывает. С 31.07.2026 публичный Hermes-Pyth станет платным - поэтому Finnhub оставлен ОСНОВНЫМ клоуз-источником, а источник переключаем параметром.</p>
<h3>Про «0-fee» (важно)</h3>
<p>«0 Fees» в интерфейсе BingX - это <b>временное промо</b> (≈до 31.07.2026), а не постоянная фича, и с catch'ем: только реферальные юзеры и только <b>ручная</b> торговля. <b>Любой включённый API-ключ лишает льготы - даже для ручных ордеров.</b> Значит через API/бота списывается стандарт 0.02%/0.05% + funding. Для автоматизации костовый потолок остаётся.</p>
<h3>Про график и TradingView</h3>
<p>Клик по строке разворачивает график-аккордеон (TradingView <b>lightweight-charts</b>, Apache 2.0, вендорится локально): сплошная линия <b>BingX</b> (перп, WS) против <b>реальной базы</b> (Pyth real-time, иначе Yahoo <i>delayed</i>) - базис вживую. Буфер 10 мин/символ на сервере (сид из 1m-свечей + WS), real-time через <code>series.update</code>. У TradingView нет data-API, поэтому их линию не тянем; готовый виджет TV - отдельной кнопкой для сверки. Атрибуция TradingView обязательна по лицензии.</p>
</div></div>

</div>
<script src="/static/lightweight-charts.standalone.production.js"></script>
<script>
const REFRESH=__REFRESH__;
const SHOW_WEAK_LEG=__WEAKLEG__;   /* группа "лонг 1-2% (слабая нога)": отключена 02.07.2026, код в запасе */
const f2=(x,d=2)=>x==null?'—':Number(x).toFixed(d);
const sgn=(x,d=2)=>x==null?'—':(x>0?'+':'')+Number(x).toFixed(d);
const cls=x=>x==null?'mut':(x>0?'up':(x<0?'dn':''));
let LAST=null, SEL=null, SELTICK=null, SELTVSYM=null, SELSRC=null, TVON=false, LASTORDER='';
let SORT={key:null,dir:0}, FROZEN=false, FROZEN_ORDER=null;
let CHART=null,BXS=null,BASES=null,CHARTBOX=null,CHARTROW=null,lastBxT=0,lastBaseT=0;
const rowEls=new Map();
/* TV-символ (ссылка + виджет) берём точным из данных строки (row.tvsym = "EXCHANGE:SYMBOL" из screener), без угадайки биржи */

function show(t){['num','strat','faq'].forEach(x=>{
  document.getElementById(x).className=(x==t?'':'hide');
  document.getElementById('t-'+x).className='tab'+(x==t?' on':'');});applyFilter();}

/* ---- поиск по тикеру: клиентский слой ВИДИМОСТИ строк (не трогает данные/порядок/график/бэкенд) ---- */
let SEARCH='';
function matchTick(t){return !SEARCH||(''+(t||'')).toLowerCase().includes(SEARCH);}
function onSearch(v){SEARCH=(''+(v||'')).trim().toLowerCase();applyFilter();}
function applyFilter(){
  for(const[,o]of rowEls)o.tr.style.display=matchTick(o.tr.dataset.tick)?'':'none';
  if(SEL&&SELSRC==='num'&&CHARTROW){const o=rowEls.get(SEL);CHARTROW.style.display=(o&&matchTick(o.tr.dataset.tick))?'':'none';}
  for(const[,o]of srowEls)o.el.style.display=matchTick(o.el.dataset.tick)?'':'none';
  for(const key in stratSec){const body=stratSec[key].body;let vis=0;
    for(const ch of body.children)if(ch.classList&&ch.classList.contains('srow')&&ch.style.display!=='none')vis++;
    const sec=body.parentNode;if(sec)sec.style.display=(!SEARCH||vis>0)?'':'none';}
}

/* ---- sort / order ---- */
function val(r,k){switch(k){
  case 'ticker':return r.ticker||'';
  case 'live':return r.live;case 'close':return r.close;case 'gap':return r.gap;
  case 'funding':return r.funding?r.funding.rate:null;}return null;}
function defcmp(a,b){const sa=a.gap!=null?Math.abs(a.gap):-1;
  const sb=b.gap!=null?Math.abs(b.gap):-1;return sb-sa;}
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
  tr.innerHTML=`<td class="l"><span class="dot ${x.fam}"></span>${x.ticker}${x.is_regime?' ★':''}</td>`+
    `<td></td><td class="mut"></td><td></td><td class="mut"></td>`;
  return {tr};}
function updateRow(o,x){const ch=o.tr.children;
  o.tr.className=(x.in_win?'win ':'')+(x.api==SEL?'sel':'');
  ch[1].textContent=f2(x.live);
  ch[2].textContent=f2(x.close)+(x.close_mismatch?' ⚠':'');ch[2].className=x.close_mismatch?'warn':'mut';
  ch[2].title=x.close_mismatch?(x.reason||'клоуз расходится TV/Finnhub'):'';
  const g=ch[3];
  if(x.gap!=null){g.textContent=sgn(x.gap);g.className=cls(x.gap);g.title='';}
  else if(x.gap_raw!=null){g.textContent='—';g.className='susp';g.title=(x.reason||'данные сомнительны')+' · сырой '+sgn(x.gap_raw)+'%';}
  else{g.textContent='—';g.className='mut';g.title=x.reason||'';}
  ch[4].textContent=(x.funding&&x.funding.rate!=null)?sgn(x.funding.rate,4)+'%/'+(x.funding.ih||'?')+'h':'—';}
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
  if(SEL&&SELSRC==='num')positionChart();applyFilter();}

/* ---- «Стратегия»: секции строятся 1 раз, строки обновляются ПО МЕСТУ (список НЕ пересоздаётся →
   TV-iframe не перезагружается, аккордеон-график цел; при открытом графике структура заморожена) ---- */
const STRAT_DEFS=[['fade_short','fade','✅ Фейд-шорт 1-2%'],
  ['cont_long','long','🚀 Континуация-лонг >5% (gap-and-go) <span class="mut" style="font-weight:400">вход 16:00, стоп 2% ОБЯЗАТЕЛЕН, чище от >7%</span> <span class="qhelp" title="in-sample: >5% только со стопом 2% (PF 1.13, N125), >7% PF 1.29 (N46); walk-forward НЕ пройден (оверфит) - спекулятивный сетап">(?)</span>'],
  ...(SHOW_WEAK_LEG?[['long_weak','long','Лонг 1-2% (слабая нога)']]:[]),
  ['skip','skip','Скип 2-5% / down >5%'],['noise','noise','Шум <1%']];
let stratBuilt=false;const stratSec={};const srowEls=new Map();
function buildStrat(){const wrap=document.getElementById('buckets');wrap.innerHTML='';
  for(const [key,c,title] of STRAT_DEFS){const sec=document.createElement('div');sec.className='bk '+c;
    const h=document.createElement('h3');h.innerHTML=title+' <span class="mut cnt">(0)</span>';sec.appendChild(h);
    const body=document.createElement('div');sec.appendChild(body);wrap.appendChild(sec);
    stratSec[key]={body,cnt:h.querySelector('.cnt')};}
  stratBuilt=true;}
function makeSrow(it){const el=document.createElement('div');el.className='srow';
  el.dataset.api=it.api;el.dataset.tick=it.ticker;
  el.innerHTML=`<span class="l"><b>${it.ticker}</b></span><span class="g"></span>`;
  return {el,bucket:null,g:el.querySelector('.g')};}
function renderStrat(s){const rg=s.regime,el=document.getElementById('regime');
  const q='<span class="qhelp" title="QQQ = ETF на Nasdaq-100, датчик режима. Band +-0.2% как REG_BAND в движке клеток: рос / падал / нейтр. QQQ рос + down-гэп тикера 2-5% = условие Cell C; фейд-шорт против роста рискован.">(?)</span>';
  if(rg&&rg.gap!=null){const g=rg.gap;   /* режим по клеточному порогу +-0.2%, как в шапке-индикаторе */
    const reg=g>0.2?'<b style="color:var(--up)">рос</b>':(g<-0.2?'<b style="color:var(--dn)">падал</b>':'нейтр');
    el.innerHTML=`QQQ ${sgn(g)}% ${q} - режим: `+reg;
  }else el.innerHTML='QQQ - '+q;
  if(!stratBuilt)buildStrat();
  const b=(s.strategy&&s.strategy.buckets)||{};
  const pinned=(SEL&&SELSRC=='strat');           // график открыт → структуру не трогаем
  const now=new Map();
  for(const [key] of STRAT_DEFS)for(const it of (b[key]||[]))now.set(it.api,{it,bucket:key});
  for(const [api,ob] of now){const it=ob.it,bucket=ob.bucket;let o=srowEls.get(api);
    if(!o){o=makeSrow(it);srowEls.set(api,o);}
    o.g.textContent=sgn(it.gap)+'%';o.g.className='g '+cls(it.gap);
    o.el.className='srow'+(it.in_win?' win':'')+(api==SEL?' sel':'')+(it.mismatch?' warn':'');
    o.el.title=it.mismatch?'клоуз расходится TV/Finnhub':'';
    if(o.bucket===null||(!pinned&&o.bucket!==bucket)){stratSec[bucket].body.appendChild(o.el);o.bucket=bucket;}}
  if(!pinned)for(const [api,o] of srowEls){if(!now.has(api)){o.el.remove();srowEls.delete(api);}}
  const fidx={};if(FROZEN&&FROZEN_ORDER)FROZEN_ORDER.forEach((id,i)=>fidx[id]=i);
  for(const [key] of STRAT_DEFS){let items=(b[key]||[]).slice();
    if(FROZEN&&FROZEN_ORDER)items.sort((x,y)=>((fidx[x.api]??9999)-(fidx[y.api]??9999)));
    stratSec[key].cnt.textContent='('+items.length+')';
    if(!pinned){const body=stratSec[key].body;
      for(const it of items){const o=srowEls.get(it.api);if(o&&o.bucket===key)body.appendChild(o.el);}}}
  if(SEL&&SELSRC=='strat')positionChart();applyFilter();}

/* ---- график: lightweight-charts, аккордеон в ОБЕИХ вкладках ---- */
function selectRow(api,tick,source){
  if(SEL===api){closeChart();return;}
  closeChart();
  SEL=api;SELTICK=tick;SELSRC=source;TVON=false;
  fillChartBox(tick,api);positionChart();initChart();refreshChart(true);}
function fillChartBox(tick,api){
  if(!CHARTBOX){CHARTBOX=document.createElement('div');CHARTBOX.className='cbox';}
  const rr=LAST?LAST.rows.find(r=>r.api===api):null;
  const tvsym=(rr&&rr.tvsym)||null; SELTVSYM=tvsym;   // точный TV-символ из screener; нет → без TV-навигации
  const tvrow=tvsym
    ? `<div class="tvrow"><span class="tvbtn" id="tvbtn" onclick="toggleTV()">показать TradingView</span>`
      +`<a class="tvchart" target="_blank" rel="noopener" href="https://www.tradingview.com/chart/?symbol=${tvsym}">график на TradingView ↗</a></div>`
    : `<div class="tvrow"><span class="mut" style="font-size:11px">TradingView: символ не найден</span></div>`;
  CHARTBOX.innerHTML=
    `<div class="chead"><b>${tick} · ${api}</b>`+
    `<a class="mut" style="margin-left:8px" target="_blank" rel="noopener" href="https://bingx.com/en/perpetual/${api}">BingX ↗</a>`+
    `<span id="cbasis" class="mut"></span><span class="cx" onclick="closeChart()">✕</span></div>`+
    `<div id="cchart"></div>`+
    `<div class="cleg"><span style="color:#2f81f7">━ BingX перп</span> &nbsp; <span style="color:#d29922">━ <span id="lbase">база</span></span>`+
    `<span class="tvattr">графики: <a href="https://www.tradingview.com" target="_blank" rel="noopener">TradingView</a> Lightweight Charts™</span></div>`+
    tvrow+
    `<div id="tvwrap"></div>`;}
function ensureChartRow(){if(!CHARTROW){CHARTROW=document.createElement('tr');CHARTROW.className='chartrow';
    CHARTROW.innerHTML='<td colspan="5"></td>';}return CHARTROW;}
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
    ('BingX '+bl.toFixed(2)+(ba?(' · база '+ba.toFixed(2)+' (TW·15м) · базис <b class="'+cls(basis)+'">'+sgn(basis,3)+'%</b>'):' · базы нет')):'ждём тики…';
  const lb=document.getElementById('lbase');if(lb)lb.textContent=(d.base_src=='tw')?'база: TW (задержка 15м)':'база: нет данных';}
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
  sc.text=JSON.stringify({symbols:[[SELTVSYM]],chartOnly:false,width:'100%',height:300,colorTheme:'dark',isTransparent:true,locale:'ru'});
  wrap.querySelector('.tradingview-widget-container').appendChild(sc);}

/* ---- poll ---- */
async function tick(){try{const s=await(await fetch('/data',{cache:'no-store'})).json();LAST=s;
  document.getElementById('upd').textContent=s.updated||'—';
  document.getElementById('ws').textContent=s.ws||'—';
  document.getElementById('tw').textContent=s.tw||'—';
  const sp=document.getElementById('sesspill'),us=s.us_session||'—';
  sp.textContent='US: '+us;
  sp.className='sess '+(us.indexOf('RTH')>=0?'rth':(us.indexOf('премаркет')>=0?'pre':'ah'));
  /* индикатор Cell C: QQQ-режим "рос" = gap > +0.2 (band как REG_BAND в gap_cells_1m.py);
     down-бакет ровно как в движке: 2 <= |gap| < 5, т.е. gap<=-2 && gap>-5. Чистая индикация. */
  const cc=document.getElementById('cellc');
  if(cc){const qg=(s.regime&&s.regime.gap!=null)?s.regime.gap:null;let names=[];
    if(qg!=null&&qg>0.2){for(const r of (s.rows||[]))if(r.gap!=null&&r.gap<=-2&&r.gap>-5)names.push(r.ticker);}
    if(names.length){cc.textContent='Cell C: АКТИВНА (тикеры: '+names.join(', ')+')';cc.className='up';}
    else{cc.textContent='Cell C: нет условий';cc.className='mut';}}
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
            # база = TradingView (delayed ~15м). Нет TW → базовой линии нет.
            src = "tw" if base else "none"
            self._json({"symbol": sym, "bingx": bx, "base": base,
                        "base_src": src, "base_delayed": True})
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
            body = (HTML.replace("__REFRESH__", str(BROWSER_REFRESH))
                        .replace("__WEAKLEG__", "true" if SHOW_WEAK_LEG_GROUP else "false"))
            self.wfile.write(body.encode("utf-8"))


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
    tw   = TWFeed(inst)
    siglog = SignalLog(DB_PATH)
    pf.start(); prem.start(); ref.start(); oif.start(); tw.start(); SERIES.start(); siglog.start()
    if USE_PYTH: pyth.start()            # Pyth за флагом (по умолчанию выкл.); объект и код на месте
    # Батч 2.5: форвард-фундамент - outcome-джоб (live + recon) и коллектор 1m klines
    ostore = OutcomeStore(DB_PATH); ostore.start()
    OutcomeTracker(inst, SERIES, ref, ostore).start()
    OutcomeRecon(inst, ostore).start()
    kcol = KlinesCollector(symbols); kcol.start()
    print(f"Outcome-джоб: entry {OUTCOME_ENTRY_UTC[0]:02d}:{OUTCOME_ENTRY_UTC[1]:02d} / exit "
          f"{OUTCOME_EXIT_UTC[0]:02d}:{OUTCOME_EXIT_UTC[1]:02d} UTC (EDT-константы), live-снапшот + recon "
          f"(старт + ночь {RECON_HOUR_UTC:02d}:30 UTC, глубина {RECON_DEPTH_DATES} дат) -> таблица outcomes.")
    print(f"Коллектор 1m klines: {len(symbols)} символов раз в {KL1M_INTERVAL//60} мин -> {KL1M_DB_PATH}")
    print(f"WS: подписка на {len(symbols)} символов ({(len(symbols)+SUBS_PER_WS-1)//SUBS_PER_WS} соединений).")
    print(f"График: буфер 10 мин/символ — сид из 1m-klines (фоном) + добивка из WS; линия базы = TradingView (delayed 15м).")
    print(f"Close-референс: источник='{CLOSE_SOURCE}' (TradingView screener bulk, keyless — основной; "
          f"Finnhub {'есть' if FINNHUB_KEY else 'НЕТ ключа'} — фолбэк/кросс-чек >2%).")
    print(f"SQLite-лог сигналов: {DB_PATH if siglog.ok else '(отключён)'}")
    time.sleep(2.0)  # дать ws/premium наполниться
    cov_c, cov_t = tw.coverage()
    print(f"Андерлайн TradingView (delayed 15м): покрытие {cov_c}/{cov_t} акций (тот же screener, что и клоуз).")
    print("Pyth: " + ("ВКЛ (USE_PYTH=true)" if USE_PYTH else "выключен (USE_PYTH=false; код на месте, вернуть env USE_PYTH=true)"))

    threading.Thread(target=updater, args=(inst, pf, prem, pyth, tw, ref, oif, siglog), daemon=True).start()

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
