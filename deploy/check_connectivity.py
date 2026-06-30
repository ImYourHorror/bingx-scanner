#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Проверка с ЭТОГО сервера: проходят ли BingX REST/WS, Yahoo, Pyth.
Запуск:  python3 deploy/check_connectivity.py
Код возврата 1, если недоступен BingX (без него сканер бесполезен)."""
import json, socket, ssl, base64, os, urllib.request, sys

# на всякий случай (POSIX/C-локаль на сервере) — не падать на эмодзи в выводе
for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except Exception:
        pass

def http(url, timeout=12):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 conncheck"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()

def check(name, fn):
    try:
        fn(); print(f"  PASS  {name}"); return True
    except Exception as e:
        print(f"  FAIL  {name}: {type(e).__name__}: {str(e)[:140]}"); return False

def bingx_rest():
    j = json.loads(http("https://open-api.bingx.com/openApi/swap/v2/quote/contracts"))
    assert j.get("code") == 0 and j.get("data"), "bad payload"

def bingx_ws():
    host, path = "open-api-swap.bingx.com", "/swap-market"
    raw = socket.create_connection((host, 443), timeout=12)
    s = ssl.create_default_context().wrap_socket(raw, server_hostname=host); s.settimeout(12)
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
    assert b" 101 " in resp.split(b"\r\n", 1)[0], "нет WS upgrade (101)"
    data = json.dumps({"id": "c", "reqType": "sub",
                       "dataType": "NCSKNVDA2USD-USDT@lastPrice"}).encode()
    mask = os.urandom(4)
    s.send(bytes([0x81, 0x80 | len(data)]) + mask +
           bytes(b ^ mask[i % 4] for i, b in enumerate(data)))
    assert s.recv(2), "WS без ответа на подписку"
    s.close()

def yahoo():
    j = json.loads(http("https://query1.finance.yahoo.com/v8/finance/chart/NVDA?interval=1d&range=5d"))
    assert j["chart"]["result"][0]["meta"], "нет meta"

def pyth():
    j = json.loads(http("https://hermes.pyth.network/v2/price_feeds?query=NVDA&asset_type=equity"))
    assert isinstance(j, list) and j, "пустой каталог"

print("Доступность источников с этого сервера:")
r_rest = check("BingX REST  (contracts)", bingx_rest)
r_ws   = check("BingX WS    (swap-market)", bingx_ws)
r_yh   = check("Yahoo  (close/регион)", yahoo)
r_py   = check("Pyth   (кросс-чек)", pyth)
print()
if not (r_rest and r_ws):
    print("❌ BingX недоступен с этого IP — цен не будет. Смени локацию/провайдера VPS (EU обычно ок).")
    sys.exit(1)
if not (r_yh and r_py):
    print("⚠️  BingX OK, но Yahoo/Pyth частично недоступны — гэп/кросс-чек будут неполными (спред/премиум работают).")
else:
    print("✅ Все источники доступны — деплой можно продолжать.")
