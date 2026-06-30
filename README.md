# BingX TradFi scanner — деплой на VPS

Always-on real-time сканер (перп BingX vs реальная база), читает только публичные API.
**Торговых ключей на сервере нет и быть не должно** — сканер ничего не торгует, только смотрит.

```
bingx_gap_scanner.py        # сам сканер (stdlib-only, без pip)
deploy.sh                   # обновление на сервере в 1 команду
deploy/bootstrap.sh         # развёртывание на чистой Ubuntu
deploy/bingx-scanner.service# systemd-юнит (автозапуск + авто-рестарт)
deploy/check_connectivity.py# проверка доступа к BingX/Yahoo/Pyth с IP сервера
deploy/Caddyfile.example    # пример reverse-proxy для публичного режима
```

---

## 0. Что нужно один раз
- VPS: **Ubuntu 22.04/24.04 LTS, EU**, 1 vCPU / 1 GB — хватит за глаза.
- Git-репозиторий (приватный GitHub) — чтобы правка «в одном месте» катилась на сервер одной командой.

## 1. Залить код в git (с твоей машины, разово)
Репозиторий уже инициализирован локально в этой папке (первый коммит сделан). Создай приватный
репозиторий на GitHub (через сайт или `gh repo create bingx-scanner --private`) и запушь:
```bash
git remote add origin https://github.com/<ТЫ>/bingx-scanner.git
git push -u origin main
```

## 2. Развернуть на VPS (одна вставка, от root)
Зайди на сервер по SSH и выполни (подставь свой REPO_URL):
```bash
ssh root@<IP-сервера>
apt-get update -y && apt-get install -y git
git clone https://github.com/<ТЫ>/bingx-scanner.git /opt/bingx-scanner
cd /opt/bingx-scanner
chmod +x deploy.sh deploy/bootstrap.sh
# выбери ОДИН режим доступа (см. §4):
REPO_URL=https://github.com/<ТЫ>/bingx-scanner.git ./deploy/bootstrap.sh tailscale
#   ИЛИ
REPO_URL=https://github.com/<ТЫ>/bingx-scanner.git ./deploy/bootstrap.sh caddy scanner.твойдомен.com partner
```
bootstrap сам: поставит python3, заведёт пользователя `gapscan`, проверит связь с биржей,
поставит systemd-сервис (автозапуск + рестарт при падении), настроит ufw и выбранный доступ.

## 3. Обновление «правлю → у всех обновилось» (одна команда)
Один сервер обслуживает всех зрителей, поэтому «выкатить всем» = обновить сервер.
- На своей машине: `git commit -am "правка" && git push`
- Выкатить на сервере (любой из вариантов):
```bash
# с сервера:
bash /opt/bingx-scanner/deploy.sh
# или прямо с ноутбука одной строкой:
ssh root@<IP-сервера> 'bash /opt/bingx-scanner/deploy.sh'
```
`deploy.sh` = `git pull --ff-only` + `systemctl restart bingx-scanner`.

## 4. Доступ для партнёра — выбери ОДИН
**Рекомендую Tailscale** — наружу не открыто вообще ничего (максимум безопасности), и тебе
проще всего: ни домена, ни сертификатов, ни паролей.
- `./deploy/bootstrap.sh tailscale` → `tailscale up` (залогинься).
- Партнёру: поставить Tailscale, а ты в админке Tailscale жмёшь **Share** на этой ноде (выдаёт
  только её, не всю сеть). Партнёр открывает `http://<tailscale-ip>:8787` (узнать: `tailscale ip -4`).

**Caddy (публичный URL)** — если партнёр не хочет ничего ставить, только открыть ссылку.
- Нужен домен с A-записью на IP сервера (или бесплатный, напр. DuckDNS).
- `./deploy/bootstrap.sh caddy scanner.твойдомен.com partner` — спросит пароль, выдаст HTTPS + Basic Auth.
- Наружу открыты только 80/443, всё остальное режет ufw. **Логин/пароль передаёшь партнёру лично, в коде их нет.**

## 5. Связь с биржей с IP сергера
`bootstrap.sh` гоняет `check_connectivity.py` и печатает PASS/FAIL по BingX REST, BingX WS, Yahoo, Pyth.
- Если **BingX FAIL** — этот IP режется, сканер цен не получит → смени локацию/провайдера (EU обычно ок).
- Можно проверить вручную в любой момент: `sudo -u gapscan python3 /opt/bingx-scanner/deploy/check_connectivity.py`

## 6. Безопасность
- На сервере **только чтение** публичных эндпоинтов. Никаких BingX API-ключей. Единственный
  опциональный ключ — `FINNHUB_API_KEY` (read-only котировки), и тот не обязателен; задаётся
  через env в юните, не в коде.
- Сервис бежит под непривилегированным `gapscan`, с systemd-hardening, ничего не пишет на диск.
- ufw: входящие закрыты, открыт только SSH + (Tailscale-интерфейс ИЛИ 80/443 для Caddy).

## Диагностика
```bash
systemctl status bingx-scanner          # состояние
journalctl -u bingx-scanner -f          # живые логи
journalctl -u bingx-scanner -n 50       # последние строки
```
