#!/usr/bin/env bash
# Провижн BingX-сканера на ЧИСТОЙ Ubuntu LTS. Запускать от root (или через sudo).
# Идемпотентен: можно гонять повторно.
#
# Использование:
#   REPO_URL=https://github.com/USER/bingx-scanner.git ./bootstrap.sh tailscale
#   REPO_URL=https://github.com/USER/bingx-scanner.git ./bootstrap.sh caddy scanner.example.com partner
#   REPO_URL=https://github.com/USER/bingx-scanner.git ./bootstrap.sh local
set -euo pipefail

REPO_URL="${REPO_URL:?Задай REPO_URL=https://github.com/USER/bingx-scanner.git}"
APP_DIR=/opt/bingx-scanner
SVC_USER=gapscan
MODE="${1:-local}"

echo "[1/6] Пакеты…"
apt-get update -y
apt-get install -y python3 tzdata git ufw curl ca-certificates

echo "[2/6] Системный пользователь + код…"
id -u "$SVC_USER" >/dev/null 2>&1 || useradd --system --create-home --shell /usr/sbin/nologin "$SVC_USER"
if [ -d "$APP_DIR/.git" ]; then
  sudo -u "$SVC_USER" git config --global --add safe.directory "$APP_DIR" || true
  git -C "$APP_DIR" pull --ff-only
else
  git clone "$REPO_URL" "$APP_DIR"
fi
chown -R "$SVC_USER:$SVC_USER" "$APP_DIR"

echo "[3/6] Проверка связи с биржей/фидами с этого IP…"
sudo -u "$SVC_USER" python3 "$APP_DIR/deploy/check_connectivity.py" || \
  echo "!! см. вывод выше — возможно, нужна другая локация VPS"

echo "[4/6] systemd-сервис (автозапуск + авто-рестарт)…"
install -m 644 "$APP_DIR/deploy/bingx-scanner.service" /etc/systemd/system/bingx-scanner.service
systemctl daemon-reload
systemctl enable --now bingx-scanner

echo "[5/6] firewall (ufw): по умолчанию всё закрыто, SSH открыт…"
ufw --force reset >/dev/null
ufw default deny incoming
ufw default allow outgoing
ufw allow OpenSSH

case "$MODE" in
  tailscale)
    echo "  → режим Tailscale (наружу НИЧЕГО не открываем; доступ только из tailnet)"
    command -v tailscale >/dev/null 2>&1 || curl -fsSL https://tailscale.com/install.sh | sh
    sed -i 's/GAP_HOST=127.0.0.1/GAP_HOST=0.0.0.0/' /etc/systemd/system/bingx-scanner.service
    systemctl daemon-reload && systemctl restart bingx-scanner
    ufw allow in on tailscale0
    ufw --force enable
    tailscale up
    echo "  Адрес для партнёра: http://<tailscale-ip этого сервера>:8787"
    echo "  (узнать ip:  tailscale ip -4 ; партнёра добавить через 'share node' в админке Tailscale)"
    ;;
  caddy)
    DOMAIN="${2:?нужен домен: ./bootstrap.sh caddy <домен> <логин>}"
    LOGIN="${3:?нужен логин партнёра}"
    echo "  → режим Caddy: публичный https://$DOMAIN, Basic Auth, наружу только 80/443"
    apt-get install -y debian-keyring debian-archive-keyring apt-transport-https
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg
    curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' > /etc/apt/sources.list.d/caddy-stable.list
    apt-get update -y && apt-get install -y caddy
    echo "  Введи пароль для партнёра (он его и получит):"
    HASH="$(caddy hash-password)"
    cat > /etc/caddy/Caddyfile <<EOF
$DOMAIN {
    encode zstd gzip
    basic_auth {
        $LOGIN $HASH
    }
    reverse_proxy 127.0.0.1:8787
}
EOF
    systemctl restart caddy
    ufw allow 80/tcp
    ufw allow 443/tcp
    ufw --force enable
    echo "  Партнёру: https://$DOMAIN  (логин: $LOGIN, пароль — что ты ввёл)"
    ;;
  local)
    ufw --force enable
    echo "  → локальный режим: слушает 127.0.0.1, наружу закрыто. Для доступа перезапусти с tailscale|caddy."
    ;;
  *)
    echo "Неизвестный режим '$MODE'. Используй: tailscale | caddy <домен> <логин> | local"; exit 2;;
esac

echo "[6/6] Готово. Статус сервиса:"
systemctl --no-pager --full status bingx-scanner | head -n 8
