#!/usr/bin/env bash
# Выкат новой версии НА СЕРВЕРЕ в одну команду: подтянуть git + перезапустить сервис.
#   bash /opt/bingx-scanner/deploy.sh
# (git pull делаем от владельца кода gapscan, чтобы не ломать права)
set -euo pipefail
APP_DIR=/opt/bingx-scanner
SVC_USER=gapscan
sudo -u "$SVC_USER" git -C "$APP_DIR" pull --ff-only
sudo systemctl restart bingx-scanner
echo "✅ Обновлено. Статус:"
systemctl --no-pager status bingx-scanner | head -n 6
