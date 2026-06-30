#!/usr/bin/env bash
# Выкат новой версии НА СЕРВЕРЕ в одну команду: подтянуть git + перезапустить сервис.
#   bash /opt/bingx-scanner/deploy.sh
# (git pull делаем от владельца кода gapscan, чтобы не ломать права)
set -euo pipefail
APP_DIR=/opt/bingx-scanner
SVC_USER=gapscan
# Шаг 1: подтянуть код и ПЕРЕЗАПУСТИТЬСЯ свежей версией скрипта. Без re-exec bash доисполняет
# СТАРУЮ версию deploy.sh (загруженную в память до pull) и новые шаги не применятся.
if [ "${DEPLOY_REEXEC:-}" != "1" ]; then
  sudo -u "$SVC_USER" git -C "$APP_DIR" pull --ff-only
  DEPLOY_REEXEC=1 exec bash "$APP_DIR/deploy.sh"
fi
# Шаг 2 (уже в свежей версии): папка под sqlite-лог сигналов (юнит разрешает запись только сюда)
sudo install -d -o "$SVC_USER" -g "$SVC_USER" "$APP_DIR/data"
# пересобрать systemd-юнит из репо (мог измениться) и перечитать
sudo install -m644 "$APP_DIR/deploy/bingx-scanner.service" /etc/systemd/system/bingx-scanner.service
sudo systemctl daemon-reload
sudo systemctl restart bingx-scanner
echo "✅ Обновлено. Статус:"
systemctl --no-pager status bingx-scanner | head -n 6
