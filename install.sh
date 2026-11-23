#!/usr/bin/env bash
# Installation Wärmemonitor (Ubuntu/Debian). Aufruf: sudo ./install.sh
set -euo pipefail
DEST=/opt/waermemonitor
STATE=/var/lib/waermemonitor
SVC_USER=${SVC_USER:-waerme}

apt-get install -y python3-venv >/dev/null
id "$SVC_USER" >/dev/null 2>&1 || useradd --system --no-create-home --shell /usr/sbin/nologin "$SVC_USER"
mkdir -p "$DEST" "$STATE"
cp common.py waerme_calc.py analyse.py requirements.txt "$DEST/"
if [ ! -f "$DEST/config.yaml" ]; then
  cp config.yaml "$DEST/"
else
  echo "config.yaml existiert bereits – nicht überschrieben (Vorlage: $DEST/config.yaml.neu)"
  cp config.yaml "$DEST/config.yaml.neu"
fi
[ -d "$DEST/venv" ] || python3 -m venv "$DEST/venv"
"$DEST/venv/bin/pip" install -q --upgrade pip
"$DEST/venv/bin/pip" install -q -r "$DEST/requirements.txt"
chown -R "$SVC_USER": "$STATE"
sed -i "s/^User=.*/User=$SVC_USER/" systemd/*.service
cp systemd/* /etc/systemd/system/
systemctl daemon-reload
echo
echo "Installiert. Nächste Schritte:"
echo "  1. $DEST/config.yaml anpassen"
echo "  2. systemctl enable --now waerme-calc waerme-analyse.timer"
echo "  3. journalctl -fu waerme-calc   (zeigt fehlende Eingänge)"
