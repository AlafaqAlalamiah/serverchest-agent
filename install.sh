#!/bin/bash
# ServerChest Agent Installer
# Usage: curl -fsSL https://serverchest.com/install.sh | sudo bash -s -- --key=YOUR_API_KEY
set -eE

# ── Colors ────────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}⚠${NC} $*"; }
err()  { echo -e "${RED}✗ $*${NC}"; exit 1; }
info() { echo -e "${CYAN}→${NC} $*"; }

# ── Args ──────────────────────────────────────────────────────────────────────
API_KEY=""
RELAY_URL="wss://app.serverchest.com/ws/agent"
for arg in "$@"; do
  case "$arg" in
    --key=*)   API_KEY="${arg#--key=}" ;;
    --relay=*) RELAY_URL="${arg#--relay=}" ;;
  esac
done

[[ -z "$API_KEY" ]] && err "Usage: sudo bash install.sh --key=YOUR_API_KEY [--relay=wss://...]"
[[ "$EUID" -ne 0 ]] && err "Please run as root: sudo bash install.sh --key=..."

echo ""
echo -e "${CYAN}╔══════════════════════════════════════╗${NC}"
echo -e "${CYAN}║     ServerChest Agent Installer      ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════╝${NC}"
echo ""

# ── Detect Odoo ───────────────────────────────────────────────────────────────
info "Detecting Odoo installation..."

# odoo-bin
ODOO_BIN=""
for p in /opt/odoo17/odoo17/odoo-bin /opt/odoo16/odoo16/odoo-bin /opt/odoo/odoo-bin \
         /usr/lib/python3/dist-packages/odoo/odoo-bin /usr/share/odoo/odoo-bin; do
  [[ -f "$p" ]] && ODOO_BIN="$p" && break
done

# Python binary (prefer venv next to odoo-bin)
PYTHON_BIN=""
for p in /opt/odoo17/odoo17-venv/bin/python /opt/odoo16/odoo16-venv/bin/python \
         /opt/odoo/venv/bin/python /opt/odoo/odoo-venv/bin/python \
         /usr/bin/python3; do
  [[ -f "$p" ]] && PYTHON_BIN="$p" && break
done
[[ -z "$PYTHON_BIN" ]] && err "Could not find Python 3. Install python3 first."

# odoo conf
ODOO_CONF=""
for p in /etc/odoo17.conf /etc/odoo16.conf /etc/odoo.conf /etc/odoo/odoo.conf; do
  [[ -f "$p" ]] && ODOO_CONF="$p" && break
done

# Odoo user (owner of odoo-bin or conf file)
ODOO_USER="odoo17"
if [[ -n "$ODOO_BIN" ]]; then
  ODOO_USER=$(stat -c '%U' "$ODOO_BIN" 2>/dev/null || echo "odoo17")
elif [[ -n "$ODOO_CONF" ]]; then
  ODOO_USER=$(stat -c '%U' "$ODOO_CONF" 2>/dev/null || echo "odoo17")
fi

# DB name from conf
DB_NAME=""
if [[ -n "$ODOO_CONF" ]]; then
  DB_NAME=$(grep -E '^\s*db_name\s*=' "$ODOO_CONF" 2>/dev/null | head -1 \
            | sed 's/.*=\s*//' | tr -d ' ' | tr -d '\r' || true)
fi

# Service name
SERVICE_NAME="odoo17"
for s in odoo17 odoo16 odoo; do
  systemctl list-units --type=service --state=loaded 2>/dev/null \
    | grep -q "^  ${s}.service" && SERVICE_NAME="$s" && break
done

# Paths
BACKUP_SCRIPT="/opt/odoo17/odoo_backup.sh"
for p in /opt/odoo17/odoo_backup.sh /opt/odoo17/backup_to_onedrive.sh /opt/odoo/backup_to_onedrive.sh; do
  [[ -f "$p" ]] && BACKUP_SCRIPT="$p" && break
done

BACKUP_LOG="/var/log/odoo/backup.log"
ODOO_LOG="/var/log/odoo/odoo17.log"
for p in /var/log/odoo/odoo17.log /var/log/odoo/odoo16.log /var/log/odoo/odoo.log /var/log/odoo.log; do
  [[ -f "$p" ]] && ODOO_LOG="$p" && break
done

RCLONE_CONF="/opt/odoo17/rclone.conf"
for p in /opt/odoo17/rclone.conf /opt/odoo16/rclone.conf /opt/odoo/rclone.conf; do
  [[ -f "$p" ]] && RCLONE_CONF="$p" && break
done

echo ""
echo -e "  ${CYAN}Detected configuration:${NC}"
echo "  ─────────────────────────────"
echo "  Odoo user  : $ODOO_USER"
echo "  Python     : $PYTHON_BIN"
echo "  odoo-bin   : ${ODOO_BIN:-not found}"
echo "  odoo.conf  : ${ODOO_CONF:-not found}"
echo "  DB name    : ${DB_NAME:-not detected}"
echo "  Service    : $SERVICE_NAME"
echo ""

# ── Install websockets ────────────────────────────────────────────────────────
info "Installing Python dependency (websockets)..."
"$PYTHON_BIN" -m pip install --quiet websockets 2>/dev/null \
  || "$PYTHON_BIN" -m pip install --quiet --break-system-packages websockets 2>/dev/null \
  || warn "Could not auto-install websockets. Run manually: $PYTHON_BIN -m pip install websockets"
ok "websockets ready"

# ── Download agent ────────────────────────────────────────────────────────────
AGENT_DIR="/opt/serverchest-agent"
info "Creating agent directory at $AGENT_DIR..."
mkdir -p "$AGENT_DIR"

info "Downloading agent.py from serverchest.com..."
curl -fsSL "https://raw.githubusercontent.com/AlafaqAlalamiah/serverchest-agent/main/agent.py" -o "$AGENT_DIR/agent.py"
chmod +x "$AGENT_DIR/agent.py"
chown -R "$ODOO_USER:$ODOO_USER" "$AGENT_DIR"
ok "Agent downloaded"

# Download backup script if not already present
if [ ! -f "/opt/odoo17/odoo_backup.sh" ]; then
    info "Downloading odoo_backup.sh..."
    curl -fsSL "https://raw.githubusercontent.com/AlafaqAlalamiah/serverchest-agent/main/odoo_backup.sh" -o /opt/odoo17/odoo_backup.sh
    sed -i "s/YOUR_DB_NAME/$DB_NAME/" /opt/odoo17/odoo_backup.sh
    chmod +x /opt/odoo17/odoo_backup.sh
    chown "$ODOO_USER:$ODOO_USER" /opt/odoo17/odoo_backup.sh
    BACKUP_SCRIPT="/opt/odoo17/odoo_backup.sh"
    ok "Backup script installed at /opt/odoo17/odoo_backup.sh"
else
    ok "Backup script already exists at $BACKUP_SCRIPT — skipping"
fi

# ── Write config ──────────────────────────────────────────────────────────────
info "Writing /etc/serverchest-agent.conf..."
cat > /etc/serverchest-agent.conf << EOF
[agent]
relay_url     = $RELAY_URL
api_key       = $API_KEY
backup_script = $BACKUP_SCRIPT
backup_log    = $BACKUP_LOG
odoo_log      = $ODOO_LOG
rclone_config = $RCLONE_CONF
odoo_conf     = ${ODOO_CONF:-/etc/odoo17.conf}
odoo_bin      = $PYTHON_BIN
odoo_src      = ${ODOO_BIN:-/opt/odoo17/odoo17/odoo-bin}
db_name       = $DB_NAME
service_name  = $SERVICE_NAME
EOF
chmod 600 /etc/serverchest-agent.conf
ok "Config written to /etc/serverchest-agent.conf"

# ── Systemd service ───────────────────────────────────────────────────────────
info "Creating systemd service..."
cat > /etc/systemd/system/serverchest-agent.service << EOF
[Unit]
Description=ServerChest Agent
After=network.target
Wants=network-online.target

[Service]
Type=simple
User=$ODOO_USER
Group=$ODOO_USER
ExecStart=$PYTHON_BIN $AGENT_DIR/agent.py
Restart=always
RestartSec=10
StandardOutput=journal
StandardError=journal
SyslogIdentifier=serverchest-agent
Environment=SERVERCHEST_CONFIG=/etc/serverchest-agent.conf

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable serverchest-agent --quiet
systemctl restart serverchest-agent
sleep 2

if systemctl is-active --quiet serverchest-agent; then
  ok "Agent service started successfully"
else
  warn "Service may not have started. Check logs:"
  journalctl -u serverchest-agent -n 15 --no-pager 2>/dev/null || true
fi

# ── Done ──────────────────────────────────────────────────────────────────────
echo ""
echo -e "${GREEN}✓ ServerChest agent installed!${NC}"
echo ""
echo "  Useful commands:"
echo "    systemctl status serverchest-agent"
echo "    journalctl -u serverchest-agent -f"
echo "    cat /etc/serverchest-agent.conf"
echo ""
echo "  Your server should appear as Connected in the dashboard within seconds."
echo ""
