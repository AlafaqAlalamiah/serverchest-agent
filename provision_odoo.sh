#!/bin/bash
# ServerChest Odoo Provisioner
# Installs Odoo (16/17/18) from source on a fresh Ubuntu 22.04/24.04 or Debian 12
# server using the /opt/odooXX layout the ServerChest agent autodetects, then
# installs the ServerChest agent.
#
# Usage:
#   curl -fsSL https://serverchest.com/provision.sh | sudo bash -s -- --key=YOUR_API_KEY [--version=17] [--db=mydb] [--admin-pwd=secret]
set -eE

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'; NC='\033[0m'
ok()   { echo -e "${GREEN}✓${NC} $*"; }
warn() { echo -e "${YELLOW}⚠${NC} $*"; }
err()  { echo -e "${RED}✗ $*${NC}"; exit 1; }
info() { echo -e "${CYAN}→${NC} $*"; }

# ── Args ──────────────────────────────────────────────────────────────────────
API_KEY=""
ODOO_VERSION="17"
ODOO_COMMIT=""
INIT_DB=""
ADMIN_PWD=""
RELAY_URL="wss://app.serverchest.com/ws/agent"
AGENT_INSTALL_URL="https://raw.githubusercontent.com/AlafaqAlalamiah/serverchest-agent/main/install.sh"
for arg in "$@"; do
  case "$arg" in
    --key=*)       API_KEY="${arg#--key=}" ;;
    --version=*)   ODOO_VERSION="${arg#--version=}" ;;
    --commit=*)    ODOO_COMMIT="${arg#--commit=}" ;;
    --db=*)        INIT_DB="${arg#--db=}" ;;
    --admin-pwd=*) ADMIN_PWD="${arg#--admin-pwd=}" ;;
    --relay=*)     RELAY_URL="${arg#--relay=}" ;;
  esac
done

[[ -z "$API_KEY" ]] && err "Usage: sudo bash provision_odoo.sh --key=YOUR_API_KEY [--version=17] [--db=mydb]"
[[ "$EUID" -ne 0 ]] && err "Please run as root (sudo)."
case "$ODOO_VERSION" in 16|17|18) ;; *) err "--version must be 16, 17 or 18" ;; esac
if [[ -n "$INIT_DB" && ! "$INIT_DB" =~ ^[A-Za-z0-9_-]+$ ]]; then err "--db may only contain letters, digits, _ and -"; fi

ODOO_USER="odoo${ODOO_VERSION}"
ODOO_HOME="/opt/odoo${ODOO_VERSION}"
ODOO_SRC="${ODOO_HOME}/odoo${ODOO_VERSION}"
ODOO_VENV="${ODOO_HOME}/odoo${ODOO_VERSION}-venv"
ODOO_CONF="/etc/odoo${ODOO_VERSION}.conf"
ODOO_SVC="odoo${ODOO_VERSION}"
ODOO_LOG_DIR="/var/log/odoo${ODOO_VERSION}"
[[ -z "$ADMIN_PWD" ]] && ADMIN_PWD=$(tr -dc 'A-Za-z0-9' </dev/urandom | head -c 20)

echo ""
echo -e "${CYAN}╔══════════════════════════════════════╗${NC}"
echo -e "${CYAN}║    ServerChest Odoo Provisioner      ║${NC}"
echo -e "${CYAN}╚══════════════════════════════════════╝${NC}"
echo ""
info "Odoo ${ODOO_VERSION}.0 → ${ODOO_SRC}"

[[ -d "$ODOO_SRC" ]] && err "$ODOO_SRC already exists — this server appears to have Odoo ${ODOO_VERSION} installed."
command -v apt-get >/dev/null || err "This installer supports Debian/Ubuntu (apt) only."

# ── System packages ──────────────────────────────────────────────────────────
info "Installing system packages (this can take a few minutes)..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git curl python3 python3-venv python3-dev python3-pip \
  build-essential libpq-dev libxml2-dev libxslt1-dev libldap2-dev libsasl2-dev \
  libjpeg-dev zlib1g-dev libffi-dev libssl-dev pkg-config \
  postgresql postgresql-client node-less npm >/dev/null
apt-get install -y -qq wkhtmltopdf >/dev/null 2>&1 || warn "wkhtmltopdf not installed — PDF reports may need it later"
npm install -g rtlcss >/dev/null 2>&1 || warn "rtlcss not installed — RTL (Arabic) assets need it"
ok "System packages installed"

# ── PostgreSQL ────────────────────────────────────────────────────────────────
systemctl enable --now postgresql >/dev/null 2>&1 || true
if ! sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${ODOO_USER}'" | grep -q 1; then
  sudo -u postgres createuser --createdb "$ODOO_USER"
  ok "PostgreSQL role ${ODOO_USER} created"
else
  ok "PostgreSQL role ${ODOO_USER} already exists"
fi

# ── System user + source ──────────────────────────────────────────────────────
id "$ODOO_USER" >/dev/null 2>&1 || useradd --system --create-home --home-dir "$ODOO_HOME" --shell /bin/bash "$ODOO_USER"
mkdir -p "$ODOO_HOME"

info "Cloning Odoo ${ODOO_VERSION}.0 source (shallow)..."
git clone --depth 1 --branch "${ODOO_VERSION}.0" https://github.com/odoo/odoo "$ODOO_SRC" 2>/dev/null
if [[ -n "$ODOO_COMMIT" ]]; then
  # Pin to the source server's exact commit so standby code matches, not just the branch
  if git -C "$ODOO_SRC" fetch --depth 1 origin "$ODOO_COMMIT" 2>/dev/null \
     && git -C "$ODOO_SRC" checkout -q "$ODOO_COMMIT" 2>/dev/null; then
    ok "Pinned to commit ${ODOO_COMMIT:0:10}"
  else
    warn "Could not pin commit ${ODOO_COMMIT:0:10} — using branch tip (minor drift possible)"
  fi
fi
ok "Source cloned"

info "Creating virtualenv and installing Python requirements (the long part)..."
python3 -m venv "$ODOO_VENV"
"$ODOO_VENV/bin/pip" install --quiet --upgrade pip wheel setuptools
"$ODOO_VENV/bin/pip" install --quiet -r "$ODOO_SRC/requirements.txt"
ok "Python requirements installed"

# Custom addons dir — first addons_path entry is core, the rest are custom
# (the ServerChest agent and module upload rely on this convention).
mkdir -p "$ODOO_SRC/addons/custom"

# ── Config ────────────────────────────────────────────────────────────────────
mkdir -p "$ODOO_LOG_DIR"
cat > "$ODOO_CONF" <<CONF
[options]
admin_passwd = ${ADMIN_PWD}
db_host = False
db_port = False
db_user = ${ODOO_USER}
db_password = False
addons_path = ${ODOO_SRC}/addons,${ODOO_SRC}/addons/custom
logfile = ${ODOO_LOG_DIR}/odoo.log
log_level = info
proxy_mode = True
CONF
chown -R "$ODOO_USER:$ODOO_USER" "$ODOO_HOME" "$ODOO_LOG_DIR"
chown "$ODOO_USER:$ODOO_USER" "$ODOO_CONF"
chmod 640 "$ODOO_CONF"
ok "Config written to ${ODOO_CONF}"

# ── systemd service ───────────────────────────────────────────────────────────
cat > "/etc/systemd/system/${ODOO_SVC}.service" <<UNIT
[Unit]
Description=Odoo ${ODOO_VERSION}
After=network.target postgresql.service

[Service]
Type=simple
User=${ODOO_USER}
Group=${ODOO_USER}
ExecStart=${ODOO_VENV}/bin/python ${ODOO_SRC}/odoo-bin -c ${ODOO_CONF}
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
UNIT
systemctl daemon-reload

# ── Optional initial database ─────────────────────────────────────────────────
if [[ -n "$INIT_DB" ]]; then
  info "Initializing database ${INIT_DB} (base module)..."
  sudo -u "$ODOO_USER" "$ODOO_VENV/bin/python" "$ODOO_SRC/odoo-bin" -c "$ODOO_CONF" \
    -d "$INIT_DB" -i base --stop-after-init --no-http >/dev/null 2>&1 \
    || warn "Database init reported issues — check ${ODOO_LOG_DIR}/odoo.log"
  ok "Database ${INIT_DB} initialized"
fi

systemctl enable --now "$ODOO_SVC" >/dev/null 2>&1
sleep 5
if systemctl is-active --quiet "$ODOO_SVC"; then
  ok "Odoo service ${ODOO_SVC} is running (port 8069)"
else
  warn "Odoo service is not active yet — check: journalctl -u ${ODOO_SVC} -n 50"
fi

# ── ServerChest agent ─────────────────────────────────────────────────────────
info "Installing ServerChest agent..."
curl -fsSL "$AGENT_INSTALL_URL" | bash -s -- --key="$API_KEY" --relay="$RELAY_URL"

echo ""
echo -e "${GREEN}╔══════════════════════════════════════╗${NC}"
echo -e "${GREEN}║        Provisioning complete         ║${NC}"
echo -e "${GREEN}╚══════════════════════════════════════╝${NC}"
echo ""
echo "  Odoo ${ODOO_VERSION}:      http://$(hostname -I | awk '{print $1}'):8069"
echo "  Master password (admin_passwd): ${ADMIN_PWD}"
[[ -n "$INIT_DB" ]] && echo "  Database:     ${INIT_DB} (login: admin / admin — change it immediately)"
echo "  Config:       ${ODOO_CONF}"
echo "  Service:      systemctl status ${ODOO_SVC}"
echo ""
echo "  Save the master password somewhere safe — it is not stored by ServerChest."
echo ""
