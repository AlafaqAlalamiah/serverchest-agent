#!/bin/bash
# ServerChest Odoo Provisioner
# Installs Odoo (16/17/18) from source on a fresh Ubuntu 22.04/24.04 or Debian 12
# server using the /opt/odooXX layout the ServerChest agent autodetects, then
# installs the ServerChest agent.
#
# Usage:
#   curl -fsSL https://serverchest.com/provision.sh | sudo bash -s -- --key=YOUR_API_KEY [--version=17] [--db=mydb] [--admin-pwd=secret]
set -eE -o pipefail

# ── Styling ───────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; CYAN='\033[0;36m'
BLUE='\033[0;34m'; BOLD='\033[1m'; DIM='\033[2m'; NC='\033[0m'

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

_die() { printf "\n  ${RED}${BOLD}✗ %s${NC}\n\n" "$1"; exit 1; }
[[ -z "$API_KEY" ]] && _die "Missing --key. Usage: sudo bash provision.sh --key=YOUR_API_KEY [--version=17]"
[[ "$EUID" -ne 0 ]] && _die "Please run as root (sudo)."
case "$ODOO_VERSION" in 16|17|18) ;; *) _die "--version must be 16, 17 or 18" ;; esac
if [[ -n "$INIT_DB" && ! "$INIT_DB" =~ ^[A-Za-z0-9_-]+$ ]]; then _die "--db may only contain letters, digits, _ and -"; fi

ODOO_USER="odoo${ODOO_VERSION}"
ODOO_HOME="/opt/odoo${ODOO_VERSION}"
ODOO_SRC="${ODOO_HOME}/odoo${ODOO_VERSION}"
ODOO_VENV="${ODOO_HOME}/odoo${ODOO_VERSION}-venv"
ODOO_CONF="/etc/odoo${ODOO_VERSION}.conf"
ODOO_SVC="odoo${ODOO_VERSION}"
ODOO_LOG_DIR="/var/log/odoo${ODOO_VERSION}"
if [[ -z "$ADMIN_PWD" ]]; then
  # Read a bounded chunk first (head at the front never SIGPIPEs), then slice —
  # a trailing "| head -c N" would SIGPIPE tr and, under pipefail+set -e, abort.
  _rand=$(head -c 60 /dev/urandom | base64 | tr -dc 'A-Za-z0-9')
  ADMIN_PWD=${_rand:0:20}
fi
export DEBIAN_FRONTEND=noninteractive
LOG_FILE="/var/log/serverchest-provision.log"
: > "$LOG_FILE"

# ── Progress framework ────────────────────────────────────────────────────────
TOTAL=9; STEP=0; PHASE=""; T0=$SECONDS
PYBIN="python3"; PYVER="?"

_bar() {
  local pct=$(( STEP * 100 / TOTAL )) w=32 i filled b=""
  filled=$(( pct * w / 100 ))
  for ((i=0; i<w; i++)); do [[ $i -lt $filled ]] && b+="━" || b+="─"; done
  printf "  ${BLUE}%s${NC} ${BOLD}%d%%${NC}\n" "$b" "$pct"
}
phase() { STEP=$((STEP+1)); PHASE="$1"; printf "\n${CYAN}${BOLD}[%d/%d]${NC} ${BOLD}%s${NC}\n" "$STEP" "$TOTAL" "$1"; _bar; }
ok()    { printf "      ${GREEN}✓${NC} %s\n" "$1"; }
note()  { printf "      ${DIM}%s${NC}\n" "$1"; }
warn()  { printf "      ${YELLOW}⚠${NC} %s\n" "$1"; }

# run "message" cmd args… — live spinner + elapsed, output logged; fails → ERR trap
run() {
  local msg="$1"; shift
  local start=$SECONDS
  "$@" >>"$LOG_FILE" 2>&1 &
  local pid=$! sp='⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏' i=0
  while kill -0 "$pid" 2>/dev/null; do
    printf "\r      ${YELLOW}%s${NC} %s ${DIM}(%ds)${NC}   " "${sp:$((i % 10)):1}" "$msg" "$((SECONDS-start))"
    i=$((i+1)); sleep 0.15
  done
  if wait "$pid"; then
    printf "\r      ${GREEN}✓${NC} %s ${DIM}(%ds)${NC}%*s\n" "$msg" "$((SECONDS-start))" 18 ""
  else
    printf "\r      ${RED}✗${NC} %s ${DIM}(%ds)${NC}\n" "$msg" "$((SECONDS-start))"
    return 1
  fi
}

on_error() {
  local rc=$?
  printf "\n${RED}${BOLD}  ✗ Provisioning failed${NC}  ${DIM}(after %ds)${NC}\n" "$((SECONDS-T0))"
  printf "    at step ${BOLD}[%d/%d]  %s${NC}\n\n" "$STEP" "$TOTAL" "$PHASE"
  printf "  ${DIM}── last output ──────────────────────────────${NC}\n"
  tail -n 20 "$LOG_FILE" 2>/dev/null | sed 's/^/    /'
  printf "  ${DIM}─────────────────────────────────────────────${NC}\n"
  printf "  Full log:  ${BOLD}%s${NC}\n" "$LOG_FILE"
  printf "  Fix the issue above, then re-run the same command\n"
  printf "  ${DIM}(it auto-cleans this partial install and retries).${NC}\n\n"
  exit "$rc"
}
trap on_error ERR

# ── Compound step helpers (run in the logged subshell) ────────────────────────
_clone_odoo() {
  git clone --depth 1 --branch "${ODOO_VERSION}.0" https://github.com/odoo/odoo "$ODOO_SRC"
  if [[ -n "$ODOO_COMMIT" ]]; then
    if git -C "$ODOO_SRC" fetch --depth 1 origin "$ODOO_COMMIT" && git -C "$ODOO_SRC" checkout -q "$ODOO_COMMIT"; then
      echo "pinned to commit $ODOO_COMMIT"
    else
      echo "WARN: could not pin commit $ODOO_COMMIT — using branch tip"
    fi
  fi
}
_install_py311() {
  apt-get install -y software-properties-common
  add-apt-repository -y ppa:deadsnakes/ppa
  apt-get update
  apt-get install -y python3.11 python3.11-venv python3.11-dev
}
_pg_setup() {
  systemctl enable --now postgresql
  local exists
  exists=$(sudo -u postgres psql -tAc "SELECT 1 FROM pg_roles WHERE rolname='${ODOO_USER}'")
  if [[ "$exists" != "1" ]]; then
    sudo -u postgres createuser --createdb "$ODOO_USER"
  fi
}
_venv_deps() {
  "$PYBIN" -m venv "$ODOO_VENV"
  # Pin setuptools: Odoo's own code does `import pkg_resources` directly, which
  # newer setuptools releases dropped entirely (moved to importlib.metadata).
  # An unpinned install grabs the latest setuptools and Odoo crashes on its very
  # first import — before logging even initializes, so it looks like a silent hang.
  "$ODOO_VENV/bin/pip" install --upgrade pip wheel "setuptools<81"
  "$ODOO_VENV/bin/pip" install -r "$ODOO_SRC/requirements.txt"
}
_agent_install() {
  curl -fsSL "$AGENT_INSTALL_URL" | bash -s -- --key="$API_KEY" --relay="$RELAY_URL"
}

# ── Banner ────────────────────────────────────────────────────────────────────
printf "\n"
printf "${CYAN}  ┌──────────────────────────────────────────────┐${NC}\n"
printf "${CYAN}  │${NC}    ${BOLD}ServerChest — Odoo Provisioner${NC}              ${CYAN}│${NC}\n"
printf "${CYAN}  └──────────────────────────────────────────────┘${NC}\n"
printf "  Target:  ${BOLD}Odoo %s.0${NC}  →  ${DIM}%s${NC}\n" "$ODOO_VERSION" "$ODOO_SRC"
printf "  Log:     ${DIM}%s${NC}\n" "$LOG_FILE"

# ── [1/9] Preparing ───────────────────────────────────────────────────────────
phase "Preparing the server"
command -v apt-get >/dev/null || _die "This installer supports Debian/Ubuntu (apt) only."
if [[ -d "$ODOO_SRC" ]]; then
  if systemctl is-active --quiet "$ODOO_SVC" 2>/dev/null; then
    _die "$ODOO_SRC already exists and Odoo ${ODOO_VERSION} is running — this server is already provisioned."
  fi
  warn "Found an incomplete previous install — cleaning it up to retry"
  systemctl disable --now "$ODOO_SVC" >/dev/null 2>&1 || true
  rm -rf "$ODOO_HOME"
fi
ok "Ready ($(. /etc/os-release; echo "$PRETTY_NAME"))"

# ── [2/9] System packages ─────────────────────────────────────────────────────
phase "Installing system packages"
run "Refreshing package lists" apt-get update
run "Build tools, PostgreSQL, Node & libraries" apt-get install -y \
  git curl build-essential pkg-config \
  python3 python3-venv python3-dev python3-pip \
  libpq-dev libxml2-dev libxslt1-dev libldap2-dev libsasl2-dev \
  libjpeg-dev zlib1g-dev libffi-dev libssl-dev \
  postgresql postgresql-client node-less npm
apt-get install -y wkhtmltopdf >>"$LOG_FILE" 2>&1 || warn "wkhtmltopdf not installed (PDF reports may need it)"
npm install -g rtlcss >>"$LOG_FILE" 2>&1 || warn "rtlcss not installed (RTL/Arabic assets)"
ok "System packages installed"

# ── [3/9] Python runtime ──────────────────────────────────────────────────────
# Odoo 17 pins gevent==22.10.2 etc., which only ship wheels for Python 3.10/3.11.
# On a newer default Python (Ubuntu 24.04=3.12, 26.04=3.14) pip would build from
# source and fail — so install python3.11 explicitly when needed.
phase "Selecting a compatible Python"
SYS_MINOR=$(python3 -c 'import sys; print(sys.version_info[1])')
if [[ "$SYS_MINOR" -ge 12 ]]; then
  OS_ID=$(. /etc/os-release; echo "$ID")
  note "System Python is 3.${SYS_MINOR}; Odoo ${ODOO_VERSION} needs 3.10/3.11"
  if [[ "$OS_ID" == "ubuntu" ]]; then
    run "Installing Python 3.11 (deadsnakes)" _install_py311
  fi
  command -v python3.11 >/dev/null && PYBIN=python3.11 \
    || _die "Could not obtain Python 3.11 on ${OS_ID}. Use Ubuntu 22.04/24.04 or Debian 12."
fi
PYVER=$($PYBIN -c 'import sys; print("%d.%d" % sys.version_info[:2])')
ok "Using Python ${PYVER}"

# ── [4/9] PostgreSQL ──────────────────────────────────────────────────────────
phase "Configuring PostgreSQL"
run "Starting PostgreSQL & creating the '${ODOO_USER}' role" _pg_setup
ok "Database engine ready"

# ── [5/9] Odoo source ─────────────────────────────────────────────────────────
phase "Fetching Odoo source"
id "$ODOO_USER" >/dev/null 2>&1 || useradd --system --create-home --home-dir "$ODOO_HOME" --shell /bin/bash "$ODOO_USER"
# So the agent (running as this user) can actually read `journalctl -u <service>`
# for diagnostics — without this, journalctl silently returns nothing (no error).
usermod -aG systemd-journal "$ODOO_USER" 2>/dev/null || true
mkdir -p "$ODOO_HOME"
run "Cloning Odoo ${ODOO_VERSION}.0${ODOO_COMMIT:+ @ ${ODOO_COMMIT:0:10}}" _clone_odoo
mkdir -p "$ODOO_SRC/addons/custom"   # first path = core, rest = custom (agent convention)
ok "Source ready"

# ── [6/9] Python dependencies ─────────────────────────────────────────────────
phase "Installing Python dependencies"
note "~150 packages — the longest step, please wait"
run "Building virtualenv & installing requirements" _venv_deps
ok "Dependencies installed"

# ── [7/9] Configuration & service ─────────────────────────────────────────────
phase "Configuring Odoo service"
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
chown "$ODOO_USER:$ODOO_USER" "$ODOO_CONF"; chmod 640 "$ODOO_CONF"
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
ok "Config written to ${ODOO_CONF}"
if [[ -n "$INIT_DB" ]]; then
  run "Initializing database '${INIT_DB}'" sudo -u "$ODOO_USER" "$ODOO_VENV/bin/python" "$ODOO_SRC/odoo-bin" \
    -c "$ODOO_CONF" -d "$INIT_DB" -i base --stop-after-init --no-http
fi
run "Starting Odoo" bash -c "systemctl enable --now '$ODOO_SVC'; sleep 5"
if systemctl is-active --quiet "$ODOO_SVC"; then ok "Odoo is running (port 8069)"; else warn "Odoo not active yet — journalctl -u ${ODOO_SVC} -n 50"; fi

# ── [8/9] Reverse proxy ───────────────────────────────────────────────────────
# nginx + Odoo vhost bound to THIS host's identity (server_name _) so the box
# serves on :80 immediately. DNS + TLS for a production domain are the operator's
# failover step — deliberately not configured here.
phase "Setting up the reverse proxy"
if run "Installing nginx" apt-get install -y nginx && command -v nginx >/dev/null; then
  CHAT_PORT=$((8069 + 3))
  cat > /etc/nginx/sites-available/serverchest-odoo <<NGINX
# Managed by ServerChest — Odoo reverse proxy (standby-ready).
upstream sc_odoo { server 127.0.0.1:8069; }
upstream sc_odoo_chat { server 127.0.0.1:${CHAT_PORT}; }
server {
    listen 80 default_server;
    server_name _;
    client_max_body_size 200m;
    proxy_read_timeout 720s;
    proxy_connect_timeout 720s;
    proxy_send_timeout 720s;
    location / {
        proxy_pass http://sc_odoo;
        proxy_set_header Host \$host;
        proxy_set_header X-Real-IP \$remote_addr;
        proxy_set_header X-Forwarded-For \$proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto \$scheme;
        proxy_redirect off;
    }
    location /longpolling { proxy_pass http://sc_odoo_chat; }
    location /websocket {
        proxy_pass http://sc_odoo_chat;
        proxy_set_header Upgrade \$http_upgrade;
        proxy_set_header Connection "upgrade";
        proxy_set_header Host \$host;
    }
}
NGINX
  ln -sf /etc/nginx/sites-available/serverchest-odoo /etc/nginx/sites-enabled/serverchest-odoo
  rm -f /etc/nginx/sites-enabled/default
  if nginx -t >>"$LOG_FILE" 2>&1; then
    systemctl enable --now nginx >>"$LOG_FILE" 2>&1; systemctl reload nginx >>"$LOG_FILE" 2>&1
    ok "nginx serving Odoo on :80"
  else
    warn "nginx config test failed — Odoo still reachable on :8069"
  fi
else
  warn "nginx not installed — Odoo reachable on :8069"
fi

# ── [9/9] ServerChest agent ───────────────────────────────────────────────────
phase "Installing the ServerChest agent"
run "Installing agent & connecting to ServerChest" _agent_install
ok "Agent installed"

# ── Done ──────────────────────────────────────────────────────────────────────
STEP=$TOTAL
printf "\n${GREEN}  ┌──────────────────────────────────────────────┐${NC}\n"
printf "${GREEN}  │${NC}    ${BOLD}✓ Provisioning complete${NC}                     ${GREEN}│${NC}\n"
printf "${GREEN}  └──────────────────────────────────────────────┘${NC}\n"
IP=$(hostname -I | awk '{print $1}')
printf "  ${BOLD}Odoo %s${NC}         http://%s/  ${DIM}(and :8069)${NC}\n" "$ODOO_VERSION" "$IP"
printf "  ${BOLD}Master password${NC}   %s\n" "$ADMIN_PWD"
[[ -n "$INIT_DB" ]] && printf "  ${BOLD}Database${NC}          %s  ${DIM}(login admin / admin — change it)${NC}\n" "$INIT_DB"
printf "  ${BOLD}Config${NC}            %s\n" "$ODOO_CONF"
printf "  ${BOLD}Service${NC}           systemctl status %s\n" "$ODOO_SVC"
printf "  ${DIM}Total time: %dm %ds${NC}\n" "$(((SECONDS-T0)/60))" "$(((SECONDS-T0)%60))"
printf "\n  ${YELLOW}Save the master password — ServerChest does not store it.${NC}\n\n"
