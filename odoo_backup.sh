#!/bin/bash
set -eE
export TZ='Asia/Riyadh'   # force consistent timestamps regardless of caller

# ─── Config ───────────────────────────────────────────────────────────────────
SCRIPT_VERSION="20260528"
DB_NAME="YOUR_DB_NAME"
BACKUP_DIR="/opt/odoo17/backup/tmp"
FILESTORE="/opt/odoo17/.local/share/Odoo/filestore/$DB_NAME"
# Legacy single-destination (used if backup_destinations.json doesn't exist)
BACKUP_DB_REMOTE="onedrive:Odoo-Backups/database"
BACKUP_FILESTORE_REMOTE="onedrive:Odoo-Backups/filestore"
LOG_FILE="/var/log/odoo/backup.log"
RCLONE_LOG="/var/log/odoo/rclone_detail.log"
RCLONE_CONFIG="/opt/odoo17/rclone.conf"
CLEANUP_LOCAL="true"
PRE_HOOK=""    # Optional shell command to run BEFORE backup (e.g. put Odoo in maintenance)
POST_HOOK=""   # Optional shell command to run AFTER successful backup
DESTINATIONS_FILE="/opt/odoo17/backup_destinations.json"

DATE=$(date +%Y%m%d_%H%M)
DAY_OF_WEEK=$(date +%u)    # 1=Mon ... 7=Sun
DAY_OF_MONTH=$(date +%d)

BACKUP_START=$SECONDS
CURRENT_STEP="init"

# ─── Logging ──────────────────────────────────────────────────────────────────
log() { echo "[$(date '+%Y-%m-%d %H:%M:%S')] $*" | tee -a "$LOG_FILE"; }

STEP_START=$SECONDS
step_begin() {
    CURRENT_STEP="$1"
    STEP_START=$SECONDS
    log "[STEP] $1"
}
step_end() {
    local secs=$(( SECONDS - STEP_START ))
    local min=$(( secs / 60 )) sec=$(( secs % 60 ))
    [ $min -gt 0 ] && log "[DONE] $CURRENT_STEP — ${min}m ${sec}s" || log "[DONE] $CURRENT_STEP — ${sec}s"
}

# ─── ServerChest backup-report (webhook + log sync) ───────────────────────────
_SC_CONF="/etc/serverchest-agent.conf"
_SC_API_KEY=""
_SC_APP_URL=""
if [ -f "$_SC_CONF" ]; then
    _SC_API_KEY=$(awk -F'[[:space:]]*=[[:space:]]*' '/^api_key/{print $2; exit}' "$_SC_CONF")
    _SC_RELAY=$(awk  -F'[[:space:]]*=[[:space:]]*' '/^relay_url/{print $2; exit}' "$_SC_CONF")
    # Derive app base URL: wss://app.serverchest.com/ws/agent → https://app.serverchest.com
    _SC_APP_URL=$(echo "$_SC_RELAY" | sed 's|^wss://|https://|; s|/ws/.*||')
fi

report_backup() {
    local status="$1"  # success | failed
    local secs=$(( SECONDS - BACKUP_START ))
    [ -z "$_SC_API_KEY" ] || [ -z "$_SC_APP_URL" ] && return 0
    curl -sf --max-time 10 \
        -X POST "$_SC_APP_URL/api/internal/backup-report" \
        -H "Content-Type: application/json" \
        -H "x-api-key: $_SC_API_KEY" \
        -d "{\"status\":\"$status\",\"at\":\"$(date -u +%Y-%m-%dT%H:%M:%SZ)\",\"duration_secs\":$secs,\"db\":\"$DB_NAME\",\"dump_size_bytes\":${DUMP_SIZE_BYTES:-0}}" \
        > /dev/null 2>&1 || true
}

# Extract stats written by rclone to RCLONE_LOG since $1 (line number)
rclone_stats() {
    local since_line="$1"
    local stats
    stats=$(tail -n "+${since_line}" "$RCLONE_LOG" \
        | grep -E "^(Transferred|Checks|Deleted|Elapsed time)" \
        | paste -sd ' | ')
    [ -n "$stats" ] && log "[STATS] $stats"
}

trap 'log "[ERROR] Failed at step: $CURRENT_STEP"; log "===== Backup FAILED ====="; [ "${CLEANUP_LOCAL:-true}" = "true" ] && rm -f "${DUMP_FILE:-}" 2>/dev/null || true; report_backup failed' ERR

# ─── Start ────────────────────────────────────────────────────────────────────
touch "$RCLONE_LOG" 2>/dev/null || true
log "===== Backup started ====="
echo "=== rclone run: $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$RCLONE_LOG"

# ─── Pre-hook ─────────────────────────────────────────────────────────────────
if [ -n "${PRE_HOOK:-}" ]; then
    step_begin "Pre-hook"
    eval "$PRE_HOOK" || { log "[WARN] Pre-hook exited non-zero — continuing"; }
    step_end
fi

# ─── Step 1: Database dump ────────────────────────────────────────────────────
step_begin "Database dump"
mkdir -p "$BACKUP_DIR"
DUMP_FILE="$BACKUP_DIR/db_daily_$DATE.dump"

if [ "$(whoami)" = "odoo17" ]; then
    pg_dump -Fc "$DB_NAME" > "$DUMP_FILE"
else
    sudo -u odoo17 pg_dump -Fc "$DB_NAME" > "$DUMP_FILE"
fi

DUMP_SIZE=$(du -sh "$DUMP_FILE" | cut -f1)
DUMP_SIZE_BYTES=$(stat -c%s "$DUMP_FILE" 2>/dev/null || stat -f%z "$DUMP_FILE" 2>/dev/null || echo 0)
step_end
log "[INFO] Dump size: $DUMP_SIZE"

# ─── Load destinations ────────────────────────────────────────────────────────
# Read backup_destinations.json into parallel arrays (one call to python3)
DEST_COUNT=0
declare -a DEST_NAMES DEST_DB_PATHS DEST_FS_PATHS DEST_RETAIN_DAILY DEST_RETAIN_WEEKLY DEST_RETAIN_MONTHLY
declare -a DEST_USE_DAILY DEST_USE_WEEKLY DEST_USE_MONTHLY

if [ -f "$DESTINATIONS_FILE" ] && command -v python3 &>/dev/null; then
    _dest_env=$(python3 - "$DESTINATIONS_FILE" << 'PYEOF'
import sys, json
try:
    dests = json.load(open(sys.argv[1]))
    if not dests:
        print("DEST_COUNT=0")
        sys.exit(0)
    def q(s): return "'" + str(s).replace("'", "'\\''") + "'"
    print(f"DEST_COUNT={len(dests)}")
    print("DEST_NAMES=(" + " ".join(q(d['name']) for d in dests) + ")")
    print("DEST_DB_PATHS=(" + " ".join(q(d['db_path']) for d in dests) + ")")
    print("DEST_FS_PATHS=(" + " ".join(q(d['fs_path']) for d in dests) + ")")
    print("DEST_RETAIN_DAILY=(" + " ".join(str(d.get('retain_daily',7)) for d in dests) + ")")
    print("DEST_RETAIN_WEEKLY=(" + " ".join(str(d.get('retain_weekly',28)) for d in dests) + ")")
    print("DEST_RETAIN_MONTHLY=(" + " ".join(str(d.get('retain_monthly',365)) for d in dests) + ")")
    print("DEST_USE_DAILY=(" + " ".join('true' if d.get('use_for_daily', True) else 'false' for d in dests) + ")")
    print("DEST_USE_WEEKLY=(" + " ".join('true' if d.get('use_for_weekly', True) else 'false' for d in dests) + ")")
    print("DEST_USE_MONTHLY=(" + " ".join('true' if d.get('use_for_monthly', True) else 'false' for d in dests) + ")")
except Exception as e:
    print("DEST_COUNT=0")
    print(f"# destinations load error: {e}", file=sys.stderr)
PYEOF
)
    eval "$_dest_env"
fi

# ─── Upload function (called per destination) ─────────────────────────────────
upload_to_dest() {
    local dest_name="$1"
    local db_path="$2"
    local fs_path="$3"
    local retain_daily="$4"
    local retain_weekly="$5"
    local retain_monthly="$6"
    local use_daily="${7:-true}"
    local use_weekly="${8:-true}"
    local use_monthly="${9:-true}"

    log "[INFO] === Destination: $dest_name ==="
    local db_ok="ok" fs_ok="ok"

    if [ "$use_daily" = "true" ]; then
        step_begin "Upload daily [$dest_name]"
        rclone --config "$RCLONE_CONFIG" copy "$DUMP_FILE" "$db_path/daily/" \
            --log-level NOTICE --log-file "$RCLONE_LOG" \
            || { log "[WARN] Upload failed for $dest_name (daily)"; db_ok="fail"; }
        step_end
    else
        log "[SKIP] Daily upload disabled for $dest_name"
    fi

    if [ "$DAY_OF_WEEK" -eq 7 ] && [ "$use_weekly" = "true" ]; then
        step_begin "Upload weekly [$dest_name]"
        rclone --config "$RCLONE_CONFIG" copyto "$DUMP_FILE" "$db_path/weekly/db_weekly_$DATE.dump" \
            --log-level NOTICE --log-file "$RCLONE_LOG" \
            || log "[WARN] Upload failed for $dest_name (weekly)"
        step_end
    elif [ "$DAY_OF_WEEK" -eq 7 ]; then
        log "[SKIP] Weekly upload disabled for $dest_name"
    fi

    if [ "$DAY_OF_MONTH" -eq 01 ] && [ "$use_monthly" = "true" ]; then
        step_begin "Upload monthly [$dest_name]"
        rclone --config "$RCLONE_CONFIG" copyto "$DUMP_FILE" "$db_path/monthly/db_monthly_$DATE.dump" \
            --log-level NOTICE --log-file "$RCLONE_LOG" \
            || log "[WARN] Upload failed for $dest_name (monthly)"
        step_end
    elif [ "$DAY_OF_MONTH" -eq 01 ]; then
        log "[SKIP] Monthly upload disabled for $dest_name"
    fi

    step_begin "Prune old backups [$dest_name]"
    [ "$use_daily"   = "true" ] && rclone --config "$RCLONE_CONFIG" delete "$db_path/daily/"   --min-age ${retain_daily}d  --log-level NOTICE --log-file "$RCLONE_LOG" || true
    [ "$use_weekly"  = "true" ] && rclone --config "$RCLONE_CONFIG" delete "$db_path/weekly/"  --min-age ${retain_weekly}d --log-level NOTICE --log-file "$RCLONE_LOG" || true
    [ "$use_monthly" = "true" ] && rclone --config "$RCLONE_CONFIG" delete "$db_path/monthly/" --min-age ${retain_monthly}d --log-level NOTICE --log-file "$RCLONE_LOG" || true
    step_end

    step_begin "Filestore sync [$dest_name]"
    FILESTORE_SIZE=$(du -sh "$FILESTORE" 2>/dev/null | cut -f1)
    log "[INFO] Filestore size: $FILESTORE_SIZE"
    SYNC_START_LINE=$(( $(wc -l < "$RCLONE_LOG") + 1 ))
    rclone --config "$RCLONE_CONFIG" sync "$FILESTORE" "$fs_path" \
        --transfers 8 \
        --checksum \
        --log-level INFO \
        --log-file "$RCLONE_LOG" \
        || { log "[WARN] Filestore sync failed for $dest_name"; fs_ok="fail"; }
    rclone_stats "$SYNC_START_LINE"
    step_end
    log "[DEST_RESULT] name=${dest_name} db=${db_ok} fs=${fs_ok}"
}

# ─── Steps 2–4: Upload, prune, sync ──────────────────────────────────────────
if [ "$DEST_COUNT" -gt 0 ]; then
    log "[INFO] Multi-destination mode: $DEST_COUNT destination(s)"
    for (( i=0; i<DEST_COUNT; i++ )); do
        upload_to_dest \
            "${DEST_NAMES[$i]}" \
            "${DEST_DB_PATHS[$i]}" \
            "${DEST_FS_PATHS[$i]}" \
            "${DEST_RETAIN_DAILY[$i]}" \
            "${DEST_RETAIN_WEEKLY[$i]}" \
            "${DEST_RETAIN_MONTHLY[$i]}" \
            "${DEST_USE_DAILY[$i]:-true}" \
            "${DEST_USE_WEEKLY[$i]:-true}" \
            "${DEST_USE_MONTHLY[$i]:-true}"
    done
else
    # ─── Legacy single-destination mode ──────────────────────────────────────
    log "[INFO] Single-destination mode (legacy)"
    upload_to_dest \
        "OneDrive" \
        "$BACKUP_DB_REMOTE" \
        "$BACKUP_FILESTORE_REMOTE" \
        "7" "28" "365"
fi

if [ "${CLEANUP_LOCAL:-true}" = "true" ]; then rm -f "$DUMP_FILE" 2>/dev/null || true; log "[INFO] Local dump deleted"; fi

# ─── Post-hook ────────────────────────────────────────────────────────────────
if [ -n "${POST_HOOK:-}" ]; then
    step_begin "Post-hook"
    eval "$POST_HOOK" || { log "[WARN] Post-hook exited non-zero — continuing"; }
    step_end
fi

# ─── Done ─────────────────────────────────────────────────────────────────────
TOTAL_SECS=$(( SECONDS - BACKUP_START ))
TOTAL_MIN=$(( TOTAL_SECS / 60 ))
TOTAL_SEC=$(( TOTAL_SECS % 60 ))
log "===== Backup complete — total: ${TOTAL_MIN}m ${TOTAL_SEC}s ====="
report_backup success
