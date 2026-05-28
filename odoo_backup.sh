#!/bin/bash
set -eE
export TZ='Asia/Riyadh'   # force consistent timestamps regardless of caller

# ─── Config ───────────────────────────────────────────────────────────────────
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

# Extract stats written by rclone to RCLONE_LOG since $1 (line number)
rclone_stats() {
    local since_line="$1"
    local stats
    stats=$(tail -n "+${since_line}" "$RCLONE_LOG" \
        | grep -E "^(Transferred|Checks|Deleted|Elapsed time)" \
        | paste -sd ' | ')
    [ -n "$stats" ] && log "[STATS] $stats"
}

trap 'log "[ERROR] Failed at step: $CURRENT_STEP"; log "===== Backup FAILED ====="; [ "${CLEANUP_LOCAL:-true}" = "true" ] && rm -f "${DUMP_FILE:-}" 2>/dev/null || true' ERR

# ─── Start ────────────────────────────────────────────────────────────────────
touch "$RCLONE_LOG" 2>/dev/null || true
log "===== Backup started ====="
echo "=== rclone run: $(date '+%Y-%m-%d %H:%M:%S') ===" >> "$RCLONE_LOG"

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
step_end
log "[INFO] Dump size: $DUMP_SIZE"

# ─── Load destinations ────────────────────────────────────────────────────────
# Read backup_destinations.json into parallel arrays (one call to python3)
DEST_COUNT=0
declare -a DEST_NAMES DEST_DB_PATHS DEST_FS_PATHS DEST_RETAIN_DAILY DEST_RETAIN_WEEKLY DEST_RETAIN_MONTHLY

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

    log "[INFO] === Destination: $dest_name ==="

    step_begin "Upload daily [$dest_name]"
    rclone --config "$RCLONE_CONFIG" copy "$DUMP_FILE" "$db_path/daily/" \
        --log-level NOTICE --log-file "$RCLONE_LOG" \
        || { log "[WARN] Upload failed for $dest_name (daily)"; return; }
    step_end

    if [ "$DAY_OF_WEEK" -eq 7 ]; then
        step_begin "Upload weekly [$dest_name]"
        rclone --config "$RCLONE_CONFIG" copyto "$DUMP_FILE" "$db_path/weekly/db_weekly_$DATE.dump" \
            --log-level NOTICE --log-file "$RCLONE_LOG" \
            || log "[WARN] Upload failed for $dest_name (weekly)"
        step_end
    fi

    if [ "$DAY_OF_MONTH" -eq 01 ]; then
        step_begin "Upload monthly [$dest_name]"
        rclone --config "$RCLONE_CONFIG" copyto "$DUMP_FILE" "$db_path/monthly/db_monthly_$DATE.dump" \
            --log-level NOTICE --log-file "$RCLONE_LOG" \
            || log "[WARN] Upload failed for $dest_name (monthly)"
        step_end
    fi

    step_begin "Prune old backups [$dest_name]"
    rclone --config "$RCLONE_CONFIG" delete "$db_path/daily/"   --min-age ${retain_daily}d  --log-level NOTICE --log-file "$RCLONE_LOG" || true
    rclone --config "$RCLONE_CONFIG" delete "$db_path/weekly/"  --min-age ${retain_weekly}d --log-level NOTICE --log-file "$RCLONE_LOG" || true
    rclone --config "$RCLONE_CONFIG" delete "$db_path/monthly/" --min-age ${retain_monthly}d --log-level NOTICE --log-file "$RCLONE_LOG" || true
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
        || log "[WARN] Filestore sync failed for $dest_name"
    rclone_stats "$SYNC_START_LINE"
    step_end
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
            "${DEST_RETAIN_MONTHLY[$i]}"
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

# ─── Done ─────────────────────────────────────────────────────────────────────
TOTAL_SECS=$(( SECONDS - BACKUP_START ))
TOTAL_MIN=$(( TOTAL_SECS / 60 ))
TOTAL_SEC=$(( TOTAL_SECS % 60 ))
log "===== Backup complete — total: ${TOTAL_MIN}m ${TOTAL_SEC}s ====="
