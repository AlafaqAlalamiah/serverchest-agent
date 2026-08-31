# Server Migration Checklist

Moving an Odoo + ServerChest server to a new machine? **A filesystem rsync of
`/opt/odoo17` and `/etc` does NOT carry the whole backup stack.** Berr
Production was migrated this way on 2026-08-17 and silently took zero backups
for 12 days: the new VM had no `rclone` binary and no odoo17 crontab, so the
backup script never ran and never reported anything.

Copy/verify each of these on the new machine before decommissioning the old one:

| # | Item | Where it lives | Why rsync misses it |
|---|------|----------------|---------------------|
| 1 | **Backup cron entry** | `crontab -u odoo17 -l` (`/var/spool/cron/crontabs/odoo17`) | Spool dir is outside the usual copied trees |
| 2 | **rclone binary** | `/usr/bin/rclone` (`command -v rclone`) | System package, not in `/opt` |
| 3 | rclone config | `/opt/odoo17/rclone.conf` | Copied only if `/opt/odoo17` was copied — verify remotes with `rclone --config /opt/odoo17/rclone.conf listremotes` |
| 4 | Agent config | `/etc/serverchest-agent.conf` (mode 640, owner odoo17) | Only if `/etc` was copied |
| 5 | Backup script | `/opt/odoo17/odoo_backup.sh` (executable, odoo17-owned) | Holds per-server config vars (DB_NAME etc.) — don't replace with a fresh copy |
| 6 | Destinations file | `/opt/odoo17/backup_destinations.json` | — |
| 7 | Log directory | `/var/log/odoo/` writable by odoo17 | Fresh OS may lack it |
| 8 | Agent service | `systemctl status serverchest-agent` | Reinstall via install.sh if missing |

## Verify after migration

1. Dashboard → server → Overview: no "Backups need attention" banner
   (checks `rclone_installed`, `backup_script_exists`, `backup_cron_found` from
   the agent's `get_health`).
2. Trigger a manual backup from the dashboard and confirm it reports **success
   with a cloud path** in Backups → History.
3. `sudo -u odoo17 crontab -l` shows the schedule (default `0 23 * * *` UTC =
   02:00 Riyadh).

## Safety nets (added after the incident, Aug 2026)

- The relay's overdue-backup watchdog emails when a server with backup alerts
  enabled reports no backup for >26h (`BACKUP_OVERDUE_HOURS`).
- A failed backup report now emails too (`backupFailAlertEnabled`).
- `odoo_backup.sh` reports **failed** (and keeps the local dump) when every
  upload fails, instead of "success" with zero copies.

These nets catch the failure *after the fact* — the checklist above prevents it.
