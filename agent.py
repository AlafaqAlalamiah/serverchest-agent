#!/usr/bin/env python3
"""
ServerChest Agent
-----------------
Runs on the Odoo server. Makes an outbound WebSocket connection to the
ServerChest relay and executes commands sent by the dashboard.

Config file: /etc/serverchest-agent.conf
"""

import asyncio
import configparser
import datetime
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import urllib.request

try:
    import websockets
except ImportError:
    sys.exit("Missing dependency: pip install websockets")

# ── Config ────────────────────────────────────────────────────────────────────
CONFIG_FILE = os.environ.get('SERVERCHEST_CONFIG', '/etc/serverchest-agent.conf')

def load_config():
    cfg = configparser.ConfigParser()
    cfg.read(CONFIG_FILE)
    s = cfg['agent'] if 'agent' in cfg else {}
    return {
        'relay_url':     s.get('relay_url',     'ws://localhost:3003'),
        'api_key':       s.get('api_key',        ''),
        'backup_script': s.get('backup_script',  '/opt/odoo17/odoo_backup.sh'),
        'backup_log':    s.get('backup_log',     '/var/log/odoo/backup.log'),
        'odoo_log':      s.get('odoo_log',       '/var/log/odoo/odoo17.log'),
        'rclone_config': s.get('rclone_config',  '/opt/odoo17/rclone.conf'),
        'odoo_conf':     s.get('odoo_conf',      '/etc/odoo17.conf'),
        'odoo_bin':      s.get('odoo_bin',       '/opt/odoo17/odoo17-venv/bin/python'),
        'odoo_src':      s.get('odoo_src',       '/opt/odoo17/odoo17/odoo-bin'),
        'db_name':       s.get('db_name',        ''),
        'service_name':  s.get('service_name',   'odoo17'),
        'backup_remote': s.get('backup_remote',   ''),
    }

# ── Logging ───────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [agent] %(levelname)s %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger('serverchest-agent')

# ── Helpers ───────────────────────────────────────────────────────────────────
def _human_size(n):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if n < 1024:
            return f'{n:.1f} {unit}'
        n /= 1024
    return f'{n:.1f} PB'

def _run(cmd, timeout=30, input=None):
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, input=input)
    return r.stdout, r.stderr, r.returncode

# ── Action handlers ───────────────────────────────────────────────────────────

def action_ping(params, cfg):
    import platform
    return {
        'status': 'ok',
        'hostname': platform.node(),
        'agent_version': '2.0.0',
        'db': cfg.get('db_name', ''),
        'odoo_version': 'Odoo 17',
    }

def action_get_disk_usage(params, cfg):
    result = {}
    for label, path in [('root', '/'), ('odoo', '/opt/odoo17')]:
        if os.path.exists(path):
            u = shutil.disk_usage(path)
            result[label] = {
                'path': path,
                'total_gb': round(u.total / 1024**3, 2),
                'used_gb':  round(u.used  / 1024**3, 2),
                'free_gb':  round(u.free  / 1024**3, 2),
                'used_pct': round(u.used / u.total * 100, 1),
            }
    return result

def action_get_logs(params, cfg):
    log_path = cfg['odoo_log']
    if not os.path.isfile(log_path):
        raise FileNotFoundError(f'Log file not found: {log_path}')
    n = max(1, min(int(params.get('n', 100)), 5000))
    stdout, _, _ = _run(['tail', '-n', str(n), log_path])
    return {'lines': stdout.splitlines(), 'path': log_path}

def action_get_rclone_log(params, cfg):
    log_path = cfg['backup_log']
    if not os.path.isfile(log_path):
        raise FileNotFoundError(f'Backup log not found: {log_path}')
    n = max(1, min(int(params.get('n', 500)), 5000))
    stdout, _, _ = _run(['tail', '-n', str(n), log_path])
    return {'lines': stdout.splitlines(), 'path': log_path}

def action_get_rclone_remotes(params, cfg):
    rclone = shutil.which('rclone')
    if not rclone:
        raise RuntimeError('rclone not installed')
    cmd = ['rclone', '--config', cfg['rclone_config'], 'listremotes'] if cfg['rclone_config'] else ['rclone', 'listremotes']
    stdout, _, _ = _run(cmd)
    remotes = [x.rstrip(':') for x in stdout.splitlines() if x]
    return {'remotes': remotes}

def action_trigger_backup(params, cfg):
    script = cfg['backup_script']
    if not os.path.isfile(script):
        raise FileNotFoundError(f'Backup script not found: {script}')
    # Record current log position so the watcher only looks at new output
    log_path = cfg.get('backup_log', '/var/log/odoo/backup.log')
    try:
        start_pos = os.path.getsize(log_path) if os.path.isfile(log_path) else 0
    except Exception:
        start_pos = 0
    subprocess.Popen(
        ['/bin/bash', script],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
    # Spawn background thread to watch for completion and fire webhook
    server_name = cfg.get('server_name', '')
    t = threading.Thread(
        target=_watch_backup_log,
        args=(log_path, start_pos, server_name),
        daemon=True,
    )
    t.start()
    return {'status': 'triggered', 'script': script}

def action_get_backup_config(params, cfg):
    script = cfg['backup_script']
    if not os.path.isfile(script):
        raise FileNotFoundError(f'Script not found: {script}')
    with open(script) as f:
        content = f.read()

    def _get(var):
        m = re.search(rf'^{var}=["\']?([^"\'\n]+)["\']?', content, re.MULTILINE)
        return m.group(1).strip() if m else ''

    config = {
        'db_name':            _get('DB_NAME'),
        'backup_dir':         _get('BACKUP_DIR'),
        'db_remote':        _get('BACKUP_DB_REMOTE'),
        'filestore_remote': _get('BACKUP_FILESTORE_REMOTE'),
        'rclone_config':      _get('RCLONE_CONFIG'),
        'script_path':        script,
    }
    daily_m   = re.search(r'daily.*?--min-age\s+(\d+)d',   content)
    weekly_m  = re.search(r'weekly.*?--min-age\s+(\d+)d',  content)
    monthly_m = re.search(r'monthly.*?--min-age\s+(\d+)d', content)
    config['retain_daily_days']   = daily_m.group(1)   if daily_m   else '7'
    config['retain_weekly_days']  = weekly_m.group(1)  if weekly_m  else '28'
    config['retain_monthly_days'] = monthly_m.group(1) if monthly_m else '365'

    cleanup_m = re.search(r'^CLEANUP_LOCAL=["\']?(true|false)["\']?', content, re.MULTILINE | re.IGNORECASE)
    config['cleanup_local'] = (cleanup_m.group(1).lower() == 'true') if cleanup_m else True

    version_m = re.search(r'^SCRIPT_VERSION=["\']?([^"\'\'\n]+)["\']?', content, re.MULTILINE)
    config['script_version'] = version_m.group(1).strip() if version_m else None

    stdout, _, _ = _run(['crontab', '-u', 'odoo17', '-l'])
    cron_schedule = '0 2 * * *'
    for line in stdout.splitlines():
        if script in line and not line.startswith('#'):
            parts = line.split()
            if len(parts) >= 5:
                cron_schedule = ' '.join(parts[:5])
            break
    config['cron_schedule'] = cron_schedule
    return config

def action_set_backup_config(params, cfg):
    script = cfg['backup_script']
    if not os.path.isfile(script):
        raise FileNotFoundError(f'Script not found: {script}')
    with open(script) as f:
        content = f.read()

    def _set(var, val):
        nonlocal content
        content = re.sub(
            rf'^({var}=)["\']?[^"\'\n]*["\']?',
            rf'\g<1>"{val}"',
            content, flags=re.MULTILINE
        )

    if 'db_name'            in params: _set('DB_NAME',            params['db_name'])
    if 'backup_dir'         in params: _set('BACKUP_DIR',         params['backup_dir'])
    if 'db_remote'        in params: _set('BACKUP_DB_REMOTE',        params['db_remote'])
    if 'filestore_remote' in params: _set('BACKUP_FILESTORE_REMOTE', params['filestore_remote'])
    if 'rclone_config'      in params: _set('RCLONE_CONFIG',      params['rclone_config'])

    def _set_retention(tier, days):
        nonlocal content
        content = re.sub(
            rf'(# {tier}.*?--min-age\s+)\d+(d)',
            rf'\g<1>{days}\g<2>',
            content, flags=re.IGNORECASE | re.DOTALL
        )

    if 'retain_daily_days'   in params: _set_retention('daily',   params['retain_daily_days'])
    if 'retain_weekly_days'  in params: _set_retention('weekly',  params['retain_weekly_days'])
    if 'retain_monthly_days' in params: _set_retention('monthly', params['retain_monthly_days'])

    if 'cleanup_local' in params:
        val = 'true' if params['cleanup_local'] else 'false'
        if re.search(r'^CLEANUP_LOCAL=', content, re.MULTILINE):
            _set('CLEANUP_LOCAL', val)
        else:
            # Variable not yet in script — insert after RCLONE_CONFIG line
            content = re.sub(
                r'(RCLONE_CONFIG=.*?\n)',
                rf'\g<1>CLEANUP_LOCAL="{val}"\n',
                content, count=1
            )

    with open(script, 'w') as f:
        f.write(content)

    # Update cron
    if 'cron_schedule' in params:
        stdout, _, _ = _run(['crontab', '-u', 'odoo17', '-l'])
        lines = [l for l in stdout.splitlines() if script not in l]
        lines.append(f"{params['cron_schedule']} {script}")
        new_crontab = '\n'.join(lines) + '\n'
        _run(['crontab', '-u', 'odoo17', '-'], input=new_crontab)

    return {'status': 'saved'}

BACKUP_SCRIPT_URL = 'https://raw.githubusercontent.com/AlafaqAlalamiah/serverchest-agent/main/odoo_backup.sh'

def action_update_backup_script(params, cfg):
    """Download the latest odoo_backup.sh template from GitHub, transplant the
    existing server-specific config values, and atomically replace the script."""
    import urllib.request, shutil, tempfile
    script = cfg['backup_script']

    # Read current config values to preserve them
    existing = {}
    if os.path.isfile(script):
        with open(script) as f:
            old = f.read()
        def _get(var):
            m = re.search(rf'^{var}=["\']?([^"\'\n]+)["\']?', old, re.MULTILINE)
            return m.group(1).strip() if m else None
        for var in ('DB_NAME', 'BACKUP_DIR', 'BACKUP_DB_REMOTE', 'BACKUP_FILESTORE_REMOTE',
                    'RCLONE_CONFIG', 'CLEANUP_LOCAL', 'DESTINATIONS_FILE'):
            v = _get(var)
            if v:
                existing[var] = v

    # Download latest template
    try:
        with urllib.request.urlopen(BACKUP_SCRIPT_URL, timeout=15) as resp:
            new_content = resp.read().decode()
    except Exception as e:
        raise RuntimeError(f'Failed to download backup script: {e}')

    # Transplant preserved values into the new template
    for var, val in existing.items():
        new_content = re.sub(
            rf'^({var}=)["\']?[^"\'\n]*["\']?',
            rf'\g<1>"{val}"',
            new_content, flags=re.MULTILINE
        )

    # Write atomically via temp file
    script_dir = os.path.dirname(script)
    with tempfile.NamedTemporaryFile('w', dir=script_dir, delete=False, suffix='.tmp') as tmp:
        tmp.write(new_content)
        tmp_path = tmp.name
    os.chmod(tmp_path, 0o755)
    os.replace(tmp_path, script)

    return {'status': 'updated', 'script': script, 'preserved': list(existing.keys())}

def action_check_backup_script_update(params, cfg):
    """Compare the local script version with the latest on GitHub."""
    import urllib.request
    script = cfg['backup_script']
    current_version = None
    if os.path.isfile(script):
        with open(script) as f:
            content = f.read()
        m = re.search(r'^SCRIPT_VERSION=["\']?([^"\'\'\n]+)["\']?', content, re.MULTILINE)
        current_version = m.group(1).strip() if m else None

    try:
        with urllib.request.urlopen(BACKUP_SCRIPT_URL, timeout=15) as resp:
            remote = resp.read().decode()
        m = re.search(r'^SCRIPT_VERSION=["\']?([^"\'\'\n]+)["\']?', remote, re.MULTILINE)
        latest_version = m.group(1).strip() if m else None
    except Exception as e:
        return {'error': f'Could not fetch remote script: {e}'}

    return {
        'current_version': current_version,
        'latest_version':  latest_version,
        'update_available': latest_version is not None and latest_version != current_version,
    }

def action_service_status(params, cfg):
    svc = cfg['service_name']
    stdout, _, rc = _run(['systemctl', 'is-active', svc])
    is_active = stdout.strip() == 'active'
    # Check HTTP
    http_ok = False
    try:
        import urllib.request
        urllib.request.urlopen('http://localhost:8069/web/health', timeout=5)
        http_ok = True
    except Exception:
        pass
    return {'service': svc, 'active': is_active, 'http_ok': http_ok, 'systemctl_status': stdout.strip()}

def action_get_journal(params, cfg):
    svc = cfg['service_name']
    n = max(1, min(int(params.get('n', 80)), 500))
    stdout, stderr, rc = _run(
        ['journalctl', '-u', svc, '-n', str(n), '--no-pager', '-o', 'short-iso'],
        timeout=15,
    )
    return {'lines': stdout.splitlines(), 'service': svc}

def action_service_control(params, cfg):
    svc = cfg['service_name']
    action = params.get('svc_action') or params.get('action', '')
    if action not in ('start', 'stop', 'restart'):
        raise ValueError(f'Invalid action: {action}')
    stdout, stderr, rc = _run(['sudo', 'systemctl', action, svc], timeout=60)
    # Brief settle time, then capture journal for caller
    import time; time.sleep(2)
    j_out, _, _ = _run(['journalctl', '-u', svc, '-n', '60', '--no-pager', '-o', 'short-iso'], timeout=10)
    status_out, _, _ = _run(['systemctl', 'is-active', svc], timeout=5)
    return {
        'action': action,
        'service': svc,
        'rc': rc,
        'active': status_out.strip() == 'active',
        'journal': j_out.splitlines(),
    }

def action_get_odoo_info(params, cfg):
    """Query installed modules and Odoo version directly from PostgreSQL."""
    db = cfg['db_name']
    if not db:
        raise ValueError('db_name not configured in agent config')
    query = """
        SELECT name, state, latest_version, author
        FROM ir_module_module
        WHERE state IN ('installed','to upgrade','to remove')
        ORDER BY name
    """
    stdout, stderr, rc = _run(
        ['psql', '-d', db, '-t', '-A', '-F', '\t', '-c', query],
        timeout=30
    )
    if rc != 0:
        raise RuntimeError(f'psql error: {stderr.strip()}')
    modules = []
    for line in stdout.strip().splitlines():
        parts = line.split('\t')
        if len(parts) >= 3:
            modules.append({'name': parts[0], 'state': parts[1], 'version': parts[2], 'author': parts[3] if len(parts) > 3 else ''})
    count = len(modules)
    # Get DB size
    db_size_bytes = 0
    size_out, _, size_rc = _run(['psql', '-d', db, '-t', '-A', '-c', f"SELECT pg_database_size('{db}')"], timeout=10)
    if size_rc == 0 and size_out.strip().isdigit():
        db_size_bytes = int(size_out.strip())
    def _human(n):
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if n < 1024: return f'{n:.1f} {unit}'
            n /= 1024
        return f'{n:.1f} PB'
    return {'modules': modules, 'count': count, 'installed_modules': count,
            'db': db, 'db_size_bytes': db_size_bytes, 'db_size': _human(db_size_bytes)}

def action_read_odoo_conf(params, cfg):
    conf_path = cfg['odoo_conf']
    if not os.path.isfile(conf_path):
        raise FileNotFoundError(f'Config not found: {conf_path}')
    with open(conf_path) as f:
        content = f.read()
    # Parse key=value pairs from [options] section
    parser = configparser.ConfigParser()
    parser.read(conf_path)
    options = dict(parser['options']) if 'options' in parser else {}
    return {'options': options, 'raw': content, 'path': conf_path}


def action_get_health(params, cfg):
    disk = shutil.disk_usage('/')
    rclone_installed = shutil.which('rclone') is not None
    http_ok = False
    try:
        import urllib.request
        urllib.request.urlopen('http://localhost:8069/web/health', timeout=5)
        http_ok = True
    except Exception:
        pass
    script = cfg.get('backup_script', '')
    return {
        'rclone_installed': rclone_installed,
        'disk_total_gb': round(disk.total / 1024**3, 2),
        'disk_free_gb':  round(disk.free  / 1024**3, 2),
        'disk_used_gb':  round(disk.used  / 1024**3, 2),
        'disk_free_pct': round(disk.free / disk.total * 100, 1),
        'backup_script_configured': bool(script),
        'backup_script_exists': os.path.isfile(script) if script else False,
        'db_connection': http_ok,
    }


def action_dump_database(params, cfg):
    """pg_dump in custom format, returned as base64."""
    import base64
    import subprocess as _sp
    db = cfg['db_name']
    if not db:
        raise ValueError('db_name not configured')
    r = _sp.run(['pg_dump', '-Fc', db], capture_output=True, timeout=300)
    if r.returncode != 0:
        raise RuntimeError(f'pg_dump failed: {r.stderr.decode(errors="replace").strip()}')
    data_b64 = base64.b64encode(r.stdout).decode('ascii')
    return {
        'filename': f'{db}.dump',
        'size_bytes': len(r.stdout),
        'data_b64': data_b64,
    }


def action_list_backups(params, cfg):
    # List available backup files from rclone remote, grouped by tier.
    if not shutil.which('rclone'):
        raise RuntimeError('rclone not installed')

    backup_remote = cfg.get('backup_remote', '').rstrip('/')
    if not backup_remote:
        script = cfg.get('backup_script', '')
        if os.path.isfile(script):
            with open(script) as fh:
                content = fh.read()
            m = re.search(r'BACKUP_DB_REMOTE=["\']?([^"\'\n ]+)', content, re.MULTILINE)
            if m:
                backup_remote = m.group(1).strip().rstrip('/')
    if not backup_remote:
        raise ValueError('backup_remote not configured')

    rclone_conf = cfg.get('rclone_config', '')
    base_cmd = ['rclone', '--config', rclone_conf, 'lsjson'] if rclone_conf else ['rclone', 'lsjson']

    backups = {}
    for tier in ('daily', 'weekly', 'monthly'):
        remote_path = f'{backup_remote}/{tier}/'
        stdout, stderr, rc = _run(base_cmd + [remote_path], timeout=60)
        tier_files = []
        if rc == 0 and stdout.strip():
            try:
                for entry in json.loads(stdout):
                    name = entry.get('Name', '')
                    if entry.get('IsDir') or not name.endswith('.dump'):
                        continue
                    tier_files.append({
                        'name': name,
                        'path': f'{backup_remote}/{tier}/{name}',
                        'size': entry.get('Size', 0),
                        'size_human': _human_size(entry.get('Size', 0)),
                        'modified': entry.get('ModTime', ''),
                    })
                tier_files.sort(key=lambda x: x['modified'], reverse=True)
            except (json.JSONDecodeError, KeyError):
                pass
        backups[tier] = tier_files

    return {'backups': backups, 'total': sum(len(v) for v in backups.values()), 'remote': backup_remote}


def action_verify_backup(params, cfg):
    """Verify a backup by date: checks DB dump exists in cloud and filestore sync is non-empty.
    Reads remote paths from the backup script — no hardcoded remote names.
    """
    date_str = params.get('date', '').strip()
    if not re.match(r'^\d{4}-\d{2}-\d{2}$', date_str):
        raise ValueError('date must be YYYY-MM-DD')

    if not shutil.which('rclone'):
        raise RuntimeError('rclone not installed')

    script = cfg.get('backup_script', '')
    script_content = ''
    if os.path.isfile(script):
        with open(script) as fh:
            script_content = fh.read()

    def _get_var(var):
        m = re.search(rf'{var}=["\'\']?([^"\'\' \n]+)', script_content, re.MULTILINE)
        return m.group(1).strip().rstrip('/') if m else ''

    # Resolve DB remote (prefer agent config, fall back to script var)
    db_remote = cfg.get('backup_remote', '').rstrip('/') or _get_var('BACKUP_DB_REMOTE')
    if not db_remote:
        raise ValueError('backup_remote / BACKUP_DB_REMOTE not configured')

    # Resolve filestore remote from script var
    fs_remote = _get_var('BACKUP_FILESTORE_REMOTE')

    rclone_conf = cfg.get('rclone_config', '')
    base_cmd = ['rclone', '--config', rclone_conf, 'lsjson'] if rclone_conf else ['rclone', 'lsjson']

    # --- DB dump check ---
    # Filenames use compact date format: db_daily_20260524_0200.dump
    date_compact = date_str.replace('-', '')
    db_ok = False
    db_path = None
    db_size = 0
    db_size_human = ''
    for tier in ('daily', 'weekly', 'monthly'):
        stdout, _, rc = _run(base_cmd + [f'{db_remote}/{tier}/'], timeout=30)
        if rc != 0 or not stdout.strip():
            continue
        try:
            for entry in json.loads(stdout):
                name = entry.get('Name', '')
                if date_compact in name and name.endswith('.dump'):
                    db_ok = entry.get('Size', 0) > 0
                    db_size = entry.get('Size', 0)
                    db_size_human = _human_size(db_size)
                    db_path = f'{db_remote}/{tier}/{name}'
                    break
        except (json.JSONDecodeError, KeyError):
            pass
        if db_path:
            break

    # --- Filestore check ---
    fs_ok = False
    fs_files = 0
    if fs_remote:
        stdout, _, rc = _run(base_cmd + ['--max-depth', '1', fs_remote + '/'], timeout=30)
        if rc == 0 and stdout.strip():
            try:
                entries = json.loads(stdout)
                fs_files = len(entries)
                fs_ok = fs_files > 0
            except (json.JSONDecodeError, KeyError):
                pass

    return {
        'db':        {'ok': db_ok, 'path': db_path, 'size': db_size, 'size_human': db_size_human},
        'filestore': {'ok': fs_ok, 'path': fs_remote or None, 'files': fs_files},
    }

def action_restore_backup(params, cfg):
    # Download a specific backup from rclone and restore it to the database.
    import tempfile as _tempfile
    rclone_path = params.get('path', '').strip()
    if not rclone_path:
        raise ValueError('path is required')
    if not rclone_path.endswith('.dump'):
        raise ValueError('Invalid backup path: must be a .dump file')

    backup_remote = cfg.get('backup_remote', '')
    if backup_remote:
        remote_name = backup_remote.split(':')[0]
        if not rclone_path.startswith(remote_name + ':'):
            raise ValueError(f'Invalid backup path: expected remote {remote_name}:')

    db = cfg['db_name']
    if not db:
        raise ValueError('db_name not configured')
    svc = cfg['service_name']
    rclone_conf = cfg.get('rclone_config', '')
    base_rclone = ['rclone', '--config', rclone_conf] if rclone_conf else ['rclone']

    tmp_dir = _tempfile.mkdtemp(prefix='sc_restore_')
    dump_file = os.path.join(tmp_dir, 'restore.dump')
    try:
        log.info('[restore] Downloading %s', rclone_path)
        dl_stdout, dl_stderr, dl_rc = _run(
            base_rclone + ['copyto', rclone_path, dump_file], timeout=300)
        if dl_rc != 0:
            raise RuntimeError(f'Download failed: {dl_stderr.strip()}')
        if not os.path.isfile(dump_file) or os.path.getsize(dump_file) == 0:
            raise RuntimeError('Downloaded file is empty')
        dump_size = os.path.getsize(dump_file)
        log.info('[restore] Downloaded %s (%s)', os.path.basename(rclone_path), _human_size(dump_size))

        log.info('[restore] Stopping %s', svc)
        _run(['sudo', 'systemctl', 'stop', svc], timeout=60)

        _run(['psql', '-d', 'postgres', '-c',
              f"SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
              f"WHERE datname='{db}' AND pid <> pg_backend_pid();"], timeout=15)

        log.info('[restore] Recreating database %s', db)
        _run(['psql', '-d', 'postgres', '-c', f'DROP DATABASE IF EXISTS "{db}"'], timeout=30)
        _run(['psql', '-d', 'postgres', '-c', f'CREATE DATABASE "{db}"'], timeout=30)

        log.info('[restore] Running pg_restore')
        pg_stdout, pg_stderr, pg_rc = _run(
            ['pg_restore', '-Fc', '-d', db, dump_file], timeout=600)
        if pg_rc > 1:
            raise RuntimeError(f'pg_restore failed (rc={pg_rc}): {pg_stderr.strip()[:500]}')

        log.info('[restore] Starting %s', svc)
        _run(['sudo', 'systemctl', 'start', svc], timeout=60)

        log.info('[restore] Complete')
        return {
            'status': 'ok',
            'restored_from': rclone_path,
            'db': db,
            'dump_size': dump_size,
            'dump_size_human': _human_size(dump_size),
        }
    except Exception:
        try:
            _run(['sudo', 'systemctl', 'start', svc], timeout=60)
        except Exception:
            pass
        raise
    finally:
        shutil.rmtree(tmp_dir, ignore_errors=True)


def action_create_rclone_remote(params, cfg):
    """Create a new rclone remote. params: name, type, token (OAuth JSON str), fields (dict)."""
    rclone = shutil.which('rclone') or 'rclone'
    rclone_cfg = cfg.get('rclone_config', '')
    name  = params.get('name', '').strip()
    rtype = params.get('type', '').strip()
    if not name or not rtype:
        raise ValueError('name and type are required')
    config_flag = ['--config', rclone_cfg] if rclone_cfg else []
    stdout, _, _ = _run([rclone] + config_flag + ['listremotes'])
    existing = [x.rstrip(':') for x in stdout.splitlines() if x]
    if name in existing:
        # Already configured — treat as success (idempotent)
        return {'ok': True, 'remote': name, 'remotes': existing}
    cmd = [rclone] + config_flag + ['config', 'create', name, rtype, '--non-interactive']
    token = params.get('token', '').strip()
    if token:
        cmd += ['token', token]
    fields = params.get('fields') or {}
    for k, v in fields.items():
        if k and v:
            cmd += [str(k), str(v)]
    stdout, stderr, rc = _run(cmd, timeout=15)
    if rc != 0:
        raise RuntimeError(f'rclone config create failed: {stderr.strip() or stdout.strip()}')
    stdout2, _, _ = _run([rclone] + config_flag + ['listremotes'])
    remotes = [x.rstrip(':') for x in stdout2.splitlines() if x]
    return {'ok': True, 'remote': name, 'remotes': remotes}


def action_delete_rclone_remote(params, cfg):
    """Delete a rclone remote from rclone.conf."""
    rclone = shutil.which('rclone') or 'rclone'
    rclone_cfg = cfg.get('rclone_config', '')
    name = params.get('name', '').strip()
    if not name:
        raise ValueError('name is required')
    config_flag = ['--config', rclone_cfg] if rclone_cfg else []
    _, stderr, rc = _run([rclone] + config_flag + ['config', 'delete', name], timeout=10)
    if rc != 0:
        raise RuntimeError(f'rclone config delete failed: {stderr.strip()}')
    stdout, _, _ = _run([rclone] + config_flag + ['listremotes'])
    remotes = [x.rstrip(':') for x in stdout.splitlines() if x]
    return {'ok': True, 'remotes': remotes}



def action_test_rclone_remote(params, cfg):
    """Test rclone remote connectivity by listing a path. params: path (e.g. onedrive:Odoo-Backups/database)"""
    rclone = shutil.which('rclone') or 'rclone'
    rclone_cfg = cfg.get('rclone_config', '')
    path = params.get('path', '').strip()
    if not path:
        raise ValueError('path is required')
    config_flag = ['--config', rclone_cfg] if rclone_cfg else []
    stdout, stderr, rc = _run([rclone] + config_flag + ['lsd', path, '--max-depth', '1'], timeout=20)
    if rc != 0:
        raise RuntimeError(stderr.strip() or stdout.strip() or 'Connection failed')
    entries = len([l for l in stdout.splitlines() if l.strip()])
    return {'ok': True, 'path': path, 'entries': entries}


def action_rclone_about(params, cfg):
    """Get storage quota for an rclone remote. params: remote (e.g. 'onedrive:' or 'onedrive:path')"""
    import json as _json
    rclone = shutil.which('rclone') or 'rclone'
    rclone_cfg = cfg.get('rclone_config', '')
    remote = params.get('remote', '').strip()
    if not remote:
        raise ValueError('remote is required')
    # Normalize to just 'remotename:'
    remote = remote.split(':')[0] + ':'
    config_flag = ['--config', rclone_cfg] if rclone_cfg else []
    stdout, stderr, rc = _run([rclone] + config_flag + ['about', remote, '--json'], timeout=20)
    if rc != 0:
        raise RuntimeError(stderr.strip() or stdout.strip() or 'rclone about failed')
    data = _json.loads(stdout)
    def to_gb(b):
        return round(b / 1073741824, 2) if b else None
    total = data.get('total')
    used  = data.get('used')
    free  = data.get('free')
    return {
        'remote':   remote,
        'total_gb': to_gb(total),
        'used_gb':  to_gb(used),
        'free_gb':  to_gb(free),
        'used_pct': round(used / total * 100) if total and used else None,
    }


def action_sync_destinations(params, cfg):
    """Write backup_destinations.json from the destinations array. params: destinations list"""
    import json as _json
    destinations = params.get('destinations', [])
    dest_file = '/opt/odoo17/backup_destinations.json'
    with open(dest_file, 'w') as f:
        _json.dump(destinations, f, indent=2)
    return {'ok': True, 'count': len(destinations), 'file': dest_file}


def action_list_databases(params, cfg):
    """List PostgreSQL databases (excluding template/system DBs)."""
    query = ("SELECT datname FROM pg_database "
             "WHERE datistemplate = false AND datname NOT IN ('postgres') "
             "ORDER BY datname")
    connect_candidates = [c for c in [cfg.get('db_name', ''), 'template1', 'postgres'] if c]
    last_err = 'psql not found'
    for connect_db in connect_candidates:
        stdout, stderr, rc = _run(
            ['psql', '-d', connect_db, '-t', '-A', '-c', query], timeout=15)
        if rc == 0:
            dbs = [ln.strip() for ln in stdout.splitlines()
                   if ln.strip() and not ln.strip().startswith('-')]
            return {'databases': dbs}
        last_err = stderr.strip() or stdout.strip() or f'psql rc={rc}'
    # Fallback: sudo -u odoo17
    stdout, stderr, rc = _run(
        ['sudo', '-u', 'odoo17', 'psql', '-t', '-A', '-c', query], timeout=15)
    if rc == 0:
        dbs = [ln.strip() for ln in stdout.splitlines()
               if ln.strip() and not ln.strip().startswith('-')]
        return {'databases': dbs}
    raise RuntimeError(f'list_databases failed: {last_err}')


# ── SSH key management ────────────────────────────────────────────────────────

def _auth_keys_path(user=''):
    """Return authorized_keys path for user (or current effective user)."""
    import pwd
    if user:
        try:
            pw = pwd.getpwnam(user)
            return os.path.join(pw.pw_dir, '.ssh', 'authorized_keys')
        except KeyError:
            raise RuntimeError(f'User not found: {user}')
    home = os.path.expanduser('~')
    return os.path.join(home, '.ssh', 'authorized_keys')

def action_list_ssh_keys(params, cfg):
    """List authorized SSH public keys."""
    import tempfile
    path = _auth_keys_path(params.get('user', ''))
    if not os.path.exists(path):
        return {'keys': [], 'path': path}
    keys = []
    with open(path) as f:
        raw_lines = f.readlines()
    for i, raw_line in enumerate(raw_lines):
        line = raw_line.strip()
        if not line or line.startswith('#'):
            continue
        parts = line.split(None, 2)
        if len(parts) < 2:
            continue
        key_type = parts[0]
        key_b64  = parts[1]
        comment  = parts[2] if len(parts) > 2 else ''
        # Get fingerprint via ssh-keygen using a temp file
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.pub', delete=False) as tf:
                tf.write(line)
                tmp = tf.name
            stdout, _, rc = _run(['ssh-keygen', '-l', '-f', tmp], timeout=5)
            fp_parts = stdout.strip().split()
            fingerprint = fp_parts[1] if rc == 0 and len(fp_parts) >= 2 else 'unknown'
        finally:
            try: os.unlink(tmp)
            except Exception: pass
        keys.append({
            'index':        i,
            'type':         key_type,
            'comment':      comment,
            'fingerprint':  fingerprint,
            'key_preview':  key_b64[:20] + '…',
            'line_content': line,
        })
    return {'keys': keys, 'path': path}

_SSH_KEY_TYPES = {
    'ssh-rsa', 'ssh-ed25519', 'ssh-dss',
    'ecdsa-sha2-nistp256', 'ecdsa-sha2-nistp384', 'ecdsa-sha2-nistp521',
    'sk-ssh-ed25519@openssh.com', 'sk-ecdsa-sha2-nistp256@openssh.com',
}

def action_add_ssh_key(params, cfg):
    """Append a public SSH key to authorized_keys."""
    key = params.get('key', '').strip()
    if not key:
        raise ValueError('key is required')
    parts = key.split(None, 2)
    if len(parts) < 2 or parts[0] not in _SSH_KEY_TYPES:
        raise ValueError(f'Invalid SSH public key format. Must start with one of: {", ".join(sorted(_SSH_KEY_TYPES))}')
    key_b64 = parts[1]
    path = _auth_keys_path(params.get('user', ''))
    ssh_dir = os.path.dirname(path)
    os.makedirs(ssh_dir, exist_ok=True)
    os.chmod(ssh_dir, 0o700)
    # Duplicate check
    if os.path.exists(path):
        with open(path) as f:
            for line in f:
                ep = line.strip().split(None, 2)
                if len(ep) >= 2 and ep[1] == key_b64:
                    raise ValueError('This key is already in authorized_keys')
    with open(path, 'a') as f:
        f.write(key + '\n')
    os.chmod(path, 0o600)
    return {'ok': True, 'path': path}

def action_remove_ssh_key(params, cfg):
    """Remove a specific key from authorized_keys by its full line content."""
    line_content = params.get('line_content', '').strip()
    if not line_content:
        raise ValueError('line_content is required')
    path = _auth_keys_path(params.get('user', ''))
    if not os.path.exists(path):
        raise RuntimeError('authorized_keys file not found')
    with open(path) as f:
        lines = f.readlines()
    new_lines = [l for l in lines if l.strip() != line_content]
    if len(new_lines) == len(lines):
        raise RuntimeError('Key not found in authorized_keys')
    with open(path, 'w') as f:
        f.writelines(new_lines)
    os.chmod(path, 0o600)
    return {'ok': True}


# ── Webhook ───────────────────────────────────────────────────────────────────
_WEBHOOK_CFG = '/opt/serverchest-agent/webhook.json'

def _load_webhook_cfg():
    if not os.path.exists(_WEBHOOK_CFG):
        return {'enabled': False, 'url': '', 'on_success': True, 'on_failure': True}
    try:
        with open(_WEBHOOK_CFG) as f:
            return json.load(f)
    except Exception:
        return {'enabled': False, 'url': '', 'on_success': True, 'on_failure': True}

def _fire_webhook(event, details, server_name=''):
    """Fire webhook silently — never raises."""
    try:
        cfg = _load_webhook_cfg()
        if not cfg.get('enabled') or not cfg.get('url', '').strip():
            return
        if event == 'success' and not cfg.get('on_success', True):
            return
        if event == 'failure' and not cfg.get('on_failure', True):
            return
        payload = json.dumps({
            'event': event,
            'server': server_name,
            'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
            **details,
        }).encode()
        req = urllib.request.Request(
            cfg['url'].strip(),
            data=payload,
            headers={'Content-Type': 'application/json', 'User-Agent': 'ServerChest/1.0'},
            method='POST',
        )
        urllib.request.urlopen(req, timeout=10)
    except Exception:
        pass  # always silent

def _watch_backup_log(log_path, start_pos, server_name, timeout=7200):
    """
    Background thread: tail backup log from start_pos, detect SUCCESS/FAILED
    line and fire webhook. Gives up after `timeout` seconds.
    """
    import time
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            with open(log_path) as f:
                f.seek(start_pos)
                new_text = f.read()
            for line in new_text.splitlines():
                lower = line.lower()
                if 'backup completed successfully' in lower or '] success' in lower or 'backup success' in lower:
                    _fire_webhook('success', {'message': line.strip()}, server_name)
                    return
                if 'backup failed' in lower or 'error:' in lower or '] failed' in lower or 'backup error' in lower:
                    _fire_webhook('failure', {'message': line.strip()}, server_name)
                    return
        except Exception:
            pass
        time.sleep(15)

def action_get_webhook_config(params, cfg):
    return _load_webhook_cfg()

def action_set_webhook_config(params, cfg):
    current = _load_webhook_cfg()
    for key in ('enabled', 'url', 'on_success', 'on_failure'):
        if key in params:
            current[key] = params[key]
    os.makedirs(os.path.dirname(_WEBHOOK_CFG), exist_ok=True)
    with open(_WEBHOOK_CFG, 'w') as f:
        json.dump(current, f)
    return current

def action_test_webhook(params, cfg):
    url = str(params.get('url', '') or _load_webhook_cfg().get('url', '')).strip()
    if not url:
        return {'error': 'No webhook URL configured'}
    payload = json.dumps({
        'event': 'test',
        'server': cfg.get('server_name', 'serverchest'),
        'timestamp': datetime.datetime.utcnow().isoformat() + 'Z',
        'message': 'Test webhook from ServerChest',
    }).encode()
    req = urllib.request.Request(
        url,
        data=payload,
        headers={'Content-Type': 'application/json', 'User-Agent': 'ServerChest/1.0'},
        method='POST',
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return {'ok': True, 'http_status': r.status}
    except urllib.error.HTTPError as e:
        return {'ok': False, 'http_status': e.code, 'error': str(e)}
    except Exception as e:
        return {'error': str(e)}


# ── Maintenance mode ──────────────────────────────────────────────────────────
_MAINTENANCE_FLAG = '/opt/odoo17/maintenance.flag'

def action_get_maintenance_status(params, cfg):
    """Return current maintenance mode status."""
    import json as _json
    if os.path.exists(_MAINTENANCE_FLAG):
        try:
            with open(_MAINTENANCE_FLAG) as f:
                data = _json.load(f)
        except Exception:
            data = {}
        return {'active': True, 'since': data.get('since'), 'message': data.get('message', '')}
    return {'active': False, 'since': None, 'message': ''}

def action_enable_maintenance(params, cfg):
    """Enable maintenance mode: write flag file and stop Odoo service."""
    import json as _json, datetime
    message = str(params.get('message', '') or 'System is under maintenance.')
    since = datetime.datetime.utcnow().isoformat() + 'Z'
    with open(_MAINTENANCE_FLAG, 'w') as f:
        _json.dump({'since': since, 'message': message}, f)
    svc = cfg.get('service_name', 'odoo17')
    _, _, rc = _run(['systemctl', 'stop', svc], timeout=30)
    return {'active': True, 'since': since, 'message': message, 'service_stopped': rc == 0}

def action_disable_maintenance(params, cfg):
    """Disable maintenance mode: remove flag file and start Odoo service."""
    if os.path.exists(_MAINTENANCE_FLAG):
        os.remove(_MAINTENANCE_FLAG)
    svc = cfg.get('service_name', 'odoo17')
    _, _, rc = _run(['systemctl', 'start', svc], timeout=30)
    return {'active': False, 'service_started': rc == 0}


# ── Odoo version & module info ────────────────────────────────────────────────
_ODOO_CONF    = '/etc/odoo17.conf'
_ODOO_RELEASE = '/opt/odoo17/odoo17/odoo/release.py'

def action_get_odoo_info(params, cfg):
    result = {}

    # 1. Odoo version
    if os.path.isfile(_ODOO_RELEASE):
        with open(_ODOO_RELEASE) as f:
            rc_txt = f.read()
        m = re.search(r"^version\s*=\s*'([^']+)'", rc_txt, re.MULTILINE)
        result['version'] = m.group(1) if m else None
    else:
        result['version'] = None

    # 2. Service status
    out, _, _ = _run(['systemctl', 'show', 'odoo17',
                      '--property=ActiveState,SubState,ActiveEnterTimestamp'])
    svc = {}
    for line in out.splitlines():
        if '=' in line:
            k, v = line.split('=', 1)
            svc[k] = v
    result['service_state']    = svc.get('ActiveState')
    result['service_substate'] = svc.get('SubState')
    result['service_since']    = svc.get('ActiveEnterTimestamp')

    # 3. Odoo config: db_name + addons_path
    conf_txt = ''
    if os.path.isfile(_ODOO_CONF):
        with open(_ODOO_CONF) as f:
            conf_txt = f.read()
    db_m = re.search(r'^\s*db_name\s*=\s*(\S+)', conf_txt, re.MULTILINE)
    db_name = db_m.group(1).strip() if db_m else ''
    result['db_name'] = db_name

    addons_m = re.search(r'^\s*addons_path\s*=\s*(.+)', conf_txt, re.MULTILINE)
    all_paths = [p.strip() for p in addons_m.group(1).split(',')] if addons_m else []
    standard_path = all_paths[0] if all_paths else ''
    custom_paths  = all_paths[1:] if len(all_paths) > 1 else []

    # 4. Custom addons from filesystem
    custom_addons = []
    for path in custom_paths:
        if os.path.isdir(path):
            for d in sorted(os.listdir(path)):
                full = os.path.join(path, d)
                if os.path.isdir(full) and not d.startswith(('.', '_')):
                    manifest = os.path.join(full, '__manifest__.py')
                    version = ''
                    if os.path.isfile(manifest):
                        with open(manifest) as mf:
                            mv = re.search(r"'version'\s*:\s*'([^']+)'", mf.read())
                            version = mv.group(1) if mv else ''
                    custom_addons.append({'name': d, 'version': version, 'path': path})
    result['custom_addons'] = custom_addons

    # 5. Installed modules from PostgreSQL
    if db_name and db_name not in ('False', 'false', ''):
        query = ("SELECT name, installed_version, author "
                 "FROM ir_module_module WHERE state='installed' ORDER BY name;")
        out, _, rc = _run(
            ['sudo', '-u', 'odoo17', 'psql', '-d', db_name, '-A', '-F', '\t', '-t', '-c', query],
            timeout=30
        )
        modules = []
        if rc == 0:
            for line in out.strip().splitlines():
                parts = line.split('\t')
                if len(parts) >= 1 and parts[0]:
                    modules.append({
                        'name':    parts[0],
                        'version': parts[1] if len(parts) > 1 else '',
                        'author':  parts[2] if len(parts) > 2 else '',
                    })
        result['installed_modules'] = modules
        result['installed_count']   = len(modules)
    else:
        result['installed_modules'] = []
        result['installed_count']   = 0

    return result


# ── Dispatch table ────────────────────────────────────────────────────────────
ACTIONS = {
    'ping':               action_ping,
    'get_disk_usage':     action_get_disk_usage,
    'get_logs':           action_get_logs,
    'get_rclone_log':     action_get_rclone_log,
    'get_rclone_remotes': action_get_rclone_remotes,
    'trigger_backup':     action_trigger_backup,
    'get_backup_config':    action_get_backup_config,
    'set_backup_config':    action_set_backup_config,
    'update_backup_script':       action_update_backup_script,
    'check_backup_script_update': action_check_backup_script_update,
    'service_status':     action_service_status,
    'service_control':    action_service_control,
    'get_journal':        action_get_journal,
    'get_odoo_info':      action_get_odoo_info,
    'read_odoo_conf':     action_read_odoo_conf,
    'get_health':         action_get_health,
    'dump_database':      action_dump_database,
    'list_backups':     action_list_backups,
    'verify_backup':    action_verify_backup,
    'restore_backup':        action_restore_backup,
    'test_rclone_remote':    action_test_rclone_remote,
    'rclone_about':          action_rclone_about,
    'sync_destinations':     action_sync_destinations,
    'create_rclone_remote':  action_create_rclone_remote,
    'delete_rclone_remote':  action_delete_rclone_remote,
    'list_databases':         action_list_databases,
    'list_ssh_keys':          action_list_ssh_keys,
    'add_ssh_key':            action_add_ssh_key,
    'remove_ssh_key':         action_remove_ssh_key,
    'get_maintenance_status': action_get_maintenance_status,
    'enable_maintenance':     action_enable_maintenance,
    'disable_maintenance':    action_disable_maintenance,
    'get_webhook_config':     action_get_webhook_config,
    'set_webhook_config':     action_set_webhook_config,
    'test_webhook':           action_test_webhook,
    'get_odoo_info':          action_get_odoo_info,
}

def dispatch(action, params, cfg):
    if action not in ACTIONS:
        raise ValueError(f'Unknown action: {action}. Allowed: {sorted(ACTIONS)}')
    return ACTIONS[action](params, cfg)

# ── WebSocket client loop ─────────────────────────────────────────────────────

MONITOR_INTERVAL = 300   # 5 minutes between checks
MONITOR_COOLDOWN = 7200  # 2-hour local cooldown per alert type (server also enforces cooldown)
DISK_MIN_PCT     = 70    # only send disk_warning if above this % (server checks its threshold)


async def _receive_commands(ws, cfg):
    """Handle incoming commands from the relay."""
    async for raw in ws:
        msg = json.loads(raw)
        if msg.get('type') != 'command':
            continue
        cmd_id = msg.get('id')
        action = msg.get('action', '')
        params = msg.get('params', {})
        log.info('Executing action: %s (id=%s)', action, cmd_id)
        try:
            data = dispatch(action, params, cfg)
            response = {'type': 'response', 'id': cmd_id, 'data': data}
        except Exception as e:
            log.warning('Action %s failed: %s', action, e)
            response = {'type': 'response', 'id': cmd_id, 'error': str(e)}
        await ws.send(json.dumps(response))


async def _monitor_loop(ws, cfg):
    """Background task: detect failures and push alert messages through the WebSocket."""
    cooldown = {}          # event_key -> monotonic timestamp of last alert sent
    await asyncio.sleep(30)  # brief startup delay

    while True:
        try:
            loop_time = asyncio.get_event_loop().time()

            # ── Odoo service check ────────────────────────────────────────────
            try:
                status = action_service_status({}, cfg)
                if not (status['active'] and status['http_ok']):
                    event = 'odoo_down'
                    if loop_time - cooldown.get(event, 0) >= MONITOR_COOLDOWN:
                        await ws.send(json.dumps({
                            'type': 'alert', 'event': event,
                            'data': {
                                'service_active': str(status['active']),
                                'http_ok':        str(status['http_ok']),
                                'status':         status.get('systemctl_status', 'unknown'),
                            },
                        }))
                        cooldown[event] = loop_time
                        log.info('[monitor] Alert sent: %s', event)
            except Exception as exc:
                log.warning('[monitor] Service check error: %s', exc)

            # ── Disk usage check ──────────────────────────────────────────────
            try:
                disk = action_get_disk_usage({}, cfg)
                worst = max(disk.values(), key=lambda x: x.get('used_pct', 0), default=None)
                if worst and worst.get('used_pct', 0) >= DISK_MIN_PCT:
                    event = 'disk_warning'
                    if loop_time - cooldown.get(event, 0) >= MONITOR_COOLDOWN:
                        info = worst
                        await ws.send(json.dumps({
                            'type': 'alert', 'event': event,
                            'data': {
                                'partition': next((k for k, v in disk.items() if v is info), '?'),
                                'path':      info['path'],
                                'used_pct':  info['used_pct'],
                                'used_gb':   f"{info['used_gb']} GB",
                                'free_gb':   f"{info['free_gb']} GB",
                                'total_gb':  f"{info['total_gb']} GB",
                            },
                        }))
                        cooldown[event] = loop_time
                        log.info('[monitor] Alert sent: disk_warning (%s%% on %s)', info['used_pct'], info['path'])
            except Exception as exc:
                log.warning('[monitor] Disk check error: %s', exc)

            # ── Last backup status check ──────────────────────────────────────
            try:
                log_path = cfg['backup_log']
                if os.path.isfile(log_path):
                    stdout, _, _ = _run(['tail', '-n', '300', log_path])
                    last_result = last_line = None
                    for line in reversed(stdout.splitlines()):
                        if 'BACKUP COMPLETED' in line or 'SUCCESS' in line.upper():
                            last_result, last_line = 'success', line[:40]
                            break
                        if 'FAILED' in line.upper() or 'ERROR' in line.upper():
                            last_result, last_line = 'failed', line[:40]
                            break
                    if last_result == 'failed':
                        event = 'backup_failed'
                        if loop_time - cooldown.get(event, 0) >= MONITOR_COOLDOWN:
                            await ws.send(json.dumps({
                                'type': 'alert', 'event': event,
                                'data': {'last_result': 'failed', 'log_excerpt': last_line or ''},
                            }))
                            cooldown[event] = loop_time
                            log.info('[monitor] Alert sent: backup_failed')
            except Exception as exc:
                log.warning('[monitor] Backup check error: %s', exc)

            # ── Health report (always emitted, every cycle) ───────────────────
            try:
                hr_status = action_service_status({}, cfg)
                hr_disk   = action_get_disk_usage({}, cfg)
                hr_worst  = max(hr_disk.values(), key=lambda x: x.get('used_pct', 0), default=None)
                await ws.send(json.dumps({
                    'type': 'health_report',
                    'data': {
                        'odoo_active': hr_status.get('active', False),
                        'http_ok':     hr_status.get('http_ok', False),
                        'disk_pct':    hr_worst.get('used_pct') if hr_worst else None,
                    },
                }))
            except Exception as exc:
                log.warning('[monitor] Health report error: %s', exc)

        except Exception as exc:
            log.warning('[monitor] Unexpected error: %s', exc)

        await asyncio.sleep(MONITOR_INTERVAL)

async def agent_loop(cfg):
    url = cfg['relay_url']
    api_key = cfg['api_key']

    if not api_key:
        log.error('api_key not set in %s — cannot connect', CONFIG_FILE)
        sys.exit(1)

    backoff = 2
    while True:
        try:
            log.info('Connecting to relay at %s', url)
            async with websockets.connect(url, ping_interval=30, ping_timeout=10) as ws:
                backoff = 2  # reset on successful connect

                # Auth
                await ws.send(json.dumps({'type': 'auth', 'api_key': api_key}))
                auth_reply = json.loads(await ws.recv())
                if not auth_reply.get('ok'):
                    log.error('Auth failed: %s', auth_reply.get('reason', 'unknown'))
                    await asyncio.sleep(60)
                    continue

                log.info('Authenticated as server %s (%s)',
                         auth_reply.get('server_id'), auth_reply.get('server_name'))

                # Run command handler and monitoring loop concurrently.
                # If either task finishes (command loop closed, WS error), cancel the other
                # and fall through to the reconnect logic.
                recv_task    = asyncio.create_task(_receive_commands(ws, cfg))
                monitor_task = asyncio.create_task(_monitor_loop(ws, cfg))
                done, pending = await asyncio.wait(
                    [recv_task, monitor_task],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                for task in pending:
                    task.cancel()
                # Re-raise any exception from the completed task so it's logged properly
                for task in done:
                    if task.exception():
                        raise task.exception()

        except (websockets.exceptions.ConnectionClosed,
                ConnectionRefusedError, OSError) as e:
            log.warning('Connection lost: %s — retrying in %ds', e, backoff)
        except Exception as e:
            log.error('Unexpected error: %s — retrying in %ds', e, backoff)

        await asyncio.sleep(backoff)
        backoff = min(backoff * 2, 60)  # exponential backoff, cap at 60s

# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    cfg = load_config()
    log.info('ServerChest agent starting (relay=%s)', cfg['relay_url'])
    try:
        asyncio.run(agent_loop(cfg))
    except (KeyboardInterrupt, SystemExit):
        log.info('Shutting down...')

if __name__ == '__main__':
    main()
