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
import json
import logging
import os
import re
import shutil
import subprocess
import sys

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
    subprocess.Popen(
        ['/bin/bash', script],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
        start_new_session=True,
    )
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

def action_service_control(params, cfg):
    svc = cfg['service_name']
    action = params.get('action', '')
    if action not in ('start', 'stop', 'restart'):
        raise ValueError(f'Invalid action: {action}')
    stdout, stderr, rc = _run(['sudo', 'systemctl', action, svc], timeout=60)
    return {'action': action, 'service': svc, 'rc': rc, 'output': stdout + stderr}

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


# ── Dispatch table ────────────────────────────────────────────────────────────
ACTIONS = {
    'ping':               action_ping,
    'get_disk_usage':     action_get_disk_usage,
    'get_logs':           action_get_logs,
    'get_rclone_log':     action_get_rclone_log,
    'get_rclone_remotes': action_get_rclone_remotes,
    'trigger_backup':     action_trigger_backup,
    'get_backup_config':  action_get_backup_config,
    'set_backup_config':  action_set_backup_config,
    'service_status':     action_service_status,
    'service_control':    action_service_control,
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
