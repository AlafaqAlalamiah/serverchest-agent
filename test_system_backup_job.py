"""system_backup must run as a background job: a full /opt/odoo17 archive takes
far longer than the relay's 30s command timeout, and a synchronous run blocks
the agent's command channel for its whole duration."""
import os
import tarfile
import time

import pytest

import agent

FAKE_TAR = """#!/bin/sh
# GNU tar shim for macOS dev boxes: drop --ignore-failed-read, delegate to /usr/bin/tar
for a in "$@"; do [ "$a" = "--ignore-failed-read" ] || set -- "$@" "$a"; shift; done
exec /usr/bin/tar "$@"
"""

# Minimal `rclone copy <src> <remote:dir>` → copies into $FAKE_REMOTE_ROOT/<dir>
FAKE_RCLONE = """#!/bin/sh
while [ "$1" != "copy" ]; do shift; done
src="$2"; dest="${3#*:}"
mkdir -p "$FAKE_REMOTE_ROOT/$dest"
cp "$src" "$FAKE_REMOTE_ROOT/$dest/"
echo "Transferred:   	    1 KiB / 1 KiB, 100%, 1 KiB/s, ETA 0s"
"""


@pytest.fixture
def env(tmp_path, monkeypatch):
    bindir = tmp_path / 'bin'
    bindir.mkdir()
    for name, body in (('tar', FAKE_TAR), ('rclone', FAKE_RCLONE)):
        p = bindir / name
        p.write_text(body)
        p.chmod(0o755)
    monkeypatch.setenv('PATH', f'{bindir}:{os.environ["PATH"]}')
    remote_root = tmp_path / 'remote'
    monkeypatch.setenv('FAKE_REMOTE_ROOT', str(remote_root))
    src = tmp_path / 'odoo_home'
    src.mkdir()
    (src / 'odoo.conf').write_text('[options]\n')
    return {'src': str(src), 'remote_root': remote_root}


def _wait(job_id, timeout=30):
    deadline = time.time() + timeout
    while time.time() < deadline:
        st = agent.action_job_status({'job_id': job_id}, {})
        if st['status'] != 'running':
            return st
        time.sleep(0.1)
    raise AssertionError('job did not finish')


def test_system_backup_is_background_capable():
    assert 'system_backup' in agent.JOB_ACTIONS


def test_system_backup_job_uploads_archive_and_reports_location(env):
    started = agent.action_start_job(
        {'action': 'system_backup',
         'params': {'items': [env['src']], 'destination': 'fake:system-backups'}}, {})
    st = _wait(started['job_id'])

    assert st['status'] == 'success', st.get('error')
    res = st['result']
    assert res['path'] == f"fake:system-backups/{res['archive']}"
    uploaded = env['remote_root'] / 'system-backups' / res['archive']
    assert uploaded.is_file()
    with tarfile.open(uploaded) as tf:
        assert any(n.endswith('odoo_home/odoo.conf') for n in tf.getnames())
    assert not os.path.exists(os.path.join('/tmp/serverchest_sysbackup', res['archive']))


def test_system_backup_sync_call_still_works(env):
    # Older dashboards call it directly via dispatch (no job).
    res = agent.dispatch('system_backup',
                         {'items': [env['src']], 'destination': 'fake:system-backups'}, {})
    assert res['status'] == 'ok'
    assert (env['remote_root'] / 'system-backups' / res['archive']).is_file()
