"""Unit tests for the module-zip validation logic in agent.py."""
import io
import zipfile

import pytest

from agent import _validate_module_zip


def make_zip(entries):
    """entries: dict of {archive_path: content_bytes}"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as zf:
        for name, content in entries.items():
            zf.writestr(name, content)
    return buf.getvalue()


def test_valid_module_zip():
    data = make_zip({
        'my_module/__manifest__.py': b"{'name': 'My Module', 'version': '1.0'}",
        'my_module/__init__.py': b'',
        'my_module/models/thing.py': b'',
    })
    assert _validate_module_zip(data) == 'my_module'


def test_legacy_openerp_manifest():
    data = make_zip({'old_mod/__openerp__.py': b"{}", 'old_mod/__init__.py': b''})
    assert _validate_module_zip(data) == 'old_mod'


def test_not_a_zip():
    with pytest.raises(ValueError, match='valid zip'):
        _validate_module_zip(b'this is not a zip file')


def test_empty_zip():
    with pytest.raises(ValueError, match='empty'):
        _validate_module_zip(make_zip({}))


def test_oversize_compressed():
    data = make_zip({'m/__manifest__.py': b'{}'})
    with pytest.raises(ValueError, match='too large'):
        _validate_module_zip(data, max_size=10)


def test_zip_bomb_uncompressed():
    data = make_zip({'m/__manifest__.py': b'{}', 'm/big.bin': b'\0' * 1024})
    with pytest.raises(ValueError, match='expands too large'):
        _validate_module_zip(data, max_uncompressed=100)


def test_path_traversal_rejected():
    data = make_zip({'m/__manifest__.py': b'{}', '../evil.py': b''})
    with pytest.raises(ValueError, match='Unsafe path'):
        _validate_module_zip(data)


def test_absolute_path_rejected():
    data = make_zip({'m/__manifest__.py': b'{}', '/etc/cron.d/evil': b''})
    with pytest.raises(ValueError, match='Unsafe path'):
        _validate_module_zip(data)


def test_multiple_top_level_dirs_rejected():
    data = make_zip({'a/__manifest__.py': b'{}', 'b/__manifest__.py': b'{}'})
    with pytest.raises(ValueError, match='exactly one'):
        _validate_module_zip(data)


def test_no_manifest_rejected():
    data = make_zip({'m/__init__.py': b''})
    with pytest.raises(ValueError, match='manifest'):
        _validate_module_zip(data)


def test_macosx_junk_ignored():
    data = make_zip({
        'my_module/__manifest__.py': b'{}',
        '__MACOSX/my_module/._junk': b'',
    })
    assert _validate_module_zip(data) == 'my_module'


def test_bad_module_name_rejected():
    data = make_zip({'my module!/__manifest__.py': b'{}'})
    with pytest.raises(ValueError, match='module name'):
        _validate_module_zip(data)
