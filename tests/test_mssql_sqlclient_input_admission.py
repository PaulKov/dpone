"""Borrowed FD admission detects observable changes without claiming ABA custody."""

import os
from dataclasses import replace

import pytest

from tests.test_mssql_sqlclient_launch import typed_descriptor


def test_parent_binding_and_offset_preserve_borrowed_descriptor(tmp_path):
    from dpone.adapters.mssql_sqlclient_input_admission import require_input_descriptor

    path = tmp_path / "input"
    path.write_bytes(b"payload")
    with path.open("rb") as stream:
        fd = stream.fileno()
        original = typed_descriptor(fd)
        assert require_input_descriptor(fd, original, parent=True) == original.file_identity
        duplicate = os.dup(fd)
        try:
            with pytest.raises(ValueError):
                require_input_descriptor(duplicate, original, parent=True)
            assert require_input_descriptor(duplicate, original) == original.file_identity
        finally:
            os.close(duplicate)
        stream.read(1)
        with pytest.raises(ValueError):
            require_input_descriptor(fd, original, parent=True)
        assert stream.tell() == 1
        assert os.fstat(fd).st_ino == original.file_identity.inode


def test_deep_alias_rejected_before_os_access(tmp_path, monkeypatch):
    from dpone.adapters.mssql_sqlclient_input_admission import require_input_descriptor

    path = tmp_path / "input"
    path.write_bytes(b"payload")
    with path.open("rb") as stream:
        value = typed_descriptor(stream.fileno())
        object.__setattr__(value.expected, "rows", True)
        monkeypatch.setattr(os, "fstat", lambda _: pytest.fail("OS access before nested validation"))
        with pytest.raises(ValueError):
            require_input_descriptor(stream.fileno(), value, parent=True)


def test_changed_identity_is_rejected_without_closing_original(tmp_path):
    from dpone.adapters.mssql_sqlclient_input_admission import require_input_descriptor

    path = tmp_path / "input"
    path.write_bytes(b"payload")
    with path.open("rb") as stream:
        value = typed_descriptor(stream.fileno())
        wrong = replace(value, file_identity=replace(value.file_identity, inode=value.file_identity.inode + 1))
        with pytest.raises(ValueError):
            require_input_descriptor(stream.fileno(), wrong, parent=True)
        assert not stream.closed
