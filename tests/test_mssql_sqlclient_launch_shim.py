"""Shim gate parsing units; actual guard/exec coverage is separately recorded."""

import pytest

from dpone.adapters.mssql_sqlclient_launch_shim import _main


def test_shim_rejects_missing_fixed_arguments():
    with pytest.raises(SystemExit):
        _main([])


def test_shim_rejects_descriptor_alias_before_any_gate_read(monkeypatch):
    argv = []
    strings = {
        "package-root": "/framework",
        "source-sha256": "a" * 64,
        "dotnet-host": "/runtime/dotnet",
        "runtime-root": "/runtime",
        "companion-root": "/companion",
        "worker-assembly": "/companion/Worker.dll",
        "build-manifest": "/manifest",
        "build-sha256": "b" * 64,
    }
    numbers = {
        "parent": 123,
        "address-space": 8 * 1024**3,
        "startup-deadline-ns": 10**18,
        "operation-deadline-ns": 10**18,
        "gate-fd": 40,
        "startup-fd": 41,
        "credentials-fd": 42,
        "session-fd": 43,
        "grant-fd": 44,
        "result-fd": 45,
        "input-fd": 41,
    }
    for key, value in (strings | numbers).items():
        argv.extend(["--" + key, str(value)])
    with pytest.raises(ValueError, match="descriptor_alias"):
        _main(argv)
