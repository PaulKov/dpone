"""Actual isolated Python source loading with harmless synthetic guard fixtures."""

from __future__ import annotations

import json
import os
import py_compile
import subprocess
import sys
from pathlib import Path

import pytest

from dpone.adapters.mssql_tds_launch_shim import (
    COORDINATOR_SOURCE_SHIM,
    DEPARTURE_SOURCE_SHIM,
    PERMISSION_GRANT_SOURCE_SHIM,
    SOURCE_SHIM,
)


def _write(path: Path, text: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
    return path


def _record(log: Path, value: str) -> str:
    return f"with open({str(log)!r}, 'a', encoding='utf-8') as trace:\n    trace.write({value!r} + '\\n')\n"


@pytest.fixture(params=["bulk", "coordinator", "departure", "permission"])
def tree(tmp_path, request):
    root = tmp_path / "source"
    dependencies = tmp_path / "dependencies"
    cwd = tmp_path / "cwd"
    userbase = tmp_path / "userbase"
    cache = tmp_path / "fresh-bytecode"
    log = tmp_path / "order.txt"
    flags = tmp_path / "flags.json"
    for directory in (root, dependencies, cwd, userbase, cache):
        directory.mkdir()
    _write(root / "dpone" / "__init__.py", _record(log, "trusted-package"))
    _write(root / "dpone" / "adapters" / "__init__.py", "")
    _write(root / "dpone" / "runtime" / "__init__.py", "")
    guard = _write(
        root / "dpone" / "adapters" / "mssql_tds_worker_guard.py",
        _record(log, "guard-source")
        + "def install_worker_guard(*, expected_parent_pid, max_address_space_bytes):\n"
        + "    assert expected_parent_pid == 123\n    assert max_address_space_bytes == 67108864\n"
        + "\n".join("    " + line for line in _record(log, "guard-installed").splitlines())
        + "\n",
    )
    bootstrap_name, shim = {
        "bulk": ("mssql_tds_worker_bootstrap.py", SOURCE_SHIM),
        "coordinator": ("mssql_tds_coordinator_bootstrap.py", COORDINATOR_SOURCE_SHIM),
        "departure": ("mssql_sqlclient_departure_bootstrap.py", DEPARTURE_SOURCE_SHIM),
        "permission": ("mssql_sqlclient_permission_grant_bootstrap.py", PERMISSION_GRANT_SOURCE_SHIM),
    }[request.param]
    bootstrap = _write(
        root / "dpone" / "app" / bootstrap_name,
        "import json, sys\n"
        + _record(log, "bootstrap-source")
        + "import dpone\nimport synthetic_optional\n"
        + f'with open({str(flags)!r}, "w") as output:\n'
        + '    json.dump({"isolated":sys.flags.isolated,"no_site":sys.flags.no_site,"no_user_site":sys.flags.no_user_site,"ignore_environment":sys.flags.ignore_environment,"argv":sys.argv[1:]}, output)\n',
    )
    optional = _write(dependencies / "synthetic_optional.py", _record(log, "optional-source"))
    _write(cwd / "dpone" / "__init__.py", _record(log, "BAD-cwd-package"))
    _write(cwd / "synthetic_optional.py", _record(log, "BAD-cwd-optional"))
    return dict(
        root=root,
        dependencies=dependencies,
        cwd=cwd,
        userbase=userbase,
        cache=cache,
        log=log,
        flags=flags,
        guard=guard,
        bootstrap=bootstrap,
        optional=optional,
        shim=shim,
    )


def _run(tree):
    env = os.environ.copy()
    env["PYTHONPATH"] = str(tree["cwd"])
    env["PYTHONUSERBASE"] = str(tree["userbase"])
    result = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-B",
            "-X",
            f"pycache_prefix={tree['cache']}",
            "-c",
            tree["shim"],
            "--package-root",
            str(tree["root"]),
            "--dependency-path",
            str(tree["dependencies"]),
            "--parent",
            "123",
            "--address-space",
            "67108864",
        ],
        cwd=tree["cwd"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    events = tree["log"].read_text().splitlines() if tree["log"].exists() else []
    return result, events


def test_current_guard_precedes_bootstrap_package_and_optional_imports(tree):
    result, events = _run(tree)
    assert result.returncode == 0, result.stderr
    assert events == ["guard-source", "guard-installed", "bootstrap-source", "trusted-package", "optional-source"]
    flags = json.loads(tree["flags"].read_text())
    assert {key: flags[key] for key in ("isolated", "no_site", "no_user_site", "ignore_environment")} == dict.fromkeys(
        ("isolated", "no_site", "no_user_site", "ignore_environment"), 1
    )
    assert flags["argv"] == ["--parent", "123", "--address-space", "67108864"]
    assert not list(tree["cache"].rglob("*.pyc"))


def test_site_hooks_and_pythonpath_shadow_are_not_executed(tree):
    bad = _record(tree["log"], "BAD-site-hook")
    _write(tree["dependencies"] / "sitecustomize.py", bad)
    _write(tree["dependencies"] / "usercustomize.py", bad)
    hook_module = _write(tree["dependencies"] / "synthetic_hook.py", bad)
    _write(tree["dependencies"] / "injected.pth", "import synthetic_hook\n")
    env = os.environ.copy()
    env["PYTHONUSERBASE"] = str(tree["userbase"])
    discovered = subprocess.run(
        [sys.executable, "-S", "-c", "import site; print(site.getusersitepackages())"],
        env=env,
        capture_output=True,
        text=True,
        check=True,
        timeout=5,
    )
    usersite = Path(discovered.stdout.strip())
    assert usersite.is_relative_to(tree["userbase"])
    _write(usersite / "synthetic_hook.py", hook_module.read_text())
    _write(usersite / "injected.pth", "import synthetic_hook\n")
    _write(usersite / "usercustomize.py", bad)
    result, events = _run(tree)
    assert result.returncode == 0, result.stderr
    assert not any(event.startswith("BAD-") for event in events)
    assert events[-1] == "optional-source"


def test_stale_unchecked_hash_framework_and_optional_bytecode_is_ignored(tree):
    stale = []
    for key in ("guard", "bootstrap", "optional"):
        path = tree[key]
        current = path.read_text()
        path.write_text(_record(tree["log"], "BAD-stale-" + key))
        cached = Path(
            py_compile.compile(str(path), doraise=True, invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH)
        )
        assert int.from_bytes(cached.read_bytes()[4:8], "little") == 1
        stale.append(cached)
        path.write_text(current)
    assert all(path.exists() for path in stale)
    # Confirm this is executable stale unchecked-hash bytecode, not a fixture
    # that Python would have invalidated even without the fresh cache prefix.
    control = subprocess.run(
        [
            sys.executable,
            "-I",
            "-S",
            "-B",
            "-c",
            "import sys; sys.path.insert(0, sys.argv[1]); import synthetic_optional",
            str(tree["dependencies"]),
        ],
        cwd=tree["cwd"],
        capture_output=True,
        text=True,
        timeout=10,
        check=False,
    )
    assert control.returncode == 0, control.stderr
    assert tree["log"].read_text().splitlines() == ["BAD-stale-optional"]
    tree["log"].unlink()
    result, events = _run(tree)
    assert result.returncode == 0, result.stderr
    assert events == ["guard-source", "guard-installed", "bootstrap-source", "trusted-package", "optional-source"]
    assert all(path.exists() for path in stale)


def test_guard_failure_prevents_bootstrap_and_optional_imports(tree):
    _write(
        tree["guard"],
        _record(tree["log"], "guard-source")
        + 'def install_worker_guard(**kwargs):\n    raise RuntimeError("synthetic-guard-rejected")\n',
    )
    result, events = _run(tree)
    assert result.returncode != 0
    assert "synthetic-guard-rejected" in result.stderr
    assert events == ["guard-source"]
    assert not tree["flags"].exists()


def test_missing_selected_bootstrap_never_falls_back_to_other_role(tree):
    other = (
        "mssql_tds_coordinator_bootstrap.py"
        if tree["bootstrap"].name == "mssql_tds_worker_bootstrap.py"
        else "mssql_tds_worker_bootstrap.py"
    )
    _write(tree["bootstrap"].with_name(other), _record(tree["log"], "BAD-other-role"))
    tree["bootstrap"].unlink()
    result, events = _run(tree)
    assert result.returncode != 0
    assert events == ["guard-source", "guard-installed"]
    assert not tree["flags"].exists()
