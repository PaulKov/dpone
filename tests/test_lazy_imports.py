from __future__ import annotations

import subprocess
import sys
from pathlib import Path


def test_imports_do_not_require_optional_runtime_deps() -> None:
    """Importing dpone DAG tooling must not require optional connector deps.

    In CI environments (and for manifest-only usage) we often do not install
    heavy connector extras like google-cloud-bigquery.
    """

    for name in list(sys.modules):
        if name == "google" or name.startswith("google.") or name == "vault_kv_client":
            sys.modules.pop(name, None)

    import dpone  # noqa: F401
    import dpone.dag  # noqa: F401
    import dpone.runtime.connectors as _connectors  # noqa: F401
    import dpone.runtime.connectors.api as _api_connectors  # noqa: F401
    import dpone.runtime.connectors.api.appsflyer as _appsflyer  # noqa: F401
    import dpone.runtime.connectors.api.cbr as _cbr  # noqa: F401
    import dpone.runtime.connectors.api.fasttrack as _fasttrack  # noqa: F401
    import dpone.runtime.connectors.api.mindbox as _mindbox  # noqa: F401
    import dpone.runtime.sinks.strategies.bigquery.bigquery_base as _bq_base  # noqa: F401
    import dpone.runtime.sources.api.appsflyer as _appsflyer_source  # noqa: F401
    import dpone.runtime.sources.api.cbr as _cbr_source  # noqa: F401
    import dpone.runtime.sources.api.fasttrack as _fasttrack_source  # noqa: F401
    import dpone.runtime.sources.api.mindbox as _mindbox_source  # noqa: F401

    # Optional deps should not be imported implicitly.
    assert "google" not in sys.modules
    assert "google.cloud" not in sys.modules
    assert "vault_kv_client" not in sys.modules


def test_ci_snapshot_version_imports_do_not_pull_requests() -> None:
    cached_requests_modules = {
        name: module for name, module in sys.modules.items() if name == "requests" or name.startswith("requests.")
    }
    for name in cached_requests_modules:
        sys.modules.pop(name, None)

    try:
        import dpone.services.ci as _ci  # noqa: F401
        import dpone.services.ci.snapshot_version as _snapshot_version  # noqa: F401

        assert "requests" not in sys.modules
    finally:
        for name in list(sys.modules):
            if name == "requests" or name.startswith("requests."):
                sys.modules.pop(name, None)
        sys.modules.update(cached_requests_modules)


def test_legacy_vault_import_path_is_not_used() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    legacy_import = "_".join(("vault", "client"))
    search_roots = [
        repo_root / "src",
        repo_root / "tests",
        repo_root / "docs",
        repo_root / "README.md",
    ]
    offenders: list[str] = []
    for root in search_roots:
        paths = [root] if root.is_file() else [path for path in root.rglob("*") if path.is_file()]
        for path in paths:
            if (
                ".venv" in path.parts
                or ".pytest_cache" in path.parts
                or ".mypy_cache" in path.parts
                or "__pycache__" in path.parts
                or path.suffix == ".pyc"
            ):
                continue
            text = path.read_text(encoding="utf-8", errors="ignore")
            if legacy_import in text:
                offenders.append(str(path.relative_to(repo_root)))

    assert offenders == []


def test_airflow_connection_secret_gc_import_path_does_not_load_kubernetes_sdk() -> None:
    script = """
import sys
import dpone.commands.airflow_connection_secret_gc_cmd
import dpone_airflow_pack.connection_secret_lifecycle
assert not any(name == 'kubernetes' or name.startswith('kubernetes.') for name in sys.modules)
"""

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_airflow_artifact_delivery_help_path_does_not_load_storage_or_secret_sdks() -> None:
    script = """
import sys
import dpone.commands.airflow_self_service_cmd
for prefix in ('boto3', 'botocore', 'google.cloud.storage', 'azure.identity', 'azure.storage', 'vault_kv_client'):
    assert not any(name == prefix or name.startswith(prefix + '.') for name in sys.modules), prefix
"""

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_airflow_provider_loader_import_has_no_remote_or_secret_side_effects() -> None:
    script = """
import builtins
import socket

original_import = builtins.__import__
blocked_prefixes = (
    'boto3',
    'botocore',
    'google.cloud.storage',
    'azure.identity',
    'azure.storage',
    'vault_kv_client',
    'kubernetes',
)
attempted = []

def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    if any(name == prefix or name.startswith(prefix + '.') for prefix in blocked_prefixes):
        attempted.append(name)
        raise AssertionError(name)
    return original_import(name, globals, locals, fromlist, level)

def forbidden_socket(*args, **kwargs):
    raise AssertionError('provider loader import attempted network I/O')

builtins.__import__ = guarded_import
socket.socket = forbidden_socket
import dpone_airflow_pack.dag_loader
assert attempted == [], attempted
"""

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr


def test_base_cli_import_does_not_attempt_airflow_or_vault_runtime_imports() -> None:
    script = """
import builtins
import importlib

original_import = builtins.__import__
original_import_module = importlib.import_module
attempted = []
blocked_prefixes = ("airflow", "vault_kv_client")

def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    if any(name == prefix or name.startswith(prefix + ".") for prefix in blocked_prefixes):
        attempted.append(name)
        raise ModuleNotFoundError(name)
    return original_import(name, globals, locals, fromlist, level)

def guarded_import_module(name, package=None):
    if any(name == prefix or name.startswith(prefix + ".") for prefix in blocked_prefixes):
        attempted.append(name)
        raise ModuleNotFoundError(name)
    return original_import_module(name, package)

builtins.__import__ = guarded_import
importlib.import_module = guarded_import_module
import dpone.cli.main
assert attempted == [], attempted
"""

    completed = subprocess.run(
        [sys.executable, "-c", script],
        check=False,
        capture_output=True,
        text=True,
    )

    assert completed.returncode == 0, completed.stderr
