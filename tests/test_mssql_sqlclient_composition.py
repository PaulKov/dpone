from types import SimpleNamespace

import pytest

from dpone.contracts.mssql_native_chunks import NativeChunkPlan
from dpone.runtime.sinks.mssql_sqlclient_composition import (
    compose_sqlclient_stage_context,
    resolve_sqlclient_backend,
)


def _plan() -> NativeChunkPlan:
    return NativeChunkPlan("run", "target", "query", "window", "schema", "wire")


def test_resolver_lazy_loads_verified_companion_and_returns_matching_writer(monkeypatch):
    companion = SimpleNamespace(
        command=("dotnet", "/verified/companion.dll"),
        package_version="0.88.0",
        artifact_sha256="a" * 64,
        writer_identity_sha256="b" * 64,
        runtime_identity_sha256="c" * 64,
        runtime_major=10,
        protocol="dpone.mssql-sqlclient.ipc.v1",
        protocols=("dpone.mssql-sqlclient.ipc.v1", "dpone.mssql-sqlclient.ipc.v2"),
    )
    provider = SimpleNamespace(locate=lambda: companion)
    monkeypatch.setattr(
        "dpone.runtime.sinks.mssql_sqlclient_composition.import_module",
        lambda name: provider if name == "dpone_mssql_sqlclient" else pytest.fail(name),
    )

    def credentials(request):
        return SimpleNamespace()

    diagnostics = []
    resolved = resolve_sqlclient_backend(
        _plan(),
        timeout_seconds=60,
        credentials_provider=credentials,
        diagnostic_sink=diagnostics.append,
    )

    assert resolved.companion is companion
    assert resolved.identity.import_backend == "mssql_sqlclient"
    assert resolved.identity.companion_package_sha256 == "a" * 64
    assert resolved.writer._command == companion.command
    assert resolved.writer._diagnostic_sink == diagnostics.append


def test_resolver_fails_closed_when_optional_package_is_absent(monkeypatch):
    def missing(name):
        raise ImportError(name)

    monkeypatch.setattr("dpone.runtime.sinks.mssql_sqlclient_composition.import_module", missing)
    with pytest.raises(RuntimeError, match="mssql_sqlclient.optional_package_required"):
        resolve_sqlclient_backend(_plan(), timeout_seconds=60, credentials_provider=lambda request: None)


def test_resolver_rejects_incompatible_companion_minor_before_writer_creation(monkeypatch):
    companion = SimpleNamespace(package_version="0.86.0")
    provider = SimpleNamespace(locate=lambda: companion)
    monkeypatch.setattr("dpone.runtime.sinks.mssql_sqlclient_composition.import_module", lambda _name: provider)
    monkeypatch.setattr("dpone.runtime.sinks.mssql_sqlclient_composition.installed_version", lambda: "0.85.9")

    with pytest.raises(RuntimeError, match="mssql_sqlclient.incompatible_package_version"):
        resolve_sqlclient_backend(_plan(), timeout_seconds=60, credentials_provider=lambda request: None)


def test_resolver_accepts_same_minor_companion_patch(monkeypatch):
    companion = SimpleNamespace(
        command=("dotnet", "/verified/companion.dll"),
        package_version="0.85.7",
        artifact_sha256="a" * 64,
        writer_identity_sha256="b" * 64,
        runtime_identity_sha256="c" * 64,
        runtime_major=10,
        protocol="dpone.mssql-sqlclient.ipc.v1",
    )
    provider = SimpleNamespace(locate=lambda: companion)
    monkeypatch.setattr("dpone.runtime.sinks.mssql_sqlclient_composition.import_module", lambda _name: provider)
    monkeypatch.setattr("dpone.runtime.sinks.mssql_sqlclient_composition.installed_version", lambda: "0.85.3")

    resolved = resolve_sqlclient_backend(_plan(), timeout_seconds=60, credentials_provider=lambda request: None)

    assert resolved.companion is companion


def test_production_composition_resolves_backend_and_injects_identity_and_writer(monkeypatch):
    backend = SimpleNamespace(identity=object(), writer=object(), companion=object())
    observed = {}
    monkeypatch.setattr(
        "dpone.runtime.sinks.mssql_sqlclient_composition.resolve_sqlclient_backend",
        lambda plan, **options: observed.update(plan=plan, resolve=options) or backend,
    )
    monkeypatch.setattr(
        "dpone.runtime.sinks.mssql_sqlclient_composition.compose_native_stage_context",
        lambda **options: observed.update(compose=options) or "stage-context",
    )

    write_observer = object()
    diagnostic_sink = object()
    composed = compose_sqlclient_stage_context(
        plan=_plan(),
        timeout_seconds=60,
        credentials_provider=lambda request: None,
        write_observer=write_observer,
        diagnostic_sink=diagnostic_sink,
        persisted_hash_layout=True,
        store="store",
        lease="lease",
    )

    assert composed.stage_context == "stage-context"
    assert composed.backend is backend
    assert observed["resolve"]["write_observer"] is write_observer
    assert observed["resolve"]["diagnostic_sink"] is diagnostic_sink
    assert observed["resolve"]["layout_version"] == 2
    assert observed["compose"] == {
        "store": "store",
        "lease": "lease",
        "plan": _plan(),
        "verification_identity": backend.identity,
        "native_stage_writer": backend.writer,
        "target_local_timeout_seconds": 60,
        "persisted_hash_layout": True,
    }
