from contextlib import contextmanager
from dataclasses import replace
from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace

from dpone.adapters.mssql_sqlclient_input_custody import FileSqlClientInputCustody
from dpone.app.mssql_sqlclient_fresh_chunk_executor import FreshSqlClientChunkExecutor
from dpone.app.mssql_sqlclient_native_capability_bundle import SqlClientNativeBindingCapabilities
from dpone.app.mssql_sqlclient_native_parent_composition import SqlClientNativeParentCapabilities
from dpone.app.mssql_sqlclient_native_runtime_composition import (
    SqlClientNativeImportCapabilities,
    SqlClientNativeInputCustody,
    compose_sqlclient_native_runtime_binding,
)
from dpone.contracts.mssql_native_chunks import EncodedNativeFile, NativeChunkPlan
from dpone.contracts.mssql_tds_directory import TdsDirectoryLimits
from dpone.ports.mssql_native_route_backend import NativeActorCapacity, SqlClientNativeRuntimeBinding


class Journal:
    data = {"version": 4, "identity": {"target_id": "t", "window_fingerprint": "w"}, "chunks": {}}

    def retirement_receipt(self):
        return None


class DurableCustody:
    def observe_or_advance(self, _request_sha, advance):
        return advance()


def test_import_custody_retains_encoded_file_without_materializing_it(tmp_path, monkeypatch):
    payload = b"streamed-input" * 100_000
    source = tmp_path / "source.bin"
    source.write_bytes(payload)
    storage = FileSqlClientInputCustody(tmp_path / "custody")
    custody = SqlClientNativeInputCustody(storage)
    plan = NativeChunkPlan("run", "target", "query", "window", "schema", "wire")
    encoded = EncodedNativeFile(source, 0, 10, len(payload), sha256(payload).hexdigest(), "a" * 64)

    def forbidden_read_bytes(_path: Path) -> bytes:
        raise AssertionError("production custody materialized the encoded chunk")

    monkeypatch.setattr(Path, "read_bytes", forbidden_read_bytes)
    monkeypatch.setattr(
        storage,
        "observe",
        lambda _receipt: (_ for _ in ()).throw(AssertionError("custody observation materialized the chunk")),
    )
    receipt = custody.retain(plan, encoded, "run-0-0", object())

    assert receipt.encoded_bytes == len(payload)
    assert receipt.file_sha256 == encoded.file_sha256
    monkeypatch.setattr(
        "dpone.app.mssql_sqlclient_native_runtime_composition.validate_native_chunk_receipt",
        lambda _receipt: SimpleNamespace(input_custody=receipt),
    )
    assert custody.observe(plan, object(), object()) == receipt


def test_root_assembles_closed_runtime_binding_without_opening_a_session(tmp_path):
    calls = []
    custody = FileSqlClientInputCustody(tmp_path / "custody")
    imports = SqlClientNativeImportCapabilities(
        custody=SqlClientNativeInputCustody(custody),
        executor=object.__new__(FreshSqlClientChunkExecutor),
        inspect_projection=lambda *args: None,
        failed_settlement=SimpleNamespace(settle=lambda *args: None),
        allocated_bytes=lambda: 0,
    )

    @contextmanager
    def open_imports():
        calls.append("open")
        yield imports

    parent = SqlClientNativeParentCapabilities(
        file_custody=custody,
        journal=Journal(),
        fence=lambda: 1,
        rollback_no_commit=lambda: {"proof": "ok"},
        inspection_index=object(),
        lifecycle_observer=object(),
        directory_observer=object(),
        evidence_reader=object(),
        retirement_state=object(),
        retirement_effects=object(),
        retirement_authorizations=object(),
        release_inputs=lambda value: "a" * 64,
        input_custody=DurableCustody(),
        checkpoint_advance=lambda request: None,
        directory_limits=TdsDirectoryLimits(2, 1, 2, 1),
        prepare_retirement=lambda request: None,
        close_retained=lambda: None,
    )
    binding = compose_sqlclient_native_runtime_binding(
        imports=imports,
        parent=parent,
        capacity=NativeActorCapacity(3, 1, 1, 1),
        implementation_sha256="a" * 64,
        open_import_capabilities=open_imports,
    )
    assert type(binding) is SqlClientNativeRuntimeBinding
    assert calls == []


def test_root_accepts_one_preconstructed_capability_bundle(tmp_path):
    custody = FileSqlClientInputCustody(tmp_path / "custody")
    imports = SqlClientNativeImportCapabilities(
        custody=SqlClientNativeInputCustody(custody),
        executor=object.__new__(FreshSqlClientChunkExecutor),
        inspect_projection=lambda *args: None,
        failed_settlement=SimpleNamespace(settle=lambda *args: None),
        allocated_bytes=lambda: 0,
    )
    parent = SqlClientNativeParentCapabilities(
        file_custody=custody,
        journal=Journal(),
        fence=lambda: 1,
        rollback_no_commit=lambda: {"proof": "ok"},
        inspection_index=object(),
        lifecycle_observer=object(),
        directory_observer=object(),
        evidence_reader=object(),
        retirement_state=object(),
        retirement_effects=object(),
        retirement_authorizations=object(),
        release_inputs=lambda value: "a" * 64,
        input_custody=DurableCustody(),
        checkpoint_advance=lambda request: None,
        directory_limits=TdsDirectoryLimits(2, 1, 2, 1),
        prepare_retirement=lambda request: None,
        close_retained=lambda: None,
    )

    @contextmanager
    def open_imports():
        yield imports

    bundle = SqlClientNativeBindingCapabilities(
        imports=imports,
        parent=parent,
        capacity=NativeActorCapacity(3, 1, 1, 1),
        implementation_sha256="a" * 64,
        open_import_capabilities=open_imports,
    )

    assert type(compose_sqlclient_native_runtime_binding(capabilities=bundle)) is SqlClientNativeRuntimeBinding


def test_root_rejects_ambiguous_bundle_and_legacy_arguments():
    bundle = SqlClientNativeBindingCapabilities(
        imports=object(),
        parent=object(),
        capacity=NativeActorCapacity(3, 1, 1, 1),
        implementation_sha256="a" * 64,
        open_import_capabilities=lambda: None,
    )

    try:
        compose_sqlclient_native_runtime_binding(capabilities=bundle, imports=object())
    except ValueError as error:
        assert str(error) == "mssql_native.sqlclient_capability_bundle_ambiguous"
    else:
        raise AssertionError("ambiguous capability construction accepted")


def test_root_rejects_different_import_and_parent_custody_roots(tmp_path):
    custody = FileSqlClientInputCustody(tmp_path / "import")
    imports = SqlClientNativeImportCapabilities(
        custody=SqlClientNativeInputCustody(custody),
        executor=object.__new__(FreshSqlClientChunkExecutor),
        inspect_projection=lambda *args: None,
        failed_settlement=SimpleNamespace(settle=lambda *args: None),
        allocated_bytes=lambda: 0,
    )
    parent = SqlClientNativeParentCapabilities(
        file_custody=FileSqlClientInputCustody(tmp_path / "parent"),
        journal=Journal(),
        fence=lambda: 1,
        rollback_no_commit=lambda: {"proof": "ok"},
        inspection_index=object(),
        lifecycle_observer=object(),
        directory_observer=object(),
        evidence_reader=object(),
        retirement_state=object(),
        retirement_effects=object(),
        retirement_authorizations=object(),
        release_inputs=lambda value: "a" * 64,
        input_custody=DurableCustody(),
        checkpoint_advance=lambda request: None,
        directory_limits=TdsDirectoryLimits(2, 1, 2, 1),
        prepare_retirement=lambda request: None,
        close_retained=lambda: None,
    )
    try:
        compose_sqlclient_native_runtime_binding(
            imports=imports,
            parent=parent,
            capacity=NativeActorCapacity(3, 1, 1, 1),
            implementation_sha256="a" * 64,
            open_import_capabilities=lambda: None,
        )
    except ValueError as error:
        assert str(error) == "mssql_native.sqlclient_custody_capability_mismatch"
    else:
        raise AssertionError("different custody capabilities accepted")


def test_backend_session_closes_context_when_custody_changes(tmp_path):
    events = []
    custody = FileSqlClientInputCustody(tmp_path / "expected")
    imports = SqlClientNativeImportCapabilities(
        custody=SqlClientNativeInputCustody(custody),
        executor=object.__new__(FreshSqlClientChunkExecutor),
        inspect_projection=lambda *args: None,
        failed_settlement=SimpleNamespace(settle=lambda *args: None),
        allocated_bytes=lambda: 0,
    )

    @contextmanager
    def changed_imports():
        try:
            yield SqlClientNativeImportCapabilities(
                custody=SqlClientNativeInputCustody(FileSqlClientInputCustody(tmp_path / "changed")),
                executor=object.__new__(FreshSqlClientChunkExecutor),
                inspect_projection=lambda *args: None,
                failed_settlement=imports.failed_settlement,
                allocated_bytes=lambda: 0,
            )
        finally:
            events.append("closed")

    parent = SqlClientNativeParentCapabilities(
        file_custody=custody,
        journal=Journal(),
        fence=lambda: 1,
        rollback_no_commit=lambda: {"proof": "ok"},
        inspection_index=object(),
        lifecycle_observer=object(),
        directory_observer=object(),
        evidence_reader=object(),
        retirement_state=object(),
        retirement_effects=object(),
        retirement_authorizations=object(),
        release_inputs=lambda value: "a" * 64,
        input_custody=DurableCustody(),
        checkpoint_advance=lambda request: None,
        directory_limits=TdsDirectoryLimits(2, 1, 2, 1),
        prepare_retirement=lambda request: None,
        close_retained=lambda: None,
    )
    binding = compose_sqlclient_native_runtime_binding(
        imports=imports,
        parent=parent,
        capacity=NativeActorCapacity(3, 1, 1, 1),
        implementation_sha256="a" * 64,
        open_import_capabilities=changed_imports,
    )
    try:
        with binding.open_backend():
            raise AssertionError("mismatched backend opened")
    except ValueError as error:
        assert str(error) == "mssql_native.sqlclient_custody_capability_mismatch"
    assert events == ["closed"]


def test_backend_session_rejects_factory_drift_after_first_open(tmp_path):
    events = []
    custody = FileSqlClientInputCustody(tmp_path / "custody")
    imports = SqlClientNativeImportCapabilities(
        custody=SqlClientNativeInputCustody(custody),
        executor=object.__new__(FreshSqlClientChunkExecutor),
        inspect_projection=lambda *args: None,
        failed_settlement=SimpleNamespace(settle=lambda *args: None),
        allocated_bytes=lambda: 0,
    )
    drifted = replace(imports, allocated_bytes=lambda: 1)
    snapshots = iter((imports, drifted))

    @contextmanager
    def open_imports():
        try:
            yield next(snapshots)
        finally:
            events.append("closed")

    parent = SqlClientNativeParentCapabilities(
        file_custody=custody,
        journal=Journal(),
        fence=lambda: 1,
        rollback_no_commit=lambda: {"proof": "ok"},
        inspection_index=object(),
        lifecycle_observer=object(),
        directory_observer=object(),
        evidence_reader=object(),
        retirement_state=object(),
        retirement_effects=object(),
        retirement_authorizations=object(),
        release_inputs=lambda value: "a" * 64,
        input_custody=DurableCustody(),
        checkpoint_advance=lambda request: None,
        directory_limits=TdsDirectoryLimits(2, 1, 2, 1),
        prepare_retirement=lambda request: None,
        close_retained=lambda: None,
    )
    binding = compose_sqlclient_native_runtime_binding(
        capabilities=SqlClientNativeBindingCapabilities(
            imports=imports,
            parent=parent,
            capacity=NativeActorCapacity(3, 1, 1, 1),
            implementation_sha256="a" * 64,
            open_import_capabilities=open_imports,
        )
    )

    with binding.open_backend():
        pass
    try:
        with binding.open_backend():
            raise AssertionError("drifted backend opened")
    except ValueError as error:
        assert str(error) == "mssql_native.sqlclient_import_capability_identity_changed"
    assert events == ["closed", "closed"]
