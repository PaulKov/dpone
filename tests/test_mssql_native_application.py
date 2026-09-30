"""Production composition keeps source identity, target types and defaults closed."""

from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

import pytest

from dpone.runtime.mssql_native_application import (
    DefaultMssqlNativeRuntimeFactory,
    _NativeRuntimeAssembly,
    _sqlclient_credentials,
    _wire_schema,
)
from dpone.runtime.native_delivery_observations import BoundedNativeDeliveryObserver


def test_wire_schema_uses_admitted_target_types_in_source_order() -> None:
    preplan = SimpleNamespace(
        column_mapping=(),
        target_column_types=(
            ("text_value", "nvarchar(max)"),
            ("row_key", "bigint"),
            ("__dpone__load_id", "varchar(64)"),
        ),
    )
    assert _wire_schema((("row_key", "Int64"), ("text_value", "Nullable(String)")), preplan) == (
        ("row_key", "bigint"),
        ("text_value", "nvarchar(max) nullable"),
    )


def test_wire_schema_rejects_rename_or_missing_target_type() -> None:
    with pytest.raises(ValueError, match="renamed_wire"):
        _wire_schema(
            (("source_name", "Int64"),),
            SimpleNamespace(column_mapping=(("source_name", "target_name"),), target_column_types=()),
        )
    with pytest.raises(ValueError, match="target_type_missing"):
        _wire_schema(
            (("missing", "Int64"),),
            SimpleNamespace(column_mapping=(), target_column_types=(("other", "bigint"),)),
        )


def test_sqlclient_credentials_preserve_tls_policy_without_rendering_secret() -> None:
    credentials = _sqlclient_credentials(
        SimpleNamespace(
            host="db",
            port=1433,
            database="warehouse",
            user="writer",
            password="private",
            encrypt="yes",
            trust_server_certificate="no",
        )
    )
    assert credentials.encrypt is True
    assert credentials.trust_server_certificate is False
    assert "private" not in repr(credentials)


def test_default_factory_is_lazy_until_run() -> None:
    process = SimpleNamespace()
    runtime = DefaultMssqlNativeRuntimeFactory()(process)
    assert runtime._process_config is process
    assert runtime._observer_factory is None
    assert runtime._write_observer is None


@dataclass(frozen=True)
class _LoadConfig:
    options: dict[str, object]


def test_observer_factory_creates_one_observer_per_run_and_forwards_writer_sink(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    observers: list[BoundedNativeDeliveryObserver] = []
    assemblies: list[tuple[object, object]] = []
    committed: list[object] = []

    def writer_observer(_observation: object) -> None:
        return None

    def observer_factory() -> BoundedNativeDeliveryObserver:
        observer = BoundedNativeDeliveryObserver()
        observers.append(observer)
        return observer

    class Assembly:
        def __init__(self, _process, _config, *, observer, write_observer):
            assemblies.append((observer, write_observer))
            self.runtime = SimpleNamespace(
                run=lambda _prepared, *, owner: SimpleNamespace(
                    inserted_rows=1,
                    updated_rows=0,
                    final_rows=1,
                    extracted_rows=1,
                    details={"commit_receipt_id": owner},
                )
            )

    record = SimpleNamespace(run_id="run", load_id="load")
    identity = SimpleNamespace(
        start=lambda *_args, **_kwargs: record,
        mark_committed=lambda _record, result: committed.append(result),
        mark_failed=lambda *_args: pytest.fail("successful runtime must not be marked failed"),
    )
    process = SimpleNamespace(
        name="native",
        source_obj=object(),
        sink_obj=object(),
        load_identity_service=identity,
        ensure_runtime_bindings=lambda: None,
    )
    config = _LoadConfig(options={})
    monkeypatch.setattr(
        "dpone.runtime.mssql_native_application.MssqlTransactionAdmissionService.prepare",
        lambda *_args, **_kwargs: config,
    )
    monkeypatch.setattr("dpone.runtime.mssql_native_application._NativeRuntimeAssembly", Assembly)
    runtime = DefaultMssqlNativeRuntimeFactory(
        observer_factory=observer_factory,
        write_observer=writer_observer,
    )(process)

    runtime.run(config, owner="owner-1")
    runtime.run(config, owner="owner-2")

    assert len(observers) == 2
    assert assemblies == [(observers[0], writer_observer), (observers[1], writer_observer)]
    assert len(committed) == 2


def test_default_assembly_forwards_existing_sqlclient_writer_observer(monkeypatch: pytest.MonkeyPatch) -> None:
    observer = BoundedNativeDeliveryObserver()
    writer_observations: list[object] = []
    captured: dict[str, object] = {}
    identity = object()
    context = SimpleNamespace(recovery_bindings=None)

    def compose(plan, **options):
        captured.update(plan=plan, **options)
        return SimpleNamespace(stage_context=context, backend=SimpleNamespace(identity=identity))

    monkeypatch.setattr(
        "dpone.runtime.mssql_native_application_assembly.native_limits",
        lambda _config: SimpleNamespace(max_total_encoded_bytes=1),
    )
    monkeypatch.setattr(
        "dpone.runtime.mssql_native_application_assembly.native_import_backend",
        lambda _config: SimpleNamespace(value="mssql_sqlclient"),
    )
    monkeypatch.setattr(
        "dpone.runtime.mssql_native_application_assembly.native_verification_backend",
        lambda _config: SimpleNamespace(value="target_local"),
    )
    monkeypatch.setattr("dpone.runtime.mssql_native_application_assembly.native_window", lambda _config: None)
    monkeypatch.setattr(
        "dpone.runtime.mssql_native_application_assembly.native_sqlclient_layout_version", lambda _config: 1
    )
    monkeypatch.setattr("dpone.runtime.mssql_native_application_assembly.compose_sqlclient_stage_context", compose)
    assembly: Any = _NativeRuntimeAssembly.__new__(_NativeRuntimeAssembly)
    assembly.target = SimpleNamespace(bcp_path="bcp")
    assembly.store = object()
    assembly.wire = object()
    assembly.plan = object()
    assembly.work_dir = object()
    assembly._artifact = SimpleNamespace(iter_native_rows=lambda: iter(()))
    assembly.observer = observer
    assembly._write_observer = writer_observations.append
    assembly._recovery_only = True
    assembly.sink = object()
    assembly.admission = object()
    config = SimpleNamespace(target_database="db", staging_schema="stage", target_schema="target")

    binding = assembly._bindings(config, "owner", object(), object())

    assert binding.stage_context is context
    assert captured["observer"] is observer
    assert captured["write_observer"] == writer_observations.append


@pytest.mark.parametrize("raw_mode,schema_changed", [(True, False), (True, True), (False, False)])
def test_snapshot_profile_is_resolved_before_plan_and_preserves_legacy_binding(
    tmp_path, monkeypatch, raw_mode, schema_changed
):
    from dpone.contracts.clickhouse_raw_snapshot import raw_source_query_binding
    from dpone.runtime import mssql_native_application_assembly as assembly_module
    from tests.test_mssql_native_raw_snapshot_recovery import _config, _payload

    payload, legacy = _payload()
    profile = payload.artifact.raw_snapshot_profile
    events = []

    class Source:
        def __init__(self, *args, **kwargs):
            pass

        def snapshot_profile(self, config):
            events.append("profile")
            return profile if raw_mode else None

        def extract(self, config, **snapshot_options):
            if raw_mode:
                assert snapshot_options["expected_profile"] is profile
                assert snapshot_options["source_query_binding"] == raw_source_query_binding(
                    legacy.source_query_id, profile
                )
            else:
                assert snapshot_options == {}
            events.append("extract")
            return SimpleNamespace(
                artifact=payload.artifact,
                relation_schema=payload.relation_schema,
                relation_dialect=None,
                extraction_lifecycle=None,
            )

    real_plan = assembly_module._chunk_plan

    def plan(*args, **kwargs):
        assert events == ["profile"]
        events.append("plan")
        return real_plan(*args, **kwargs)

    config = _config()
    if not raw_mode:
        config.options["native_transfer"].pop("source_snapshot")
    preplan = SimpleNamespace(
        source_relation_identity=profile.relation_uuid,
        source_schema_sha256=bytes.fromhex(legacy.schema_fingerprint),
        target_column_types=payload.schema,
        column_mapping=(),
        target_mutation_plan=None,
    )
    config.options[assembly_module.ADMISSION_OPTION] = payload.mssql_transaction_admission
    config.options[assembly_module.MSSQL_SCHEMA_PREPLAN_OPTION] = preplan
    source = SimpleNamespace(
        connector=object(),
        fetch_schema_projection=lambda c: SimpleNamespace(
            relation_schema=payload.relation_schema, relation_metadata=None, projected_schema=(("id", "Int64", False),)
        ),
    )
    process = SimpleNamespace(source_obj=source, sink_obj=SimpleNamespace(connector=object()), raw_config={})
    policy = SimpleNamespace(work_dir=tmp_path / "work", checkpoint_dir=tmp_path / "state")
    monkeypatch.setattr(assembly_module, "ClickHouseNativeSource", Source)
    monkeypatch.setattr(assembly_module, "_chunk_plan", plan)
    monkeypatch.setattr(assembly_module.RuntimeStoragePolicy, "from_sources", lambda **kw: policy)
    monkeypatch.setattr(assembly_module.StoragePreflightService, "check", lambda *a: SimpleNamespace(passed=True))
    monkeypatch.setattr(_NativeRuntimeAssembly, "_initialize_runtime", lambda *a, **kw: None)
    payload.artifact.terminate = lambda outcome: SimpleNamespace(cleanup_succeeded=True)
    if schema_changed:
        preplan.source_schema_sha256 = b"x" * 32
        with pytest.raises(ValueError, match="source_snapshot_schema_changed"):
            _NativeRuntimeAssembly(process, config)
        assert events == ["profile"]
        return
    assembly = _NativeRuntimeAssembly(process, config)
    assert assembly.plan.source_query_id == (
        raw_source_query_binding(legacy.source_query_id, profile) if raw_mode else legacy.source_query_id
    )
    with assembly._source(config, None):
        pass
    assert events == ["profile", "plan", "extract"]
