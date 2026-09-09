from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.readiness.nested_certification import NestedNormalizationCertificationService
from dpone.readiness.nested_live_certification import (
    NESTED_LIVE_REQUIRED_CHECKS,
    NestedLiveCertificationCase,
    NestedLiveCertificationRunner,
)
from dpone.readiness.nested_stress import NestedStressCertificationService
from dpone.runtime.artifacts import InMemoryRowsArtifact, StreamingRowsArtifact
from dpone.runtime.etl.nested_load import NestedLoadService
from dpone.runtime.lineage import LineageOptions
from dpone.runtime.normalization.child_quality import ChildQualityService
from dpone.runtime.normalization.fast_path import NestedSpillFastPathPlanner
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.snapshot_factory import ChildSnapshotStoreFactory, ChildSnapshotStoreOptions
from dpone.runtime.normalization.snapshot_sql import SqlChildSnapshotStore
from dpone.runtime.normalization.snapshot_store import JsonFileChildSnapshotStore
from dpone.runtime.sinks import LoadPayload
from tests.nested_package_test_support import (
    RecordingStagedSink,
    identity_evaluate,
    identity_prepare,
)


class _SqlExecutor:
    def __init__(self) -> None:
        self.statements: list[tuple[str, dict[str, object]]] = []
        self.committed_payload = '[{"order_id": 1, "sku": "A"}]'

    def execute(self, statement: str, params: dict[str, object]) -> None:
        self.statements.append((statement, params))

    def fetch_value(self, statement: str, params: dict[str, object]) -> object:
        self.statements.append((statement, params))
        return self.committed_payload


def test_sql_child_snapshot_store_renders_durable_stage_commit_rollback_contract() -> None:
    executor = _SqlExecutor()
    store = SqlChildSnapshotStore(dialect="postgres", table="etl_state.__dpone__child_snapshots", executor=executor)

    store.stage_snapshot(
        root_table="orders",
        child_table="order_lines",
        unique_key=["order_id", "sku"],
        keys=[{"order_id": 1, "sku": "A"}],
        load_id="01JZ0000000000000000000001",
    )
    committed = store.load_committed(root_table="orders", child_table="order_lines")
    store.commit_snapshot(root_table="orders", child_table="order_lines", load_id="01JZ0000000000000000000001")
    store.rollback_staged(root_table="orders", child_table="order_lines", load_id="01JZ0000000000000000000002")

    assert committed == [{"order_id": 1, "sku": "A"}]
    rendered_sql = "\n".join(statement for statement, _params in executor.statements)
    assert "DELETE FROM etl_state.__dpone__child_snapshots WHERE root_table = %(root_table)s" in rendered_sql
    assert "INSERT INTO etl_state.__dpone__child_snapshots" in rendered_sql
    assert "status = 'committed'" in rendered_sql
    assert executor.statements[0][1]["keys_json"] == '[{"order_id": 1, "sku": "A"}]'


def test_fast_path_planner_falls_back_until_each_sink_wire_is_certified() -> None:
    planner = NestedSpillFastPathPlanner()

    for sink_type, spill_format in (
        ("mssql", "tsv"),
        ("postgres", "tsv"),
        ("clickhouse", "json_each_row"),
        ("clickhouse", "tsv"),
        ("bigquery", "jsonl"),
        ("kafka", "ndjson"),
    ):
        decision = planner.plan(sink_type=sink_type, spill_format=spill_format)
        assert decision.artifact_kind == "streaming"
        assert decision.native_route is None


def test_nested_load_service_uses_typed_streaming_until_sink_wire_is_certified(tmp_path: Path) -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A", "qty": 2}]}]),
        schema=[("order_id", "integer"), ("items", "json")],
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={"sink_type": "mssql"},
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = "01JZ0000000000000000000001"

    class _Identity:
        def __init__(self) -> None:
            self.events: list[str] = []

        def mark_staged(self, *_args, **_kwargs) -> None:
            self.events.append("staged")

        def mark_committed(self, *_args, **_kwargs) -> None:
            self.events.append("committed")

    identity = _Identity()
    sink = RecordingStagedSink()

    result = NestedLoadService(load_identity_service=identity).load(
        load_config=load_config,
        payload=payload,
        extract_result=SimpleNamespace(),
        load_record=_Audit(),
        lineage_options=LineageOptions(enabled=True),
        nested_options=NestedNormalizationOptions.from_config(
            {
                "enabled": True,
                "materialization": "spill_to_disk",
                "spill_output_dir": str(tmp_path),
                "spill_output_format": "tsv",
                "split_paths": [{"path": "$.items[*]", "table": "order_lines", "unique_key": ["order_id", "sku"]}],
            }
        ),
        package_mutation=sink.mutation(),
        prepare_member=identity_prepare,
        evaluate_package=identity_evaluate,
    )

    assert isinstance(sink.artifacts["orders"], StreamingRowsArtifact)
    assert isinstance(sink.artifacts["order_lines"], StreamingRowsArtifact)
    assert result.reconciliation_metrics["nested_normalization"]["native_fast_paths"] == {}
    assert identity.events == ["staged", "committed"]


def test_nested_load_service_rolls_back_package_when_child_load_fails(tmp_path: Path) -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}]}]),
        schema=[("order_id", "integer"), ("items", "json")],
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={"sink_type": "mssql"},
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = "01JZ0000000000000000000001"

    class _Identity:
        def __init__(self) -> None:
            self.events: list[str] = []

        def mark_staged(self, *_args, **_kwargs) -> None:
            self.events.append("staged")

        def mark_committed(self, *_args, **_kwargs) -> None:
            self.events.append("committed")

        def mark_failed(self, *_args, **_kwargs) -> None:
            self.events.append("failed")

    identity = _Identity()
    sink = RecordingStagedSink(fail_stage_table="order_lines")

    with pytest.raises(RuntimeError, match="child load failed"):
        NestedLoadService(load_identity_service=identity).load(
            load_config=load_config,
            payload=payload,
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=NestedNormalizationOptions.from_config(
                {
                    "enabled": True,
                    "materialization": "spill_to_disk",
                    "spill_output_dir": str(tmp_path),
                    "spill_output_format": "tsv",
                    "split_paths": [{"path": "$.items[*]", "table": "order_lines", "unique_key": ["order_id", "sku"]}],
                }
            ),
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )

    assert identity.events == ["staged", "failed"]


def test_nested_load_preserves_primary_when_failure_audit_write_fails() -> None:
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}]}]),
        schema=[("order_id", "integer"), ("items", "json")],
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={"sink_type": "mssql"},
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = "01JZ0000000000000000000001"

    class _FailingAuditIdentity:
        def mark_staged(self, _record, *, extracted_rows: int) -> None:
            assert extracted_rows == 2

        def mark_failed(self, _record, exc: Exception) -> None:
            assert str(exc) == "primary child failure"
            raise RuntimeError("audit backend unavailable")

    sink = RecordingStagedSink(
        fail_stage_table="orders__items",
        fail_stage_error=ValueError("primary child failure"),
    )

    with pytest.raises(ValueError, match="primary child failure"):
        NestedLoadService(load_identity_service=_FailingAuditIdentity()).load(
            load_config=load_config,
            payload=payload,
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=NestedNormalizationOptions.from_config({"enabled": True}),
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )


def test_child_quality_detects_duplicate_keys_and_orphans() -> None:
    result = ChildQualityService().check_tables(
        root_table="orders",
        root_rows=[{"order_id": 1}, {"order_id": 2}],
        child_table="order_lines",
        child_rows=[
            {"order_id": 1, "sku": "A"},
            {"order_id": 1, "sku": "A"},
            {"order_id": 3, "sku": "B"},
        ],
        parent_key=["order_id"],
        child_unique_key=["order_id", "sku"],
    )

    assert result.status == "failed"
    assert result.duplicate_child_keys == 1
    assert result.orphan_child_rows == 1
    assert result.parent_rows == 2
    assert result.child_rows == 3


def test_nested_options_parse_child_snapshot_store_atomicity_and_quality_contracts() -> None:
    options = NestedNormalizationOptions.from_config(
        {
            "enabled": True,
            "child_snapshot_store": {
                "enabled": True,
                "backend": "postgres",
                "table": "etl_state.__dpone__child_snapshots",
                "path": ".dpone/state/child_snapshots.json",
            },
            "atomicity": {"mode": "single_transaction", "on_failure": "rollback_staged"},
            "child_quality": {"duplicate_child_key": "fail", "orphan_child_rows": "fail"},
        }
    )

    assert options.child_snapshot_store.enabled is True
    assert options.child_snapshot_store.backend == "postgres"
    assert options.child_snapshot_store.table == "etl_state.__dpone__child_snapshots"
    assert options.atomicity.mode == "single_transaction"
    assert options.atomicity.on_failure == "rollback_staged"
    assert options.child_quality.duplicate_child_key == "fail"
    assert options.child_quality.orphan_child_rows == "fail"


def test_child_snapshot_store_factory_creates_json_and_sql_stores(tmp_path: Path) -> None:
    executor = _SqlExecutor()
    factory = ChildSnapshotStoreFactory(executors={"postgres": executor})

    json_store = factory.create(
        ChildSnapshotStoreOptions(enabled=True, backend="json", path=str(tmp_path / "snapshots.json"))
    )
    sql_store = factory.create(
        ChildSnapshotStoreOptions(enabled=True, backend="postgres", table="etl_state.__dpone__child_snapshots")
    )

    assert json_store is not None
    assert sql_store is not None
    sql_store.stage_snapshot(
        root_table="orders",
        child_table="order_lines",
        unique_key=["order_id", "sku"],
        keys=[{"order_id": 1, "sku": "A"}],
        load_id="load-1",
    )
    assert "INSERT INTO etl_state.__dpone__child_snapshots" in "\n".join(stmt for stmt, _ in executor.statements)


def test_nested_load_service_commits_child_snapshot_only_after_success(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "state" / "child_snapshots.json"
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}]}]),
        schema=[("order_id", "integer"), ("items", "json")],
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={"sink_type": "mssql"},
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = "01JZ0000000000000000000001"

    class _Identity:
        def mark_staged(self, *_args, **_kwargs) -> None:
            return None

        def mark_committed(self, *_args, **_kwargs) -> None:
            return None

    sink = RecordingStagedSink()
    NestedLoadService(load_identity_service=_Identity()).load(
        load_config=load_config,
        payload=payload,
        extract_result=SimpleNamespace(),
        load_record=_Audit(),
        lineage_options=LineageOptions(enabled=True),
        nested_options=NestedNormalizationOptions.from_config(
            {
                "enabled": True,
                "split_paths": [{"path": "$.items[*]", "table": "order_lines", "unique_key": ["order_id", "sku"]}],
                "child_snapshot_store": {"enabled": True, "backend": "json", "path": str(snapshot_path)},
            }
        ),
        package_mutation=sink.mutation(),
        prepare_member=identity_prepare,
        evaluate_package=identity_evaluate,
    )

    committed = JsonFileChildSnapshotStore(snapshot_path).load_committed(root_table="orders", child_table="order_lines")
    assert committed == [{"order_id": 1, "sku": "A"}]


def test_nested_load_service_rolls_back_child_snapshot_after_failure(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "state" / "child_snapshots.json"
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}]}]),
        schema=[("order_id", "integer"), ("items", "json")],
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={"sink_type": "mssql"},
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = "01JZ0000000000000000000001"

    class _Identity:
        def mark_staged(self, *_args, **_kwargs) -> None:
            return None

        def mark_failed(self, *_args, **_kwargs) -> None:
            return None

    sink = RecordingStagedSink(fail_stage_table="order_lines", fail_stage_error=RuntimeError("boom"))
    with pytest.raises(RuntimeError, match="boom"):
        NestedLoadService(load_identity_service=_Identity()).load(
            load_config=load_config,
            payload=payload,
            extract_result=SimpleNamespace(),
            load_record=_Audit(),
            lineage_options=LineageOptions(enabled=True),
            nested_options=NestedNormalizationOptions.from_config(
                {
                    "enabled": True,
                    "split_paths": [{"path": "$.items[*]", "table": "order_lines", "unique_key": ["order_id", "sku"]}],
                    "child_snapshot_store": {"enabled": True, "backend": "json", "path": str(snapshot_path)},
                }
            ),
            package_mutation=sink.mutation(),
            prepare_member=identity_prepare,
            evaluate_package=identity_evaluate,
        )

    assert (
        JsonFileChildSnapshotStore(snapshot_path).load_committed(root_table="orders", child_table="order_lines") == []
    )


def test_nested_spill_load_commits_child_snapshot_before_native_file_cleanup(tmp_path: Path) -> None:
    snapshot_path = tmp_path / "state" / "child_snapshots.json"
    payload = LoadPayload(
        artifact=InMemoryRowsArtifact([{"order_id": 1, "items": [{"sku": "A"}]}]),
        schema=[("order_id", "integer"), ("items", "json")],
    )
    load_config = LoadConfig(
        source_conn_id="src",
        target_conn_id="sink",
        source_schema="api",
        source_table="orders",
        target_schema="landing",
        target_table="orders",
        load_strategy=LoadStrategy.INCREMENTAL_APPEND,
        unique_key=["order_id"],
        options={"sink_type": "mssql"},
    )

    class _Audit:
        run_id = "01JZ0000000000000000000000"
        load_id = "01JZ0000000000000000000001"

    class _Identity:
        def mark_staged(self, *_args, **_kwargs) -> None:
            return None

        def mark_committed(self, *_args, **_kwargs) -> None:
            return None

    sink = RecordingStagedSink()
    NestedLoadService(load_identity_service=_Identity()).load(
        load_config=load_config,
        payload=payload,
        extract_result=SimpleNamespace(),
        load_record=_Audit(),
        lineage_options=LineageOptions(enabled=True),
        nested_options=NestedNormalizationOptions.from_config(
            {
                "enabled": True,
                "materialization": "spill_to_disk",
                "spill_output_dir": str(tmp_path / "spill"),
                "spill_output_format": "tsv",
                "split_paths": [{"path": "$.items[*]", "table": "order_lines", "unique_key": ["order_id", "sku"]}],
                "child_snapshot_store": {"enabled": True, "backend": "json", "path": str(snapshot_path)},
            }
        ),
        package_mutation=sink.mutation(),
        prepare_member=identity_prepare,
        evaluate_package=identity_evaluate,
    )

    committed = JsonFileChildSnapshotStore(snapshot_path).load_committed(root_table="orders", child_table="order_lines")
    assert committed == [{"order_id": 1, "sku": "A"}]
    assert isinstance(committed[0]["order_id"], int)


def test_nested_live_certification_runner_writes_per_sink_evidence(tmp_path: Path) -> None:
    artifact = NestedLiveCertificationRunner(
        cases=(
            NestedLiveCertificationCase(sink_type="mssql", check=lambda: {"native_route": "bcp"}),
            NestedLiveCertificationCase(sink_type="clickhouse", check=lambda: {"native_route": "json_each_row"}),
            NestedLiveCertificationCase(sink_type="bigquery", available=lambda: False, check=lambda: {}),
        )
    ).run(output_dir=tmp_path)

    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    assert payload["status"] == "unverified"
    assert payload["sinks"]["mssql"]["status"] == "unverified"
    assert payload["sinks"]["clickhouse"]["details"] == {"native_route": "json_each_row"}
    assert payload["sinks"]["bigquery"]["status"] == "skipped"
    assert artifact.markdown_path.exists()


def test_nested_live_certification_passes_only_complete_data_path_evidence(tmp_path: Path) -> None:
    artifact = NestedLiveCertificationRunner(
        cases=(
            NestedLiveCertificationCase(
                sink_type="mssql",
                check=lambda: {
                    "native_route": "bcp",
                    "checks": {check: "passed" for check in NESTED_LIVE_REQUIRED_CHECKS},
                },
            ),
        )
    ).run(output_dir=tmp_path)

    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    assert payload["status"] == "passed"
    assert payload["sinks"]["mssql"]["status"] == "passed"
    assert payload["sinks"]["mssql"]["checks"] == {check: "passed" for check in NESTED_LIVE_REQUIRED_CHECKS}


def test_nested_live_certification_is_unverified_when_one_required_check_is_missing(tmp_path: Path) -> None:
    checks = {check: "passed" for check in NESTED_LIVE_REQUIRED_CHECKS}
    checks.pop("state_not_advanced_after_failure")

    artifact = NestedLiveCertificationRunner(
        cases=(
            NestedLiveCertificationCase(
                sink_type="clickhouse",
                check=lambda: {"native_route": "json_each_row", "checks": checks},
            ),
        )
    ).run(output_dir=tmp_path)

    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    assert payload["status"] == "unverified"
    assert payload["sinks"]["clickhouse"]["checks"]["state_not_advanced_after_failure"] == "missing"


def test_child_snapshot_store_factory_loads_sql_executors_from_runtime_options() -> None:
    executor = _SqlExecutor()
    factory = ChildSnapshotStoreFactory.from_runtime_options({"child_snapshot_executors": {"mssql": executor}})
    store = factory.create(
        ChildSnapshotStoreOptions(enabled=True, backend="mssql", table="etl_state.__dpone__child_snapshots")
    )

    assert store is not None
    store.stage_snapshot(
        root_table="orders",
        child_table="order_lines",
        unique_key=["order_id", "sku"],
        keys=[{"order_id": 7, "sku": "A"}],
        load_id="load-7",
    )
    assert executor.statements[0][1]["keys_json"] == '[{"order_id": 7, "sku": "A"}]'


def test_nested_stress_certification_writes_skewed_large_payload_evidence(tmp_path: Path) -> None:
    artifact = NestedStressCertificationService(max_rows=100000).run(
        output_dir=tmp_path,
        row_count=1000,
        root_table="orders",
        skew_pattern=(0, 1, 10, 1000),
        spill_output_format="json_each_row",
    )

    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))
    assert payload["status"] == "passed"
    assert payload["row_count"] == 1000
    assert payload["skew_pattern"] == [0, 1, 10, 1000]
    assert payload["max_child_rows_per_parent"] == 1000
    assert payload["spill_formats"]["orders"] == "json_each_row"
    assert payload["row_counts"]["order_lines"] > payload["row_count"]


def test_nested_certification_artifact_includes_final_industrial_sections(tmp_path: Path) -> None:
    artifact = NestedNormalizationCertificationService().certify(output_dir=tmp_path, row_count=50, root_table="orders")
    payload = json.loads(artifact.json_path.read_text(encoding="utf-8"))

    assert payload["status"] == "unverified"
    assert payload["checks"]["live_certification_harness"] == "unverified"
    assert payload["checks"]["child_schema_evolution_contract"] == "unverified"
    assert payload["checks"]["native_fast_path_execution_contract"] == "unverified"
    assert payload["industrial_evidence"]["child_schema_evolution"]["status"] == "unverified"
    assert payload["industrial_evidence"]["native_fast_path_execution"]["status"] == "unverified"
    assert payload["industrial_evidence"]["native_fast_path_execution"]["mssql"] == "typed_streaming_fallback"


def test_nested_live_readiness_workflow_is_manual_and_documents_required_sinks() -> None:
    workflow = Path(".github/workflows/nested-live-certification.yml")
    assert workflow.exists()
    content = workflow.read_text(encoding="utf-8")
    assert "workflow_dispatch:" in content
    assert "integration_nested_live" in content
    assert "Nested live readiness (not certification)" in content
    assert "nested-live-readiness-evidence" in content
    for sink in ("postgres", "mssql", "clickhouse", "kafka"):
        assert sink in content.lower()
