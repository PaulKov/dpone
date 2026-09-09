from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest

from dpone.services.safe_sample_execution_plan import (
    AirflowDeploymentContext,
    SafeSampleExecutionPlanBuilder,
    SafeSampleSourceSnapshot,
)
from dpone.services.safe_sample_policy import (
    SafeSamplePolicyEvaluator,
    SafeSamplePolicySet,
    SampleRunRequest,
    SampleTarget,
    SourceSamplingCapabilityDetector,
    TemporaryTargetPlan,
)

_MSSQL_ALIASES = (
    "mssql",
    "MSSQL",
    "microsoft mssql",
    "microsoft_mssql",
    "odbc",
    "sqlserver",
    "sql_server",
    "sql-server",
)


def _target_plan() -> TemporaryTargetPlan:
    return TemporaryTargetPlan(
        mode="temporary",
        pipeline_id="orders_daily",
        process="orders_daily",
        sink_type="clickhouse",
        connection_ref="clickhouse_dev",
        original_table={"schema": "analytics", "name": "orders"},
        temporary_table={"schema": "dpone_tmp_production", "name": "orders_daily_abc123"},
        ttl_seconds=3600,
        cleanup_required=True,
        pii_policy="masked",
    )


def _init_fetch_delivery() -> dict[str, object]:
    return {
        "mode": "init_fetch",
        "artifact_registry_ref": "dpone-prod-artifacts",
        "identity": {"method": "kubernetes_workload_identity", "service_account": "dpone-runtime"},
        "source": {"artifact_registry_ref": "dpone-prod-artifacts"},
        "verify": {"checksums": "required", "attestations": "required_for_prod"},
    }


def _execution_plan():
    policy = SafeSamplePolicySet.default().for_environment("production")
    policy_result = SafeSamplePolicyEvaluator().evaluate(
        SampleRunRequest(sample_rows=1000, target=SampleTarget.TEMPORARY, environment="production"),
        policy,
        SourceSamplingCapabilityDetector(verified_route_ids=("mssql_clickhouse_incremental_merge_airflow_kpo",)).detect(
            _pipeline_source()
        ),
    )
    return SafeSampleExecutionPlanBuilder().build(
        sample_rows=1000,
        environment="production",
        policy_result=policy_result,
        temporary_target_plan=_target_plan(),
        deployment_context=AirflowDeploymentContext(
            release_id="sha256:" + "a" * 64,
            deployment_id="sha256:" + "b" * 64,
            deployment_type="environment",
            runnable=True,
            runtime_artifact_delivery=_init_fetch_delivery(),
            workload_packs=(),
            index_path=".dpone-cache/current/airflow-index.json",
            deployment_path=".dpone-cache/current/deployment.json",
        ),
        source_snapshot=SafeSampleSourceSnapshot(
            pipeline_id="orders_daily",
            path="pipeline.yaml",
            sha256="sha256:" + "c" * 64,
        ),
    )


def _pipeline_source(
    *,
    source_type: str = "mssql",
    sink_type: str = "clickhouse",
    strategy: str = "incremental_merge",
) -> dict[str, Any]:
    return {
        "kind": "dpone.batch.v1",
        "metadata": {"id": "orders_daily"},
        "processes": [
            {
                "name": "orders_daily",
                "source": {
                    "type": source_type,
                    "connection_ref": "mssql_dev",
                    "table": {"schema": "dbo", "name": "orders"},
                },
                "sink": {
                    "type": sink_type,
                    "connection_ref": "clickhouse_dev",
                    "table": {"schema": "analytics", "name": "orders"},
                    "strategy": {"mode": strategy, "unique_key": "id"},
                },
            }
        ],
    }


def test_mssql_clickhouse_safe_sample_copy_config_builder_reads_pipeline_source() -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.mssql_clickhouse_safe_sample_copier import MssqlClickHouseSafeSampleCopyConfigBuilder

    config = MssqlClickHouseSafeSampleCopyConfigBuilder().build_from_pipeline_source(
        _pipeline_source(),
        process_name="orders_daily",
    )
    schema = json.loads(
        Path("docs/schemas/gitops/mssql-clickhouse-safe-sample-copy-config.schema.json").read_text(encoding="utf-8")
    )

    payload = config.to_dict()
    jsonschema.validate(payload, schema)
    assert payload == {
        "schema": "dpone.mssql-clickhouse-safe-sample-copy-config.v1",
        "source_connection_ref": "mssql_dev",
        "source_table": {"schema": "dbo", "name": "orders"},
    }


@pytest.mark.parametrize("source_type", _MSSQL_ALIASES)
def test_mssql_clickhouse_safe_sample_copy_config_builder_accepts_source_aliases(source_type: str) -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import MssqlClickHouseSafeSampleCopyConfigBuilder

    config = MssqlClickHouseSafeSampleCopyConfigBuilder().build_from_pipeline_source(
        _pipeline_source(source_type=source_type),
        process_name="orders_daily",
    )

    assert config.source_connection_ref == "mssql_dev"
    assert config.source_table == {"schema": "dbo", "name": "orders"}


def test_mssql_clickhouse_safe_sample_copy_config_builder_rejects_route_mismatch() -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopyConfigBuilder,
        MssqlClickHouseSafeSampleCopyConfigError,
    )

    with pytest.raises(MssqlClickHouseSafeSampleCopyConfigError) as exc:
        MssqlClickHouseSafeSampleCopyConfigBuilder().build_from_pipeline_source(
            _pipeline_source(sink_type="postgres"),
            process_name="orders_daily",
        )

    assert exc.value.code == "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_ROUTE_UNSUPPORTED"
    assert exc.value.to_error()["schema"] == "dpone.error.v1"
    assert exc.value.to_error()["stage"] == "mssql_clickhouse_safe_sample_copier_config"


def test_mssql_clickhouse_safe_sample_copier_builds_fail_closed_certified_request() -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )

    copier = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    )

    payload = copier.copy(plan=_execution_plan(), target_plan=_target_plan(), init_fetch={"passed": True})
    schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-certified-copy-request.schema.json").read_text(encoding="utf-8")
    )
    data_copy_schema = json.loads(
        Path("docs/schemas/gitops/safe-sample-data-copy.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload["copy_request"], schema)
    jsonschema.validate(payload, data_copy_schema)
    assert payload["schema"] == "dpone.safe-sample-data-copy.v1"
    assert payload["status"] == "blocked"
    assert payload["copy_request"] == {
        "schema": "dpone.safe-sample-certified-copy-request.v1",
        "certification_id": "mssql_clickhouse_incremental_merge_airflow_kpo",
        "source": {
            "type": "mssql",
            "connection_ref": "mssql_dev",
            "table": {"schema": "dbo", "name": "orders"},
        },
        "sink": {
            "type": "clickhouse",
            "connection_ref": "clickhouse_dev",
            "temporary_table": {"schema": "dpone_tmp_production", "name": "orders_daily_abc123"},
        },
        "strategy": "incremental_merge",
        "sample_rows": 1000,
        "max_bytes": 1024**3,
        "timeout_seconds": 60,
        "source_read_only": True,
        "pii_policy": "masked",
        "proof": "route_certification:mssql_clickhouse_incremental_merge_airflow_kpo",
    }
    assert payload["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED"


def test_mssql_clickhouse_safe_sample_copier_delegates_to_injected_executor() -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )

    seen_requests: list[dict[str, Any]] = []

    class FakeExecutor:
        def copy(self, request: dict[str, Any]) -> dict[str, Any]:
            seen_requests.append(request)
            return {
                "status": "copied",
                "rows_read": 25,
                "rows_written": 25,
                "bytes_read": 4096,
                "access_token": "must-not-leak",
                "diagnostics": {"password": "must-not-leak", "batch_count": 1},
            }

    copier = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        ),
        executor=FakeExecutor(),
    )

    payload = copier.copy(plan=_execution_plan(), target_plan=_target_plan(), init_fetch={"passed": True})

    assert seen_requests[0]["certification_id"] == "mssql_clickhouse_incremental_merge_airflow_kpo"
    assert payload["status"] == "copied"
    assert payload["rows_read"] == 25
    assert payload["rows_written"] == 25
    assert payload["bytes_read"] == 4096
    assert payload["errors"] == []
    assert "must-not-leak" not in repr(payload)


def test_mssql_clickhouse_bounded_copy_executor_reads_and_writes_without_leaking_rows() -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import (
        MssqlClickHouseBoundedSafeSampleCopyExecutor,
        MssqlSafeSampleBatch,
    )

    reader_requests: list[dict[str, Any]] = []
    writer_batches: list[MssqlSafeSampleBatch] = []

    class FakeReader:
        def read(self, request: dict[str, Any]) -> MssqlSafeSampleBatch:
            reader_requests.append(request)
            return MssqlSafeSampleBatch(
                rows=(
                    {"id": 1, "email": "alice@example.test"},
                    {"id": 2, "email": "bob@example.test"},
                ),
                rows_read=2,
                bytes_read=512,
                diagnostics={"sampling": "pushdown_top", "password": "must-not-leak"},
            )

    class FakeWriter:
        def write(self, request: dict[str, Any], batch: MssqlSafeSampleBatch) -> dict[str, Any]:
            writer_batches.append(batch)
            return {
                "rows_written": batch.rows_read,
                "diagnostics": {
                    "temporary_table": request["sink"]["temporary_table"]["name"],
                    "token": "must-not-leak",
                },
            }

    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())

    result = MssqlClickHouseBoundedSafeSampleCopyExecutor(reader=FakeReader(), writer=FakeWriter()).copy(copy_request)

    assert reader_requests == [copy_request]
    assert writer_batches[0].rows_read == 2
    assert result == {
        "status": "copied",
        "rows_read": 2,
        "rows_written": 2,
        "bytes_read": 512,
        "diagnostics": {
            "source": {"sampling": "pushdown_top"},
            "sink": {"temporary_table": "orders_daily_abc123"},
        },
        "errors": [],
    }
    assert "alice@example.test" not in repr(result)
    assert "bob@example.test" not in repr(result)
    assert "must-not-leak" not in repr(result)


def test_mssql_clickhouse_bounded_copy_executor_blocks_budget_excess_before_writer() -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import (
        MssqlClickHouseBoundedSafeSampleCopyExecutor,
        MssqlSafeSampleBatch,
    )

    class OversizedReader:
        def read(self, request: dict[str, Any]) -> MssqlSafeSampleBatch:
            return MssqlSafeSampleBatch(
                rows=(),
                rows_read=int(request["sample_rows"]) + 1,
                bytes_read=int(request["max_bytes"]) + 1,
                diagnostics={},
            )

    class ExplodingWriter:
        def write(self, request: dict[str, Any], batch: MssqlSafeSampleBatch) -> dict[str, Any]:
            raise AssertionError("writer must not run when source sample exceeds budget")

    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())

    result = MssqlClickHouseBoundedSafeSampleCopyExecutor(reader=OversizedReader(), writer=ExplodingWriter()).copy(
        copy_request
    )

    assert result["status"] == "failed"
    assert result["rows_written"] == 0
    assert [error["code"] for error in result["errors"]] == [
        "DPONE_SAFE_SAMPLE_ROW_BUDGET_EXCEEDED",
        "DPONE_SAFE_SAMPLE_BYTE_BUDGET_EXCEEDED",
    ]


def test_mssql_clickhouse_bounded_copy_executor_rejects_non_read_only_request() -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import MssqlClickHouseBoundedSafeSampleCopyExecutor

    class ExplodingReader:
        def read(self, request: dict[str, Any]) -> Any:
            raise AssertionError("reader must not run for a non-read-only request")

    class ExplodingWriter:
        def write(self, request: dict[str, Any], batch: Any) -> Any:
            raise AssertionError("writer must not run for a non-read-only request")

    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())
    copy_request["source_read_only"] = False

    result = MssqlClickHouseBoundedSafeSampleCopyExecutor(reader=ExplodingReader(), writer=ExplodingWriter()).copy(
        copy_request
    )

    assert result["status"] == "failed"
    assert result["rows_read"] == 0
    assert result["rows_written"] == 0
    assert result["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_COPY_REQUEST_NOT_READ_ONLY"


def test_mssql_clickhouse_credential_resolving_executor_builds_ports_at_runtime() -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import (
        CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor,
        MssqlSafeSampleBatch,
    )

    @dataclass(frozen=True)
    class FakeResolvedConnection:
        credentials: dict[str, Any]
        safe_metadata: dict[str, Any]

    class FakeResolver:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def resolve(self, connection_ref: str) -> FakeResolvedConnection:
            self.calls.append(connection_ref)
            return FakeResolvedConnection(
                credentials={"connection_ref": connection_ref, "password": "must-not-leak"},
                safe_metadata={
                    "connection_ref": connection_ref,
                    "resolver": "vault_kv",
                    "resolved_version": 17 if connection_ref == "mssql_dev" else 23,
                    "password": "must-not-leak",
                },
            )

    class FakeReaderFactory:
        def __init__(self) -> None:
            self.credentials: list[dict[str, Any]] = []

        def create(self, credentials: Any):
            self.credentials.append(credentials)

            class Reader:
                def read(self, request: dict[str, Any]) -> MssqlSafeSampleBatch:
                    return MssqlSafeSampleBatch(
                        rows=({"id": 1, "email": "alice@example.test"},), rows_read=1, bytes_read=128
                    )

            return Reader()

    class FakeWriterFactory:
        def __init__(self) -> None:
            self.credentials: list[dict[str, Any]] = []

        def create(self, credentials: Any):
            self.credentials.append(credentials)

            class Writer:
                def write(self, request: dict[str, Any], batch: MssqlSafeSampleBatch) -> dict[str, Any]:
                    return {"rows_written": batch.rows_read, "diagnostics": {"password": "must-not-leak"}}

            return Writer()

    resolver = FakeResolver()
    reader_factory = FakeReaderFactory()
    writer_factory = FakeWriterFactory()
    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())

    result = CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor(
        credential_resolver=resolver,
        reader_factory=reader_factory,
        writer_factory=writer_factory,
    ).copy(copy_request)

    assert resolver.calls == ["mssql_dev", "clickhouse_dev"]
    assert reader_factory.credentials == [{"connection_ref": "mssql_dev", "password": "must-not-leak"}]
    assert writer_factory.credentials == [{"connection_ref": "clickhouse_dev", "password": "must-not-leak"}]
    assert result["status"] == "copied"
    assert result["diagnostics"]["credential_resolution"] == {
        "source": {"connection_ref": "mssql_dev", "resolver": "vault_kv", "resolved_version": 17},
        "sink": {"connection_ref": "clickhouse_dev", "resolver": "vault_kv", "resolved_version": 23},
    }
    assert "alice@example.test" not in repr(result)
    assert "must-not-leak" not in repr(result)


def test_mssql_clickhouse_credential_resolving_executor_fails_closed_before_io_on_resolution_error() -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import (
        CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor,
    )

    class FailingResolver:
        def resolve(self, connection_ref: str) -> Any:
            raise RuntimeError(f"vault token for {connection_ref} is must-not-leak")

    class ExplodingFactory:
        def create(self, credentials: Any) -> Any:
            raise AssertionError("ports must not be created when credential resolution fails")

    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())

    result = CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor(
        credential_resolver=FailingResolver(),
        reader_factory=ExplodingFactory(),
        writer_factory=ExplodingFactory(),
    ).copy(copy_request)

    assert result["status"] == "failed"
    assert result["rows_read"] == 0
    assert result["rows_written"] == 0
    assert result["errors"][0]["code"] == "DPONE_SAFE_SAMPLE_CREDENTIAL_RESOLUTION_FAILED"
    assert result["diagnostics"] == {"credential_resolution": {"failed_role": "source", "connection_ref": "mssql_dev"}}
    assert "must-not-leak" not in repr(result)


def test_mssql_clickhouse_safe_sample_copier_runs_through_registry_backed_runtime_path(tmp_path: Path) -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        build_mssql_clickhouse_safe_sample_copier_from_pipeline_source,
    )
    from dpone.services.safe_sample_data_copier_registry import (
        RegistryBackedSafeSampleDataCopier,
        SafeSampleDataCopierRegistry,
    )
    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunner
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    calls: list[str] = []

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            calls.append("init_fetch")
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("prepare_target")
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            calls.append("cleanup_target")
            return {"backend": "fake-clickhouse", "dropped": True}

    class FakeCopyExecutor:
        def copy(self, request: dict[str, Any]) -> dict[str, Any]:
            calls.append("copy_sample")
            assert request["source"]["connection_ref"] == "mssql_dev"
            assert request["sink"]["connection_ref"] == "clickhouse_dev"
            return {"status": "copied", "rows_read": 7, "rows_written": 7, "bytes_read": 2048}

    certified_copier = build_mssql_clickhouse_safe_sample_copier_from_pipeline_source(
        _pipeline_source(),
        process_name="orders_daily",
        executor=FakeCopyExecutor(),
    )
    registry = SafeSampleDataCopierRegistry.with_copiers(
        {"mssql_clickhouse_incremental_merge_airflow_kpo": certified_copier}
    )
    runner = SafeSampleRuntimeRunner(
        executor=SafeSampleRuntimeExecutor(
            artifact_fetcher=FakeArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
            data_copier=RegistryBackedSafeSampleDataCopier(registry),
        ),
        evidence_writer=SafeSampleRuntimeEvidenceWriter(),
    )

    report = runner.run(_execution_plan(), output_dir=tmp_path / "evidence").to_dict()

    assert calls == ["init_fetch", "prepare_target", "copy_sample", "cleanup_target"]
    assert report["execution_status"] == "succeeded"
    assert report["data_outcome"] == "passed"
    assert report["runtime_execution"]["data_copy"]["copy_request"]["source"]["table"] == {
        "schema": "dbo",
        "name": "orders",
    }
    assert report["runtime_execution"]["data_copy"]["rows_written"] == 7


def test_mssql_clickhouse_runtime_assembly_builds_registry_backed_copier(tmp_path: Path) -> None:
    from dpone.services.mssql_clickhouse_safe_sample_execution import MssqlSafeSampleBatch
    from dpone.services.mssql_clickhouse_safe_sample_runtime_assembly import (
        build_mssql_clickhouse_safe_sample_registry_from_pipeline_source,
    )
    from dpone.services.safe_sample_data_copier_registry import RegistryBackedSafeSampleDataCopier
    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunner
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    @dataclass(frozen=True)
    class FakeResolvedConnection:
        credentials: dict[str, Any]
        safe_metadata: dict[str, Any]

    class FakeResolver:
        def __init__(self) -> None:
            self.calls: list[str] = []

        def resolve(self, connection_ref: str) -> FakeResolvedConnection:
            self.calls.append(connection_ref)
            return FakeResolvedConnection(
                credentials={"connection_ref": connection_ref, "password": "must-not-leak"},
                safe_metadata={"connection_ref": connection_ref, "resolver": "vault_kv", "resolved_version": 7},
            )

    class FakeReaderFactory:
        def create(self, credentials: Any):
            assert credentials["connection_ref"] == "mssql_dev"

            class Reader:
                def read(self, request: dict[str, Any]) -> MssqlSafeSampleBatch:
                    return MssqlSafeSampleBatch(
                        rows=({"id": 1, "email": "alice@example.test"},), rows_read=1, bytes_read=256
                    )

            return Reader()

    class FakeWriterFactory:
        def create(self, credentials: Any):
            assert credentials["connection_ref"] == "clickhouse_dev"

            class Writer:
                def write(self, request: dict[str, Any], batch: MssqlSafeSampleBatch) -> dict[str, Any]:
                    return {
                        "rows_written": batch.rows_read,
                        "diagnostics": {"temporary_table": request["sink"]["temporary_table"]["name"]},
                    }

            return Writer()

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    resolver = FakeResolver()
    registry = build_mssql_clickhouse_safe_sample_registry_from_pipeline_source(
        _pipeline_source(),
        process_name="orders_daily",
        credential_resolver=resolver,
        reader_factory=FakeReaderFactory(),
        writer_factory=FakeWriterFactory(),
    )
    runner = SafeSampleRuntimeRunner(
        executor=SafeSampleRuntimeExecutor(
            artifact_fetcher=FakeArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
            data_copier=RegistryBackedSafeSampleDataCopier(registry),
        ),
        evidence_writer=SafeSampleRuntimeEvidenceWriter(),
    )

    report = runner.run(_execution_plan(), output_dir=tmp_path / "evidence").to_dict()

    assert resolver.calls == ["mssql_dev", "clickhouse_dev"]
    assert registry.has_copier_for(_execution_plan()) is True
    assert report["execution_status"] == "succeeded"
    assert report["data_outcome"] == "passed"
    assert report["runtime_execution"]["data_copy"]["diagnostics"]["credential_resolution"] == {
        "source": {"connection_ref": "mssql_dev", "resolver": "vault_kv", "resolved_version": 7},
        "sink": {"connection_ref": "clickhouse_dev", "resolver": "vault_kv", "resolved_version": 7},
    }
    assert "alice@example.test" not in repr(report)
    assert "must-not-leak" not in repr(report)


def test_mssql_safe_sample_sql_reader_builds_bounded_read_plan_without_secret_diagnostics() -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import MssqlSafeSampleSqlReader

    seen_plans: list[dict[str, Any]] = []

    class FakeClient:
        def fetch(self, plan: Any) -> dict[str, Any]:
            seen_plans.append(plan.to_dict())
            return {
                "rows": [{"id": 1, "email": "alice@example.test"}],
                "bytes_read": 128,
                "diagnostics": {"query_id": "q-1", "password": "must-not-leak"},
            }

    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())

    batch = MssqlSafeSampleSqlReader(client=FakeClient()).read(copy_request)
    schema = json.loads(Path("docs/schemas/gitops/mssql-safe-sample-read-plan.schema.json").read_text(encoding="utf-8"))

    jsonschema.validate(seen_plans[0], schema)
    assert seen_plans == [
        {
            "schema": "dpone.mssql-safe-sample-read-plan.v1",
            "sql": "SELECT TOP (@sample_rows) * FROM [dbo].[orders]",
            "parameters": {"sample_rows": 1000},
            "sample_rows": 1000,
            "max_bytes": 1024**3,
            "timeout_seconds": 60,
            "source_read_only": True,
            "source": {
                "type": "mssql",
                "connection_ref": "mssql_dev",
                "table": {"schema": "dbo", "name": "orders"},
            },
        }
    ]
    assert batch.rows == ({"id": 1, "email": "alice@example.test"},)
    assert batch.rows_read == 1
    assert batch.bytes_read == 128
    assert batch.diagnostics == {
        "query_template": "mssql_top_sample_v1",
        "source_table": {"schema": "dbo", "name": "orders"},
        "client": {"query_id": "q-1"},
    }
    assert "must-not-leak" not in repr(batch.diagnostics)


def test_mssql_safe_sample_sql_reader_rejects_non_read_only_request_before_client_call() -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import MssqlSafeSampleSqlReader

    class ExplodingClient:
        def fetch(self, plan: Any) -> Any:
            raise AssertionError("MSSQL client must not be called for a non-read-only request")

    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())
    copy_request["source_read_only"] = False

    with pytest.raises(ValueError, match="source_read_only=true"):
        MssqlSafeSampleSqlReader(client=ExplodingClient()).read(copy_request)


def test_clickhouse_safe_sample_sql_writer_builds_secret_free_insert_plan() -> None:
    jsonschema = pytest.importorskip("jsonschema")

    from dpone.services.clickhouse_safe_sample_sql_writer import ClickHouseSafeSampleSqlWriter
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import MssqlSafeSampleBatch

    seen_plans: list[dict[str, Any]] = []

    class FakeClient:
        def insert(self, plan: Any) -> dict[str, Any]:
            seen_plans.append(plan.to_dict())
            return {
                "rows_written": 2,
                "bytes_written": 256,
                "diagnostics": {"query_id": "q-2", "token": "must-not-leak"},
            }

    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())
    batch = MssqlSafeSampleBatch(
        rows=(
            {"id": 1, "email": "alice@example.test"},
            {"id": 2, "email": "bob@example.test"},
        ),
        rows_read=2,
        bytes_read=256,
    )

    result = ClickHouseSafeSampleSqlWriter(client=FakeClient()).write(copy_request, batch)
    schema = json.loads(
        Path("docs/schemas/gitops/clickhouse-safe-sample-insert-plan.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(seen_plans[0], schema)
    assert seen_plans == [
        {
            "schema": "dpone.clickhouse-safe-sample-insert-plan.v1",
            "sql": "INSERT INTO `dpone_tmp_production`.`orders_daily_abc123` (`email`, `id`) VALUES",
            "row_count": 2,
            "temporary_table": {"schema": "dpone_tmp_production", "name": "orders_daily_abc123"},
            "sink": {
                "type": "clickhouse",
                "connection_ref": "clickhouse_dev",
                "temporary_table": {"schema": "dpone_tmp_production", "name": "orders_daily_abc123"},
            },
        }
    ]
    assert result == {
        "rows_written": 2,
        "bytes_written": 256,
        "diagnostics": {
            "insert_template": "clickhouse_native_values_v1",
            "temporary_table": {"schema": "dpone_tmp_production", "name": "orders_daily_abc123"},
            "client": {"query_id": "q-2"},
        },
    }
    assert "alice@example.test" not in repr(seen_plans)
    assert "alice@example.test" not in repr(result)
    assert "must-not-leak" not in repr(result)


def test_clickhouse_safe_sample_sql_writer_rejects_inconsistent_row_columns_before_client_call() -> None:
    from dpone.services.clickhouse_safe_sample_sql_writer import ClickHouseSafeSampleSqlWriter
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import MssqlSafeSampleBatch

    class ExplodingClient:
        def insert(self, plan: Any) -> Any:
            raise AssertionError("inconsistent columns must fail before client I/O")

    request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())
    batch = MssqlSafeSampleBatch(
        rows=({"id": 1, "email": "alice@example.test"}, {"id": 2}),
        rows_read=2,
        bytes_read=64,
    )

    with pytest.raises(ValueError, match="same column set"):
        ClickHouseSafeSampleSqlWriter(client=ExplodingClient()).write(request, batch)


def test_clickhouse_safe_sample_sql_writer_rejects_missing_temporary_table_before_client_call() -> None:
    from dpone.services.clickhouse_safe_sample_sql_writer import ClickHouseSafeSampleSqlWriter
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import MssqlSafeSampleBatch

    class ExplodingClient:
        def insert(self, plan: Any) -> Any:
            raise AssertionError("ClickHouse client must not be called without a temporary table")

    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())
    copy_request["sink"]["temporary_table"] = {"schema": "dpone_tmp_production", "name": ""}

    with pytest.raises(ValueError, match="sink.temporary_table.name"):
        ClickHouseSafeSampleSqlWriter(client=ExplodingClient()).write(
            copy_request,
            MssqlSafeSampleBatch(rows=(), rows_read=0, bytes_read=0),
        )


def test_mssql_clickhouse_sql_port_factories_wire_resolved_credentials_into_copy_executor() -> None:
    from dpone.services.mssql_clickhouse_safe_sample_copier import (
        MssqlClickHouseSafeSampleCopier,
        MssqlClickHouseSafeSampleCopyConfig,
    )
    from dpone.services.mssql_clickhouse_safe_sample_execution import (
        CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor,
    )
    from dpone.services.mssql_clickhouse_safe_sample_runtime_assembly import (
        ClickHouseSafeSampleSqlWriterPortFactory,
        MssqlSafeSampleSqlReaderPortFactory,
    )

    class FakeResolver:
        def resolve(self, connection_ref: str) -> Any:
            return type(
                "Resolved",
                (),
                {
                    "credentials": {"connection_ref": connection_ref, "password": "must-not-leak"},
                    "safe_metadata": {"connection_ref": connection_ref, "resolver": "vault_kv"},
                },
            )()

    class FakeMssqlClientFactory:
        def __init__(self) -> None:
            self.credentials: list[dict[str, Any]] = []

        def create(self, credentials: Any) -> Any:
            self.credentials.append(dict(credentials))

            class Client:
                def fetch(self, plan: Any) -> dict[str, Any]:
                    return {
                        "rows": [{"id": 1, "email": "alice@example.test"}],
                        "rows_read": 1,
                        "bytes_read": 128,
                    }

            return Client()

    class FakeClickHouseClientFactory:
        def __init__(self) -> None:
            self.credentials: list[dict[str, Any]] = []

        def create(self, credentials: Any) -> Any:
            self.credentials.append(dict(credentials))

            class Client:
                def insert(self, plan: Any) -> dict[str, Any]:
                    return {"rows_written": plan.row_count, "bytes_written": 128}

            return Client()

    mssql_clients = FakeMssqlClientFactory()
    clickhouse_clients = FakeClickHouseClientFactory()
    copy_request = MssqlClickHouseSafeSampleCopier(
        MssqlClickHouseSafeSampleCopyConfig(
            source_connection_ref="mssql_dev",
            source_table={"schema": "dbo", "name": "orders"},
        )
    ).build_copy_request(plan=_execution_plan(), target_plan=_target_plan())

    result = CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor(
        credential_resolver=FakeResolver(),
        reader_factory=MssqlSafeSampleSqlReaderPortFactory(client_factory=mssql_clients),
        writer_factory=ClickHouseSafeSampleSqlWriterPortFactory(client_factory=clickhouse_clients),
    ).copy(copy_request)

    assert mssql_clients.credentials == [{"connection_ref": "mssql_dev", "password": "must-not-leak"}]
    assert clickhouse_clients.credentials == [{"connection_ref": "clickhouse_dev", "password": "must-not-leak"}]
    assert result["status"] == "copied"
    assert result["rows_read"] == 1
    assert result["rows_written"] == 1
    assert "alice@example.test" not in repr(result)
    assert "must-not-leak" not in repr(result)


def test_mssql_clickhouse_sql_registry_builder_assembles_concrete_ports_from_pipeline_source(
    tmp_path: Path,
) -> None:
    from dpone.services.mssql_clickhouse_safe_sample_runtime_assembly import (
        build_mssql_clickhouse_safe_sample_sql_registry_from_pipeline_source,
    )
    from dpone.services.safe_sample_data_copier_registry import RegistryBackedSafeSampleDataCopier
    from dpone.services.safe_sample_runtime_evidence import SafeSampleRuntimeEvidenceWriter
    from dpone.services.safe_sample_runtime_executor import SafeSampleRuntimeExecutor
    from dpone.services.safe_sample_runtime_runner import SafeSampleRuntimeRunner
    from dpone.services.safe_sample_target_lifecycle import TemporaryTargetLifecycleExecutor

    class FakeResolver:
        def resolve(self, connection_ref: str) -> Any:
            return type(
                "Resolved",
                (),
                {
                    "credentials": {"connection_ref": connection_ref, "password": "must-not-leak"},
                    "safe_metadata": {"connection_ref": connection_ref, "resolver": "vault_kv", "resolved_version": 3},
                },
            )()

    class FakeMssqlClientFactory:
        def create(self, credentials: Any) -> Any:
            class Client:
                def fetch(self, plan: Any) -> dict[str, Any]:
                    return {
                        "rows": [{"id": 1, "email": "alice@example.test"}],
                        "rows_read": 1,
                        "bytes_read": 128,
                        "diagnostics": {"query_id": "mssql-1"},
                    }

            return Client()

    class FakeClickHouseClientFactory:
        def create(self, credentials: Any) -> Any:
            class Client:
                def insert(self, plan: Any) -> dict[str, Any]:
                    return {
                        "rows_written": plan.row_count,
                        "bytes_written": 128,
                        "diagnostics": {"query_id": "ch-1"},
                    }

            return Client()

    class FakeArtifactFetcher:
        def fetch(self, plan: Any) -> dict[str, object]:
            return {"schema": "dpone.init-fetch-result.v1", "passed": True}

    class FakeTargetAdapter:
        def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse"}

        def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
            return {"backend": "fake-clickhouse", "dropped": True}

    registry = build_mssql_clickhouse_safe_sample_sql_registry_from_pipeline_source(
        _pipeline_source(),
        process_name="orders_daily",
        credential_resolver=FakeResolver(),
        mssql_client_factory=FakeMssqlClientFactory(),
        clickhouse_client_factory=FakeClickHouseClientFactory(),
    )
    runner = SafeSampleRuntimeRunner(
        executor=SafeSampleRuntimeExecutor(
            artifact_fetcher=FakeArtifactFetcher(),
            temporary_target_executor=TemporaryTargetLifecycleExecutor(adapter=FakeTargetAdapter()),
            data_copier=RegistryBackedSafeSampleDataCopier(registry),
        ),
        evidence_writer=SafeSampleRuntimeEvidenceWriter(),
    )

    report = runner.run(_execution_plan(), output_dir=tmp_path / "evidence").to_dict()

    assert report["execution_status"] == "succeeded"
    assert report["data_outcome"] == "passed"
    assert report["runtime_execution"]["data_copy"]["rows_read"] == 1
    assert report["runtime_execution"]["data_copy"]["rows_written"] == 1
    assert report["runtime_execution"]["data_copy"]["diagnostics"]["source"]["client"] == {"query_id": "mssql-1"}
    assert report["runtime_execution"]["data_copy"]["diagnostics"]["sink"]["client"] == {"query_id": "ch-1"}
    assert "alice@example.test" not in repr(report)
    assert "must-not-leak" not in repr(report)
