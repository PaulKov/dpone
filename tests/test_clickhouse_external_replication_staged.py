from __future__ import annotations

from dataclasses import replace
from types import SimpleNamespace

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.clickhouse_external_replication import ExternalArtifactReceipt, ExternalPublicationRequest
from dpone.runtime.sinks.clickhouse_external_replication_context import ExternalStagedContext
from dpone.runtime.sinks.clickhouse_external_replication_receipt import ExternalReplicationReceipt
from dpone.runtime.sinks.clickhouse_staged_load import ClickHouseStagedLoadService
from dpone.runtime.sinks.load_result import AtomicCommitOutcome, LoadResult


class _ExternalRouter:
    def __init__(self) -> None:
        self.events: list[str] = []
        self.context = _context()

    def is_external(self, config: object) -> bool:
        return True

    def stage_external(self, config: object, payload: object) -> ExternalStagedContext:
        self.events.append("stage")
        self.payload = payload
        return self.context

    def validate_external(self, context: ExternalStagedContext) -> object:
        assert context is self.context
        self.events.append("validate")
        return "token"

    def publish_external(self, context: ExternalStagedContext, validation: object) -> object:
        assert context is self.context and validation == "token"
        self.events.append("publish")
        return replace(context.staged_receipt, phase="COMMITTED")

    def external_result(self, receipt: object, *, staged_rows: int) -> LoadResult:
        return LoadResult(
            staged_rows, 0, staged_rows, staging_rows=staged_rows, commit_outcome=AtomicCommitOutcome.COMMITTED
        )

    def cleanup_external(self, context: ExternalStagedContext) -> None:
        assert context is self.context
        self.events.append("cleanup")

    def abort_external(self, context: ExternalStagedContext) -> None:
        assert context is self.context
        self.events.append("abort")


def _context() -> ExternalStagedContext:
    artifact = ExternalArtifactReceipt("binding", "a" * 64, 1, 1, "b" * 64, "c" * 64, True)
    request = ExternalPublicationRequest("cluster", "database", "target", "run", "d" * 64, artifact)
    receipt = ExternalReplicationReceipt(
        target_key=request.target_key,
        operation_id=request.operation_id,
        generation_id=request.generation_id,
        inventory_digest="f" * 64,
        plan_digest="d" * 64,
        artifact_sha256="a" * 64,
        member_ids=("member-a", "member-b"),
        authority_version=1,
        phase="STAGED",
    )
    return ExternalStagedContext(request, receipt, "target__candidate")


def _config() -> LoadConfig:
    return LoadConfig(
        source_conn_id="src",
        target_conn_id="dst",
        source_schema="s",
        source_table="t",
        target_schema="database",
        target_table="target",
        load_strategy=LoadStrategy.FULL_REFRESH,
    )


def test_staged_service_routes_external_lifecycle_without_local_table_mutation() -> None:
    router = _ExternalRouter()
    sink = SimpleNamespace(_full_refresh_publication=router)
    service = ClickHouseStagedLoadService(sink)
    handle = service.stage(_config(), SimpleNamespace(schema=(("id", "Int64"),)))
    token = service.validate(_config(), handle)
    result = service.finalize_validated(_config(), handle, token)
    service.cleanup(handle)

    assert handle.staging_config.target_table == "target__candidate"
    assert handle.staged_rows == 1
    assert result.commit_outcome is AtomicCommitOutcome.COMMITTED
    assert router.events == ["stage", "validate", "publish", "cleanup"]


def test_external_abort_uses_exact_context_cleanup() -> None:
    router = _ExternalRouter()
    service = ClickHouseStagedLoadService(SimpleNamespace(_full_refresh_publication=router))
    handle = service.stage(_config(), SimpleNamespace(schema=(("id", "Int64"),)))

    service.abort(handle)

    assert router.events == ["stage", "abort"]


def test_external_stage_does_not_query_the_local_connector() -> None:
    router = _ExternalRouter()
    connector = _ProbeConnector()
    payload = SimpleNamespace(schema=(("id", "Int64"),))
    sink = SimpleNamespace(_full_refresh_publication=router, connector=connector)

    handle = ClickHouseStagedLoadService(sink).stage(_config(), payload)

    assert router.payload is payload
    assert connector.calls == 0
    assert handle.staged_rows == 1
    assert router.events == ["stage"]


class _ProbeConnector:
    def __init__(self) -> None:
        self.calls = 0

    def get_records(self, query: str, as_dict: bool = False) -> list[dict[str, int]]:
        del query, as_dict
        self.calls += 1
        return []
