from __future__ import annotations

from types import SimpleNamespace

from dpone.runtime.object_storage_access import ObjectStorageReadContract
from dpone.runtime.route_capabilities import (
    CapabilityRequirement,
    RouteCandidate,
    RouteCapabilityPlanner,
    RuntimeCapability,
)
from dpone.runtime.sinks.clickhouse_capabilities import ClickHouseColumnarCapabilityProbe
from dpone.storage import ObjectStorageUri


def test_clickhouse_probe_records_version_and_supported_columnar_features() -> None:
    connector = _FakeClickHouse(version="24.8.1")

    evidence = ClickHouseColumnarCapabilityProbe(connector=connector).probe_columnar_pull(
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        sentinel_uri=ObjectStorageUri.parse("s3://bucket/msql/run-1/__dpone_sentinel.parquet"),
        use_cluster_function="s3Cluster",
        cluster="dwh",
        settings={"input_format_parquet_allow_missing_columns": False},
    )

    assert evidence["sink.object_storage_pull.s3"].passed is True
    assert evidence["sink.object_storage_pull.s3_cluster"].passed is True
    assert evidence["sink.auth.named_collection"].passed is True
    assert evidence["format.parquet.read"].passed is True
    assert evidence["sink.setting.input_format_parquet_allow_missing_columns"].passed is True
    assert {item.server_version for item in evidence.values()} == {"24.8.1"}


def test_clickhouse_probe_explains_s3cluster_fallback_when_cluster_function_is_missing() -> None:
    connector = _FakeClickHouse(version="22.3.1", fail_contains=("s3Cluster",))

    evidence = ClickHouseColumnarCapabilityProbe(connector=connector).probe_columnar_pull(
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        sentinel_uri=ObjectStorageUri.parse("s3://bucket/msql/run-1/__dpone_sentinel.parquet"),
        use_cluster_function="s3Cluster",
        cluster="dwh",
    )

    assert evidence["sink.object_storage_pull.s3"].passed is True
    assert evidence["sink.object_storage_pull.s3_cluster"].passed is False
    assert evidence["sink.object_storage_pull.s3_cluster"].blockers == ("sink.cluster_pull_unsupported",)
    assert "Use single-node object-storage pull" in evidence["sink.object_storage_pull.s3_cluster"].alternatives[0]


def test_clickhouse_probe_reuses_successful_object_storage_preflight_without_second_s3_read() -> None:
    connector = _FakeClickHouse(version="26.2.1")

    evidence = ClickHouseColumnarCapabilityProbe(connector=connector).probe_columnar_pull_from_preflight(
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        access_evidence=SimpleNamespace(passed=True, checks=("clickhouse_read",)),
        cluster="dwh",
        require_cluster_read=True,
        settings={"input_format_parquet_allow_missing_columns": False},
    )

    assert evidence["sink.object_storage_pull.s3"].passed is True
    assert evidence["sink.object_storage_pull.s3_cluster"].passed is True
    assert evidence["sink.auth.named_collection"].passed is True
    assert evidence["format.parquet.read"].passed is True
    assert not any("s3(" in query or "s3Cluster(" in query for query in connector.queries)


def test_clickhouse_probe_blocks_missing_named_collection() -> None:
    connector = _FakeClickHouse(version="24.8.1", fail_contains=("missing_collection",))

    evidence = ClickHouseColumnarCapabilityProbe(connector=connector).probe_columnar_pull(
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="missing_collection"),
        sentinel_uri=ObjectStorageUri.parse("s3://bucket/msql/run-1/__dpone_sentinel.parquet"),
        use_cluster_function="s3",
        cluster=None,
    )

    assert evidence["sink.auth.named_collection"].passed is False
    assert evidence["sink.auth.named_collection"].blockers == ("sink.auth.named_collection_missing",)
    assert "Create the required named collection" in evidence["sink.auth.named_collection"].alternatives[0]


def test_route_planner_selects_s3_pull_when_s3cluster_is_unsupported() -> None:
    evidence = ClickHouseColumnarCapabilityProbe(
        connector=_FakeClickHouse(version="22.3.1", fail_contains=("s3Cluster",))
    ).probe_columnar_pull(
        read_contract=ObjectStorageReadContract(mode="named_collection", named_collection="dpone_stage"),
        sentinel_uri=ObjectStorageUri.parse("s3://bucket/msql/run-1/__dpone_sentinel.parquet"),
        use_cluster_function="s3Cluster",
        cluster="dwh",
    )
    s3_cluster = RouteCandidate(
        route_id="object_storage_pull_s3cluster",
        requirements=(
            _requirement("sink.object_storage_pull.s3_cluster", RuntimeCapability.CLUSTER),
            _requirement("sink.auth.named_collection", RuntimeCapability.AUTH),
            _requirement("format.parquet.read", RuntimeCapability.FILE_FORMAT),
        ),
        priority=10,
    )
    s3_single = RouteCandidate(
        route_id="object_storage_pull_s3",
        requirements=(
            _requirement("sink.object_storage_pull.s3", RuntimeCapability.TRANSPORT),
            _requirement("sink.auth.named_collection", RuntimeCapability.AUTH),
            _requirement("format.parquet.read", RuntimeCapability.FILE_FORMAT),
        ),
        priority=20,
    )

    decision = RouteCapabilityPlanner().decide(candidates=(s3_cluster, s3_single), evidence=evidence, mode="auto")

    assert decision.selected_route_id == "object_storage_pull_s3"
    assert decision.fallback_reason == "sink.cluster_pull_unsupported"


def _requirement(requirement_id: str, domain: RuntimeCapability) -> CapabilityRequirement:
    return CapabilityRequirement(id=requirement_id, domain=domain, required_for="object_storage_pull")


class _FakeClickHouse:
    def __init__(self, *, version: str, fail_contains: tuple[str, ...] = ()) -> None:
        self.version = version
        self.fail_contains = fail_contains
        self.queries: list[str] = []

    def get_records(self, query: str):
        self.queries.append(query)
        if query == "SELECT version()":
            return [(self.version,)]
        return [(1,)]

    def execute_query(self, query: str):
        self.queries.append(query)
        if any(token in query for token in self.fail_contains):
            raise RuntimeError("unsupported")
        return 1
