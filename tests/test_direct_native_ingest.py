from __future__ import annotations

from types import SimpleNamespace

from dpone.runtime.bulk_options import ClickHouseBulkOptionsResolver
from dpone.runtime.direct_ingest import (
    DIRECT_INGEST_SCHEMA_VERSION,
    ClickHouseDirectNativeCredentials,
    ClickHouseDirectNativeOptions,
    ClickHouseDirectNativeRunner,
    DirectIngestResolver,
    DirectIngestRouteRequest,
)


def test_auto_backend_uses_certified_direct_provider() -> None:
    decision = DirectIngestResolver(provider_loader=lambda: _provider(certified=True)).decide(
        DirectIngestRouteRequest(
            bulk_options=ClickHouseBulkOptionsResolver.resolve(
                {"clickhouse_bulk": {"mode": "native_tcp", "native_tcp": {"backend": "auto"}}}
            ),
            input_format="Native",
            route_certified=True,
        )
    )

    evidence = decision.to_evidence()

    assert decision.selected_backend == "direct"
    assert decision.release_gate == "green"
    assert evidence["schema_version"] == DIRECT_INGEST_SCHEMA_VERSION
    assert evidence["selected_backend"] == "direct"
    assert evidence["fallback_reason"] is None


def test_auto_backend_falls_back_to_client_when_provider_missing() -> None:
    decision = DirectIngestResolver(provider_loader=lambda: None).decide(
        DirectIngestRouteRequest(
            bulk_options=ClickHouseBulkOptionsResolver.resolve(
                {"clickhouse_bulk": {"mode": "native_tcp", "native_tcp": {"backend": "auto"}}}
            ),
            input_format="Native",
            route_certified=True,
        )
    )

    assert decision.selected_backend == "client"
    assert decision.release_gate == "warning"
    assert decision.fallback_reason == "direct_ingest_provider_missing"
    assert "direct_ingest_provider_missing" in decision.warnings


def test_forced_direct_backend_blocks_before_source_io_when_uncertified() -> None:
    decision = DirectIngestResolver(provider_loader=lambda: _provider(certified=False)).decide(
        DirectIngestRouteRequest(
            bulk_options=ClickHouseBulkOptionsResolver.resolve(
                {"clickhouse_bulk": {"mode": "native_tcp", "native_tcp": {"backend": "direct"}}}
            ),
            input_format="Native",
            route_certified=True,
        )
    )

    assert decision.selected_backend is None
    assert decision.release_gate == "blocked"
    assert "direct_ingest_required_unavailable" in decision.blockers
    assert "direct_ingest_backend_uncertified" in decision.blockers


def test_forced_direct_backend_allows_uncertified_route_in_advisory_mode() -> None:
    decision = DirectIngestResolver(provider_loader=lambda: _provider(certified=True)).decide(
        DirectIngestRouteRequest(
            bulk_options=ClickHouseBulkOptionsResolver.resolve(
                {"clickhouse_bulk": {"mode": "native_tcp", "native_tcp": {"backend": "direct"}}}
            ),
            input_format="Native",
            route_certified=False,
            certification_mode="advisory",
        )
    )

    assert decision.selected_backend == "direct"
    assert decision.certified is False
    assert decision.release_gate == "warning"
    assert decision.warnings == ("direct_ingest_route_uncertified_advisory",)
    assert not decision.blockers


def test_forced_direct_backend_blocks_uncertified_route_in_certified_only_mode() -> None:
    decision = DirectIngestResolver(provider_loader=lambda: _provider(certified=True)).decide(
        DirectIngestRouteRequest(
            bulk_options=ClickHouseBulkOptionsResolver.resolve(
                {"clickhouse_bulk": {"mode": "native_tcp", "native_tcp": {"backend": "direct"}}}
            ),
            input_format="Native",
            route_certified=False,
            certification_mode="certified_only",
        )
    )

    assert decision.selected_backend is None
    assert decision.release_gate == "blocked"
    assert "direct_ingest_required_unavailable" in decision.blockers
    assert "direct_ingest_route_certification_missing" in decision.blockers


def test_direct_runner_delegates_to_provider_without_exposing_password() -> None:
    captured: dict[str, object] = {}

    class Provider:
        def insert_clickhouse_native(self, request):
            captured["request"] = request
            captured["payload"] = b"".join(request["byte_stream"])
            return {"rows": 2, "query_id": "q-direct-1", "compressed_bytes": 17}

    runner = ClickHouseDirectNativeRunner(
        credentials=ClickHouseDirectNativeCredentials(
            host="ch.local",
            port=9000,
            database="default",
            user="svc",
            password="super-secret",
            secure=False,
        ),
        options=ClickHouseDirectNativeOptions(compression="lz4", timeout_seconds=30, query_id="q-direct-1"),
        provider_loader=lambda: Provider(),
    )

    result = runner.insert_stream("landing.orders", ["id"], [b"block-1", b"block-2"])

    request = captured["request"]
    assert result.rows == 2
    assert result.query_id == "q-direct-1"
    assert captured["payload"] == b"block-1block-2"
    assert request["table"] == "landing.orders"
    assert request["columns"] == ["id"]
    assert request["credentials"]["password"] == "super-secret"
    assert "super-secret" not in str(result.to_evidence())


def _provider(*, certified: bool) -> SimpleNamespace:
    return SimpleNamespace(
        direct_ingest_capabilities=lambda: {
            "schema_version": DIRECT_INGEST_SCHEMA_VERSION,
            "backends": [
                {
                    "backend_id": "clickhouse_native_tcp_direct",
                    "sink": "clickhouse",
                    "input_format": "Native",
                    "certified": certified,
                    "compression": ["none", "lz4", "zstd"],
                }
            ],
        },
        insert_clickhouse_native=lambda request: {"rows": 0},
    )
