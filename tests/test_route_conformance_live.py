from __future__ import annotations

import json
from pathlib import Path

from dpone.ops.route_conformance_live import RouteConformanceLiveService
from dpone.ops.routes import RouteConformanceDatasetProfile, RouteConformanceLiveConfig
from dpone.ops.routes.conformance_live_adapters import InMemoryRouteConformanceLiveAdapter
from dpone.ops.routes.conformance_live_models import RouteConformanceLiveStep


def _payload(path: str) -> dict[str, object]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def test_route_conformance_live_public_exports_are_available() -> None:
    from dpone.ops import RouteConformanceLiveService as ExportedLiveService
    from dpone.ops.routes import RouteConformanceLiveReport

    assert ExportedLiveService is RouteConformanceLiveService
    assert RouteConformanceLiveReport.__name__ == "RouteConformanceLiveReport"


def test_route_conformance_live_default_registry_exposes_vendor_live_adapters() -> None:
    from dpone.ops.routes.conformance_live_adapters import default_live_adapter_registry

    registry = default_live_adapter_registry()

    assert "in_memory" in registry
    assert "vendor_live" in registry
    assert "docker" in registry


def test_route_conformance_live_service_runs_in_memory_adapter_and_writes_evidence(tmp_path: Path) -> None:
    report = RouteConformanceLiveService(adapter_registry={"in_memory": InMemoryRouteConformanceLiveAdapter()}).run(
        output_dir=tmp_path / "live",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        config=RouteConformanceLiveConfig(
            adapter="in_memory",
            dataset=RouteConformanceDatasetProfile(
                name="wide_live_contract",
                row_count=120,
                column_count=24,
                chunk_size=30,
                include_nested=True,
                include_schema_evolution=True,
            ),
            min_rows=100,
            min_columns=20,
            require_schema_evolution=True,
        ),
    )
    payload = _payload(report.json_path)

    assert payload["schema_version"] == "dpone.route_conformance_live.v1"
    assert payload["route"]["case_id"] == "mssql_to_clickhouse__incremental_merge"
    assert payload["passed"] is True
    assert payload["adapter"] == "in_memory"
    assert payload["live_steps"][0]["name"] == "seed_source"
    assert payload["verification"]["chunk_count"] == 4
    assert payload["conformance"]["schema_version"] == "dpone.route_conformance.v1"
    assert Path(payload["artifacts"]["live_source_snapshot"]).exists()
    assert Path(payload["artifacts"]["live_sink_snapshot"]).exists()
    assert "Route Conformance Live Runner" in Path(report.markdown_path).read_text(encoding="utf-8")


def test_route_conformance_live_service_blocks_when_sink_drift_is_observed(tmp_path: Path) -> None:
    report = RouteConformanceLiveService(
        adapter_registry={"in_memory": InMemoryRouteConformanceLiveAdapter(drift_mode="value")}
    ).run(
        output_dir=tmp_path / "live-drift",
        source="postgres",
        sink="mssql",
        strategy="incremental_merge",
        config=RouteConformanceLiveConfig(
            adapter="in_memory",
            dataset=RouteConformanceDatasetProfile(name="drift", row_count=16, column_count=10, chunk_size=4),
            min_rows=10,
            min_columns=8,
        ),
    )
    payload = _payload(report.json_path)

    assert payload["passed"] is False
    assert "typed_hash.mismatch" in payload["blockers"]
    assert payload["verification"]["mismatch_samples"][0]["column"] == "text_001"
    assert payload["live_steps"][-1]["status"] == "blocked"


def test_route_conformance_live_service_short_circuits_blocked_adapter_step(tmp_path: Path) -> None:
    class _BlockedAdapter:
        def seed_source(self, **kwargs: object) -> RouteConformanceLiveStep:
            del kwargs
            return RouteConformanceLiveStep(
                name="seed_source",
                status="blocked",
                summary="vendor-live opt-in is missing",
                blockers=("vendor_live.opt_in_missing",),
            )

        def apply_schema_evolution(self, **kwargs: object) -> RouteConformanceLiveStep:
            raise AssertionError("schema evolution should not run after a blocked seed")

        def execute_route(self, **kwargs: object) -> RouteConformanceLiveStep:
            raise AssertionError("route execution should not run after a blocked seed")

        def read_source_snapshot(self, **kwargs: object) -> object:
            raise AssertionError("source snapshot should not be read after a blocked seed")

        def read_sink_snapshot(self, **kwargs: object) -> object:
            raise AssertionError("sink snapshot should not be read after a blocked seed")

    report = RouteConformanceLiveService(adapter_registry={"blocked": _BlockedAdapter()}).run(
        output_dir=tmp_path / "blocked-live",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        config=RouteConformanceLiveConfig(
            adapter="blocked",
            dataset=RouteConformanceDatasetProfile(name="blocked", row_count=16, column_count=10, chunk_size=4),
            require_schema_evolution=True,
        ),
    )
    payload = _payload(report.json_path)

    assert payload["passed"] is False
    assert payload["conformance"] is None
    assert "vendor_live.opt_in_missing" in payload["blockers"]
    assert payload["live_steps"] == [
        {
            "name": "seed_source",
            "status": "blocked",
            "summary": "vendor-live opt-in is missing",
            "rows": 0,
            "blockers": ["vendor_live.opt_in_missing"],
        }
    ]


def test_vendor_live_adapter_runs_postgres_mssql_and_mssql_clickhouse_bindings(tmp_path: Path) -> None:
    from dpone.ops.routes.conformance_vendor_live import (
        MemoryRouteConformanceLiveStore,
        VendorLiveRouteBinding,
        VendorRouteConformanceLiveAdapter,
    )

    adapter = VendorRouteConformanceLiveAdapter(
        bindings=(
            VendorLiveRouteBinding(
                source="postgres",
                sink="mssql",
                strategy="incremental_merge",
                source_store=MemoryRouteConformanceLiveStore("postgres"),
                sink_store=MemoryRouteConformanceLiveStore("mssql"),
            ),
            VendorLiveRouteBinding(
                source="mssql",
                sink="clickhouse",
                strategy="incremental_merge",
                source_store=MemoryRouteConformanceLiveStore("mssql"),
                sink_store=MemoryRouteConformanceLiveStore("clickhouse"),
            ),
        ),
        require_opt_in=False,
    )

    for source, sink in (("postgres", "mssql"), ("mssql", "clickhouse")):
        report = RouteConformanceLiveService(adapter_registry={"vendor_live": adapter}).run(
            output_dir=tmp_path / f"{source}-to-{sink}",
            source=source,
            sink=sink,
            strategy="incremental_merge",
            config=RouteConformanceLiveConfig(
                adapter="vendor_live",
                dataset=RouteConformanceDatasetProfile(
                    name="wide_vendor_contract",
                    row_count=128,
                    column_count=32,
                    chunk_size=32,
                    include_nested=True,
                    include_schema_evolution=True,
                ),
                min_rows=100,
                min_columns=32,
                require_schema_evolution=True,
            ),
        )
        payload = _payload(report.json_path)

        assert payload["passed"] is True
        assert payload["adapter"] == "vendor_live"
        assert payload["route"]["case_id"] == f"{source}_to_{sink}__incremental_merge"
        assert payload["verification"]["source_rows"] == 128
        assert payload["verification"]["sink_rows"] == 128
        assert payload["schema_evolution"]["status"] == "verified"
        assert payload["live_steps"][1]["name"] == "apply_schema_evolution"
        assert payload["live_steps"][2]["name"] == "execute_route"


def test_route_conformance_live_service_blocks_unknown_adapter(tmp_path: Path) -> None:
    report = RouteConformanceLiveService(adapter_registry={}).run(
        output_dir=tmp_path / "missing-adapter",
        source="mssql",
        sink="clickhouse",
        strategy="incremental_merge",
        config=RouteConformanceLiveConfig(
            adapter="docker_mssql_clickhouse",
            dataset=RouteConformanceDatasetProfile(name="missing", row_count=16, column_count=8, chunk_size=4),
        ),
    )
    payload = _payload(report.json_path)

    assert payload["passed"] is False
    assert "adapter.docker_mssql_clickhouse.missing" in payload["blockers"]
