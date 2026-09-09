from __future__ import annotations

from dpone.runtime.export_decision_cache import ExportDecisionCache, ExportDecisionCacheKey
from dpone.runtime.export_optimizer_models import (
    EXPORT_OPTIMIZER_SCHEMA_VERSION,
    ExportOptimizerPolicy,
    ExportProviderProbe,
)
from dpone.runtime.source_export_optimizer import SourceExportOptimizer
from dpone.runtime.sources.strategies.mssql.mssql_export_providers import MssqlExportProviderCatalog


def test_export_optimizer_policy_parses_public_snapshot_options() -> None:
    policy = ExportOptimizerPolicy.from_source_options(
        {
            "native_transfer": {
                "snapshot": {
                    "export_optimizer": {
                        "mode": "required",
                        "candidates": ["mssql_odbc_array", "mssql_bcp_native"],
                        "probe_rows": 200000,
                        "max_probe_seconds": 45,
                        "min_speedup_pct": 20,
                        "cache_policy": "route_schema_hash",
                        "rebenchmark_policy": "schema_or_source_shape_change",
                        "source_impact_policy": "throughput",
                        "bcp_probe_packets": [4096, 65535],
                        "odbc_fetch_size": 25000,
                    }
                }
            }
        }
    )

    assert policy.mode == "required"
    assert policy.candidates == ("mssql_odbc_array", "mssql_bcp_native")
    assert policy.probe_rows == 200000
    assert policy.max_probe_seconds == 45
    assert policy.min_speedup_pct == 20.0
    assert policy.source_impact_policy == "throughput"
    assert policy.bcp_probe_packets == (4096, 65535)
    assert policy.odbc_fetch_size == 25000


def test_auto_selects_fastest_safe_provider_above_threshold() -> None:
    decision = SourceExportOptimizer().decide(
        ExportOptimizerPolicy(min_speedup_pct=15),
        current_default="mssql_bcp_native",
        probes=[
            ExportProviderProbe(provider_id="mssql_bcp_native", rows_per_second=100_000, bytes_per_second=40_000_000),
            ExportProviderProbe(provider_id="mssql_odbc_array", rows_per_second=142_000, bytes_per_second=48_000_000),
            ExportProviderProbe(provider_id="mssql_driver_rowset", rows_per_second=80_000, bytes_per_second=20_000_000),
        ],
        cache_key=_cache_key(),
    )

    evidence = decision.to_evidence()

    assert decision.selected_provider == "mssql_odbc_array"
    assert decision.recommended_provider == "mssql_odbc_array"
    assert decision.measured_speedup_pct == 42.0
    assert decision.release_gate == "green"
    assert "export_optimizer_selected_faster_provider" in decision.reasons
    assert evidence["schema_version"] == EXPORT_OPTIMIZER_SCHEMA_VERSION
    assert evidence["source_bottleneck"] == "export"
    assert evidence["cache_key"] == _cache_key().fingerprint()


def test_auto_keeps_default_when_speedup_is_below_threshold() -> None:
    decision = SourceExportOptimizer().decide(
        ExportOptimizerPolicy(min_speedup_pct=15),
        current_default="mssql_bcp_native",
        probes=[
            ExportProviderProbe(provider_id="mssql_bcp_native", rows_per_second=100_000),
            ExportProviderProbe(provider_id="mssql_odbc_array", rows_per_second=110_000),
        ],
        cache_key=_cache_key(),
    )

    assert decision.selected_provider == "mssql_bcp_native"
    assert decision.recommended_provider == "mssql_odbc_array"
    assert decision.measured_speedup_pct == 10.0
    assert "export_optimizer_speedup_below_threshold" in decision.reasons


def test_unsafe_provider_is_rejected_even_when_faster() -> None:
    decision = SourceExportOptimizer().decide(
        ExportOptimizerPolicy(),
        current_default="mssql_bcp_native",
        probes=[
            ExportProviderProbe(provider_id="mssql_bcp_native", rows_per_second=100_000),
            ExportProviderProbe(
                provider_id="mssql_bcp_character_raw",
                rows_per_second=300_000,
                safe=False,
                blockers=("export_provider_requires_unsafe_escaping",),
            ),
        ],
        cache_key=_cache_key(),
    )

    assert decision.selected_provider == "mssql_bcp_native"
    assert decision.rejected["mssql_bcp_character_raw"] == "export_provider_requires_unsafe_escaping"


def test_benchmark_only_recommends_without_changing_runtime_provider() -> None:
    decision = SourceExportOptimizer().decide(
        ExportOptimizerPolicy(mode="benchmark_only"),
        current_default="mssql_bcp_native",
        probes=[
            ExportProviderProbe(provider_id="mssql_bcp_native", rows_per_second=100_000),
            ExportProviderProbe(provider_id="mssql_odbc_array", rows_per_second=160_000),
        ],
        cache_key=_cache_key(),
    )

    assert decision.selected_provider == "mssql_bcp_native"
    assert decision.recommended_provider == "mssql_odbc_array"
    assert decision.measured_speedup_pct == 60.0
    assert "export_optimizer_benchmark_only" in decision.reasons


def test_required_fails_before_source_io_without_safe_probe_evidence() -> None:
    decision = SourceExportOptimizer().decide(
        ExportOptimizerPolicy(mode="required"),
        current_default="mssql_bcp_native",
        probes=[
            ExportProviderProbe(provider_id="mssql_bcp_native", safe=False, blockers=("source_impact_blocked",)),
        ],
        cache_key=_cache_key(),
    )

    assert decision.selected_provider is None
    assert decision.release_gate == "blocked"
    assert "export_optimizer_no_safe_provider" in decision.blockers


def test_decision_cache_invalidates_by_route_schema_and_provider_version(tmp_path) -> None:
    cache = ExportDecisionCache(tmp_path / "export-decisions.json")
    key = _cache_key(schema_hash="schema-a", provider_versions={"mssql_bcp_native": "1"})
    changed_schema = _cache_key(schema_hash="schema-b", provider_versions={"mssql_bcp_native": "1"})
    changed_provider = _cache_key(schema_hash="schema-a", provider_versions={"mssql_bcp_native": "2"})

    decision = SourceExportOptimizer().decide(
        ExportOptimizerPolicy(),
        current_default="mssql_bcp_native",
        probes=[ExportProviderProbe(provider_id="mssql_bcp_native", rows_per_second=100_000)],
        cache_key=key,
    )

    cache.put(key, decision)

    assert cache.get(key) == decision
    assert cache.get(changed_schema) is None
    assert cache.get(changed_provider) is None


def test_mssql_export_provider_catalog_declares_all_optimizer_candidates() -> None:
    catalog = MssqlExportProviderCatalog()
    providers = {provider.provider_id: provider for provider in catalog.providers()}

    assert set(providers) >= {
        "mssql_bcp_native",
        "mssql_bcp_character_raw",
        "mssql_odbc_array",
        "mssql_driver_rowset",
        "range_partitioned",
        "single_scan_chunks",
    }
    assert providers["mssql_bcp_native"].preserves_types is True
    assert providers["mssql_bcp_character_raw"].requires_source_escaping is False
    assert providers["range_partitioned"].requires_seekable_boundary is True
    assert providers["single_scan_chunks"].supports_physical_chunking is True


def _cache_key(
    *,
    schema_hash: str = "schema",
    provider_versions: dict[str, str] | None = None,
) -> ExportDecisionCacheKey:
    return ExportDecisionCacheKey(
        source_identity="mssql://analytics_reporting.reporting.account_orders",
        query_hash="query",
        schema_hash=schema_hash,
        source_shape_hash="heap",
        provider_versions=provider_versions or {"mssql_bcp_native": "1", "mssql_odbc_array": "1"},
        dpone_version="0.36.0",
    )
