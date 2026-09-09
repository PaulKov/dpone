from __future__ import annotations

from dpone.strategy_intelligence.benchmark_certification import (
    NativeBenchmarkCertificationPolicy,
    NativeBenchmarkCertificationService,
)


def test_benchmark_certification_passes_and_detects_bottleneck_phase() -> None:
    artifact = {
        "rows": 1_000_000,
        "phase_metrics": [
            {
                "name": "mssql_to_clickhouse.source_export",
                "rows": 1_000_000,
                "seconds": 20.0,
                "rows_per_second": 50_000.0,
            },
            {
                "name": "mssql_to_clickhouse.target_load_finalize",
                "rows": 1_000_000,
                "seconds": 5.0,
                "rows_per_second": 200_000.0,
            },
        ],
        "mssql_count": 1_000_000,
        "clickhouse_count": 1_000_000,
    }

    result = NativeBenchmarkCertificationService().certify(
        artifact,
        NativeBenchmarkCertificationPolicy(
            min_rows=1_000_000,
            min_mssql_to_clickhouse_rows_per_second=30_000.0,
        ),
    )

    assert result.passed
    assert result.bottleneck_phase == "mssql_to_clickhouse.source_export"
    assert result.rows_per_second["mssql_to_clickhouse"] == 40_000.0
    assert all(check["passed"] for check in result.checks)


def test_benchmark_certification_fails_on_count_mismatch_or_slo_regression() -> None:
    artifact = {
        "rows": 1_000_000,
        "phase_metrics": [
            {
                "name": "mssql_to_clickhouse.source_export",
                "rows": 1_000_000,
                "seconds": 100.0,
                "rows_per_second": 10_000.0,
            },
            {
                "name": "mssql_to_clickhouse.target_load_finalize",
                "rows": 1_000_000,
                "seconds": 100.0,
                "rows_per_second": 10_000.0,
            },
        ],
        "mssql_count": 1_000_000,
        "clickhouse_count": 999_999,
    }

    result = NativeBenchmarkCertificationService().certify(
        artifact,
        NativeBenchmarkCertificationPolicy(
            min_rows=1_000_000,
            min_mssql_to_clickhouse_rows_per_second=30_000.0,
        ),
    )

    assert not result.passed
    failed_checks = {check["name"] for check in result.checks if not check["passed"]}
    assert failed_checks == {"clickhouse_count_matches_rows", "mssql_to_clickhouse_slo"}


def test_postgres_source_bottleneck_recommends_only_single_snapshot_tuning() -> None:
    result = NativeBenchmarkCertificationService().certify(
        {
            "rows": 1_000,
            "phase_metrics": [
                {
                    "name": "postgres_to_mssql.source_export",
                    "rows": 1_000,
                    "seconds": 10.0,
                    "rows_per_second": 100.0,
                },
                {
                    "name": "postgres_to_mssql.target_load_finalize",
                    "rows": 1_000,
                    "seconds": 1.0,
                    "rows_per_second": 1_000.0,
                },
            ],
        }
    )

    assert result.recommendations == (
        "Tune the single PostgreSQL COPY export: bounded indexed source predicate, projection width, source indexes, and network path.",
    )
    assert "partition" not in result.recommendations[0].casefold()


def test_benchmark_certification_supports_legacy_aggregate_metrics() -> None:
    artifact = {
        "rows": 1_000_000,
        "metrics": [
            {
                "name": "postgres_to_mssql_full_refresh",
                "rows": 1_000_000,
                "seconds": 10.0,
                "rows_per_second": 100_000.0,
            },
            {
                "name": "mssql_to_clickhouse_full_refresh",
                "rows": 1_000_000,
                "seconds": 25.0,
                "rows_per_second": 40_000.0,
            },
        ],
        "mssql_count": 1_000_000,
        "clickhouse_count": 1_000_000,
    }

    result = NativeBenchmarkCertificationService().certify(
        artifact,
        NativeBenchmarkCertificationPolicy(
            min_rows=1_000_000,
            min_postgres_to_mssql_rows_per_second=80_000.0,
            min_mssql_to_clickhouse_rows_per_second=30_000.0,
        ),
    )

    assert result.passed
    assert result.rows_per_second == {"postgres_to_mssql": 100_000.0, "mssql_to_clickhouse": 40_000.0}
    assert result.bottleneck_phase == "mssql_to_clickhouse_full_refresh"
