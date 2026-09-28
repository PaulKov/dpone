from __future__ import annotations

from dpone.readiness.managed_native_transfer_plan import columnar_fast_path_plan


def _manifest(*, reader_workers: int = 2, topology: str = "shared_per_run") -> dict[str, object]:
    return {
        "source": {
            "type": "mssql",
            "table": {"schema": "dbo", "name": "synthetic_events"},
            "options": {
                "partitioning": {
                    "column": "event_id",
                    "bounds": {"lower": 0, "upper": 100},
                    "num_partitions": 4,
                    "range_parallelism": {
                        "mode": "required",
                        "reader_workers": reader_workers,
                        "upload_workers": 2,
                        "load_workers": 1,
                        "max_inflight_ranges": 2,
                        "max_inflight_rows": 1000,
                        "max_inflight_bytes": 4096,
                        "consistency": "immutable",
                        "staging_topology": topology,
                    },
                },
                "native_transfer": {
                    "snapshot": {
                        "columnar_fast_path": {
                            "mode": "required",
                            "provider": "object_storage_pull",
                        }
                    }
                },
            },
        },
        "sink": {"type": "clickhouse", "table": {"schema": "analytics", "name": "events"}},
    }


def test_columnar_plan_exposes_normalized_range_policy_and_stable_fingerprint() -> None:
    first = columnar_fast_path_plan(_manifest(), source_type="mssql", sink_type="clickhouse")
    second = columnar_fast_path_plan(_manifest(), source_type="mssql", sink_type="clickhouse")

    plan = first["details"]["range_parallelism"]
    assert plan["status"] == "planned"
    assert len(plan["ranges"]) == 4
    assert plan["policy"]["reader_workers"] == 2
    assert plan["plan_fingerprint"] == second["details"]["range_parallelism"]["plan_fingerprint"]
    assert all(item["lower"].startswith("sha256:") for item in plan["ranges"])


def test_columnar_plan_fingerprint_changes_with_execution_policy() -> None:
    shared = columnar_fast_path_plan(_manifest(), source_type="mssql", sink_type="clickhouse")
    isolated = columnar_fast_path_plan(_manifest(topology="per_partition"), source_type="mssql", sink_type="clickhouse")

    assert (
        shared["details"]["range_parallelism"]["plan_fingerprint"]
        != isolated["details"]["range_parallelism"]["plan_fingerprint"]
    )


def test_columnar_plan_blocks_auto_bounds_without_runtime_source_probe() -> None:
    raw = _manifest()
    raw["source"]["options"]["partitioning"]["bounds"] = "auto"

    result = columnar_fast_path_plan(raw, source_type="mssql", sink_type="clickhouse")

    plan = result["details"]["range_parallelism"]
    assert plan["status"] == "blocked"
    assert "requires a bounds resolver" in plan["blockers"][0]
