from __future__ import annotations

from dpone.readiness.managed_native_transfer_plan import columnar_fast_path_plan


def _manifest(
    *, reader_workers: int = 2, topology: str = "shared_per_run", mode: str = "required"
) -> dict[str, object]:
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
                        "mode": mode,
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


def test_columnar_plan_exposes_required_byte_admission_blocker() -> None:
    first = columnar_fast_path_plan(_manifest(), source_type="mssql", sink_type="clickhouse")
    second = columnar_fast_path_plan(_manifest(), source_type="mssql", sink_type="clickhouse")

    plan = first["details"]["range_parallelism"]
    assert plan["status"] == "blocked"
    assert plan["policy"]["reader_workers"] == 2
    assert plan["blockers"] == ["columnar_range_pre_read_byte_admission_unavailable"]
    assert plan["policy_fingerprint"] == second["details"]["range_parallelism"]["policy_fingerprint"]


def test_columnar_plan_fingerprint_changes_with_execution_policy() -> None:
    shared = columnar_fast_path_plan(_manifest(), source_type="mssql", sink_type="clickhouse")
    isolated = columnar_fast_path_plan(_manifest(topology="per_partition"), source_type="mssql", sink_type="clickhouse")

    assert (
        shared["details"]["range_parallelism"]["policy_fingerprint"]
        != isolated["details"]["range_parallelism"]["policy_fingerprint"]
    )


def test_columnar_plan_exposes_auto_serial_fallback_reason() -> None:
    raw = _manifest(mode="auto")
    raw["source"]["options"]["partitioning"]["bounds"] = "auto"
    result = columnar_fast_path_plan(raw, source_type="mssql", sink_type="clickhouse")

    plan = result["details"]["range_parallelism"]
    assert plan["status"] == "serial_fallback"
    assert plan["planned_range_count"] == 1
    assert plan["fallback_reason"] == "columnar_range_pre_read_byte_admission_unavailable"


def test_columnar_plan_requires_explicit_consistency_before_admission_status() -> None:
    raw = _manifest()
    del raw["source"]["options"]["partitioning"]["range_parallelism"]["consistency"]

    result = columnar_fast_path_plan(raw, source_type="mssql", sink_type="clickhouse")

    assert result["details"]["range_parallelism"] == {
        "status": "blocked",
        "blockers": ["columnar_range_parallelism_requires_explicit_consistency"],
    }


def test_columnar_plan_keeps_off_mode_serial() -> None:
    raw = _manifest()
    raw["source"]["options"]["partitioning"]["range_parallelism"] = {"mode": "off"}

    result = columnar_fast_path_plan(raw, source_type="mssql", sink_type="clickhouse")

    plan = result["details"]["range_parallelism"]
    assert plan["status"] == "serial"
    assert plan["planned_range_count"] == 1
    assert plan["policy"]["mode"] == "off"
