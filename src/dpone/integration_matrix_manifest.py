"""Credential-free manifest examples for integration matrix cases."""

from __future__ import annotations

from typing import Any

from dpone.integration_matrix_behavior import _default_merge_policy_for_sink


def example_manifest_for_case(case: Any, *, name_prefix: str = "integration_matrix") -> dict[str, Any]:
    """Build a minimal manifest fragment for preflight validation."""

    source = _source_manifest(case)
    sink = _sink_manifest(case)
    _apply_source_strategy_options(case, source)
    sink["strategy"] = _sink_strategy_manifest(case)
    sink["options"] = {"schema_evolution": {"enabled": True}, "quality": {"mode": "warn"}}
    return {
        "kind": "dpone.integration_matrix.case.v1",
        "name": f"{name_prefix}.{case.case_id}",
        "source": source,
        "sink": sink,
        "certification": {
            "case_id": case.case_id,
            "guide": case.guide,
            "required_profiles": list(case.required_profiles),
        },
    }


def _source_manifest(case: Any) -> dict[str, Any]:
    if case.source == "api":
        return {
            "type": "api",
            "api_type": "rest",
            "connection_id": "api_demo",
            "connection_type": "params",
            "options": {"method": "GET", "url": "https://api.example.com/orders", "records_path": "$.data"},
        }
    if case.source == "kafka":
        return {
            "type": "kafka",
            "connection_id": "kafka_demo",
            "connection_type": "params",
            "topic": "dpone.integration.source",
            "options": {"read_mode": "offsets", "offset_storage": "dpone", "message_format": "json"},
        }
    return {
        "type": case.source,
        "connection_id": f"{case.source}_demo",
        "connection_type": "params",
        "table": {"schema": "public", "name": "orders"},
        "options": {"batch_size": 1000},
    }


def _sink_manifest(case: Any) -> dict[str, Any]:
    if case.sink == "kafka":
        return {
            "type": "kafka",
            "connection_id": "kafka_demo",
            "connection_type": "params",
            "topic": "dpone.integration.target",
        }
    return {
        "type": case.sink,
        "connection_id": f"{case.sink}_demo",
        "connection_type": "params",
        "table": {"schema": "landing", "name": "orders"},
        "staging": {"schema": "staging"},
    }


def _apply_source_strategy_options(case: Any, source: dict[str, Any]) -> None:
    if case.source == "mysql":
        source.setdefault("options", {})
        if case.sink == "mssql":
            source["options"].setdefault("export_format", "mssql-delimited")
            source["options"].setdefault("compress_export", False)
        else:
            source["options"].setdefault("export_format", "csv")
        if case.strategy in {"incremental_append", "incremental_merge"}:
            source["options"].setdefault("incremental_column", "updated_at")
    if case.strategy == "xmin":
        source.setdefault("options", {})["incremental_strategy"] = "xmin"
        source["options"]["xmin_state_key"] = f"{case.pair_id}.xmin"
    if case.strategy == "cdc":
        source.setdefault("options", {})["cdc"] = {
            "enabled": True,
            "slot": f"dpone_{case.source}_{case.sink}",
            "publication": "dpone_publication" if case.source == "postgres" else None,
            "change_tracking": case.source == "mssql",
        }


def _sink_strategy_manifest(case: Any) -> dict[str, Any]:
    strategy: dict[str, Any] = {"mode": case.sink_strategy}
    if case.sink_strategy == "incremental_merge":
        strategy.update(
            unique_key="id", merge_policy=_default_merge_policy_for_sink(case.sink), duplicate_policy="fail"
        )
    if case.sink_strategy == "snapshot_diff":
        strategy["unique_key"] = "id"
        strategy["diff"] = {"compare": "row_hash", "delete_policy": "hard_delete"}
    if case.sink_strategy == "scd2":
        strategy["unique_key"] = "id"
        strategy["scd2"] = {
            "valid_from_column": "__dpone__valid_from_at",
            "valid_to_column": "__dpone__valid_to_at",
            "current_flag_column": "__dpone__is_current",
            "row_hash_column": "__dpone__row_hash",
            "delete_policy": "expire",
        }
    if case.sink_strategy == "backfill":
        strategy["backfill"] = {
            "inner_mode": "partition_replace",
            "chunk": {"column": "business_date", "from": "2026-06-01", "to": "2026-06-03", "step": "1d"},
            "parallel_workers": 2,
        }
    if case.sink_strategy == "replace":
        strategy["custom_predicate"] = "business_date = '2026-06-03'"
    if case.sink_strategy == "partition_replace":
        strategy["partition"] = {"column": "business_date", "values_from_staging": True, "max_partitions_per_run": 64}
    return strategy


__all__ = ["example_manifest_for_case"]
