"""Bounded evidence builders for ClickHouse staged-load attempts."""

from __future__ import annotations

from typing import Any


def staged_handle_metadata(
    load_config: Any,
    staging_config: Any,
    finalization_config: Any | None,
    decoded_config: Any | None,
    payload: Any,
) -> dict[str, Any]:
    """Describe exact operation tables and native producer microsteps."""

    operation_tables = {"staging": _table_ref(staging_config)}
    if finalization_config is not None and finalization_config is not staging_config:
        operation_tables["finalization"] = _table_ref(finalization_config)
    if decoded_config is not None:
        operation_tables["decoded"] = _table_ref(decoded_config)
    metadata = {
        "strategy": load_config.load_strategy.value,
        "target_schema": getattr(load_config, "target_schema", None),
        "operation_schema": getattr(staging_config, "target_schema", None),
        "operation_tables": operation_tables,
    }
    payload_evidence = _payload_evidence(payload)
    if payload_evidence:
        metadata["payload_evidence"] = payload_evidence
        microsteps = _staged_microsteps(payload_evidence)
        if microsteps:
            metadata["staged_microsteps"] = microsteps
    return metadata


def _table_ref(config: Any) -> str:
    return f"{getattr(config, 'target_schema', '')}.{getattr(config, 'target_table', '')}"


def _payload_evidence(payload: Any) -> dict[str, Any]:
    artifact = getattr(payload, "artifact", None)
    evidence = getattr(artifact, "to_evidence", None)
    if not callable(evidence):
        return {}
    value = evidence()
    return dict(value) if isinstance(value, dict) else {}


def _staged_microsteps(payload_evidence: dict[str, Any]) -> list[dict[str, Any]]:
    metrics = payload_evidence.get("window_metrics")
    if not isinstance(metrics, list):
        return []
    rows = sum(_int(metric.get("row_count")) for metric in metrics if isinstance(metric, dict))
    durations = {
        "source_read": sum(
            _number(_producer_metric(metric, "source_read_seconds")) for metric in metrics if isinstance(metric, dict)
        ),
        "parquet_write": sum(
            _number(_producer_metric(metric, "parquet_write_seconds")) for metric in metrics if isinstance(metric, dict)
        ),
        "object_upload": sum(
            _number(_producer_metric(metric, "object_upload_seconds")) for metric in metrics if isinstance(metric, dict)
        ),
        "clickhouse_pull": sum(
            _number(metric.get("clickhouse_pull_seconds")) for metric in metrics if isinstance(metric, dict)
        ),
        "window_cleanup": sum(
            _number(metric.get("window_cleanup_seconds")) for metric in metrics if isinstance(metric, dict)
        ),
    }
    return [_microstep(name, seconds, rows) for name, seconds in durations.items() if seconds > 0]


def _microstep(name: str, seconds: float, rows: int) -> dict[str, Any]:
    item: dict[str, Any] = {"step_id": name, "duration_seconds": seconds, "rows": rows}
    if rows > 0:
        item["rows_per_second"] = rows / seconds
    return item


def _producer_metric(metric: dict[str, Any], key: str) -> object:
    producer = metric.get("producer_metrics")
    return producer.get(key) if isinstance(producer, dict) else None


def _number(value: object) -> float:
    return float(value) if isinstance(value, int | float) else 0.0


def _int(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    return 0


__all__ = ["staged_handle_metadata"]
