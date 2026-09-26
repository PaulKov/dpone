"""Bounded evidence builders for ClickHouse staged-load attempts."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

SOURCE_BYTE_BUDGET_SCHEMA_VERSION = "dpone.runtime.source-byte-budget.v1"
_INT64_MAX = (1 << 63) - 1


class SourceByteBudgetError(RuntimeError):
    """The configured full-refresh source-byte budget cannot be honored."""

    def __init__(self, code: str, *, maximum_bytes: int, observed_bytes: int | None = None) -> None:
        self.code = code
        self.maximum_bytes = maximum_bytes
        self.observed_bytes = observed_bytes
        suffix = f":observed={observed_bytes}:maximum={maximum_bytes}" if observed_bytes is not None else ""
        super().__init__(f"{code}{suffix}")


@dataclass(frozen=True, slots=True)
class SourceByteBudgetEvidence:
    """Measured, aggregate source bytes admitted before target publication."""

    maximum_bytes: int
    observed_bytes: int
    unique_parts: int
    measurement: str = "source_emitted_bytes"
    schema_version: str = SOURCE_BYTE_BUDGET_SCHEMA_VERSION

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def enforce_source_byte_budget(
    payload: Any,
    *,
    maximum_bytes: object | None,
    full_refresh: bool,
) -> SourceByteBudgetEvidence | None:
    """Admit a bounded full refresh after all source bytes reached staging."""

    if maximum_bytes is None:
        return None
    maximum = _positive_limit(maximum_bytes)
    if not full_refresh:
        raise SourceByteBudgetError("DPONE_SOURCE_BYTE_BUDGET_STRATEGY_INVALID", maximum_bytes=maximum)
    parts = _source_parts(getattr(payload, "artifact", None))
    if parts is None:
        raise SourceByteBudgetError("DPONE_SOURCE_BYTE_BUDGET_UNMEASURABLE", maximum_bytes=maximum)
    observed = sum(parts.values())
    if observed > maximum:
        raise SourceByteBudgetError(
            "DPONE_SOURCE_BYTE_BUDGET_EXCEEDED",
            maximum_bytes=maximum,
            observed_bytes=observed,
        )
    return SourceByteBudgetEvidence(maximum_bytes=maximum, observed_bytes=observed, unique_parts=len(parts))


def _positive_limit(value: object) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0 or value > _INT64_MAX:
        raise ValueError("source_byte_budget.max_source_bytes_out_of_range")
    return value


def _source_parts(artifact: Any) -> dict[tuple[object, ...], int] | None:
    lacks_receipt = getattr(artifact, "lacks_source_contract_receipt", None)
    if callable(lacks_receipt) and lacks_receipt():
        return _source_parts(artifact.completed_source_authority_artifact)
    validated = getattr(type(artifact), "validated_file_contract_artifact", None)
    if validated is not None:
        return _source_parts(artifact.validated_file_contract_artifact)
    completed = getattr(type(artifact), "completed_source_authority_artifact", None)
    if completed is not None:
        return _source_parts(artifact.completed_source_authority_artifact)
    inner = getattr(artifact, "artifact", None)
    if inner is not None and inner is not artifact:
        return _source_parts(inner)

    receipt = getattr(artifact, "integrity_receipt", None)
    if receipt is not None:
        identity = receipt.identity
        key = ("file", identity.device, identity.inode, receipt.sha256)
        return {key: int(receipt.size_bytes)}

    partitions = getattr(artifact, "partitions", None)
    if isinstance(partitions, list | tuple):
        measured: dict[tuple[object, ...], int] = {}
        for partition in partitions:
            part = _source_parts(partition)
            if part is None:
                return None
            measured.update(part)
        return measured

    slice_evidence = getattr(artifact, "slice_evidence", None)
    if isinstance(slice_evidence, list):
        if getattr(artifact, "source_byte_measurement_complete", False) is not True:
            return None
        return _evidence_parts(
            slice_evidence,
            identity_fields=("partition_index", "slice_index"),
            digest_fields=("sha256",),
        )

    events = getattr(artifact, "_events", None)
    if isinstance(events, list):
        completed = [event for event in events if event.get("status") == "loaded_to_staging"]
        if getattr(artifact, "source_byte_measurement_complete", False) is not True:
            return None
        return _evidence_parts(completed, identity_fields=("chunk_index",), digest_fields=("checksum",))

    stats = getattr(artifact, "stats", None)
    if stats is not None and getattr(artifact, "source_export_provider", None) == "mssql_bcp_pipe":
        chunks = getattr(stats, "chunks", None)
        size_bytes = getattr(stats, "size_bytes", None)
        digest = getattr(stats, "sha256", None)
        valid_stats = all(
            not isinstance(value, bool) and isinstance(value, int) and value >= 0 for value in (chunks, size_bytes)
        )
        if valid_stats and isinstance(digest, str) and digest and _extraction_complete(artifact):
            return {("stream", digest): size_bytes}
    return None


def _evidence_parts(
    evidence: list[dict[str, Any]],
    *,
    identity_fields: tuple[str, ...],
    digest_fields: tuple[str, ...],
) -> dict[tuple[object, ...], int] | None:
    measured: dict[tuple[object, ...], int] = {}
    for item in evidence:
        raw_bytes = item.get("bytes")
        if isinstance(raw_bytes, bool) or not isinstance(raw_bytes, int) or raw_bytes < 0:
            return None
        identity = tuple(item.get(field) for field in identity_fields)
        digest = next((item.get(field) for field in digest_fields if item.get(field)), None)
        if digest is None:
            return None
        key = (*identity, digest)
        conflicting = [known for known in measured if known[: len(identity)] == identity and known != key]
        if conflicting:
            return None
        previous = measured.get(key)
        if previous is not None and previous != raw_bytes:
            return None
        measured[key] = raw_bytes
    return measured if measured or not evidence else None


def _extraction_complete(artifact: Any) -> bool:
    lifecycle = getattr(artifact, "extraction_lifecycle", None)
    receipt = getattr(lifecycle, "receipt", None)
    return bool(getattr(receipt, "complete", False))


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


__all__ = [
    "SOURCE_BYTE_BUDGET_SCHEMA_VERSION",
    "SourceByteBudgetError",
    "SourceByteBudgetEvidence",
    "enforce_source_byte_budget",
    "staged_handle_metadata",
]
