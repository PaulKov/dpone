"""Validate and sanitize the safe-sample data-copy evidence boundary."""

from __future__ import annotations

import math
from collections.abc import Mapping
from importlib import import_module
from typing import TYPE_CHECKING, Any

from dpone.gitops.schema_safe_sample_route_execution_contracts import safe_sample_data_copy_contract
from dpone.services.safe_sample_redaction import is_sensitive_key_name, redact_safe_sample_text

if TYPE_CHECKING:
    from dpone.services.safe_sample_execution_plan import SafeSampleExecutionPlan
    from dpone.services.safe_sample_policy import TemporaryTargetPlan
_MESSAGE_KEYS = {"message", "detail", "details", "reason", "error", "stderr", "stdout", "exception"}
_COPY_STATUSES = frozenset({"blocked", "copied", "copied_with_quarantine", "failed"})
_SUCCESS_STATUSES = frozenset({"copied", "copied_with_quarantine"})
_DATA_COPY_SCHEMA = safe_sample_data_copy_contract().schema
_DATA_COPY_FIELDS = frozenset(_DATA_COPY_SCHEMA["properties"])
_DATA_COPY_RESULT_ERROR_MESSAGE = (
    "Safe sample data-copy contract has an invalid schema, request, diagnostic, status, error, budget, or count."
)


def plan_row_budget_matches(plan: SafeSampleExecutionPlan) -> bool:
    """Return whether the serialized plan and evaluated policy use one row budget."""

    request_rows = plan.policy_result.request.sample_rows
    return type(plan.sample_rows) is int and plan.sample_rows > 0 and request_rows == plan.sample_rows


def validate_data_copy(
    plan: SafeSampleExecutionPlan,
    target_plan: TemporaryTargetPlan,
    data_copy: Mapping[str, Any],
) -> dict[str, Any]:
    """Return canonical evidence or a fail-closed invalid-result envelope."""

    raw_result = dict(data_copy)
    diagnostics_supplied = "diagnostics" in raw_result
    diagnostics_valid = not diagnostics_supplied or (
        isinstance(raw_result.get("diagnostics"), Mapping) and _is_json_value(raw_result["diagnostics"])
    )
    if not diagnostics_valid:
        raw_result["diagnostics"] = {}
    safe_result = safe_mapping(raw_result)
    status = safe_result.get("status")
    known_status = status if isinstance(status, str) and status in _COPY_STATUSES else None
    raw_errors = safe_result.get("errors")
    errors = [error for error in data_copy_errors(safe_result) if _is_structured_error(error)]
    invalid = known_status is None or not isinstance(raw_errors, list) or len(errors) != len(raw_errors)
    invalid |= known_status in {"blocked", "failed"} and not errors
    invalid |= diagnostics_supplied and not diagnostics_valid
    invalid |= known_status in _SUCCESS_STATUSES and (
        bool(errors)
        or not diagnostics_supplied
        or not _valid_success_schema(safe_result)
        or not _success_requests_match_plan(plan, target_plan, safe_result)
        or not _valid_counts(plan, safe_result)
    )
    if not invalid:
        return safe_result
    result = {key: value for key, value in safe_result.items() if key in _DATA_COPY_FIELDS and _is_json_value(value)}
    result["schema"] = "dpone.safe-sample-data-copy.v1"
    result["status"] = known_status if known_status in {"blocked", "failed"} else "failed"
    if not isinstance(result.get("diagnostics"), Mapping):
        result["diagnostics"] = {}
    result["errors"] = [*errors, data_copy_contract_error()]
    return result


def data_copy_errors(data_copy: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Return only sanitized mapping-shaped errors from a copy result."""

    raw_errors = data_copy.get("errors")
    if not isinstance(raw_errors, list):
        return []
    return [safe_mapping(error) for error in raw_errors if isinstance(error, Mapping)]


def data_outcome(data_copy: Mapping[str, Any] | None, *, fallback: str) -> str:
    """Derive the orthogonal data outcome from a validated copy envelope."""

    if data_copy is None:
        return fallback
    errors = data_copy_errors(data_copy)
    if any(error.get("code") == "DPONE_SAFE_SAMPLE_QUALITY_GATE_FAILED" for error in errors):
        return "failed_quality_gate"
    if errors:
        return fallback
    if data_copy.get("status") == "copied" and data_copy.get("rows_read") == 0:
        return "no_data"
    if data_copy.get("status") == "copied":
        return "passed"
    if data_copy.get("status") == "copied_with_quarantine":
        return "passed_with_quarantine"
    return fallback


def safe_mapping(value: Mapping[str, Any] | None) -> dict[str, Any]:
    """Recursively remove sensitive keys and redact sensitive string values."""

    if value is None:
        return {}
    safe: dict[str, Any] = {}
    for key, raw_value in value.items():
        if is_sensitive_key_name(str(key)):
            continue
        if isinstance(raw_value, Mapping):
            safe[str(key)] = safe_mapping(raw_value)
        elif isinstance(raw_value, list | tuple):
            safe[str(key)] = [_safe_sequence_item(item) for item in raw_value]
        elif isinstance(raw_value, str) and _is_message_key(str(key)):
            safe[str(key)] = _safe_message_text(raw_value, fallback="")
        elif isinstance(raw_value, str):
            safe[str(key)] = redact_safe_sample_text(raw_value)
        else:
            safe[str(key)] = raw_value
    return safe


def safe_error_message(exc: Exception) -> str:
    """Render a bounded redacted exception message without exposing raw payloads."""

    return _safe_message_text(str(exc), fallback=exc.__class__.__name__)


def data_copy_contract_error() -> dict[str, Any]:
    """Build the stable structured error for an invalid copy envelope."""

    return {
        "schema": "dpone.error.v1",
        "code": "DPONE_SAFE_SAMPLE_DATA_COPY_RESULT_INVALID",
        "stage": "safe_sample_runtime_execution",
        "severity": "error",
        "message": _DATA_COPY_RESULT_ERROR_MESSAGE,
        "fixes": [],
    }


def _valid_success_schema(data_copy: Mapping[str, Any]) -> bool:
    jsonschema = import_module("jsonschema")
    return bool(jsonschema.Draft202012Validator(_DATA_COPY_SCHEMA).is_valid(data_copy))


def _success_requests_match_plan(
    plan: SafeSampleExecutionPlan,
    target_plan: TemporaryTargetPlan,
    data_copy: Mapping[str, Any],
) -> bool:
    source_request = _mapping_value(data_copy.get("source_request"))
    copy_request = _mapping_value(data_copy.get("copy_request"))
    source_target = _mapping_value(source_request.get("target"))
    sink = _mapping_value(copy_request.get("sink"))
    certification_id = copy_request.get("certification_id")
    proof = plan.policy_result.capabilities.proof
    expected_limits = (
        plan.sample_rows,
        plan.policy_result.policy.max_bytes,
        plan.policy_result.policy.timeout_seconds,
    )
    return (
        source_request.get("status") == "planned"
        and source_request.get("mode") in {"pushdown", "full_scan"}
        and source_request.get("errors") == []
        and _request_limits(source_request) == expected_limits
        and _request_limits(copy_request) == expected_limits
        and source_request.get("source_read_only") is True
        and copy_request.get("source_read_only") is True
        and target_plan.mode == "temporary"
        and target_plan.cleanup_required is True
        and target_plan.pii_policy == data_copy.get("pii_policy") == "masked"
        and source_request.get("pii_policy") == copy_request.get("pii_policy") == "masked"
        and source_request.get("proof") == copy_request.get("proof") == proof
        and isinstance(certification_id, str)
        and proof == f"route_certification:{certification_id}"
        and source_target.get("connection_ref") == target_plan.connection_ref
        and source_target.get("temporary_table") == target_plan.temporary_table
        and sink.get("connection_ref") == target_plan.connection_ref
        and sink.get("temporary_table") == target_plan.temporary_table
    )


def _request_limits(request: Mapping[str, Any]) -> tuple[Any, Any, Any]:
    return request.get("sample_rows"), request.get("max_bytes"), request.get("timeout_seconds")


def _mapping_value(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _valid_counts(plan: SafeSampleExecutionPlan, data_copy: Mapping[str, Any]) -> bool:
    rows_read = _non_negative_count(data_copy.get("rows_read"))
    rows_written = _non_negative_count(data_copy.get("rows_written"))
    bytes_read = _non_negative_count(data_copy.get("bytes_read"))
    if rows_read is None or rows_written is None or bytes_read is None:
        return False
    quarantined = _quarantined_count(data_copy)
    return (
        quarantined is not None
        and rows_written + quarantined == rows_read
        and rows_read <= plan.sample_rows
        and bytes_read <= plan.policy_result.policy.max_bytes
        and _diagnostic_count_matches(data_copy, "source", "rows_returned", rows_read)
        and _diagnostic_count_matches(data_copy, "sink", "rows_sent", rows_written)
    )


def _quarantined_count(data_copy: Mapping[str, Any]) -> int | None:
    value = data_copy.get("quarantined_rows")
    if data_copy.get("status") == "copied":
        count = 0 if value is None else _non_negative_count(value)
        return count if count == 0 else None
    count = _non_negative_count(value)
    return count if count is not None and count > 0 else None


def _diagnostic_count_matches(
    data_copy: Mapping[str, Any],
    section: str,
    key: str,
    expected: int,
) -> bool:
    diagnostics = data_copy.get("diagnostics")
    values = diagnostics.get(section) if isinstance(diagnostics, Mapping) else None
    if not isinstance(values, Mapping):
        return True
    client = values.get("client")
    sections = (values, client) if isinstance(client, Mapping) else (values,)
    observed = [value.get(key) for value in sections if key in value]
    return all(_non_negative_count(value) == expected for value in observed)


def _safe_sequence_item(value: object) -> object:
    if isinstance(value, Mapping):
        return safe_mapping(value)
    if isinstance(value, list | tuple):
        return [_safe_sequence_item(item) for item in value]
    if isinstance(value, str):
        return redact_safe_sample_text(value)
    return value


def _safe_message_text(value: str, *, fallback: str) -> str:
    message = " ".join(value.split()) or fallback
    return redact_safe_sample_text(message)[:500]


def _is_message_key(key: str) -> bool:
    return key.lower().replace("-", "_") in _MESSAGE_KEYS


def _is_json_value(value: Any, seen: frozenset[int] | None = None) -> bool:
    if value is None or isinstance(value, str | bool | int):
        return True
    if isinstance(value, float):
        return math.isfinite(value)
    visited = seen or frozenset()
    if not isinstance(value, Mapping | list) or len(visited) >= 100 or id(value) in visited:
        return False
    nested_seen = visited | {id(value)}
    if isinstance(value, Mapping):
        return all(isinstance(key, str) and _is_json_value(item, nested_seen) for key, item in value.items())
    return all(_is_json_value(item, nested_seen) for item in value)


def _non_negative_count(value: Any) -> int | None:
    return value if type(value) is int and value >= 0 else None


def _is_structured_error(error: Mapping[str, Any]) -> bool:
    required = ("code", "stage", "severity", "message")
    return error.get("schema") == "dpone.error.v1" and all(
        isinstance(error.get(key), str) and error[key] for key in required
    )


__all__ = [
    "data_copy_contract_error",
    "data_copy_errors",
    "data_outcome",
    "plan_row_budget_matches",
    "safe_error_message",
    "safe_mapping",
    "validate_data_copy",
]
