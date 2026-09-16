"""Pure parsing and result policy for the pinned dbt run-results contract."""

from __future__ import annotations

import math
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.contracts.dbt_contract_validation import DbtPublishingError


class _NodeStatus(Protocol):
    @property
    def status(self) -> str: ...


def run_results_passes(nodes: Sequence[_NodeStatus], warning_policy: str) -> bool:
    """Apply the shared warning policy without changing result record identity."""
    if warning_policy not in {"fail", "allow"}:
        raise _invalid("dbt warning policy is invalid")
    allowed = _ALWAYS_PASSED_STATUSES | {"warn"} if warning_policy == "allow" else _ALWAYS_PASSED_STATUSES
    return bool(nodes) and all(item.status in allowed for item in nodes)


_ROOT_REQUIRED_FIELDS = frozenset({"metadata", "results", "elapsed_time"})


_METADATA_REQUIRED_FIELDS = frozenset(
    {
        "dbt_schema_version",
        "dbt_version",
        "generated_at",
        "invocation_id",
        "env",
    }
)


_KNOWN_STATUSES = frozenset(
    {
        "success",
        "pass",
        "warn",
        "error",
        "fail",
        "skipped",
        "runtime error",
        "partial success",
        "no-op",
    }
)


_ALWAYS_PASSED_STATUSES = frozenset({"success", "pass", "no-op"})


@dataclass(frozen=True, slots=True)
class ParsedDbtNodeOutcome:
    unique_id: str
    status: str
    execution_time: float


@dataclass(frozen=True, slots=True)
class ParsedDbtRunResults:
    schema_version: str
    dbt_version: str
    invocation_id: str
    nodes: tuple[ParsedDbtNodeOutcome, ...]

    @property
    def warning_count(self) -> int:
        return sum(item.status == "warn" for item in self.nodes)

    def passes(self, warning_policy: str) -> bool:
        return run_results_passes(self.nodes, warning_policy)


def parse_dbt_run_results(
    value: Mapping[str, Any],
    *,
    expected_run_result_unique_ids: tuple[str, ...],
    expected_dbt_version: str,
    expected_schema_version: str,
) -> ParsedDbtRunResults:
    """Validate one complete result set and reject selection/schema drift."""

    payload = _required_mapping(
        value,
        "run_results",
        required=_ROOT_REQUIRED_FIELDS,
    )
    metadata = _required_mapping(
        payload.get("metadata"),
        "run_results.metadata",
        required=_METADATA_REQUIRED_FIELDS,
    )
    schema_version = _required_text(metadata.get("dbt_schema_version"), "metadata.dbt_schema_version")
    expected_schema_url = f"https://schemas.getdbt.com/dbt/run-results/{expected_schema_version}.json"
    if schema_version != expected_schema_url:
        raise _invalid("run-results schema version differs from the execution pack")
    dbt_version = _required_text(metadata.get("dbt_version"), "metadata.dbt_version")
    if dbt_version != expected_dbt_version:
        raise _invalid("run-results dbt version differs from the execution pack")
    invocation_id = _required_text(metadata.get("invocation_id"), "metadata.invocation_id")
    if not isinstance(metadata.get("env"), Mapping):
        raise _invalid("metadata.env must be an object")
    _number(payload.get("elapsed_time"), "elapsed_time")
    if "args" in payload and not isinstance(payload.get("args"), Mapping):
        raise _invalid("args must be an object")
    raw_results = payload.get("results")
    if not isinstance(raw_results, list) or not raw_results:
        raise _invalid("results must be a non-empty array")
    nodes = tuple(_node(item) for item in raw_results)
    observed = tuple(sorted(item.unique_id for item in nodes))
    if len(observed) != len(set(observed)):
        raise _invalid("run-results contains duplicate unique_id values")
    if observed != tuple(sorted(expected_run_result_unique_ids)):
        raise DbtPublishingError(
            "DPONE_DBT_SELECTION_DRIFT",
            "dbt run-results does not match the locked result selection",
        )
    return ParsedDbtRunResults(schema_version, dbt_version, invocation_id, nodes)


def _node(value: object) -> ParsedDbtNodeOutcome:
    payload = _required_mapping(
        value,
        "run_results result",
        required=frozenset(
            {
                "status",
                "timing",
                "execution_time",
                "adapter_response",
                "unique_id",
            }
        ),
    )
    status = _required_text(payload.get("status"), "result.status")
    if status not in _KNOWN_STATUSES:
        raise _invalid("result.status is unsupported")
    execution_time = _number(payload.get("execution_time"), "result.execution_time")
    if execution_time < 0:
        raise _invalid("result.execution_time must be non-negative")
    if not isinstance(payload.get("timing"), list):
        raise _invalid("result.timing must be an array")
    if not isinstance(payload.get("adapter_response"), Mapping):
        raise _invalid("result.adapter_response must be an object")
    compiled = payload.get("compiled")
    if compiled is not None and not isinstance(compiled, bool):
        raise _invalid("result.compiled must be boolean or null")
    compiled_code = payload.get("compiled_code")
    if compiled_code is not None and not isinstance(compiled_code, str):
        raise _invalid("result.compiled_code must be a string or null")
    return ParsedDbtNodeOutcome(
        unique_id=_required_text(payload.get("unique_id"), "result.unique_id"),
        status=status,
        execution_time=execution_time,
    )


def _required_mapping(
    value: object,
    field: str,
    *,
    required: frozenset[str],
) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise _invalid(f"{field} must be an object")
    if required - set(value):
        raise _invalid(f"{field} contains missing fields")
    return value


def _required_text(value: object, field: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise _invalid(f"{field} must be a non-empty string")
    return value


def _number(value: object, field: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise _invalid(f"{field} must be a number")
    parsed = float(value)
    if not math.isfinite(parsed):
        raise _invalid(f"{field} must be finite")
    return parsed


def _invalid(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_RUN_RESULTS_INVALID", message)
