"""Strict parser for the pinned dbt ``run_results.json`` artifact."""

from __future__ import annotations

import math
from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar

from dpone.contracts.dbt_contract_validation import DbtPublishingError

MAX_DBT_RUN_RESULTS_BYTES = 16 * 1024 * 1024
_NodeOutcomeT = TypeVar("_NodeOutcomeT")


class _SelectionLock(Protocol):
    @property
    def expected_run_result_unique_ids(self) -> tuple[str, ...]: ...


class _ExecutionPack(Protocol):
    @property
    def run_results_schema_version(self) -> str: ...

    @property
    def dbt_core_version(self) -> str: ...

    @property
    def selection_lock(self) -> _SelectionLock: ...


class _OutputPaths(Protocol):
    @property
    def root(self) -> Path: ...

    @property
    def target(self) -> Path: ...


class _ResultsReader(Protocol):
    def read(self, path: Path, *, root: Path, max_bytes: int) -> Mapping[str, Any]: ...


class _ResultsValidator(Protocol):
    def validate(self, payload: Mapping[str, Any], *, version: int) -> tuple[Any, ...]: ...


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
        if warning_policy not in {"fail", "allow"}:
            raise _invalid("dbt warning policy is invalid")
        allowed = _ALWAYS_PASSED_STATUSES | {"warn"} if warning_policy == "allow" else _ALWAYS_PASSED_STATUSES
        return bool(self.nodes) and all(item.status in allowed for item in self.nodes)


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


def read_dbt_run_results(
    pack: _ExecutionPack,
    output_paths: _OutputPaths,
    *,
    reader: _ResultsReader,
    validator: _ResultsValidator,
) -> tuple[ParsedDbtRunResults | None, DbtPublishingError | None]:
    """Read, schema-check and bind results to the immutable execution pack."""

    try:
        payload = reader.read(
            output_paths.target / "run_results.json",
            root=output_paths.root,
            max_bytes=MAX_DBT_RUN_RESULTS_BYTES,
        )
        version = int(pack.run_results_schema_version.removeprefix("v"))
        if validator.validate(payload, version=version):
            raise DbtPublishingError(
                "DPONE_DBT_RUN_RESULTS_INVALID",
                "dbt run-results does not satisfy the official schema",
            )
        return (
            parse_dbt_run_results(
                payload,
                expected_run_result_unique_ids=pack.selection_lock.expected_run_result_unique_ids,
                expected_dbt_version=pack.dbt_core_version,
                expected_schema_version=pack.run_results_schema_version,
            ),
            None,
        )
    except DbtPublishingError as exc:
        if exc.code == "DPONE_DBT_RESULTS_INVALID":
            exc = DbtPublishingError(
                "DPONE_DBT_RUN_RESULTS_INVALID",
                "dbt run-results artifact is invalid",
            )
        return None, exc


def dbt_node_outcomes(
    parsed: ParsedDbtRunResults | None,
    *,
    factory: Callable[[str, str, float], _NodeOutcomeT],
) -> tuple[_NodeOutcomeT, ...]:
    """Project validated dbt nodes onto the credential-free evidence contract."""

    if parsed is None:
        return ()
    return tuple(factory(item.unique_id, item.status, item.execution_time) for item in parsed.nodes)


__all__ = [
    "MAX_DBT_RUN_RESULTS_BYTES",
    "ParsedDbtNodeOutcome",
    "ParsedDbtRunResults",
    "dbt_node_outcomes",
    "parse_dbt_run_results",
    "read_dbt_run_results",
]
