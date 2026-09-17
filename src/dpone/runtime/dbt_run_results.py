"""Strict parser for the pinned dbt ``run_results.json`` artifact."""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol, TypeVar

from dpone.contracts import dbt_run_results as _canonical_results
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
        return _canonical_results.run_results_passes(self.nodes, warning_policy)


def parse_dbt_run_results(
    value: Mapping[str, Any],
    *,
    expected_run_result_unique_ids: tuple[str, ...],
    expected_dbt_version: str,
    expected_schema_version: str,
) -> ParsedDbtRunResults:
    """Validate one complete result set and reject selection/schema drift."""

    parsed = _canonical_results.parse_dbt_run_results(
        value,
        expected_run_result_unique_ids=expected_run_result_unique_ids,
        expected_dbt_version=expected_dbt_version,
        expected_schema_version=expected_schema_version,
    )
    return ParsedDbtRunResults(
        parsed.schema_version,
        parsed.dbt_version,
        parsed.invocation_id,
        tuple(ParsedDbtNodeOutcome(node.unique_id, node.status, node.execution_time) for node in parsed.nodes),
    )


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
