"""Pure in-memory adapter for the bounded hermetic strategy contract."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.contracts.hermetic_test import HermeticExecutionPlan


import copy
import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from dpone.contracts.hermetic_test import SUPPORTED_HERMETIC_STRATEGIES, HermeticExecutionResult, HermeticTestError


class InMemoryHermeticStrategyExecutor:
    """Apply final-row semantics without connectors, SQL, or durable targets."""

    def execute(
        self,
        plan: HermeticExecutionPlan,
        input_rows: Sequence[Mapping[str, Any]],
        initial_rows: Sequence[Mapping[str, Any]],
        *,
        cancelled: Callable[[], bool],
    ) -> HermeticExecutionResult:
        _check_cancelled(cancelled)
        if plan.mode not in SUPPORTED_HERMETIC_STRATEGIES:
            raise HermeticTestError(
                "DPONE_TEST_EXECUTION_UNSUPPORTED",
                "The selected load strategy is not supported by hermetic test v1.",
                stage="test_plan",
            )
        if plan.mode == "full_refresh":
            return HermeticExecutionResult(rows=_copy_rows(input_rows, cancelled=cancelled))
        if plan.mode == "incremental_append":
            return HermeticExecutionResult(
                rows=(*_copy_rows(initial_rows, cancelled=cancelled), *_copy_rows(input_rows, cancelled=cancelled))
            )
        return HermeticExecutionResult(rows=self._merge(plan, input_rows, initial_rows, cancelled=cancelled))

    @staticmethod
    def _merge(
        plan: HermeticExecutionPlan,
        input_rows: Sequence[Mapping[str, Any]],
        initial_rows: Sequence[Mapping[str, Any]],
        *,
        cancelled: Callable[[], bool],
    ) -> tuple[dict[str, Any], ...]:
        if not plan.unique_key or plan.duplicate_policy != "fail":
            raise HermeticTestError(
                "DPONE_TEST_EXECUTION_UNSUPPORTED",
                "Hermetic incremental_merge requires a unique key and duplicate_policy=fail.",
                stage="test_plan",
            )
        by_key = _index_unique(initial_rows, plan.unique_key, cancelled=cancelled, source="initial target")
        incoming = _index_unique(input_rows, plan.unique_key, cancelled=cancelled, source="input")
        by_key.update(incoming)
        return tuple(copy.deepcopy(by_key[key]) for key in sorted(by_key))


def _copy_rows(rows: Sequence[Mapping[str, Any]], *, cancelled: Callable[[], bool]) -> tuple[dict[str, Any], ...]:
    _check_cancelled(cancelled)
    copied: list[dict[str, Any]] = []
    for row in rows:
        _check_cancelled(cancelled)
        copied.append(copy.deepcopy(dict(row)))
    return tuple(copied)


def _index_unique(
    rows: Sequence[Mapping[str, Any]],
    unique_key: tuple[str, ...],
    *,
    cancelled: Callable[[], bool],
    source: str,
) -> dict[str, dict[str, Any]]:
    _check_cancelled(cancelled)
    result: dict[str, dict[str, Any]] = {}
    for index, row in enumerate(rows):
        _check_cancelled(cancelled)
        key = _row_key(row, unique_key, index=index, source=source)
        if key in result:
            raise HermeticTestError(
                "DPONE_TEST_DUPLICATE_KEY",
                f"The {source} fixture contains a duplicate unique key at row {index + 1}.",
                stage="test_execute",
            )
        result[key] = copy.deepcopy(dict(row))
    return result


def _row_key(row: Mapping[str, Any], unique_key: tuple[str, ...], *, index: int, source: str) -> str:
    values: list[Any] = []
    for field in unique_key:
        if field not in row or row[field] is None:
            raise HermeticTestError(
                "DPONE_TEST_UNIQUE_KEY_MISSING",
                f"The {source} fixture is missing a non-null unique-key field at row {index + 1}.",
                stage="test_execute",
            )
        value = row[field]
        if isinstance(value, (list, dict)):
            raise HermeticTestError(
                "DPONE_TEST_UNIQUE_KEY_INVALID",
                f"The {source} fixture has a non-scalar unique key at row {index + 1}.",
                stage="test_execute",
            )
        values.append(value)
    return json.dumps(values, ensure_ascii=False, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _check_cancelled(cancelled: Callable[[], bool]) -> None:
    if cancelled():
        raise HermeticTestError(
            "DPONE_TEST_TIMEOUT",
            "Hermetic test exceeded its configured timeout.",
            exit_code=4,
            stage="test_execute",
        )


__all__ = ["InMemoryHermeticStrategyExecutor"]
