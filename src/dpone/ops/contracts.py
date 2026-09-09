"""Data contract evaluation for operational gates."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from dataclasses import asdict, dataclass
from typing import Any


@dataclass(frozen=True, slots=True)
class ContractCheckResult:
    code: str
    severity: str
    passed: bool
    message: str
    action: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class DataContractReport:
    mode: str
    results: tuple[ContractCheckResult, ...]

    @property
    def passed(self) -> bool:
        return not any(item.severity == "fail" and not item.passed for item in self.results)

    def to_dict(self) -> dict[str, object]:
        return {"mode": self.mode, "passed": self.passed, "results": [item.to_dict() for item in self.results]}

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = ["# dpone data contract report", "", f"- Passed: `{self.passed}`", "", "| check | status | action |"]
        lines.append("|---|---|---|")
        for item in self.results:
            status = "pass" if item.passed else item.severity
            lines.append(f"| `{item.code}` | {status} | {item.action} |")
        return "\n".join(lines) + "\n"


class DataContractService:
    """Evaluates source/target data contracts without connector dependencies."""

    def evaluate(
        self,
        rows: Sequence[Mapping[str, Any]],
        contract: Mapping[str, Any],
        *,
        mode: str = "fail",
    ) -> DataContractReport:
        severity = "warn" if mode == "warn" else "fail"
        results: list[ContractCheckResult] = []
        results.extend(self._required_columns(rows, contract, severity))
        results.extend(self._not_null(rows, contract, severity))
        results.extend(self._unique(rows, contract, severity))
        results.extend(self._min_rows(rows, contract, severity))
        results.extend(self._max_null_ratio(rows, contract, severity))
        if not results:
            results.append(
                ContractCheckResult(
                    code="contract.empty",
                    severity="pass",
                    passed=True,
                    message="No contract checks configured.",
                    action="Add required_columns, not_null, unique, freshness, or checksum checks for operations.",
                )
            )
        return DataContractReport(mode=mode, results=tuple(results))

    def _required_columns(
        self,
        rows: Sequence[Mapping[str, Any]],
        contract: Mapping[str, Any],
        severity: str,
    ) -> list[ContractCheckResult]:
        required = [str(item) for item in contract.get("required_columns", [])]
        present = set(rows[0].keys()) if rows else set()
        return [
            self._result(
                code=f"required_columns.{column}",
                passed=column in present,
                severity=severity,
                message=f"Required column {column} must be present.",
                action=f"Add column `{column}` to the source query/schema contract.",
            )
            for column in required
        ]

    def _not_null(
        self,
        rows: Sequence[Mapping[str, Any]],
        contract: Mapping[str, Any],
        severity: str,
    ) -> list[ContractCheckResult]:
        return [
            self._result(
                code=f"not_null.{column}",
                passed=all(row.get(column) is not None for row in rows),
                severity=severity,
                message=f"Column {column} must not contain nulls.",
                action=f"Fix upstream null values or remove `{column}` from not_null.",
            )
            for column in [str(item) for item in contract.get("not_null", [])]
        ]

    def _unique(
        self,
        rows: Sequence[Mapping[str, Any]],
        contract: Mapping[str, Any],
        severity: str,
    ) -> list[ContractCheckResult]:
        results: list[ContractCheckResult] = []
        for column in [str(item) for item in contract.get("unique", [])]:
            values = [row.get(column) for row in rows]
            results.append(
                self._result(
                    code=f"unique.{column}",
                    passed=len(values) == len(set(values)),
                    severity=severity,
                    message=f"Column {column} must be unique.",
                    action=f"Fix duplicate values in `{column}` or configure a composite unique key.",
                )
            )
        return results

    def _min_rows(
        self,
        rows: Sequence[Mapping[str, Any]],
        contract: Mapping[str, Any],
        severity: str,
    ) -> list[ContractCheckResult]:
        if "min_rows" not in contract:
            return []
        expected = int(contract["min_rows"])
        return [
            self._result(
                code="min_rows",
                passed=len(rows) >= expected,
                severity=severity,
                message=f"Expected at least {expected} rows.",
                action="Check source filters, credentials, pagination, and state offsets.",
            )
        ]

    def _max_null_ratio(
        self,
        rows: Sequence[Mapping[str, Any]],
        contract: Mapping[str, Any],
        severity: str,
    ) -> list[ContractCheckResult]:
        thresholds = contract.get("max_null_ratio", {}) or {}
        results: list[ContractCheckResult] = []
        for column, threshold in thresholds.items():
            ratio = 0.0 if not rows else sum(1 for row in rows if row.get(column) is None) / len(rows)
            results.append(
                self._result(
                    code=f"max_null_ratio.{column}",
                    passed=ratio <= float(threshold),
                    severity=severity,
                    message=f"Column {column} null ratio {ratio:.4f} must be <= {float(threshold):.4f}.",
                    action=f"Fix unexpected null sparsity for `{column}` or adjust the contract threshold.",
                )
            )
        return results

    @staticmethod
    def _result(*, code: str, passed: bool, severity: str, message: str, action: str) -> ContractCheckResult:
        return ContractCheckResult(
            code=code,
            severity="pass" if passed else severity,
            passed=passed,
            message=message,
            action=action,
        )
