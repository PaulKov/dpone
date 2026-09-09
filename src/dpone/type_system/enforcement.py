"""Runtime schema-contract enforcement.

This module is intentionally pure and row-oriented. It does not know how a
target sink writes data; it only decides which rows are safe to stage, which
rows need diagnostics, and whether the pipeline may advance state after load.
"""

from __future__ import annotations

import json
from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, time
from decimal import Decimal, InvalidOperation
from typing import Any, Literal

from dpone.ports.dlq import DlqRejection, DlqWriter
from dpone.type_system.models import ConflictPolicy

ContractAction = Literal["accept", "fail", "warn", "quarantine", "variant_column"]

_VARIANT_PREFIX = "__dpone__nc__"


@dataclass(frozen=True, slots=True)
class ContractDiagnostic:
    row_index: int
    column: str
    action: ContractAction
    message: str
    expected_type: str
    actual_value: str
    reason_code: str
    actual_type: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    def to_evidence_dict(self) -> dict[str, Any]:
        return {
            "row_index": self.row_index,
            "column": self.column,
            "action": self.action,
            "message": self.message,
            "expected_type": self.expected_type,
            "actual_type": self.actual_type,
            "reason_code": self.reason_code,
        }


@dataclass(frozen=True, slots=True)
class ContractEnforcementResult:
    passed: bool
    target_rows: list[dict[str, Any]]
    diagnostics: tuple[ContractDiagnostic, ...]
    rejected_rows: int
    quarantined_rows: int
    state_commit_allowed: bool
    dlq_record_ids: tuple[str, ...] = ()
    dlq_reasons: Mapping[str, int] = field(default_factory=dict)
    dlq_index_ref: str | None = None
    accepted_row_count: int | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "passed": self.passed,
            "target_rows": self.target_rows,
            "diagnostics": [item.to_dict() for item in self.diagnostics],
            "rejected_rows": self.rejected_rows,
            "quarantined_rows": self.quarantined_rows,
            "state_commit_allowed": self.state_commit_allowed,
            "dlq_record_ids": list(self.dlq_record_ids),
            "dlq_reasons": dict(self.dlq_reasons),
            "dlq_index_ref": self.dlq_index_ref,
            "accepted_row_count": self.accepted_rows,
        }

    def to_evidence_dict(self) -> dict[str, Any]:
        """Return a PII-safe projection for durable evidence and lineage."""

        return {
            "passed": self.passed,
            "diagnostics": [item.to_evidence_dict() for item in self.diagnostics],
            "accepted_rows": self.accepted_rows,
            "rejected_rows": self.rejected_rows,
            "quarantined_rows": self.quarantined_rows,
            "state_commit_allowed": self.state_commit_allowed,
        }

    @property
    def data_outcome(self) -> str:
        if not self.passed:
            return "failed_quality_gate"
        if self.quarantined_rows:
            return "passed_with_quarantine"
        return "passed"

    @property
    def accepted_rows(self) -> int:
        return self.accepted_row_count if self.accepted_row_count is not None else len(self.target_rows)


class ContractEnforcementService:
    """Validate rows against portable schema contracts before staging."""

    def __init__(self, quarantine: DlqWriter | None = None) -> None:
        self._quarantine = quarantine

    def enforce(
        self,
        *,
        rows: Iterable[Mapping[str, Any]],
        contract: Any,
        run_id: str,
        load_id: str,
        conflict_policy: ConflictPolicy = "fail",
        row_offset: int = 0,
        finalize_dlq: bool = True,
    ) -> ContractEnforcementResult:
        target_rows: list[dict[str, Any]] = []
        diagnostics: list[ContractDiagnostic] = []
        rejected = 0
        quarantined = 0
        dlq_record_ids: list[str] = []
        dlq_reasons: Counter[str] = Counter()

        for row_index, raw_row in enumerate(rows):
            row = dict(raw_row)
            row_valid = True
            row_diagnostics: list[ContractDiagnostic] = []
            for column in (contract.columns or {}).values():
                ok, message = _validate_value(row.get(column.name), column)
                if ok:
                    continue
                action = _action(contract.enforcement, conflict_policy)
                diagnostic = ContractDiagnostic(
                    row_index=row_offset + row_index,
                    column=column.name,
                    action=action,
                    message=message,
                    expected_type=column.logical_type,
                    actual_value=_safe_value(row.get(column.name)),
                    reason_code=(
                        "schema.required_null"
                        if row.get(column.name) is None and not column.nullable
                        else "schema.type_mismatch"
                    ),
                    actual_type=_actual_type(row.get(column.name)),
                )
                row_diagnostics.append(diagnostic)
                row_valid = False
                if action == "variant_column":
                    row[f"{_VARIANT_PREFIX}{column.name}"] = row.get(column.name)
                    row[column.name] = None
                    row_valid = True
                elif action == "warn":
                    row_valid = True

            diagnostics.extend(row_diagnostics)
            if row_valid:
                target_rows.append(row)
                continue
            if contract.enforcement == "quarantine":
                record_id, reason_code = self._put_quarantine(
                    run_id=run_id,
                    load_id=load_id,
                    row=row,
                    diagnostics=row_diagnostics,
                    row_index=row_offset + row_index,
                )
                quarantined += 1
                dlq_record_ids.append(record_id)
                dlq_reasons[reason_code] += 1
                continue
            rejected += 1

        passed = rejected == 0
        return ContractEnforcementResult(
            passed=passed,
            target_rows=target_rows,
            diagnostics=tuple(diagnostics),
            rejected_rows=rejected,
            quarantined_rows=quarantined,
            state_commit_allowed=passed,
            dlq_record_ids=tuple(dlq_record_ids),
            dlq_reasons=dict(sorted(dlq_reasons.items())),
            dlq_index_ref=self._index_ref(run_id) if dlq_record_ids and finalize_dlq else None,
        )

    def _put_quarantine(
        self,
        *,
        run_id: str,
        load_id: str,
        row: dict[str, Any],
        diagnostics: list[ContractDiagnostic],
        row_index: int,
    ) -> tuple[str, str]:
        if self._quarantine is None:
            raise RuntimeError("DPONE_DLQ_STORE_REQUIRED: quarantine enforcement requires a durable DLQ store")
        first = diagnostics[0] if diagnostics else None
        reason_code = first.reason_code if first else "unknown.unclassified"
        receipt = self._quarantine.write_rejected(
            run_id=run_id,
            load_id=load_id,
            row=row,
            row_index=row_index,
            rejection=DlqRejection(
                reason_code=reason_code,
                stage="contract_enforcement",
                column=first.column if first else None,
                diagnostics=(
                    {
                        "column": first.column,
                        "expected_type": first.expected_type,
                        "actual_type": first.actual_type,
                    }
                    if first
                    else {}
                ),
            ),
        )
        return receipt.record_id, receipt.reason_code

    def _index_ref(self, run_id: str) -> str | None:
        if self._quarantine is None:
            return None
        index_ref = self._quarantine.finalize_run(run_id).get("index_ref")
        return str(index_ref) if index_ref else None


def _action(enforcement: str, conflict_policy: ConflictPolicy) -> ContractAction:
    if conflict_policy == "variant_column":
        return "variant_column"
    if conflict_policy == "quarantine":
        return "quarantine"
    if enforcement == "quarantine":
        return "quarantine"
    if enforcement == "warn":
        return "warn"
    return "fail"


def _validate_value(value: Any, column: Any) -> tuple[bool, str]:
    if value is None:
        return column.nullable, f"{column.name} is required but value is NULL"
    logical = column.logical_type.lower()
    if logical == "string":
        return isinstance(value, str), f"{column.name} must be string"
    if logical in {"integer", "bigint"}:
        return _is_int(value), f"{column.name} must be integer"
    if logical == "decimal":
        return _is_decimal(value, column), f"{column.name} must be decimal"
    if logical == "float":
        return _is_float(value), f"{column.name} must be float"
    if logical == "boolean":
        return _is_bool(value), f"{column.name} must be boolean"
    if logical == "timestamp":
        return _is_datetime(value), f"{column.name} must be timestamp"
    if logical == "date":
        return _is_date(value), f"{column.name} must be date"
    if logical == "time":
        return _is_time(value), f"{column.name} must be time"
    if logical == "json":
        return _is_json(value), f"{column.name} must be json"
    if logical == "array":
        return isinstance(value, list | tuple), f"{column.name} must be array"
    if logical == "binary":
        return isinstance(value, bytes | bytearray | memoryview), f"{column.name} must be binary"
    return True, ""


def _is_int(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    if isinstance(value, int):
        return True
    if isinstance(value, str):
        return value.strip().lstrip("-").isdigit()
    return False


def _is_decimal(value: Any, column: Any) -> bool:
    try:
        decimal = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return False
    exponent = decimal.as_tuple().exponent
    if isinstance(exponent, str):
        return False
    if column.scale is not None and abs(exponent) > column.scale:
        return False
    if column.precision is not None:
        digits = len(decimal.as_tuple().digits)
        return digits <= column.precision
    return True


def _is_float(value: Any) -> bool:
    if isinstance(value, bool):
        return False
    try:
        float(value)
    except (TypeError, ValueError):
        return False
    return True


def _is_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return True
    if isinstance(value, str):
        return value.strip().lower() in {"true", "false", "1", "0", "yes", "no"}
    return value in {0, 1}


def _is_datetime(value: Any) -> bool:
    if isinstance(value, datetime):
        return True
    if isinstance(value, str):
        try:
            datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return False
        return True
    return False


def _is_date(value: Any) -> bool:
    if isinstance(value, datetime):
        return False
    if isinstance(value, date):
        return True
    if isinstance(value, str):
        try:
            date.fromisoformat(value)
        except ValueError:
            return False
        return True
    return False


def _is_time(value: Any) -> bool:
    if isinstance(value, time):
        return True
    if isinstance(value, str):
        try:
            time.fromisoformat(value)
        except ValueError:
            return False
        return True
    return False


def _is_json(value: Any) -> bool:
    if isinstance(value, dict | list):
        return True
    if isinstance(value, str):
        try:
            json.loads(value)
        except json.JSONDecodeError:
            return False
        return True
    return False


def _safe_value(value: Any) -> str:
    rendered = repr(value)
    return rendered if len(rendered) <= 500 else rendered[:497] + "..."


def _actual_type(value: Any) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "float"
    if isinstance(value, str):
        return "string"
    if isinstance(value, Mapping):
        return "object"
    if isinstance(value, list | tuple):
        return "array"
    return type(value).__name__


__all__ = [
    "ContractDiagnostic",
    "ContractEnforcementResult",
    "ContractEnforcementService",
]
