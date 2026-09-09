"""Approval and expand-contract workflow artifacts for schema changes."""

from __future__ import annotations

import hashlib
import json
from dataclasses import asdict, dataclass
from pathlib import Path

from dpone.readiness.ddl_governance import DdlGovernancePolicy, OnlineSchemaPlanner
from dpone.readiness.schema_evolution import ColumnDef, SchemaComparator, SchemaEvolutionPolicy


@dataclass(frozen=True, slots=True)
class SchemaApprovalRecord:
    approval_id: str
    approval_status: str
    approver: str
    ledger_path: str
    artifact_path: str

    def to_dict(self) -> dict[str, object]:
        return asdict(self)


class SchemaApprovalService:
    def approve(self, *, ledger_path: str | Path, approver: str, output_dir: str | Path) -> SchemaApprovalRecord:
        ledger = Path(ledger_path)
        payload = json.loads(ledger.read_text(encoding="utf-8"))
        approval_id = "appr_" + hashlib.sha256(f"{ledger}:{approver}:{payload.get('status')}".encode()).hexdigest()[:24]
        target = Path(output_dir)
        target.mkdir(parents=True, exist_ok=True)
        artifact = target / f"{approval_id}.json"
        record = SchemaApprovalRecord(
            approval_id=approval_id,
            approval_status="approved",
            approver=approver,
            ledger_path=str(ledger),
            artifact_path=str(artifact),
        )
        artifact.write_text(
            json.dumps(record.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return record


@dataclass(frozen=True, slots=True)
class ExpandContractPlan:
    table: str
    dialect: str
    phases: tuple[str, str, str]
    actions: tuple[dict[str, object], ...]
    artifact_path: str

    def to_dict(self) -> dict[str, object]:
        return {
            "table": self.table,
            "dialect": self.dialect,
            "phases": list(self.phases),
            "actions": list(self.actions),
            "artifact_path": self.artifact_path,
        }


class ExpandContractService:
    def build(
        self,
        *,
        source_columns: list[ColumnDef],
        target_columns: list[ColumnDef],
        table: str,
        dialect: str,
        output_dir: str | Path,
    ) -> ExpandContractPlan:
        schema_plan = SchemaComparator(SchemaEvolutionPolicy(mode="widening", on_type_change="new_column")).compare(
            source_columns, target_columns
        )
        governed = OnlineSchemaPlanner().plan(
            schema_plan=schema_plan,
            dialect=dialect,
            table=table,
            policy=DdlGovernancePolicy(ddl_mode="manual_approval", data_type="variant_column"),
        )
        actions = tuple(
            {
                "phase": "expand" if action.ddl else "contract",
                "schema_change_id": action.schema_change_id,
                "column": action.column,
                "change_type": action.change_type,
                "decision": action.decision,
                "ddl": list(action.ddl),
                "guidance": "expand compatible schema, backfill safely, then contract manually",
            }
            for action in governed.actions
        )
        target = Path(output_dir)
        target.mkdir(parents=True, exist_ok=True)
        artifact = target / f"expand_contract_{_safe_name(table)}.json"
        plan = ExpandContractPlan(
            table=table,
            dialect=dialect,
            phases=("expand", "backfill", "contract"),
            actions=actions,
            artifact_path=str(artifact),
        )
        artifact.write_text(
            json.dumps(plan.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        return plan


def load_columns(path: str | Path) -> list[ColumnDef]:
    raw = json.loads(Path(path).read_text(encoding="utf-8"))
    return [ColumnDef(str(item["name"]), str(item["dtype"]), bool(item.get("nullable", True))) for item in raw]


def _safe_name(value: str) -> str:
    return "".join(ch if ch.isalnum() else "_" for ch in value).strip("_") or "table"
