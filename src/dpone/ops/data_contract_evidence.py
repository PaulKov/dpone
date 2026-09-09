"""Auditable evidence bundles for data contract runtime decisions."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.ops.ids import utc_now_iso
from dpone.type_system.enforcement import ContractEnforcementResult

SCHEMA_VERSION = "dpone.data_contract_evidence.v1"


@dataclass(frozen=True, slots=True)
class DataContractEvidenceArtifact:
    json_path: Path
    markdown_path: Path

    def to_dict(self) -> dict[str, str]:
        return {"json_path": str(self.json_path), "markdown_path": str(self.markdown_path)}


class DataContractEvidenceBundleWriter:
    """Write runtime contract evidence for certification and lineage systems."""

    def __init__(self, output_dir: str | Path) -> None:
        self.output_dir = Path(output_dir)

    def write(
        self,
        *,
        run_id: str,
        pipeline: str,
        enforcement: ContractEnforcementResult,
        ddl_apply: Mapping[str, Any] | None = None,
        compatibility: Mapping[str, Any] | None = None,
    ) -> DataContractEvidenceArtifact:
        directory = self.output_dir
        directory.mkdir(parents=True, exist_ok=True)
        payload = self._payload(
            run_id=run_id,
            pipeline=pipeline,
            enforcement=enforcement,
            ddl_apply=dict(ddl_apply or {}),
            compatibility=dict(compatibility or {}),
        )
        json_path = directory / "data_contract_evidence.json"
        markdown_path = directory / "data_contract_evidence.md"
        json_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
        markdown_path.write_text(_markdown(payload), encoding="utf-8")
        return DataContractEvidenceArtifact(json_path=json_path, markdown_path=markdown_path)

    def _payload(
        self,
        *,
        run_id: str,
        pipeline: str,
        enforcement: ContractEnforcementResult,
        ddl_apply: dict[str, Any],
        compatibility: dict[str, Any],
    ) -> dict[str, Any]:
        ddl_blockers = tuple(str(item) for item in ddl_apply.get("blockers", ()) if str(item))
        compatibility_passed = bool(compatibility.get("passed", True))
        passed = enforcement.passed and not ddl_blockers and compatibility_passed
        summary = {
            "accepted_rows": enforcement.accepted_rows,
            "rejected_rows": enforcement.rejected_rows,
            "quarantined_rows": enforcement.quarantined_rows,
            "diagnostics": len(enforcement.diagnostics),
            "state_commit_allowed": enforcement.state_commit_allowed,
        }
        return {
            "schema_version": SCHEMA_VERSION,
            "created_at": utc_now_iso(),
            "run_id": run_id,
            "pipeline": pipeline,
            "passed": passed,
            "execution_status": "succeeded",
            "data_outcome": enforcement.data_outcome,
            "summary": summary,
            "enforcement": enforcement.to_evidence_dict(),
            "dlq": {
                "schema": "dpone.dlq-index.v1",
                "record_count": enforcement.quarantined_rows,
                "record_ids": list(enforcement.dlq_record_ids),
                "reasons": dict(enforcement.dlq_reasons),
                "index_ref": enforcement.dlq_index_ref,
            },
            "ddl_apply": ddl_apply,
            "compatibility": compatibility,
            "openlineage_facets": {
                "dpone_data_contract": {
                    "_producer": "https://github.com/PaulKov/dpone",
                    "_schemaURL": "https://github.com/PaulKov/dpone/blob/master/docs/data-contract-runtime.md",
                    "passed": passed,
                    "summary": summary,
                }
            },
        }


def _markdown(payload: Mapping[str, Any]) -> str:
    summary = payload["summary"]
    return "\n".join(
        [
            "# dpone data contract evidence",
            "",
            f"- Schema version: `{payload['schema_version']}`",
            f"- Run ID: `{payload['run_id']}`",
            f"- Pipeline: `{payload['pipeline']}`",
            f"- Passed: `{payload['passed']}`",
            f"- Execution status: `{payload['execution_status']}`",
            f"- Data outcome: `{payload['data_outcome']}`",
            f"- Accepted rows: `{summary['accepted_rows']}`",
            f"- Rejected rows: `{summary['rejected_rows']}`",
            f"- Quarantined rows: `{summary['quarantined_rows']}`",
            f"- State commit allowed: `{summary['state_commit_allowed']}`",
            "",
            "## Runbook",
            "",
            "1. If rejected rows are non-zero, inspect contract diagnostics before advancing state.",
            "2. If quarantined rows are non-zero, export DLQ metadata and fix source/contract drift.",
            "3. If DDL blockers are present, rerun `dpone schema physical-plan` and schedule approval.",
            "4. Attach this evidence to certification and OpenLineage run artifacts.",
            "",
        ]
    )


__all__ = ["DataContractEvidenceArtifact", "DataContractEvidenceBundleWriter"]
