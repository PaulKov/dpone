from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.strategy_intelligence.native_transfer import (
    NativeTransferPlanBuilder,
    NativeTransferRequest,
)
from dpone.strategy_intelligence.native_transfer_evidence_artifacts import (
    NativeTransferEvidenceArtifactWriter,
)


def test_native_transfer_evidence_writer_emits_required_artifacts_and_index(tmp_path: Path) -> None:
    plan = _partitioned_cdc_plan()
    writer = NativeTransferEvidenceArtifactWriter(base_dir=tmp_path)

    result = writer.write(
        run_id="01J00000000000000000000000",
        evidence_contract=plan.evidence_contract,
        payloads=_payloads_for(plan.evidence_contract),
    )

    assert result.index_path.exists()
    assert result.markdown_path.exists()
    assert sorted(path.name for path in result.artifact_paths) == sorted(plan.evidence_contract["required_artifacts"])
    index = json.loads(result.index_path.read_text(encoding="utf-8"))
    assert index["run_id"] == "01J00000000000000000000000"
    assert index["schema_version"] == "dpone.native_transfer.evidence.v1"
    assert index["contract"]["route"] == "postgres_to_mssql"
    assert all(entry["sha256"] for entry in index["artifacts"])


def test_native_transfer_evidence_writer_fails_when_required_payload_is_missing(tmp_path: Path) -> None:
    plan = _partitioned_cdc_plan()
    payloads = _payloads_for(plan.evidence_contract)
    payloads.pop("typed_reconciliation.json")

    with pytest.raises(ValueError, match="missing required native transfer evidence artifact payloads"):
        NativeTransferEvidenceArtifactWriter(base_dir=tmp_path).write(
            run_id="01J00000000000000000000000",
            evidence_contract=plan.evidence_contract,
            payloads=payloads,
        )


def _partitioned_cdc_plan() -> object:
    return NativeTransferPlanBuilder().build(
        NativeTransferRequest(
            source_type="postgres",
            sink_type="mssql",
            source_table="public.events",
            target_table="dbo.events",
            strategy="cdc_apply",
            unique_key=("event_id",),
            source_options={
                "cdc": {"mode": "logical_replication", "state_boundary": "logical_lsn"},
                "partitioning": {"column": "event_id", "max_partitions": 2},
            },
            sink_options={"deletes": {"mode": "soft_delete"}},
        )
    )


def _payloads_for(contract: dict) -> dict[str, dict]:
    return {
        artifact: {
            "artifact": artifact,
            "status": "passed",
            "route": contract["route"],
        }
        for artifact in contract["required_artifacts"]
    }
