from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.strategy_intelligence.native_transfer_evidence_service import NativeTransferEvidenceBundleService


def test_native_transfer_evidence_service_accepts_contract_json(tmp_path: Path) -> None:
    contract = {
        "route": "postgres_to_mssql",
        "strategy": "cdc_apply",
        "state_commit_gate": "after_target_finalize_and_quality",
        "required_artifacts": ["typed_reconciliation.json"],
        "required_checks": ["typed_reconciliation"],
        "partition_retry": {"enabled": False},
    }
    contract_path = _write_json(tmp_path / "contract.json", contract)
    payload_path = _write_json(tmp_path / "typed_reconciliation.json", {"status": "passed"})

    result = NativeTransferEvidenceBundleService().write_from_files(
        run_id="01J00000000000000000000000",
        plan_json=None,
        contract_json=contract_path,
        payload_specs=(f"typed_reconciliation.json={payload_path}",),
        output_dir=tmp_path / "out",
    )

    assert Path(result["index_path"]).exists()


def test_native_transfer_evidence_service_rejects_missing_contract(tmp_path: Path) -> None:
    plan_path = _write_json(tmp_path / "plan.json", {"native_transfer_plan": {}})

    with pytest.raises(ValueError, match="evidence_contract was not found"):
        NativeTransferEvidenceBundleService().write_from_files(
            run_id="01J00000000000000000000000",
            plan_json=plan_path,
            contract_json=None,
            payload_specs=(),
            output_dir=tmp_path / "out",
        )


def test_native_transfer_evidence_service_rejects_bad_payload_spec(tmp_path: Path) -> None:
    contract_path = _write_json(
        tmp_path / "contract.json",
        {
            "required_artifacts": ["typed_reconciliation.json"],
        },
    )

    with pytest.raises(ValueError, match="artifact_name=path"):
        NativeTransferEvidenceBundleService().write_from_files(
            run_id="01J00000000000000000000000",
            plan_json=None,
            contract_json=contract_path,
            payload_specs=("typed_reconciliation.json",),
            output_dir=tmp_path / "out",
        )


def _write_json(path: Path, payload: dict) -> Path:
    path.write_text(json.dumps(payload), encoding="utf-8")
    return path
