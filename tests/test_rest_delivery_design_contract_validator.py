"""Executable contract tests for the governed REST delivery architecture."""

from __future__ import annotations

import json
import tomllib
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
import yaml
from jsonschema import Draft202012Validator

from dpone.cli import main as cli_main
from dpone.manifest.bounded_yaml import load_bounded_yaml
from dpone.services.docs.rest_delivery_design_contract import validate_design_contract

ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = ROOT / "docs" / "rest-bulk-delivery-design-contract-v1.yaml"
SCHEMA_PATH = ROOT / "docs" / "schema" / "rest-bulk-delivery-design-contract-v1.schema.json"
APPROVAL_SCHEMA_PATH = ROOT / "docs" / "schema" / "rest-bulk-delivery-approval-v1.schema.json"


def _loaded_contract() -> dict[str, Any]:
    loaded = load_bounded_yaml(CONTRACT_PATH.read_bytes())
    assert isinstance(loaded, dict)
    return loaded


def _validate(contract: dict[str, Any]):
    content = yaml.safe_dump(contract, sort_keys=False).encode("utf-8")
    return validate_design_contract(
        contract_content=content,
        schema_text=SCHEMA_PATH.read_text(encoding="utf-8"),
    )


def test_canonical_rest_delivery_design_contract_passes_all_gates() -> None:
    report = validate_design_contract(
        contract_content=CONTRACT_PATH.read_bytes(),
        schema_text=SCHEMA_PATH.read_text(encoding="utf-8"),
    )

    assert report.ok, report.issues
    assert report.to_dict()["kind"] == "docs.rest_delivery_design_contract"


def test_approval_receipt_schema_is_valid_and_separate_from_researched_design() -> None:
    approval_schema = json.loads(APPROVAL_SCHEMA_PATH.read_text(encoding="utf-8"))

    Draft202012Validator.check_schema(approval_schema)
    assert approval_schema["properties"]["schema"]["const"] == "dpone.rest-bulk-delivery-approval.v1"
    assert approval_schema["properties"]["decision"]["const"] == "APPROVED_FOR_IMPLEMENTATION"


def test_jsonschema_is_a_direct_base_dependency_for_public_validator_cli() -> None:
    project = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))

    assert "jsonschema>=4.23,<5" in project["project"]["dependencies"]


def test_duplicate_yaml_key_is_rejected_before_schema_validation() -> None:
    duplicate = CONTRACT_PATH.read_bytes() + b"\nversion: 1\n"

    report = validate_design_contract(
        contract_content=duplicate,
        schema_text=SCHEMA_PATH.read_text(encoding="utf-8"),
    )

    assert {issue.code for issue in report.issues} == {"yaml.duplicate_key"}


def test_cli_reports_invalid_utf8_without_traceback(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    contract_path = tmp_path / "invalid-contract.yaml"
    contract_path.write_bytes(b"schema: \xff\n")

    with pytest.raises(SystemExit) as raised:
        cli_main.main(
            [
                "docs",
                "check-rest-delivery-design-contract",
                "--contract",
                str(contract_path),
                "--schema",
                str(SCHEMA_PATH),
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert raised.value.code == 2
    assert payload["passed"] is False
    assert [issue["code"] for issue in payload["issues"]] == ["yaml.invalid_utf8"]
    assert "Traceback" not in captured.err


def test_json_schema_rejects_unknown_contract_property() -> None:
    contract = _loaded_contract()
    contract["unreviewed_extension"] = True

    report = _validate(contract)

    assert not report.ok
    assert {issue.code for issue in report.issues} == {"schema.validation"}


@pytest.mark.parametrize(
    ("mutator", "expected_code"),
    [
        (
            lambda contract: contract["state_machine"]["automatic_transitions"][3]["guards"].remove(
                "transport_proof_request_not_started"
            ),
            "retry.missing_guard",
        ),
        (
            lambda contract: contract["failure_scenarios"]["success_count_missing_unknown"].update(
                acknowledgement_preserved="FAILED"
            ),
            "failure.acknowledgement_lost",
        ),
        (
            lambda contract: contract["generation_fence"]["mutation_policies"]["incremental_append"].update(
                correction_revision_allowed=True
            ),
            "generation.append_correction",
        ),
        (
            lambda contract: contract["generation_fence"]["linearization"]["watermark"].update(representation="scalar"),
            "schema.validation",
        ),
        (
            lambda contract: contract["generation_fence"]["admission_rules"].remove(
                "lower_overlapping_generation_cannot_enter_submitting_after_higher_admission"
            ),
            "generation.admission_rules",
        ),
        (
            lambda contract: contract["identities"]["mutation_intent_digest"]["fields"].append(
                "execution_policy_digest"
            ),
            "identity.policy_changes_intent",
        ),
        (
            lambda contract: contract["identities"]["receiver_mutation_key"].update(fields=["delivery_key"]),
            "identity.receiver_mutation_key",
        ),
        (
            lambda contract: contract["identities"]["receiver_binding_semantics_digest"]["fields"].remove(
                "tls_policy_identity"
            ),
            "identity.receiver_binding_semantics",
        ),
        (
            lambda contract: contract["generation_fence"]["effect_conflict_domain_fields"].append(
                "operation_namespace"
            ),
            "generation.physical_conflict_domain",
        ),
        (
            lambda contract: contract["journal"]["immutable_pre_attempt_marker"].update(
                create_if_absent_key=["operation_id"]
            ),
            "attempt.marker_key",
        ),
        (
            lambda contract: contract["journal"]["remote_job_semaphore"].update(authority="process_memory"),
            "schema.validation",
        ),
        (
            lambda contract: contract["journal"]["mutation_gate"]["blocked_after"].remove("journal_epoch_mismatch"),
            "journal.restore_gate",
        ),
        (
            lambda contract: contract["continuations"].update(automatic=["poll_receipt", "observe_external_cancel"]),
            "continuation.automatic",
        ),
        (
            lambda contract: contract["async_execution"]["async_bulk_job"].update(max_in_flight_children=2),
            "continuation.async_parallel",
        ),
        (
            lambda contract: contract["parent_plan"].update(
                barriers=list(reversed(contract["parent_plan"]["barriers"]))
            ),
            "source.snapshot_lifetime",
        ),
        (
            lambda contract: contract["error_catalog"].pop("REST_DELIVERY_REMOTE_EFFECT_UNKNOWN"),
            "error_catalog.missing",
        ),
        (
            lambda contract: contract["promotion_strategy"]["approval_receipt"]["references"].remove("adr_digests"),
            "promotion.approval_references",
        ),
    ],
)
def test_contract_mutations_fail_with_stable_issue_codes(mutator, expected_code: str) -> None:
    contract = deepcopy(_loaded_contract())
    mutator(contract)

    report = _validate(contract)

    assert expected_code in {issue.code for issue in report.issues}


def test_graph_validator_rejects_unreachable_legal_tuple() -> None:
    contract = _loaded_contract()
    transitions = contract["state_machine"]["automatic_transitions"]
    transitions[:] = [item for item in transitions if item["to"] != "accepted"]

    report = _validate(contract)

    assert "state.unreachable" in {issue.code for issue in report.issues}


def test_terminal_tuple_cannot_have_automatic_outgoing_transition() -> None:
    contract = _loaded_contract()
    contract["state_machine"]["automatic_transitions"].append(
        {"from": "final_unknown_success_ack", "to": "final_full_eligible", "cas": "revision"}
    )

    report = _validate(contract)

    assert "state.terminal_has_automatic_exit" in {issue.code for issue in report.issues}


def test_reconciliation_requires_complete_effect_resolution_paths() -> None:
    contract = _loaded_contract()
    transitions = contract["state_machine"]["operator_only_transitions"]
    transitions[:] = [
        transition
        for transition in transitions
        if not (
            transition["from"] == "reconciling_unknown_accepted_ack"
            and transition["to"] == "final_full_accepted_ack_eligible"
        )
    ]

    report = _validate(contract)

    assert "state.reconciliation_resolution_gap" in {issue.code for issue in report.issues}


def test_reconciliation_cannot_rewrite_observed_acknowledgement() -> None:
    contract = _loaded_contract()
    transition = next(
        transition
        for transition in contract["state_machine"]["operator_only_transitions"]
        if transition["from"] == "reconciling_unknown_failed_ack"
        and transition["to"] == "final_full_failed_ack_eligible"
    )
    transition["to"] = "final_full_eligible"

    report = _validate(contract)

    assert "state.reconciliation_acknowledgement_drift" in {issue.code for issue in report.issues}
