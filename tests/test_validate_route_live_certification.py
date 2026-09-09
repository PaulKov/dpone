"""Semantic release-gate tests for PostgreSQL→MSSQL route-live evidence."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import sys
from pathlib import Path

import pytest
from tools.route_live_certification.contract import REQUIRED_SUITE_IDS
from tools.route_live_certification.evidence_writer import (
    PassedCaseObservation,
    SuiteEvidenceWriter,
)
from tools.route_live_certification.inventory_writer import write_inventory
from tools.route_live_certification.provider_binding import build_provider_binding
from tools.route_live_certification.recorder import RouteLiveObservationRecorder
from tools.route_live_certification.registry import build_inventory, release_suites
from tools.route_live_certification.reviewed_case import ReviewedSuite, case

ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools" / "validate_route_live_certification.py"
SPEC = importlib.util.spec_from_file_location("validate_route_live_certification", TOOL)
assert SPEC and SPEC.loader
module = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = module
SPEC.loader.exec_module(module)

COMMIT = "a" * 40


def _digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def _fixture(tmp_path: Path) -> tuple[Path, Path, Path, dict[str, object]]:
    evidence_root = tmp_path / "evidence"
    junit_root = tmp_path / "junit"
    evidence_root.mkdir()
    junit_root.mkdir()
    inventory_path = tmp_path / "inventory.json"
    case_set = _digest(["case_a", "case_b"])
    evidence: dict[str, object] = {
        "suite_id": "wide",
        "schema_version": "dpone.postgres_mssql.wide.v1",
        "status": "certification_passed",
        "release_ready": True,
        "connector_doubles": False,
        "commit_sha": COMMIT,
        "workflow_run_id": "123456",
        "workflow_run_attempt": 2,
        "case_count": 2,
        "case_set_sha256": case_set,
        "expected_case_ids": ["case_a", "case_b"],
        "completed_case_ids": ["case_a", "case_b"],
        "cases": [
            {
                "case_id": "case_a",
                "status": "passed",
                "config_sha256": "1" * 64,
                "action_class": "load",
                "outcome_class": "inserted",
                "before_image_sha256": "2" * 64,
                "after_image_sha256": "3" * 64,
                "expected_mutation": True,
                "observations": {"inserted_rows": 1},
            },
            {
                "case_id": "case_b",
                "status": "passed",
                "config_sha256": "4" * 64,
                "action_class": "unsupported_preflight",
                "outcome_class": "typed_reject_before_staging",
                "before_image_sha256": "5" * 64,
                "after_image_sha256": "5" * 64,
                "expected_mutation": False,
                "observations": {"staging_objects_after": 0},
            },
        ],
        "vendors": {
            "postgresql": {"version": "16.4", "image_digest": "sha256:" + "6" * 64},
            "postgis": {"version": "3.5.7", "image_digest": "sha256:" + "8" * 64},
            "mssql": {
                "version": "16.0.4165.4",
                "build": "SQL Server 2022 CU",
                "image_digest": "sha256:" + "7" * 64,
            },
            "transport": {"odbc_driver": "18.5", "bcp_version": "18.5"},
            "runtime": {
                "python_version": "3.12.7",
                "dpone_version": "0.74.0",
                "source_sha": COMMIT,
            },
        },
    }
    case_contracts = [
        {
            key: case[key]
            for key in (
                "case_id",
                "config_sha256",
                "action_class",
                "outcome_class",
                "expected_mutation",
            )
        }
        for case in evidence["cases"]
    ]
    case_contract = _digest(case_contracts)
    evidence["case_contract_sha256"] = case_contract
    testcases = "".join(
        '<testcase name="certified-case"><properties>'
        f'<property name="dpone_suite_id" value="wide"/>'
        f'<property name="dpone_case_id" value="{case["case_id"]}"/>'
        f'<property name="dpone_case_contract_sha256" value="{_digest(case)}"/>'
        f'<property name="dpone_commit_sha" value="{COMMIT}"/>'
        '<property name="dpone_workflow_run_id" value="123456"/>'
        '<property name="dpone_workflow_run_attempt" value="2"/>'
        "</properties></testcase>"
        for case in case_contracts
    )
    junit = (f'<testsuite tests="2" failures="0" errors="0" skipped="0">{testcases}</testsuite>').encode()
    (junit_root / "wide.xml").write_bytes(junit)
    evidence["junit"] = {
        "path": "wide.xml",
        "sha256": hashlib.sha256(junit).hexdigest(),
        "tests": 2,
        "failures": 0,
        "errors": 0,
        "skipped": 0,
    }
    vendor_policy = {
        section: {key: value for key, value in fields.items() if key != "source_sha"}
        for section, fields in evidence["vendors"].items()
    }
    inventory = {
        "schema_version": "dpone.route_live.postgres_mssql.inventory.v1",
        "route": "postgres_mssql",
        "vendors": vendor_policy,
        "suites": [
            {
                "suite_id": "wide",
                "evidence_file": "wide.json",
                "junit_file": "wide.xml",
                "schema_version": "dpone.postgres_mssql.wide.v1",
                "case_count": 2,
                "case_set_sha256": case_set,
                "case_contract_sha256": case_contract,
                "case_contracts": case_contracts,
            }
        ],
    }
    inventory_path.write_text(json.dumps(inventory), encoding="utf-8")
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")
    return inventory_path, evidence_root, junit_root, evidence


def _reviewed_fixture() -> ReviewedSuite:
    return ReviewedSuite.create(
        "wide",
        "dpone.postgres_mssql.wide.v1",
        [
            case(
                "wide",
                "case_a",
                {"mode": "load"},
                action="load",
                outcome="inserted",
                mutation=True,
            ),
            case(
                "wide",
                "case_b",
                {"mode": "unsupported_preflight"},
                action="unsupported_preflight",
                outcome="typed_reject_before_staging",
                mutation=False,
            ),
        ],
    )


def _vendor_policy() -> dict[str, dict[str, str]]:
    return {
        "postgresql": {
            "version": "16.4",
            "image_digest": ("sha256:44c4ee9810eff91f7eab4d822642e01115b1a9eccce4bcbdde7604752d68eac6"),
        },
        "postgis": {
            "version": "3.5.7",
            "image_digest": ("sha256:b193e996618e9e632e2c6e268462b350c28a9c871cb0352b32905fc01e0299bd"),
        },
        "mssql": {
            "version": "16.0.4165.4",
            "build": "SQL Server 2022 CU",
            "image_digest": ("sha256:ba4c8329f48fb8f02e1416be6a930ebfd71268caee78aa985f3af4315e457c89"),
        },
        "transport": {"odbc_driver": "18.5", "bcp_version": "18.5"},
        "runtime": {"python_version": "3.12.7", "dpone_version": "0.74.0"},
    }


def _observations() -> list[PassedCaseObservation]:
    return [
        PassedCaseObservation("case_a", "2" * 64, "3" * 64, {"inserted_rows": 1}),
        PassedCaseObservation("case_b", "5" * 64, "5" * 64, {"staging_objects_after": 0}),
    ]


def _validate(tmp_path: Path) -> dict[str, object]:
    inventory, evidence, junit, _payload = _fixture(tmp_path)
    return module.validate_certification(
        inventory_path=inventory,
        evidence_root=evidence,
        junit_root=junit,
        binding=module.WorkflowBinding(COMMIT, "123456", 2),
        required_suite_ids=("wide",),
    )


def test_semantic_gate_consolidates_exact_commit_inventory_and_junit(tmp_path: Path) -> None:
    result = _validate(tmp_path)

    assert result["status"] == "certification_passed"
    assert result["release_ready"] is True
    assert result["commit_sha"] == COMMIT
    assert result["suite_count"] == 1
    assert result["case_count"] == 2
    assert len(result["manifest_sha256"]) == 64
    assert result["suites"][0]["junit"]["tests"] == 2


def test_release_gate_rejects_inventory_that_omits_any_required_suite(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, _evidence = _fixture(tmp_path)

    with pytest.raises(module.CertificationValidationError, match="required_suites_mismatch"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
        )


@pytest.mark.parametrize(
    ("mutation", "code"),
    (
        (lambda value: value.update(status="passed_partial"), "evidence.status_mismatch"),
        (lambda value: value.update(release_ready=False), "evidence.release_ready_mismatch"),
        (lambda value: value.update(connector_doubles=True), "evidence.connector_doubles_mismatch"),
        (lambda value: value.update(commit_sha="b" * 40), "evidence.commit_sha_mismatch"),
        (
            lambda value: value.update(completed_case_ids=["case_a"]),
            "evidence.case_inventory_mismatch",
        ),
        (
            lambda value: value["cases"][0].update(status="skipped"),
            "evidence.case_not_passed",
        ),
        (
            lambda value: value["cases"][0].update(config_sha256="9" * 64),
            "evidence.case_contract_mismatch",
        ),
        (
            lambda value: value["cases"][0].update(action_class="different_action"),
            "evidence.case_contract_mismatch",
        ),
        (
            lambda value: value["cases"][1].update(expected_mutation=True),
            "evidence.case_expected_mutation_missing",
        ),
    ),
)
def test_semantic_gate_rejects_partial_or_unbound_evidence(
    tmp_path: Path,
    mutation,
    code: str,
) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    mutation(evidence)
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match=code):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_recomputes_junit_hash_and_zero_skip_policy(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    junit = (junit_root / "wide.xml").read_bytes()
    junit = junit.replace(b'skipped="0"', b'skipped="1"', 1).replace(
        b"</properties></testcase>",
        b"</properties><skipped/></testcase>",
        1,
    )
    (junit_root / "wide.xml").write_bytes(junit)
    evidence["junit"]["sha256"] = hashlib.sha256(junit).hexdigest()
    evidence["junit"]["skipped"] = 1
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="junit_not_green"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


@pytest.mark.parametrize("extra", ("evidence", "junit", "binary"))
def test_semantic_gate_rejects_unreviewed_extra_files(tmp_path: Path, extra: str) -> None:
    inventory, evidence_root, junit_root, _evidence = _fixture(tmp_path)
    if extra == "evidence":
        (evidence_root / "unreviewed.json").write_text("{}", encoding="utf-8")
        code = "file_inventory_mismatch"
    elif extra == "junit":
        (junit_root / "unreviewed.xml").write_text("<testsuite/>", encoding="utf-8")
        code = "junit_file_inventory_mismatch"
    else:
        (evidence_root / "unreviewed.bin").write_bytes(b"not reviewed")
        code = "file_inventory_mismatch"

    with pytest.raises(module.CertificationValidationError, match=code):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_binds_junit_testcases_to_reviewed_case_ids(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    junit = (
        (junit_root / "wide.xml")
        .read_bytes()
        .replace(
            b'value="case_b"',
            b'value="unreviewed"',
            1,
        )
    )
    (junit_root / "wide.xml").write_bytes(junit)
    evidence["junit"]["sha256"] = hashlib.sha256(junit).hexdigest()
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="junit_case_contract_mismatch"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_derives_junit_failure_children_instead_of_trusting_totals(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    junit = (
        (junit_root / "wide.xml")
        .read_bytes()
        .replace(
            b"</properties></testcase>",
            b'</properties><failure message="forged"/></testcase>',
            1,
        )
    )
    (junit_root / "wide.xml").write_bytes(junit)
    evidence["junit"]["sha256"] = hashlib.sha256(junit).hexdigest()
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="junit_declared_totals_mismatch"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_requires_unchanged_before_image_for_non_mutating_case(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    evidence["cases"][1]["after_image_sha256"] = "9" * 64
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="case_before_image_changed"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_rejects_non_digest_vendor_identity(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    evidence["vendors"]["postgresql"]["image_digest"] = "sha256:mutable-tag"
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="vendor_digest_invalid"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_rejects_unreviewed_but_well_formed_vendor_digest(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    evidence["vendors"]["postgresql"]["image_digest"] = "sha256:" + "9" * 64
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="vendor_value_mismatch"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_rejects_inventory_and_junit_path_traversal(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, _evidence = _fixture(tmp_path)
    payload = json.loads(inventory.read_text(encoding="utf-8"))
    payload["suites"][0]["evidence_file"] = "../wide.json"
    inventory.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="evidence_file_unsafe"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_rejects_duplicate_json_keys(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, _evidence = _fixture(tmp_path)
    payload = (evidence_root / "wide.json").read_text(encoding="utf-8")
    payload = payload.replace(
        '"status": "certification_passed"',
        '"status": "failed", "status": "certification_passed"',
        1,
    )
    (evidence_root / "wide.json").write_text(payload, encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="duplicate_key:status"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_rejects_unknown_semantic_fields(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    evidence["unreviewed_override"] = True
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="evidence.keys_invalid"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_rejects_boolean_as_numeric_workflow_attempt(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    evidence["workflow_run_attempt"] = True
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="workflow_run_attempt_mismatch"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 1),
            required_suite_ids=("wide",),
        )


@pytest.mark.parametrize("location", ("inventory", "evidence", "junit"))
def test_output_path_must_be_disjoint_from_every_authority_input(
    tmp_path: Path,
    location: str,
) -> None:
    inventory, evidence_root, junit_root, _evidence = _fixture(tmp_path)
    outputs = {
        "inventory": inventory,
        "evidence": evidence_root / "consolidated.json",
        "junit": junit_root / "consolidated.json",
    }

    with pytest.raises(module.CertificationValidationError, match="output.path_"):
        module.require_output_path_disjoint(
            outputs[location],
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
        )


def test_semantic_gate_rejects_numeric_case_identifier_coercion(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    evidence["expected_case_ids"] = [1, "case_b"]
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="expected_case_ids_invalid"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_output_path_rejects_hardlink_to_inventory(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, _evidence = _fixture(tmp_path)
    output = tmp_path / "consolidated.json"
    os.link(inventory, output)

    with pytest.raises(module.CertificationValidationError, match="output.path_aliases_input"):
        module.require_output_path_disjoint(
            output,
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
        )


def test_semantic_gate_rejects_namespaced_junit_outcome(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    junit = (
        (junit_root / "wide.xml")
        .read_bytes()
        .replace(
            b'<testsuite tests="2"',
            b'<testsuite xmlns:x="urn:forged" tests="2"',
            1,
        )
        .replace(
            b"</properties></testcase>",
            b'</properties><x:failure message="hidden"/></testcase>',
            1,
        )
    )
    (junit_root / "wide.xml").write_bytes(junit)
    evidence["junit"]["sha256"] = hashlib.sha256(junit).hexdigest()
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="junit_namespace_unsupported"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO is not supported on this platform")
def test_semantic_gate_rejects_special_files_in_clean_root(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, _evidence = _fixture(tmp_path)
    os.mkfifo(evidence_root / "unreviewed.fifo")

    with pytest.raises(module.CertificationValidationError, match="file_inventory_mismatch"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_semantic_gate_requires_observed_change_for_mutating_case(tmp_path: Path) -> None:
    inventory, evidence_root, junit_root, evidence = _fixture(tmp_path)
    evidence["cases"][0]["after_image_sha256"] = evidence["cases"][0]["before_image_sha256"]
    (evidence_root / "wide.json").write_text(json.dumps(evidence), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="expected_mutation_missing"):
        module.validate_certification(
            inventory_path=inventory,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            required_suite_ids=("wide",),
        )


def test_reviewed_writer_creates_exact_idempotent_pair_accepted_by_gate(tmp_path: Path) -> None:
    suite = _reviewed_fixture()
    evidence_root = tmp_path / "evidence"
    junit_root = tmp_path / "junit"
    writer = SuiteEvidenceWriter(
        suite=suite,
        evidence_root=evidence_root,
        junit_root=junit_root,
        binding=module.WorkflowBinding(COMMIT, "123456", 2),
        vendor_policy=_vendor_policy(),
    )

    first = writer.write(_observations())
    second = writer.write(_observations())

    assert first == second
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(
        json.dumps(
            build_inventory(
                _vendor_policy(),
                suites=(suite,),
                required_suite_ids=("wide",),
            )
        ),
        encoding="utf-8",
    )
    result = module.validate_certification(
        inventory_path=inventory_path,
        evidence_root=evidence_root,
        junit_root=junit_root,
        binding=module.WorkflowBinding(COMMIT, "123456", 2),
        required_suite_ids=("wide",),
        reviewed_suite_entries=(suite.inventory_entry(),),
    )
    assert result["case_count"] == 2
    assert result["suites"][0]["junit"]["case_ids"] == ["case_a", "case_b"]


@pytest.mark.parametrize(
    ("observations", "code"),
    (
        (lambda values: values[:1], "writer.case_inventory_mismatch"),
        (
            lambda values: [
                PassedCaseObservation("case_a", "2" * 64, "2" * 64, {}),
                values[1],
            ],
            "writer.case_mutation_mismatch",
        ),
        (
            lambda values: [values[0], values[0], values[1]],
            "writer.case_duplicate",
        ),
    ),
)
def test_reviewed_writer_rejects_partial_duplicate_or_false_mutation(
    tmp_path: Path,
    observations,
    code: str,
) -> None:
    writer = SuiteEvidenceWriter(
        suite=_reviewed_fixture(),
        evidence_root=tmp_path / "evidence",
        junit_root=tmp_path / "junit",
        binding=module.WorkflowBinding(COMMIT, "123456", 2),
        vendor_policy=_vendor_policy(),
    )

    with pytest.raises(module.CertificationValidationError, match=code):
        writer.write(observations(_observations()))

    assert not (tmp_path / "evidence").exists()
    assert not (tmp_path / "junit").exists()


def test_reviewed_writer_never_replaces_conflicting_existing_evidence(tmp_path: Path) -> None:
    writer = SuiteEvidenceWriter(
        suite=_reviewed_fixture(),
        evidence_root=tmp_path / "evidence",
        junit_root=tmp_path / "junit",
        binding=module.WorkflowBinding(COMMIT, "123456", 2),
        vendor_policy=_vendor_policy(),
    )
    evidence_path, _junit_path = writer.write(_observations())
    original = evidence_path.read_bytes()
    changed = [
        PassedCaseObservation("case_a", "2" * 64, "4" * 64, {"inserted_rows": 2}),
        _observations()[1],
    ]

    with pytest.raises(module.CertificationValidationError, match="writer.evidence:wide.conflict"):
        writer.write(changed)

    assert evidence_path.read_bytes() == original


def test_observation_recorder_publishes_only_complete_reviewed_campaign(tmp_path: Path) -> None:
    suite = _reviewed_fixture()
    evidence_root = tmp_path / "evidence"
    junit_root = tmp_path / "junit"

    def writer_factory(reviewed: ReviewedSuite) -> SuiteEvidenceWriter:
        return SuiteEvidenceWriter(
            suite=reviewed,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=module.WorkflowBinding(COMMIT, "123456", 2),
            vendor_policy=_vendor_policy(),
        )

    recorder = RouteLiveObservationRecorder(
        environment={"DPONE_ROUTE_LIVE_INVENTORY": str(tmp_path / "inventory.json")},
        suites=(suite,),
        writer_factory=writer_factory,
    )
    recorder.observe_parameters(
        "wide",
        {"mode": "load"},
        before_image={"rows": []},
        after_image={"rows": [{"id": 1}]},
        observations={"inserted_rows": 1},
    )
    recorder.observe_case(
        "wide",
        "case_b",
        before_image={"rows": [{"id": 1}]},
        after_image={"rows": [{"id": 1}]},
        observations={"staging_objects_after": 0},
    )

    written = recorder.write_complete_authority()

    assert len(written) == 1
    assert recorder.coverage() == {"wide": (2, 2)}
    inventory_path = tmp_path / "inventory.json"
    inventory_path.write_text(
        json.dumps(
            build_inventory(
                _vendor_policy(),
                suites=(suite,),
                required_suite_ids=("wide",),
            )
        ),
        encoding="utf-8",
    )
    result = module.validate_certification(
        inventory_path=inventory_path,
        evidence_root=evidence_root,
        junit_root=junit_root,
        binding=module.WorkflowBinding(COMMIT, "123456", 2),
        required_suite_ids=("wide",),
        reviewed_suite_entries=(suite.inventory_entry(),),
    )
    assert result["case_count"] == 2


def test_observation_recorder_rejects_gap_and_seals_authoritative_campaign(tmp_path: Path) -> None:
    recorder = RouteLiveObservationRecorder(
        environment={"DPONE_ROUTE_LIVE_INVENTORY": str(tmp_path / "inventory.json")},
        suites=(_reviewed_fixture(),),
        writer_factory=lambda _suite: pytest.fail("incomplete campaign must not create a writer"),
    )
    recorder.observe_case(
        "wide",
        "case_a",
        before_image=[],
        after_image=[1],
        observations={},
    )

    with pytest.raises(module.CertificationValidationError, match="recorder.campaign_incomplete:wide:1"):
        recorder.write_complete_authority()
    with pytest.raises(module.CertificationValidationError, match="recorder.campaign_sealed"):
        recorder.observe_case(
            "wide",
            "case_b",
            before_image=[],
            after_image=[],
            observations={},
        )


def test_observation_recorder_local_partial_session_never_publishes(tmp_path: Path) -> None:
    recorder = RouteLiveObservationRecorder(
        environment={},
        suites=(_reviewed_fixture(),),
        writer_factory=lambda _suite: pytest.fail("local session must not create a writer"),
    )

    assert recorder.write_complete_authority() == ()
    assert not tuple(tmp_path.iterdir())


@pytest.mark.parametrize(
    ("operation", "code"),
    (
        (
            lambda recorder: recorder.observe_case("wide", "missing", before_image=[], after_image=[], observations={}),
            "recorder.case_not_reviewed",
        ),
        (
            lambda recorder: recorder.observe_parameters(
                "wide", {"mode": "forged"}, before_image=[], after_image=[], observations={}
            ),
            "recorder.parameters_not_reviewed",
        ),
        (
            lambda recorder: recorder.observe_case("wide", "case_a", before_image=[], after_image=[], observations={}),
            "recorder.case_mutation_mismatch",
        ),
    ),
)
def test_observation_recorder_rejects_unreviewed_or_false_observation(operation, code: str) -> None:
    recorder = RouteLiveObservationRecorder(environment={}, suites=(_reviewed_fixture(),))

    with pytest.raises(module.CertificationValidationError, match=code):
        operation(recorder)


def test_release_reviewed_registry_is_closed_and_exhaustive() -> None:
    suites = release_suites()
    counts = {suite.suite_id: len(suite.cases) for suite in suites}

    assert tuple(counts) == REQUIRED_SUITE_IDS
    assert counts["schema_evolution"] == 4762
    assert counts["strategy_capability"] == 92
    assert counts["wide_strategy"] == 8
    assert counts["wide_performance_soak"] == 56
    assert counts["boundary_types"] >= 128
    assert counts["explicit_types"] == 546
    assert counts["boundary_types"] == 404
    assert counts["physical_design"] == 279
    assert sum(counts.values()) == 6340


def test_inventory_writer_is_create_only_and_rejects_stale_vendor_replay(tmp_path: Path) -> None:
    vendor_path = tmp_path / "vendors.json"
    output = tmp_path / "inventory.json"
    vendor_path.write_text(json.dumps(_vendor_policy()), encoding="utf-8")

    first = write_inventory(vendor_metadata_path=vendor_path, output=output)
    second = write_inventory(vendor_metadata_path=vendor_path, output=output)
    assert first == second
    assert len(first["suites"]) == len(REQUIRED_SUITE_IDS)

    changed = _vendor_policy()
    changed["postgresql"]["version"] = "16.5"
    vendor_path.write_text(json.dumps(changed), encoding="utf-8")
    with pytest.raises(module.CertificationValidationError, match="inventory_writer.output.conflict"):
        write_inventory(vendor_metadata_path=vendor_path, output=output)


def test_inventory_writer_rejects_unreviewed_image_before_writing(tmp_path: Path) -> None:
    vendors = _vendor_policy()
    vendors["mssql"]["image_digest"] = "sha256:" + "9" * 64
    vendor_path = tmp_path / "vendors.json"
    output = tmp_path / "inventory.json"
    vendor_path.write_text(json.dumps(vendors), encoding="utf-8")

    with pytest.raises(module.CertificationValidationError, match="vendor_image_not_allowed:mssql"):
        write_inventory(vendor_metadata_path=vendor_path, output=output)

    assert not output.exists()


def test_provider_binding_recomputes_manifest_semantic_digest(tmp_path: Path) -> None:
    manifest = _validate(tmp_path)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")

    binding = build_provider_binding(
        manifest_path=manifest_path,
        artifact_name="release-candidate-route",
        artifact_id=42,
        artifact_digest="sha256:" + "9" * 64,
        commit_sha=COMMIT,
        workflow_run_id=123456,
        workflow_run_attempt=2,
    )
    assert binding["manifest_sha256"] == manifest["manifest_sha256"]

    manifest["case_count"] = 3
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    with pytest.raises(
        module.CertificationValidationError,
        match="manifest_semantic_digest_mismatch",
    ):
        build_provider_binding(
            manifest_path=manifest_path,
            artifact_name="release-candidate-route",
            artifact_id=42,
            artifact_digest="sha256:" + "9" * 64,
            commit_sha=COMMIT,
            workflow_run_id=123456,
            workflow_run_attempt=2,
        )
