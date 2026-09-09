"""Suite validation and exact-commit consolidated manifest builder."""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from .contract import (
    CASE_KEYS,
    CONSOLIDATED_VERSION,
    EVIDENCE_KEYS,
    INVENTORY_KEYS,
    INVENTORY_VERSION,
    PASS_STATUS,
    REQUIRED_SUITE_IDS,
    SuiteInventory,
    WorkflowBinding,
    case_ids,
    digest,
    fail,
    required_sha256,
    required_text,
)
from .io_authority import json_object, require_exact_relative_files, resolve_under
from .junit import validate_junit
from .vendors import parse_vendor_policy, validate_vendor_metadata


def validate_certification(
    *,
    inventory_path: Path,
    evidence_root: Path,
    junit_root: Path,
    binding: WorkflowBinding,
    required_suite_ids: tuple[str, ...] = REQUIRED_SUITE_IDS,
    reviewed_suite_entries: tuple[dict[str, object], ...] | None = None,
) -> dict[str, object]:
    """Validate every required suite and return a consolidated manifest."""

    inventory_bytes = inventory_path.read_bytes()
    inventory = json_object(inventory_bytes, code="inventory.json_invalid")
    if set(inventory) != INVENTORY_KEYS:
        fail("inventory.keys_invalid")
    if inventory.get("schema_version") != INVENTORY_VERSION:
        fail("inventory.schema_version_invalid")
    if inventory.get("route") != "postgres_mssql":
        fail("inventory.route_invalid")
    vendor_policy = parse_vendor_policy(inventory.get("vendors"))
    raw_suites = inventory.get("suites")
    if not isinstance(raw_suites, list) or not raw_suites:
        fail("inventory.suites_required")
    if reviewed_suite_entries is not None and raw_suites != list(reviewed_suite_entries):
        fail("inventory.reviewed_contract_mismatch")
    suites = tuple(SuiteInventory.parse(value) for value in raw_suites)
    _validate_inventory_shape(suites, required_suite_ids=required_suite_ids)
    require_exact_relative_files(
        evidence_root,
        expected=tuple(suite.evidence_file for suite in suites),
        code="evidence.file_inventory_mismatch",
    )
    require_exact_relative_files(
        junit_root,
        expected=tuple(suite.junit_file for suite in suites),
        code="evidence.junit_file_inventory_mismatch",
    )
    validated = tuple(
        _validate_suite(
            suite,
            evidence_root=evidence_root,
            junit_root=junit_root,
            binding=binding,
            vendor_policy=vendor_policy,
        )
        for suite in suites
    )
    payload: dict[str, object] = {
        "schema_version": CONSOLIDATED_VERSION,
        "status": PASS_STATUS,
        "release_ready": True,
        "route": "postgres_mssql",
        "commit_sha": binding.commit_sha,
        "workflow_run_id": binding.run_id,
        "workflow_run_attempt": binding.run_attempt,
        "inventory_sha256": hashlib.sha256(inventory_bytes).hexdigest(),
        "required_suite_ids": [suite.suite_id for suite in suites],
        "suite_count": len(suites),
        "case_count": sum(suite.case_count for suite in suites),
        "vendor_policy_sha256": digest(vendor_policy),
        "suites": list(validated),
    }
    payload["manifest_sha256"] = digest(payload)
    return payload


def _validate_inventory_shape(
    suites: tuple[SuiteInventory, ...],
    *,
    required_suite_ids: tuple[str, ...],
) -> None:
    suite_ids = tuple(suite.suite_id for suite in suites)
    evidence_files = tuple(suite.evidence_file for suite in suites)
    junit_files = tuple(suite.junit_file for suite in suites)
    if suite_ids != tuple(sorted(suite_ids)):
        fail("inventory.suites_not_sorted")
    if suite_ids != tuple(sorted(required_suite_ids)):
        fail("inventory.required_suites_mismatch")
    if (
        len(suite_ids) != len(set(suite_ids))
        or len(evidence_files) != len(set(evidence_files))
        or len(junit_files) != len(set(junit_files))
    ):
        fail("inventory.suite_duplicate")


def _validate_suite(
    suite: SuiteInventory,
    *,
    evidence_root: Path,
    junit_root: Path,
    binding: WorkflowBinding,
    vendor_policy: dict[str, Any],
) -> dict[str, object]:
    evidence_path = resolve_under(evidence_root, suite.evidence_file, code="evidence.path_unsafe")
    try:
        evidence_bytes = evidence_path.read_bytes()
    except OSError as exc:
        from .contract import CertificationValidationError

        raise CertificationValidationError(f"evidence.missing:{suite.suite_id}") from exc
    evidence = json_object(evidence_bytes, code=f"evidence.json_invalid:{suite.suite_id}")
    if set(evidence) != EVIDENCE_KEYS:
        fail(f"evidence.keys_invalid:{suite.suite_id}")
    _validate_evidence_header(evidence, suite=suite, binding=binding)
    expected_ids = case_ids(
        evidence.get("expected_case_ids"),
        code=f"evidence.expected_case_ids_invalid:{suite.suite_id}",
    )
    completed_ids = case_ids(
        evidence.get("completed_case_ids"),
        code=f"evidence.completed_case_ids_invalid:{suite.suite_id}",
    )
    if (
        expected_ids != completed_ids
        or len(expected_ids) != suite.case_count
        or digest(list(expected_ids)) != suite.case_set_sha256
        or (suite.case_ids and expected_ids != suite.case_ids)
    ):
        fail(f"evidence.case_inventory_mismatch:{suite.suite_id}")
    case_contracts = _validate_cases(evidence.get("cases"), suite=suite, expected_ids=expected_ids)
    if digest(case_contracts) != suite.case_contract_sha256 or tuple(case_contracts) != suite.case_contracts:
        fail(f"evidence.case_contract_mismatch:{suite.suite_id}")
    contract_digests = {str(value["case_id"]): digest(value) for value in case_contracts}
    validate_vendor_metadata(
        evidence.get("vendors"),
        suite_id=suite.suite_id,
        binding=binding,
        expected=vendor_policy,
    )
    junit = validate_junit(
        evidence.get("junit"),
        root=junit_root,
        suite_id=suite.suite_id,
        expected_path=suite.junit_file,
        expected_case_ids=expected_ids,
        expected_case_contracts=contract_digests,
        binding=binding,
    )
    return {
        "suite_id": suite.suite_id,
        "schema_version": suite.schema_version,
        "case_count": suite.case_count,
        "case_set_sha256": suite.case_set_sha256,
        "case_contract_sha256": suite.case_contract_sha256,
        "evidence_file": suite.evidence_file,
        "evidence_sha256": hashlib.sha256(evidence_bytes).hexdigest(),
        "junit": junit,
        "vendors": evidence["vendors"],
    }


def _validate_evidence_header(
    evidence: dict[str, Any],
    *,
    suite: SuiteInventory,
    binding: WorkflowBinding,
) -> None:
    exact = {
        "suite_id": suite.suite_id,
        "schema_version": suite.schema_version,
        "status": PASS_STATUS,
        "release_ready": True,
        "connector_doubles": False,
        "commit_sha": binding.commit_sha,
        "workflow_run_id": binding.run_id,
        "workflow_run_attempt": binding.run_attempt,
        "case_count": suite.case_count,
        "case_set_sha256": suite.case_set_sha256,
        "case_contract_sha256": suite.case_contract_sha256,
    }
    for field, expected in exact.items():
        actual = evidence.get(field)
        if type(actual) is not type(expected) or actual != expected:
            fail(f"evidence.{field}_mismatch:{suite.suite_id}")


def _validate_cases(
    raw_cases: Any,
    *,
    suite: SuiteInventory,
    expected_ids: tuple[str, ...],
) -> list[dict[str, object]]:
    if not isinstance(raw_cases, list) or len(raw_cases) != suite.case_count:
        fail(f"evidence.cases_invalid:{suite.suite_id}")
    status_by_id: dict[str, str] = {}
    contracts: list[dict[str, object]] = []
    for raw_case in raw_cases:
        contract, status = _validate_case(raw_case, suite_id=suite.suite_id)
        case_id = str(contract["case_id"])
        if case_id in status_by_id:
            fail(f"evidence.case_duplicate:{suite.suite_id}")
        status_by_id[case_id] = status
        contracts.append(contract)
    if tuple(sorted(status_by_id)) != expected_ids or any(value != "passed" for value in status_by_id.values()):
        fail(f"evidence.case_not_passed:{suite.suite_id}")
    contracts.sort(key=lambda value: str(value["case_id"]))
    return contracts


def _validate_case(raw_case: Any, *, suite_id: str) -> tuple[dict[str, object], str]:
    if not isinstance(raw_case, dict) or set(raw_case) != CASE_KEYS:
        fail(f"evidence.case_keys_invalid:{suite_id}")
    case_id = required_text(raw_case, "case_id", f"evidence.case_id_required:{suite_id}")
    status = required_text(raw_case, "status", f"evidence.case_status_required:{suite_id}")
    config_sha256 = required_sha256(
        raw_case,
        "config_sha256",
        f"evidence.case_config_sha256_invalid:{suite_id}:{case_id}",
    )
    action_class = required_text(
        raw_case,
        "action_class",
        f"evidence.case_action_required:{suite_id}:{case_id}",
    )
    outcome_class = required_text(
        raw_case,
        "outcome_class",
        f"evidence.case_outcome_required:{suite_id}:{case_id}",
    )
    before_sha256 = required_sha256(
        raw_case,
        "before_image_sha256",
        f"evidence.case_before_image_invalid:{suite_id}:{case_id}",
    )
    after_sha256 = required_sha256(
        raw_case,
        "after_image_sha256",
        f"evidence.case_after_image_invalid:{suite_id}:{case_id}",
    )
    expected_mutation = raw_case.get("expected_mutation")
    if not isinstance(expected_mutation, bool):
        fail(f"evidence.case_expected_mutation_invalid:{suite_id}:{case_id}")
    if not expected_mutation and before_sha256 != after_sha256:
        fail(f"evidence.case_before_image_changed:{suite_id}:{case_id}")
    if expected_mutation and before_sha256 == after_sha256:
        fail(f"evidence.case_expected_mutation_missing:{suite_id}:{case_id}")
    if not isinstance(raw_case.get("observations"), dict):
        fail(f"evidence.case_observations_invalid:{suite_id}:{case_id}")
    return (
        {
            "case_id": case_id,
            "config_sha256": config_sha256,
            "action_class": action_class,
            "outcome_class": outcome_class,
            "expected_mutation": expected_mutation,
        },
        status,
    )
