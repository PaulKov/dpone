"""JUnit evidence verifier with exact case and workflow binding."""

from __future__ import annotations

import hashlib
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

from .contract import (
    JUNIT_KEYS,
    SHA256_PATTERN,
    CertificationValidationError,
    WorkflowBinding,
    fail,
    required_text,
)
from .io_authority import resolve_under


def validate_junit(
    raw: Any,
    *,
    root: Path,
    suite_id: str,
    expected_path: str,
    expected_case_ids: tuple[str, ...],
    expected_case_contracts: dict[str, str],
    binding: WorkflowBinding,
) -> dict[str, object]:
    if not isinstance(raw, dict):
        fail(f"evidence.junit_required:{suite_id}")
    if set(raw) != JUNIT_KEYS:
        fail(f"evidence.junit_keys_invalid:{suite_id}")
    relative = required_text(raw, "path", f"evidence.junit_path_required:{suite_id}")
    if relative != expected_path:
        fail(f"evidence.junit_path_mismatch:{suite_id}")
    path = resolve_under(root, relative, code=f"evidence.junit_path_unsafe:{suite_id}")
    try:
        payload = path.read_bytes()
        xml_root = ET.fromstring(payload)
    except (OSError, ET.ParseError) as exc:
        raise CertificationValidationError(f"evidence.junit_invalid:{suite_id}") from exc
    _validate_namespace_free_xml(xml_root, suite_id=suite_id)
    totals = _declared_totals(xml_root)
    if totals != _derived_totals(xml_root):
        fail(f"evidence.junit_declared_totals_mismatch:{suite_id}")
    expected_sha = required_text(raw, "sha256", f"evidence.junit_sha_required:{suite_id}")
    if SHA256_PATTERN.fullmatch(expected_sha) is None or hashlib.sha256(payload).hexdigest() != expected_sha:
        fail(f"evidence.junit_sha_mismatch:{suite_id}")
    for field, actual in totals.items():
        declared = raw.get(field)
        if type(declared) is not int or declared != actual:
            fail(f"evidence.junit_{field}_mismatch:{suite_id}")
    if totals["tests"] < 1 or any(totals[field] != 0 for field in ("failures", "errors", "skipped")):
        fail(f"evidence.junit_not_green:{suite_id}")
    parsed_case_ids = _bound_case_ids(
        xml_root,
        suite_id=suite_id,
        expected_contracts=expected_case_contracts,
        binding=binding,
    )
    if parsed_case_ids != expected_case_ids or totals["tests"] != len(parsed_case_ids):
        fail(f"evidence.junit_case_inventory_mismatch:{suite_id}")
    return {"path": relative, "sha256": expected_sha, "case_ids": list(parsed_case_ids), **totals}


def _declared_totals(root: ET.Element) -> dict[str, int]:
    suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
    if not suites:
        fail("evidence.junit_suite_missing")
    return {
        field: sum(
            _xml_nonnegative_int(
                suite.attrib.get(field, "0"),
                code=f"evidence.junit_{field}_invalid",
            )
            for suite in suites
        )
        for field in ("tests", "failures", "errors", "skipped")
    }


def _validate_namespace_free_xml(root: ET.Element, *, suite_id: str) -> None:
    """Reject namespaces that could hide JUnit outcome elements."""

    if root.tag not in {"testsuite", "testsuites"}:
        fail(f"evidence.junit_root_invalid:{suite_id}")
    for element in root.iter():
        if not isinstance(element.tag, str) or "{" in element.tag or "}" in element.tag:
            fail(f"evidence.junit_namespace_unsupported:{suite_id}")


def _derived_totals(root: ET.Element) -> dict[str, int]:
    cases = list(root.iter("testcase"))
    totals = {"tests": len(cases), "failures": 0, "errors": 0, "skipped": 0}
    for case in cases:
        markers = {
            "failures": len(case.findall("failure")),
            "errors": len(case.findall("error")),
            "skipped": len(case.findall("skipped")),
        }
        if sum(markers.values()) > 1:
            fail("evidence.junit_case_outcome_ambiguous")
        for field, count in markers.items():
            totals[field] += count
    return totals


def _bound_case_ids(
    root: ET.Element,
    *,
    suite_id: str,
    expected_contracts: dict[str, str],
    binding: WorkflowBinding,
) -> tuple[str, ...]:
    values: list[str] = []
    for case in root.iter("testcase"):
        properties = case.find("properties")
        if properties is None:
            fail(f"evidence.junit_case_binding_required:{suite_id}")
        bound: dict[str, str] = {}
        for prop in properties.findall("property"):
            name = str(prop.get("name") or "").strip()
            if not name.startswith("dpone_"):
                continue
            if name in bound:
                fail(f"evidence.junit_case_binding_duplicate:{suite_id}:{name}")
            bound[name] = str(prop.get("value") or "")
        required = {
            "dpone_suite_id": suite_id,
            "dpone_commit_sha": binding.commit_sha,
            "dpone_workflow_run_id": binding.run_id,
            "dpone_workflow_run_attempt": str(binding.run_attempt),
        }
        if any(bound.get(name) != expected for name, expected in required.items()):
            fail(f"evidence.junit_case_run_binding_mismatch:{suite_id}")
        value = str(bound.get("dpone_case_id") or "").strip()
        if not value:
            fail(f"evidence.junit_case_id_required:{suite_id}")
        if bound.get("dpone_case_contract_sha256") != expected_contracts.get(value):
            fail(f"evidence.junit_case_contract_mismatch:{suite_id}:{value}")
        values.append(value)
    result = tuple(sorted(values))
    if not result or len(result) != len(set(result)):
        fail(f"evidence.junit_case_ids_invalid:{suite_id}")
    return result


def _xml_nonnegative_int(value: Any, *, code: str) -> int:
    if not isinstance(value, str) or not value.isascii() or not value.isdecimal():
        fail(code)
    if len(value) > 1 and value.startswith("0"):
        fail(code)
    return int(value)
