"""Create-only writer for exact reviewed suite evidence and bound JUnit."""

from __future__ import annotations

import hashlib
import json
import xml.etree.ElementTree as ET
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from .contract import PASS_STATUS, SHA256_PATTERN, WorkflowBinding, digest, fail
from .io_authority import write_create_only_exact
from .reviewed_case import ReviewedSuite
from .vendors import parse_vendor_policy


@dataclass(frozen=True, slots=True)
class PassedCaseObservation:
    """Sanitized before/after proof produced by one real-vendor assertion."""

    case_id: str
    before_image_sha256: str
    after_image_sha256: str
    observations: Mapping[str, Any]

    def __post_init__(self) -> None:
        if not isinstance(self.case_id, str) or not self.case_id.strip():
            fail("writer.case_id_required")
        for field, value in (
            ("before_image_sha256", self.before_image_sha256),
            ("after_image_sha256", self.after_image_sha256),
        ):
            if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
                fail(f"writer.{field}_invalid:{self.case_id}")
        if not isinstance(self.observations, Mapping):
            fail(f"writer.observations_invalid:{self.case_id}")
        try:
            json.dumps(self.observations, ensure_ascii=False, allow_nan=False, sort_keys=True)
        except (TypeError, ValueError) as exc:
            from .contract import CertificationValidationError

            raise CertificationValidationError(f"writer.observations_not_json:{self.case_id}") from exc


class SuiteEvidenceWriter:
    """Write one complete suite only after every reviewed case passed."""

    def __init__(
        self,
        *,
        suite: ReviewedSuite,
        evidence_root: Path,
        junit_root: Path,
        binding: WorkflowBinding,
        vendor_policy: Mapping[str, Any],
    ) -> None:
        self._suite = suite
        self._evidence_root = evidence_root
        self._junit_root = junit_root
        self._binding = binding
        self._vendor_policy = parse_vendor_policy(dict(vendor_policy))

    def write(self, observations: Sequence[PassedCaseObservation]) -> tuple[Path, Path]:
        """Persist an exact idempotent evidence/JUnit pair or fail closed."""

        by_id = _exact_observations(self._suite, observations)
        cases = [_case_payload(case, by_id[case.case_id]) for case in self._suite.cases]
        junit_bytes = _junit_bytes(self._suite, binding=self._binding)
        junit_sha256 = hashlib.sha256(junit_bytes).hexdigest()
        inventory_entry = self._suite.inventory_entry()
        evidence = {
            "suite_id": self._suite.suite_id,
            "schema_version": self._suite.schema_version,
            "status": PASS_STATUS,
            "release_ready": True,
            "connector_doubles": False,
            "commit_sha": self._binding.commit_sha,
            "workflow_run_id": self._binding.run_id,
            "workflow_run_attempt": self._binding.run_attempt,
            "case_count": len(self._suite.cases),
            "case_set_sha256": inventory_entry["case_set_sha256"],
            "case_contract_sha256": inventory_entry["case_contract_sha256"],
            "expected_case_ids": [case.case_id for case in self._suite.cases],
            "completed_case_ids": [case.case_id for case in self._suite.cases],
            "cases": cases,
            "vendors": _evidence_vendors(self._vendor_policy, binding=self._binding),
            "junit": {
                "path": inventory_entry["junit_file"],
                "sha256": junit_sha256,
                "tests": len(self._suite.cases),
                "failures": 0,
                "errors": 0,
                "skipped": 0,
            },
        }
        evidence_bytes = _json_bytes(evidence)
        junit_path = self._junit_root / str(inventory_entry["junit_file"])
        evidence_path = self._evidence_root / str(inventory_entry["evidence_file"])
        write_create_only_exact(
            junit_path,
            junit_bytes,
            code=f"writer.junit:{self._suite.suite_id}",
        )
        write_create_only_exact(
            evidence_path,
            evidence_bytes,
            code=f"writer.evidence:{self._suite.suite_id}",
        )
        return evidence_path, junit_path


def _exact_observations(
    suite: ReviewedSuite,
    observations: Sequence[PassedCaseObservation],
) -> dict[str, PassedCaseObservation]:
    output: dict[str, PassedCaseObservation] = {}
    for observation in observations:
        if not isinstance(observation, PassedCaseObservation):
            fail(f"writer.case_observation_invalid:{suite.suite_id}")
        if observation.case_id in output:
            fail(f"writer.case_duplicate:{suite.suite_id}:{observation.case_id}")
        output[observation.case_id] = observation
    expected = tuple(case.case_id for case in suite.cases)
    if tuple(sorted(output)) != expected:
        fail(f"writer.case_inventory_mismatch:{suite.suite_id}")
    for reviewed in suite.cases:
        observation = output[reviewed.case_id]
        changed = observation.before_image_sha256 != observation.after_image_sha256
        if changed is not reviewed.expected_mutation:
            fail(f"writer.case_mutation_mismatch:{suite.suite_id}:{reviewed.case_id}")
    return output


def _case_payload(reviewed: Any, observation: PassedCaseObservation) -> dict[str, object]:
    return {
        "case_id": reviewed.case_id,
        "status": "passed",
        "config_sha256": reviewed.config_sha256,
        "action_class": reviewed.action_class,
        "outcome_class": reviewed.outcome_class,
        "before_image_sha256": observation.before_image_sha256,
        "after_image_sha256": observation.after_image_sha256,
        "expected_mutation": reviewed.expected_mutation,
        "observations": dict(observation.observations),
    }


def _junit_bytes(suite: ReviewedSuite, *, binding: WorkflowBinding) -> bytes:
    root = ET.Element(
        "testsuite",
        {
            "name": suite.suite_id,
            "tests": str(len(suite.cases)),
            "failures": "0",
            "errors": "0",
            "skipped": "0",
        },
    )
    for reviewed in suite.cases:
        testcase = ET.SubElement(
            root,
            "testcase",
            {"name": reviewed.case_id, "classname": f"route_live.{suite.suite_id}"},
        )
        properties = ET.SubElement(testcase, "properties")
        case_contract = reviewed.contract()
        for name, value in (
            ("dpone_suite_id", suite.suite_id),
            ("dpone_case_id", reviewed.case_id),
            ("dpone_case_contract_sha256", digest(case_contract)),
            ("dpone_commit_sha", binding.commit_sha),
            ("dpone_workflow_run_id", binding.run_id),
            ("dpone_workflow_run_attempt", str(binding.run_attempt)),
        ):
            ET.SubElement(properties, "property", {"name": name, "value": value})
    ET.indent(root, space="  ")
    return ET.tostring(root, encoding="utf-8", xml_declaration=True, short_empty_elements=True) + b"\n"


def _evidence_vendors(
    vendor_policy: Mapping[str, Any],
    *,
    binding: WorkflowBinding,
) -> dict[str, dict[str, str]]:
    result = {
        section: {str(key): str(value) for key, value in fields.items()} for section, fields in vendor_policy.items()
    }
    result["runtime"]["source_sha"] = binding.commit_sha
    return result


def _json_bytes(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=False, indent=2, allow_nan=False, sort_keys=True) + "\n").encode("utf-8")


__all__ = ["PassedCaseObservation", "SuiteEvidenceWriter"]
