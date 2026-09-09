"""Immutable contract models and validation primitives for route evidence."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

SHA256_PATTERN = re.compile(r"[0-9a-f]{64}")
COMMIT_SHA_PATTERN = re.compile(r"[0-9a-f]{40}")
INVENTORY_VERSION = "dpone.route_live.postgres_mssql.inventory.v1"
CONSOLIDATED_VERSION = "dpone.route_live.postgres_mssql.certification.v1"
PASS_STATUS = "certification_passed"
REQUIRED_SUITE_IDS = (
    "artifact_integrity_faults",
    "backfill_orchestration",
    "boundary_types",
    "explicit_types",
    "lineage_parity",
    "physical_design",
    "postgis_types",
    "schema_evolution",
    "source_identity_authority",
    "strategy_capability",
    "target_behavior",
    "target_database_authority",
    "target_identity_authority",
    "text_key_lifecycle",
    "transaction_governance",
    "wide_performance_soak",
    "wide_strategy",
    "xmin_reconciliation",
)
VENDOR_FIELDS = {
    "postgresql": frozenset({"version", "image_digest"}),
    "postgis": frozenset({"version", "image_digest"}),
    "mssql": frozenset({"version", "build", "image_digest"}),
    "transport": frozenset({"odbc_driver", "bcp_version"}),
    "runtime": frozenset({"python_version", "dpone_version"}),
}
INVENTORY_KEYS = frozenset({"schema_version", "route", "vendors", "suites"})
SUITE_KEYS = frozenset(
    {
        "suite_id",
        "evidence_file",
        "junit_file",
        "schema_version",
        "case_count",
        "case_set_sha256",
        "case_contract_sha256",
        "case_contracts",
    }
)
CASE_CONTRACT_KEYS = frozenset({"case_id", "config_sha256", "action_class", "outcome_class", "expected_mutation"})
EVIDENCE_KEYS = frozenset(
    {
        "suite_id",
        "schema_version",
        "status",
        "release_ready",
        "connector_doubles",
        "commit_sha",
        "workflow_run_id",
        "workflow_run_attempt",
        "case_count",
        "case_set_sha256",
        "case_contract_sha256",
        "expected_case_ids",
        "completed_case_ids",
        "cases",
        "vendors",
        "junit",
    }
)
CASE_KEYS = frozenset(
    {
        "case_id",
        "status",
        "config_sha256",
        "action_class",
        "outcome_class",
        "before_image_sha256",
        "after_image_sha256",
        "expected_mutation",
        "observations",
    }
)
JUNIT_KEYS = frozenset({"path", "sha256", "tests", "failures", "errors", "skipped"})


class CertificationValidationError(RuntimeError):
    """Stable fail-closed diagnostic for semantic evidence violations."""

    def __init__(self, code: str) -> None:
        self.code = code
        super().__init__(f"DPONE_ROUTE_LIVE_CERTIFICATION_INVALID:{code}")


def fail(code: str) -> None:
    raise CertificationValidationError(code)


def digest(value: object) -> str:
    return hashlib.sha256(
        json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()
    ).hexdigest()


def required_text(raw: dict[str, Any], field: str, code: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or not value.strip():
        fail(code)
    return value


def required_sha256(raw: dict[str, Any], field: str, code: str) -> str:
    value = raw.get(field)
    if not isinstance(value, str) or SHA256_PATTERN.fullmatch(value) is None:
        fail(code)
    return value


def nonnegative_int(value: Any, *, code: str) -> int:
    if type(value) is not int or value < 0:
        fail(code)
    return value


def case_ids(raw: Any, *, code: str) -> tuple[str, ...]:
    if not isinstance(raw, list) or not raw:
        fail(code)
    if any(not isinstance(value, str) for value in raw):
        fail(code)
    values = tuple(value.strip() for value in raw)
    if any(not value for value in values) or len(values) != len(set(values)) or values != tuple(sorted(values)):
        fail(code)
    return values


def safe_relative_path(value: str, *, code: str) -> Path:
    path = Path(value)
    if path.is_absolute() or not path.parts or any(part in {"", ".", ".."} for part in path.parts):
        fail(code)
    return path


@dataclass(frozen=True, slots=True)
class WorkflowBinding:
    """Exact GitHub execution identity authorized by the release candidate."""

    commit_sha: str
    run_id: str
    run_attempt: int

    def __post_init__(self) -> None:
        if COMMIT_SHA_PATTERN.fullmatch(self.commit_sha) is None:
            fail("binding.commit_sha_invalid")
        if re.fullmatch(r"[1-9][0-9]*", self.run_id) is None:
            fail("binding.run_id_required")
        if type(self.run_attempt) is not int or self.run_attempt < 1:
            fail("binding.run_attempt_invalid")


@dataclass(frozen=True, slots=True)
class SuiteInventory:
    """One reviewed suite and its exact case/JUnit authority."""

    suite_id: str
    evidence_file: str
    junit_file: str
    schema_version: str
    case_count: int
    case_set_sha256: str
    case_contract_sha256: str
    case_ids: tuple[str, ...]
    case_contracts: tuple[dict[str, object], ...]

    @classmethod
    def parse(cls, raw: Any) -> SuiteInventory:
        if not isinstance(raw, dict):
            fail("inventory.suite_invalid")
        if frozenset(raw) != SUITE_KEYS:
            fail("inventory.suite_keys_invalid")
        parsed_contracts = _case_contracts(raw.get("case_contracts"))
        parsed_case_ids = tuple(str(value["case_id"]) for value in parsed_contracts)
        parsed_case_count = nonnegative_int(
            raw.get("case_count"),
            code="inventory.case_count_invalid",
        )
        if parsed_case_count < 1:
            fail("inventory.case_count_invalid")
        suite = cls(
            suite_id=required_text(raw, "suite_id", "inventory.suite_id_required"),
            evidence_file=required_text(
                raw,
                "evidence_file",
                "inventory.evidence_file_required",
            ),
            junit_file=required_text(raw, "junit_file", "inventory.junit_file_required"),
            schema_version=required_text(
                raw,
                "schema_version",
                "inventory.schema_version_required",
            ),
            case_count=parsed_case_count,
            case_set_sha256=required_sha256(
                raw,
                "case_set_sha256",
                "inventory.case_set_sha256_invalid",
            ),
            case_contract_sha256=required_sha256(
                raw,
                "case_contract_sha256",
                "inventory.case_contract_sha256_invalid",
            ),
            case_ids=parsed_case_ids,
            case_contracts=parsed_contracts,
        )
        safe_relative_path(suite.evidence_file, code="inventory.evidence_file_unsafe")
        safe_relative_path(suite.junit_file, code="inventory.junit_file_unsafe")
        if (
            len(parsed_case_ids) != suite.case_count
            or digest(list(parsed_case_ids)) != suite.case_set_sha256
            or digest(list(parsed_contracts)) != suite.case_contract_sha256
        ):
            fail("inventory.case_contract_mismatch")
        return suite


def _case_contracts(raw: Any) -> tuple[dict[str, object], ...]:
    if not isinstance(raw, list) or not raw:
        fail("inventory.case_contracts_invalid")
    output: list[dict[str, object]] = []
    for value in raw:
        if not isinstance(value, dict) or set(value) != CASE_CONTRACT_KEYS:
            fail("inventory.case_contract_invalid")
        expected_mutation = value.get("expected_mutation")
        if not isinstance(expected_mutation, bool):
            fail("inventory.case_expected_mutation_invalid")
        output.append(
            {
                "case_id": required_text(value, "case_id", "inventory.case_id_required"),
                "config_sha256": required_sha256(
                    value,
                    "config_sha256",
                    "inventory.case_config_sha256_invalid",
                ),
                "action_class": required_text(
                    value,
                    "action_class",
                    "inventory.case_action_required",
                ),
                "outcome_class": required_text(
                    value,
                    "outcome_class",
                    "inventory.case_outcome_required",
                ),
                "expected_mutation": expected_mutation,
            }
        )
    output.sort(key=lambda value: str(value["case_id"]))
    identifiers = tuple(str(value["case_id"]) for value in output)
    if len(identifiers) != len(set(identifiers)):
        fail("inventory.case_contract_duplicate")
    return tuple(output)
