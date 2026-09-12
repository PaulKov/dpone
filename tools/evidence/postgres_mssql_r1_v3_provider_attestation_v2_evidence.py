"""Create-only evidence producer for Provider Attestation V2."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import tempfile
from pathlib import Path
from typing import Any

SCHEMA_ID = "dpone-postgres-mssql-r1-v3-provider-attestation-v2-evidence-1"
CASE_REGISTRY_ID = "dpone-postgres-mssql-r1-v3-provider-attestation-v2-cases-1"
REGISTRY = "docs/schemas/evidence/postgres-mssql-r1-v3-provider-attestation-v2-cases.json"
SCHEMA = "docs/schemas/evidence/postgres-mssql-r1-v3-provider-attestation-v2.schema.json"
PRODUCER = "tools/evidence/postgres_mssql_r1_v3_provider_attestation_v2_evidence.py"
META_TEST = "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_evidence.py"
RED_TASK = "docs/agent-task-contracts/postgres-mssql-r1-v3-provider-attestation-v2-red.yml"
EVIDENCE_TASK = "docs/agent-task-contracts/postgres-mssql-r1-v3-provider-attestation-v2-evidence.yml"
FOUNDATION = "docs/feature-design-postgres-mssql-r1-v3-provider-attestation-foundation-v2.md"
EVIDENCE_SPEC = "docs/feature-design-postgres-mssql-r1-v3-provider-attestation-evidence-v1.md"
ARTIFACT_ROOT = "test_artifacts/postgres-mssql-r1-v3/provider-attestation-v2"
ARTIFACT_NAME = "attestation-inventory.json"
TEST_TREE_DOMAIN = b"dpone-provider-attestation-v2-test-tree-v1\0"
PROTOCOL_TREE_DOMAIN = b"dpone-provider-attestation-v2-evidence-protocol-tree-v1\0"
EXECUTED_DOMAIN = b"dpone-provider-attestation-v2-executed-cases-v1\0"
TEST_TREE_PATHS = (
    "tests/support/postgres_mssql_r1_v3_provider_attestation_v2_fixtures.py",
    "tests/support/postgres_mssql_r1_v3_provider_attestation_v2_oracle.py",
    "tests/support/postgres_mssql_r1_v3_provider_attestation_v2_red_probe.py",
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_abi.py",
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_query_results.py",
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_stable_schema.py",
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_stable_catalog.py",
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_mutations.py",
    REGISTRY,
)
NODE_PREFIXES = (
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_abi.py",
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_query_results.py",
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_stable_schema.py",
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_stable_catalog.py",
    "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_mutations.py",
)
PARTITIONS = (
    "canonical_abi",
    "query_arms",
    "stable_schema",
    "stable_catalog",
    "mutation_bounds_compatibility",
)
MAX_STDOUT = 4_194_304
MAX_STDERR = 1_048_576


class EvidenceError(RuntimeError):
    """Closed producer failure."""


def jcs_bytes(value: object) -> bytes:
    return json.dumps(value, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode("utf-8")


def sha256_hex(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def load_registry(root: Path) -> dict[str, Any]:
    payload = json.loads((root / REGISTRY).read_text(encoding="utf-8"))
    if payload.get("case_registry_id") != CASE_REGISTRY_ID:
        raise EvidenceError("case_registry_id")
    return payload


def nodeids(registry: dict[str, Any]) -> list[str]:
    return [item["nodeid"] for item in registry["ordered_cases"]]


def partition_counts(registry: dict[str, Any]) -> dict[str, int]:
    counts = {key: 0 for key in PARTITIONS}
    for item in registry["ordered_cases"]:
        counts[item["partition"]] += 1
    return counts


def test_tree_digest(root: Path) -> str:
    entries = [{"path": path, "sha256": sha256_hex((root / path).read_bytes())} for path in TEST_TREE_PATHS]
    return sha256_hex(TEST_TREE_DOMAIN + jcs_bytes(entries))


def protocol_tree_digest(root: Path) -> str:
    entries = [
        {"path": path, "sha256": sha256_hex((root / path).read_bytes())} for path in (SCHEMA, PRODUCER, META_TEST)
    ]
    return sha256_hex(PROTOCOL_TREE_DOMAIN + jcs_bytes(entries))


def executed_case_digest(case_ids: list[str]) -> str:
    return sha256_hex(EXECUTED_DOMAIN + jcs_bytes(case_ids))


def normalize_architecture(package_root: Path, parsed: dict[str, Any]) -> dict[str, Any]:
    package = Path(str(parsed.get("package", "")))
    if package.resolve() != (package_root / "src/dpone").resolve():
        raise EvidenceError("architecture_package")
    normalized = dict(parsed)
    normalized["package"] = "src/dpone"
    return normalized


def architecture_metrics(raw_exit: int, parsed: dict[str, Any]) -> dict[str, Any]:
    return {
        "avg_clustering": parsed["avg_clustering"],
        "class_finding_count": len(parsed["class_findings"]),
        "cross_layer_ratio": parsed["cross_layer_ratio"],
        "issue_count": parsed["issue_count"],
        "max_module_ce": parsed["max_module_ce"],
        "raw_exit": raw_exit,
    }


class ProviderAttestationStructuredEvidencePlugin:
    """Fail-closed pytest hook recorder for attestation evidence."""

    def __init__(self, ordered_nodeids: list[str], *, kind: str) -> None:
        self.ordered_nodeids = list(ordered_nodeids)
        self.kind = kind
        self.collected: list[str] = []
        self.reports: dict[str, dict[str, Any]] = {}
        self.observations: dict[str, dict[str, Any]] = {}
        self.error: str | None = None

    def pytest_collectreport(self, report: Any) -> None:
        if report.failed:
            self.error = "collection_failed"

    def pytest_collection_modifyitems(self, session: Any, config: Any, items: list[Any]) -> None:
        del session, config
        collected = [item.nodeid for item in items]
        if collected != self.ordered_nodeids:
            self.error = "collection_mismatch"
        self.collected = collected

    def pytest_runtest_makereport(self, item: Any, call: Any):
        outcome = yield
        report = outcome.get_result()
        nodeid = item.nodeid
        bucket = self.reports.setdefault(nodeid, {})
        if report.when in bucket:
            self.error = "duplicate_phase"
            return
        bucket[report.when] = report
        if report.when != "call":
            if report.failed or report.skipped or getattr(report, "wasxfail", False):
                self.error = "setup_teardown"
            return
        properties = list(getattr(item, "user_properties", ()))
        if len(properties) != 1 or properties[0][0] != "dpone.provider_attestation.v2.case_observation":
            self.error = "observation_property"
            return
        try:
            observed = json.loads(properties[0][1])
        except json.JSONDecodeError:
            self.error = "observation_property"
            return
        if jcs_bytes(observed) != str(properties[0][1]).encode("utf-8"):
            self.error = "observation_property"
            return
        self.observations[nodeid] = observed
        if self.kind == "red":
            if (
                report.passed
                or report.excinfo is None
                or type(report.excinfo.value).__name__ != "ProviderAttestationMissingBehavior"
            ):
                self.error = "red_exception"
        elif not report.passed or report.excinfo is not None:
            self.error = "green_failed"


def expected_result(case: dict[str, Any], *, kind: str) -> dict[str, Any]:
    if kind == "red":
        return {
            "case_id": case["case_id"],
            "diagnostic_class": case["expected_diagnostic_class"],
            "nodeid": case["nodeid"],
            "observed_behavior": "not_observed",
            "pytest_outcome": case["expected_red_pytest_outcome"],
            "reason": None,
        }
    reason = case["expected_reason"] if case["expected_behavior"] == "reject" else None
    return {
        "case_id": case["case_id"],
        "diagnostic_class": None,
        "nodeid": case["nodeid"],
        "observed_behavior": case["expected_behavior"],
        "pytest_outcome": case["expected_green_pytest_outcome"],
        "reason": reason,
    }


def validate_equations(document: dict[str, Any], registry: dict[str, Any]) -> None:
    cases = registry["ordered_cases"]
    if document["node_count"] != len(document["ordered_nodeids"]):
        raise EvidenceError("node_count")
    if document["outcome_counts"]["total"] != document["node_count"]:
        raise EvidenceError("outcome_total")
    counts = document["outcome_counts"]
    if counts["passed"] + counts["failed"] + counts["skipped"] + counts["errors"] != counts["total"]:
        raise EvidenceError("outcome_sum")
    if sum(document["partition_node_counts"].values()) != document["node_count"]:
        raise EvidenceError("partition_sum")
    if document["partition_node_counts"] != partition_counts(registry):
        raise EvidenceError("partition_counts")
    if document["case_count"] != len(cases) or len(document["case_results"]) != len(cases):
        raise EvidenceError("case_count")
    if document["ordered_nodeids"] != [item["nodeid"] for item in cases]:
        raise EvidenceError("ordered_nodeids")
    if any(item["nodeid"].split("::", 1)[0] not in NODE_PREFIXES for item in cases):
        raise EvidenceError("node_prefix")
    if counts["skipped"] != 0 or counts["errors"] != 0:
        raise EvidenceError("skip_or_error")
    kind = document["evidence_kind"]
    expected = [expected_result(item, kind=kind) for item in cases]
    if document["case_results"] != expected:
        raise EvidenceError("case_results")
    if kind == "red":
        if (
            document["implementation_status"] != "absent"
            or document["certification_status"] != "unverified"
            or counts["failed"] != document["node_count"]
            or counts["passed"] != 0
        ):
            raise EvidenceError("red_status")
        failing = [item["nodeid"] for item in document["case_results"]]
        if document["failing_nodeids"] != failing:
            raise EvidenceError("failing_nodeids")
        if document["diagnostics_by_nodeid"] != {
            item["nodeid"]: item["diagnostic_class"] for item in document["case_results"]
        }:
            raise EvidenceError("diagnostics")
    else:
        if (
            document["implementation_status"] != "implemented"
            or document["certification_status"] != "local_pass"
            or counts["failed"] != 0
            or counts["passed"] != counts["total"]
            or document["failing_nodeids"]
            or document["diagnostics_by_nodeid"]
        ):
            raise EvidenceError("green_status")
    if document["executed_case_ids_sha256"] != executed_case_digest([item["case_id"] for item in cases]):
        raise EvidenceError("executed_digest")
    if document["architecture_subject_commit"] != document["exact_commit"]:
        raise EvidenceError("architecture_subject")


def publish_create_only(path: Path, payload: bytes) -> None:
    if path.exists():
        if path.read_bytes() != payload:
            raise EvidenceError("artifact_collision")
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=path.parent, prefix=".attestation-")
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(tmp, path)
        dir_fd = os.open(path.parent, os.O_DIRECTORY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
    except Exception:
        if os.path.exists(tmp):
            os.unlink(tmp)
        raise
    if path.read_bytes() != payload:
        raise EvidenceError("artifact_readback")


def _git(root: Path, *args: str) -> subprocess.CompletedProcess[bytes]:
    return subprocess.run(("git", *args), cwd=root, capture_output=True, check=False)


def require_clean(root: Path) -> str:
    status = _git(root, "status", "--porcelain")
    if status.returncode or status.stdout:
        raise EvidenceError("dirty_worktree")
    head = _git(root, "rev-parse", "HEAD").stdout.decode().strip()
    if len(head) != 40:
        raise EvidenceError("head")
    return head


def main() -> int:
    root = Path.cwd()
    try:
        require_clean(root)
        raise EvidenceError("unbound_lineage")
    except EvidenceError as error:
        raise SystemExit(str(error)) from error


if __name__ == "__main__":
    raise SystemExit(main())
