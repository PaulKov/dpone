"""Evidence-protocol meta-test for Provider Attestation V2."""

from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import jsonschema
import pytest

from tests.support.postgres_mssql_r1_v3_provider_attestation_v2_red_probe import (
    PROVIDER_ATTESTATION_OBSERVATION_PROPERTY,
    ProviderAttestationMissingBehavior,
    observation_jcs,
)

_SPEC = importlib.util.spec_from_file_location(
    "provider_attestation_v2_evidence",
    "tools/evidence/postgres_mssql_r1_v3_provider_attestation_v2_evidence.py",
)
assert _SPEC is not None and _SPEC.loader is not None
_EVIDENCE = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_EVIDENCE)
SCHEMA = _EVIDENCE.SCHEMA
SCHEMA_ID = _EVIDENCE.SCHEMA_ID
EvidenceError = _EVIDENCE.EvidenceError
ProviderAttestationStructuredEvidencePlugin = _EVIDENCE.ProviderAttestationStructuredEvidencePlugin
architecture_metrics = _EVIDENCE.architecture_metrics
expected_result = _EVIDENCE.expected_result
executed_case_digest = _EVIDENCE.executed_case_digest
jcs_bytes = _EVIDENCE.jcs_bytes
load_registry = _EVIDENCE.load_registry
normalize_architecture = _EVIDENCE.normalize_architecture
partition_counts = _EVIDENCE.partition_counts
publish_create_only = _EVIDENCE.publish_create_only
sha256_hex = _EVIDENCE.sha256_hex
validate_equations = _EVIDENCE.validate_equations

ROOT = Path(".")
GIT = "0123456789abcdef0123456789abcdef01234567"
DIGEST = "11" * 32


def _schema() -> dict[str, object]:
    return json.loads((ROOT / SCHEMA).read_text(encoding="utf-8"))


def _registry() -> dict[str, object]:
    return load_registry(ROOT)


def _document(*, kind: str = "green") -> dict[str, object]:
    registry = _registry()
    cases = registry["ordered_cases"]
    results = [expected_result(item, kind=kind) for item in cases]
    counts = {
        "errors": 0,
        "failed": 0 if kind == "green" else len(cases),
        "passed": len(cases) if kind == "green" else 0,
        "skipped": 0,
        "total": len(cases),
    }
    failing = [] if kind == "green" else [item["nodeid"] for item in results]
    diagnostics = {} if kind == "green" else {item["nodeid"]: item["diagnostic_class"] for item in results}
    architecture = {
        "avg_clustering": 0.18,
        "class_findings": [],
        "cross_layer_ratio": 0.1,
        "issue_count": 0,
        "max_module_ce": 1,
        "package": str((ROOT / "src/dpone").resolve()),
    }
    normalized = normalize_architecture(ROOT, architecture)
    retained = jcs_bytes(normalized).decode("utf-8")
    return {
        "activation_status": "blocked",
        "architecture_metrics": architecture_metrics(0, normalized),
        "architecture_normalized_output": retained,
        "architecture_normalized_output_sha256": sha256_hex(retained.encode("utf-8")),
        "architecture_subject_commit": GIT,
        "case_count": len(cases),
        "case_registry_sha256": DIGEST,
        "case_results": results,
        "certification_status": "local_pass" if kind == "green" else "unverified",
        "diagnostics_by_nodeid": diagnostics,
        "evidence_kind": kind,
        "evidence_meta_test_sha256": DIGEST,
        "evidence_protocol_commit": GIT,
        "evidence_protocol_red_commit": GIT,
        "evidence_protocol_tree_sha256": DIGEST,
        "evidence_specification_approval_commit": GIT,
        "evidence_specification_path": "docs/feature-design-postgres-mssql-r1-v3-provider-attestation-evidence-v1.md",
        "evidence_specification_sha256": DIGEST,
        "evidence_specification_status": "approved",
        "evidence_task_contract_commit": GIT,
        "evidence_task_contract_path": "docs/agent-task-contracts/postgres-mssql-r1-v3-provider-attestation-v2-evidence.yml",
        "evidence_task_contract_sha256": DIGEST,
        "exact_commit": GIT,
        "executed_case_ids_sha256": executed_case_digest([item["case_id"] for item in cases]),
        "failing_nodeids": failing,
        "generated_at": "2026-09-12T00:00:00Z",
        "implementation_status": "implemented" if kind == "green" else "absent",
        "live_behavior_status": "unverified",
        "live_gate_status": "n_a",
        "node_count": len(cases),
        "ordered_nodeids": [item["nodeid"] for item in cases],
        "outcome_counts": counts,
        "parent_commit": GIT,
        "partition_node_counts": partition_counts(registry),
        "producer_commit": GIT,
        "producer_sha256": DIGEST,
        "red_assets_commit": GIT,
        "red_task_authority_commit": GIT,
        "red_task_authority_sha256": DIGEST,
        "red_task_contract_path": "docs/agent-task-contracts/postgres-mssql-r1-v3-provider-attestation-v2-red.yml",
        "red_task_measurement_commit": GIT,
        "red_task_measurement_sha256": DIGEST,
        "schema_id": SCHEMA_ID,
        "schema_sha256": DIGEST,
        "specification_approval_commit": GIT,
        "specification_path": "docs/feature-design-postgres-mssql-r1-v3-provider-attestation-foundation-v2.md",
        "specification_sha256": DIGEST,
        "specification_status": "approved",
        "test_tree_sha256": DIGEST,
    }


def test_schema_is_draft_2020_and_rejects_additional_properties() -> None:
    schema = _schema()
    jsonschema.Draft202012Validator.check_schema(schema)
    validator = jsonschema.Draft202012Validator(schema)
    green = _document()
    validator.validate(green)
    validator.validate(_document(kind="red"))
    extra = dict(green)
    extra["foreign"] = True
    with pytest.raises(jsonschema.ValidationError):
        validator.validate(extra)


def test_closed_enums_and_status_literals() -> None:
    schema = _schema()
    validator = jsonschema.Draft202012Validator(schema)
    document = _document()
    document["evidence_kind"] = "candidate"
    with pytest.raises(jsonschema.ValidationError):
        validator.validate(document)
    document = _document()
    document["activation_status"] = "active"
    with pytest.raises(jsonschema.ValidationError):
        validator.validate(document)
    document = _document()
    document["live_gate_status"] = "pass"
    with pytest.raises(jsonschema.ValidationError):
        validator.validate(document)


def test_green_and_red_equations() -> None:
    registry = _registry()
    validate_equations(_document(kind="green"), registry)
    validate_equations(_document(kind="red"), registry)
    broken = _document(kind="green")
    broken["outcome_counts"] = dict(broken["outcome_counts"])
    broken["outcome_counts"]["skipped"] = 1
    with pytest.raises(EvidenceError):
        validate_equations(broken, registry)


def test_observation_jcs_and_missing_behavior() -> None:
    text = observation_jcs("abi.error.carrier", "not_observed", None, "missing_behavior")
    decoded = json.loads(text)
    assert decoded["case_id"] == "abi.error.carrier"
    assert jcs_bytes(decoded) == text.encode("utf-8")
    error = ProviderAttestationMissingBehavior("abi.error.carrier", "missing_behavior")
    assert error.case_id == "abi.error.carrier"
    assert error.diagnostic_class == "missing_behavior"
    assert PROVIDER_ATTESTATION_OBSERVATION_PROPERTY == "dpone.provider_attestation.v2.case_observation"


def test_plugin_rejects_collection_mismatch_and_foreign_exception() -> None:
    registry = _registry()
    nodeids = [item["nodeid"] for item in registry["ordered_cases"]]
    plugin = ProviderAttestationStructuredEvidencePlugin(nodeids, kind="green")
    plugin.pytest_collection_modifyitems(None, None, [])
    assert plugin.error == "collection_mismatch"
    plugin = ProviderAttestationStructuredEvidencePlugin(nodeids[:1], kind="red")

    class _Item:
        nodeid = nodeids[0]
        user_properties = [
            (PROVIDER_ATTESTATION_OBSERVATION_PROPERTY, observation_jcs("x", "not_observed", None, "missing_behavior"))
        ]

    class _Call:
        pass

    class _Report:
        when = "call"
        passed = False
        failed = True
        skipped = False
        wasxfail = False
        excinfo = type("Exc", (), {"value": AssertionError("no")})()

    def _hook():
        yield

    generator = plugin.pytest_runtest_makereport(_Item(), _Call())
    next(generator)
    try:
        generator.send(type("Outcome", (), {"get_result": lambda self: _Report()})())
    except StopIteration:
        pass
    assert plugin.error in {"red_exception", "observation_property"}


def test_create_only_collision_and_readback(tmp_path: Path) -> None:
    path = tmp_path / "attestation-inventory.json"
    publish_create_only(path, b'{"ok":true}')
    publish_create_only(path, b'{"ok":true}')
    with pytest.raises(EvidenceError):
        publish_create_only(path, b'{"ok":false}')
    assert path.read_bytes() == b'{"ok":true}'


def test_architecture_rejects_foreign_package(tmp_path: Path) -> None:
    with pytest.raises(EvidenceError):
        normalize_architecture(tmp_path, {"package": str(tmp_path / "foreign")})


def test_registry_is_bytewise_ordered_and_bijective() -> None:
    registry = _registry()
    cases = registry["ordered_cases"]
    ids = [item["case_id"] for item in cases]
    assert ids == sorted(ids)
    assert len(ids) == len(set(ids))
    for item in cases:
        assert item["parameter_id"] == item["case_id"]
        assert item["nodeid"] == f"{item['nodeid'].split('::', 1)[0]}::test_case[{item['case_id']}]"
        assert (item["mutation_kind"] == "valid_distinct") == (item["expected_behavior"] == "accept")
    assert set(partition_counts(registry)) == {
        "canonical_abi",
        "mutation_bounds_compatibility",
        "query_arms",
        "stable_catalog",
        "stable_schema",
    }
    assert (
        hashlib.sha256(
            Path("docs/feature-design-postgres-mssql-r1-v3-provider-attestation-foundation-v2.md").read_bytes()
        ).hexdigest()
        == registry["specification_sha256"]
    )
