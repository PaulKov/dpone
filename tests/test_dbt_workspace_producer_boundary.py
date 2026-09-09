"""Pure metadata needs exact fingerprints; only the service verifies real packs."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.dbt_workspace import DbtWorkspaceCheckReport, DbtWorkspaceDiscoveryReport
from tests.test_dbt_workspace_release_assembly import _assemble, _project


def _pure(projects, fingerprints):
    from dpone.contracts.dbt_workspace_release import assemble_dbt_workspace_release
    from dpone.services.dbt_release_assets import canonical_schema_descriptors, canonical_schema_files

    schemas = canonical_schema_files()
    check = DbtWorkspaceCheckReport(
        DbtWorkspaceDiscoveryReport(tuple(p.check.project for p in projects)),
        tuple(p.check for p in projects),
    )
    return assemble_dbt_workspace_release(
        check,
        projects,
        pack_fingerprints=fingerprints,
        schema_files=schemas,
        schema_descriptors=canonical_schema_descriptors(schemas),
        producer_version="0.74.28",
    )


@pytest.mark.parametrize("defect", ["missing_dbt", "missing_transfer", "extra", "invalid_digest"])
def test_pure_assembly_requires_exact_valid_fingerprint_map(tmp_path, defect):
    from dpone_airflow_pack.pack_identity import verify_pack_fingerprint

    projects = [_project(tmp_path, name) for name in ("alpha", "beta")]
    fingerprints = {
        key: verify_pack_fingerprint(body) for project in projects for key, body in project.artifacts.pack_files.items()
    }
    assert _pure(projects, fingerprints) == _assemble(projects)
    if defect.startswith("missing"):
        key = next(key for key in fingerprints if key.startswith("dbt__") == (defect == "missing_dbt"))
        del fingerprints[key]
    elif defect == "extra":
        fingerprints["orphan"] = next(iter(fingerprints.values()))
    else:
        fingerprints[next(iter(fingerprints))] = "not-a-fingerprint"
    with pytest.raises(ValueError, match="fingerprint"):
        _pure(projects, fingerprints)


def test_service_verifies_every_actual_pack_and_associates_its_fingerprint(tmp_path, monkeypatch):
    from dpone_airflow_pack import pack_identity

    projects = [_project(tmp_path, name) for name in ("alpha", "beta")]
    expected = {key: body for project in projects for key, body in project.artifacts.pack_files.items()}
    observed = []
    verify = pack_identity.verify_pack_fingerprint

    def record(body):
        observed.append(body)
        return verify(body)

    monkeypatch.setattr(pack_identity, "verify_pack_fingerprint", record)
    release = json.loads(_assemble(projects).files["release-set.json"])
    assert len(observed) == len(expected) and set(observed) == set(expected.values())
    for descriptor in release["artifacts"]["workload_packs"]:
        assert descriptor["pack_fingerprint"] == verify(expected[descriptor["id"]])


def test_report_mutation_inside_framework_verifier_cannot_reuse_old_receipts(tmp_path, monkeypatch):
    from dpone_airflow_pack import pack_identity

    projects = [_project(tmp_path, name) for name in ("alpha", "beta")]
    verify = pack_identity.verify_pack_fingerprint
    remaining = sum(len(project.artifacts.pack_files) for project in projects)

    def mutate_after_last_verification(body):
        nonlocal remaining
        fingerprint = verify(body)
        remaining -= 1
        if remaining == 0:
            projects[0].check.report.models[0].profile.runtime["dbt_target"] = "changed_inside_verifier"
        return fingerprint

    monkeypatch.setattr(pack_identity, "verify_pack_fingerprint", mutate_after_last_verification)
    with pytest.raises(ValueError, match="certification.*conflict"):
        _assemble(projects)


def test_correct_outer_descriptor_does_not_replace_actual_framework_verification(tmp_path):
    project = _project(tmp_path, "alpha")
    key = "dbt__alpha"
    pack = json.loads(project.artifacts.pack_files[key])
    pack["runtime_payload_ids"] = list(reversed(pack["runtime_payload_ids"]))
    project = replace(
        project,
        artifacts=replace(
            project.artifacts,
            pack_files={**project.artifacts.pack_files, key: json.dumps(pack).encode()},
        ),
    )
    with pytest.raises(ValueError, match="fingerprint"):
        _assemble([project])
