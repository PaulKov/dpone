"""Resealing hashes cannot authorize conflicting embedded transfer destinations."""

import json

import pytest
import yaml
from dpone_airflow_pack.pack_identity import compute_pack_fingerprint

from dpone.contracts.airflow_deployment import release_id
from dpone.contracts.dbt_contract_validation import DbtPublishingError, sha256_bytes
from dpone.contracts.dbt_release_workload_binding import DbtDevEvidenceReleaseError, runtime_payload_member
from dpone.gitops.airflow_compact_pack_bootstrap import RuntimePayloadBuilder
from tests.test_dbt_release_source_reader import _read, _write
from tests.test_dbt_workspace_release_assembly import _assemble, _project


@pytest.mark.parametrize("target", ["transfer", "model", "intermediate", "backup", "helper"])
def test_resealed_transfer_destination_cannot_overlap_any_selected_writer(tmp_path, target):
    projects = [_project(tmp_path, name) for name in ("alpha", "beta")]
    tree = _assemble(projects)
    root = tmp_path / "release"
    for path, body in tree.files.items():
        _write(root, path, body)
    release = json.loads(tree.files["release-set.json"])
    sources = _read(root, release)
    expected = next(
        row
        for row in sources.relation_writes
        if row.project_path == "alpha"
        and (
            row.kind == "transfer"
            if target == "transfer"
            else row.kind == "model" and row.role == ("target" if target == "model" else target)
        )
    )
    descriptor = next(item for item in release["artifacts"]["workload_packs"] if item["id"].startswith("dbt_beta_"))
    pack = json.loads((root / descriptor["path"]).read_bytes())
    path = pack["workload"]["manifest"]
    manifest = yaml.safe_load(
        runtime_payload_member(
            pack,
            expected_path=path,
            max_archive_bytes=8 * 1024 * 1024,
            max_member_bytes=1024 * 1024,
            label="transfer manifest",
        )
    )
    sink = manifest["sink"]
    sink.update(type=expected.connector, connection_ref=expected.connection_ref)
    sink["table"] = {"schema": expected.schema, "name": expected.relation}
    if expected.database is not None:
        sink["table"]["database"] = expected.database
    pack["runtime_payload"] = (
        RuntimePayloadBuilder(repo_root=tmp_path, paths=(), generated_files={path: yaml.safe_dump(manifest).encode()})
        .build()
        .to_jsonable()
    )
    pack["pack_fingerprint"] = compute_pack_fingerprint(pack)
    body = json.dumps(pack).encode()
    descriptor.update(sha256=sha256_bytes(body), bytes=len(body), pack_fingerprint=pack["pack_fingerprint"])
    _write(root, descriptor["path"], body)
    release["release_id"] = release_id(release)
    _write(root, "release-set.json", json.dumps(release).encode())
    with pytest.raises(DbtDevEvidenceReleaseError) as failure:
        _read(root, release)
    assert isinstance(failure.value.__cause__, DbtPublishingError)
    assert failure.value.__cause__.code == "DPONE_DBT_WORKSPACE_TARGET_COLLISION"
