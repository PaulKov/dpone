"""A legacy layout cannot acquire native workspace authority by transporting bytes."""

import json
import sys

import pytest

from dpone.readiness.airflow_compact_pack_release import materialize_compact_pack_release
from tests.dbt_compact_wire_v2_helpers import SIDECAR, prepare_projects, workspace_service


def test_native_execution_pack_in_legacy_root_rejects_before_publication(tmp_path):
    source = tmp_path / "workspace"
    prepare_projects(source)
    compiled = tmp_path / "compiled"
    result = workspace_service(tmp_path / "profiles").compile(source, output_dir=compiled)
    assert result.passed
    release = json.loads((compiled / "release-set.json").read_bytes())
    legacy = tmp_path / "legacy"
    for section in ("dag_specs", "workload_packs", "runtime_payloads"):
        for row in release["artifacts"][section]:
            if section == "dag_specs":
                path = f"_dags/{row['id']}.dag-spec.json"
            elif section == "workload_packs":
                path = f"{row['id']}/airflow-pack.json"
            else:
                path = row["path"]
            destination = legacy / path
            destination.parent.mkdir(parents=True, exist_ok=True)
            destination.write_bytes((compiled / row["path"]).read_bytes())
    cache = tmp_path / "cache"
    report = materialize_compact_pack_release(pack_root=legacy, cache_root=cache, xcom_sidecar_image=SIDECAR)
    assert not report.passed
    assert any("NATIVE_AUTHORITY_REQUIRED" in blocker for blocker in report.blockers)
    assert not (cache / "releases").exists()
    assert not (legacy / "release-set.json").exists()


@pytest.mark.parametrize("oversized", [False, True])
def test_descriptor_bounds_and_recursion_reject_before_source_dispatch(tmp_path, oversized):
    root = tmp_path / "source"
    root.mkdir()
    depth = max(10_000, sys.getrecursionlimit() + 100)
    payload = b'{"schema":"dpone.release-set.v3","nested":' + b"[" * depth + b"0" + b"]" * depth + b"}"
    if oversized:
        payload += b" " * (8 * 1024 * 1024 + 1 - len(payload))
    (root / "release-set.json").write_bytes(payload)
    cache = tmp_path / "cache"
    report = materialize_compact_pack_release(pack_root=root, cache_root=cache, xcom_sidecar_image=SIDECAR)
    assert not report.passed
    assert "WORKSPACE_INVALID" in report.blockers[0]
    assert not cache.exists()
