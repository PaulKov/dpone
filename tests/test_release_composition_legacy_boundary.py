"""A legacy layout cannot acquire native workspace authority by transporting bytes."""

import json

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
