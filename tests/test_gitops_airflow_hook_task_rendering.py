from __future__ import annotations

import json
from pathlib import Path

from dpone.gitops.airflow_run_spec import GitOpsAirflowRunSpecBuilder


def test_airflow_run_spec_includes_visible_source_refresh_hook_steps(tmp_path: Path, monkeypatch) -> None:
    monkeypatch.chdir(tmp_path)
    manifest = tmp_path / "manifests" / "orders.yaml"
    manifest.parent.mkdir()
    manifest.write_text(
        """
source:
  options:
    hooks:
      pre_hook:
        - id: refresh_base
          kind: source_refresh
          type: sql
          sql: "EXEC [schema].[p_refresh_base]"
          mutates_source: true
          execution:
            airflow: separate_task
        - id: refresh_final
          kind: source_refresh
          type: sql
          sql: "EXEC [schema].[p_refresh_final]"
          mutates_source: true
          depends_on: [refresh_base]
          execution:
            airflow: separate_task
sink: {}
""",
        encoding="utf-8",
    )
    bundle = {
        "entries": [
            {
                "manifest": "manifests/orders.yaml",
                "plan_path": "",
                "verify_path": "",
                "passed": True,
            }
        ]
    }

    report = GitOpsAirflowRunSpecBuilder().build(
        bundle_path=".dpone/gitops/bundle/bundle.json",
        bundle=bundle,
        image="dpone:local",
        image_digest=None,
        worktree=".",
        evidence_output=".dpone/gitops/airflow/runtime-evidence.json",
        require_attestation=False,
    )
    payload = report.to_jsonable()

    assert [step["kind"] for step in payload["steps"]] == [
        "bundle_verify",
        "source_refresh",
        "source_refresh",
        "dpone_run",
    ]
    assert payload["steps"][1]["name"] == "pre_hook_refresh_base"
    assert payload["steps"][1]["command"] == (
        "dpone hooks execute manifests/orders.yaml --phase pre_hook --hook-id refresh_base"
    )
    assert payload["steps"][2]["depends_on"] == ["pre_hook_refresh_base"]
    assert payload["steps"][3]["depends_on"] == ["pre_hook_refresh_final"]
    assert str(tmp_path) not in json.dumps(payload)
