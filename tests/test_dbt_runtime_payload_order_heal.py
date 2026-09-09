"""Regression: sorted index runtime_payload_ids must not break dbt identity."""

from __future__ import annotations

import base64
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import (
    canonical_runtime_init_fetch_plan_bytes,
    runtime_init_fetch_plan_sha256,
)
from dpone.runtime.verified_pack_launcher import VerifiedPackLauncher
from tests.test_airflow_runtime_init_fetch_cli import (
    PLAN_B64_ENV,
    PLAN_SHA256_ENV,
    _bundle,
    _dbt_runtime_fixture,
    _publish_ready_from_init_view,
)


def test_launcher_heals_order_only_runtime_payload_id_drift(tmp_path: Path) -> None:
    project = tmp_path / "source-project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: analytics\n", encoding="utf-8")
    fixture = _dbt_runtime_fixture(project)
    bundle = _bundle(
        runtime_argv=[
            "dpone",
            "dbt",
            "execute-pack",
            "runtime/dbt-execution-pack.json",
            "--format",
            "json",
        ],
        dbt_runtime=fixture,
    )
    # Simulate compact materialize that sorted workload index refs alphabetically
    # while the pack kept canonical dbt order.
    sorted_payloads = tuple(sorted(bundle.plan.runtime_payloads, key=lambda item: item.id))
    assert tuple(item.id for item in sorted_payloads) != (
        "dbt_project",
        "dbt_manifest",
        f"dbt_selection_{fixture.workflow_id}",
    )
    plan = replace(bundle.plan, runtime_payloads=sorted_payloads)
    plan_bytes = canonical_runtime_init_fetch_plan_bytes(plan)
    plan_sha256 = runtime_init_fetch_plan_sha256(plan)
    mutated = replace(
        bundle,
        plan=plan,
        plan_bytes=plan_bytes,
        plan_sha256=plan_sha256,
        environment={
            PLAN_B64_ENV: base64.b64encode(plan_bytes).decode("ascii"),
            PLAN_SHA256_ENV: plan_sha256,
        },
    )
    _publish_ready_from_init_view(tmp_path, mutated)

    VerifiedPackLauncher(
        artifact_root=tmp_path / "artifacts",
        worktree_root=tmp_path / "worktree",
    ).prepare(plan, plan_sha256=plan_sha256)


def test_launcher_still_rejects_missing_runtime_payload_identity(tmp_path: Path) -> None:
    bundle = _bundle(
        runtime_argv=[
            "dpone",
            "dbt",
            "execute-pack",
            "runtime/dbt-execution-pack.json",
            "--format",
            "json",
        ],
    )
    _publish_ready_from_init_view(tmp_path, bundle)
    with pytest.raises(InitFetchError) as exc:
        VerifiedPackLauncher(
            artifact_root=tmp_path / "artifacts",
            worktree_root=tmp_path / "worktree",
        ).prepare(bundle.plan, plan_sha256=bundle.plan_sha256)
    assert exc.value.code == "DPONE_DBT_SELECTION_DRIFT"
