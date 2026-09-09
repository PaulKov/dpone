"""Verified release producer, not guessed payload syntax, controls runtime v2."""

from __future__ import annotations

import base64
from copy import deepcopy
from dataclasses import replace
from pathlib import Path

import pytest

from dpone.contracts.dbt_runtime_payloads import DBT_RUNTIME_WIRE_V2
from dpone.readiness.airflow_runtime_init_fetch import PLAN_B64_ENV, PLAN_SHA256_ENV
from dpone.runtime.init_fetch_contract import InitFetchError
from dpone.runtime.runtime_init_fetch_plan import (
    canonical_runtime_init_fetch_plan_bytes,
    runtime_init_fetch_plan_sha256,
)
from dpone.runtime.verified_pack_launcher import VerifiedPackLauncher
from tests.test_airflow_runtime_init_fetch_cli import _bundle, _dbt_runtime_fixture, _publish_ready_from_init_view


def _prepared_bundle(tmp_path: Path, *, wire: str = DBT_RUNTIME_WIRE_V2, reorder: bool = False, mutate=None):
    project = tmp_path / "project"
    project.mkdir()
    (project / "dbt_project.yml").write_text("name: analytics\n", encoding="utf-8")
    fixture = _dbt_runtime_fixture(project, execution_pack_v2=wire == DBT_RUNTIME_WIRE_V2)
    bundle = _bundle(
        runtime_argv=["dpone", "dbt", "execute-pack", "runtime/dbt-execution-pack.json", "--format", "json"],
        dbt_runtime=fixture,
        dbt_wire_contract=wire,
        dbt_release_mutator=mutate,
    )
    if reorder:
        plan = replace(bundle.plan, runtime_payloads=tuple(reversed(bundle.plan.runtime_payloads)))
        plan_bytes = canonical_runtime_init_fetch_plan_bytes(plan)
        digest = runtime_init_fetch_plan_sha256(plan)
        bundle = replace(
            bundle,
            plan=plan,
            plan_bytes=plan_bytes,
            plan_sha256=digest,
            environment={PLAN_B64_ENV: base64.b64encode(plan_bytes).decode("ascii"), PLAN_SHA256_ENV: digest},
        )
    _publish_ready_from_init_view(tmp_path, bundle)
    return bundle


def test_launcher_prepares_content_addressed_dbt_trio(tmp_path: Path) -> None:
    bundle = _prepared_bundle(tmp_path)
    command = VerifiedPackLauncher(artifact_root=tmp_path / "artifacts", worktree_root=tmp_path / "worktree").prepare(
        bundle.plan,
        plan_sha256=bundle.plan_sha256,
    )
    assert command.argv[:3] == ("dpone", "dbt", "execute-pack")
    assert (tmp_path / "worktree/dbt-project/dbt_project.yml").is_file()


def test_launcher_does_not_heal_reordered_v2_references(tmp_path: Path) -> None:
    bundle = _prepared_bundle(tmp_path, reorder=True)
    with pytest.raises(InitFetchError) as caught:
        VerifiedPackLauncher(artifact_root=tmp_path / "artifacts", worktree_root=tmp_path / "worktree").prepare(
            bundle.plan,
            plan_sha256=bundle.plan_sha256,
        )
    assert caught.value.code == "DPONE_DBT_SELECTION_DRIFT"


def test_launcher_rejects_unknown_explicit_producer_wire(tmp_path: Path) -> None:
    bundle = _prepared_bundle(tmp_path, wire="unsupported")
    with pytest.raises(InitFetchError) as caught:
        VerifiedPackLauncher(artifact_root=tmp_path / "artifacts", worktree_root=tmp_path / "worktree").prepare(
            bundle.plan,
            plan_sha256=bundle.plan_sha256,
        )
    assert caught.value.code == "DPONE_DBT_SELECTION_DRIFT"


@pytest.mark.parametrize("mutation", ["path", "media_type", "workload_trio", "duplicate", "conflicting_duplicate"])
def test_launcher_rejects_consistently_resealed_noncanonical_v2_release(tmp_path: Path, mutation: str) -> None:
    def mutate(release, bodies):
        artifacts = release["artifacts"]
        first = artifacts["runtime_payloads"][0]
        if mutation == "path":
            body = bodies.pop(first["path"])
            first["path"] = "runtime/dbt/other-project.tar.gz"
            bodies[first["path"]] = body
        elif mutation == "media_type":
            first["media_type"] = "application/octet-stream"
        elif mutation == "workload_trio":
            artifacts["workload_packs"][0]["runtime_payload_ids"].reverse()
        else:
            duplicate = deepcopy(first)
            if mutation == "conflicting_duplicate":
                duplicate["path"] = "runtime/dbt/foreign.tar.gz"
            artifacts["runtime_payloads"] = [*artifacts["runtime_payloads"], duplicate]

    bundle = _prepared_bundle(tmp_path, mutate=mutate)
    with pytest.raises(InitFetchError) as caught:
        VerifiedPackLauncher(artifact_root=tmp_path / "artifacts", worktree_root=tmp_path / "worktree").prepare(
            bundle.plan,
            plan_sha256=bundle.plan_sha256,
        )
    assert caught.value.code == "DPONE_DBT_SELECTION_DRIFT"
