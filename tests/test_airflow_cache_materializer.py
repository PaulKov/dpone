from __future__ import annotations

import hashlib
import json
import multiprocessing
import stat
from contextlib import contextmanager
from datetime import UTC, datetime
from multiprocessing.connection import Connection
from pathlib import Path

import pytest

import dpone.runtime.deployment_cache_retention_applier as retention_applier
from dpone.app.airflow_cache_retention_composition import build_deployment_cache_retention_applier
from dpone.cli import main as cli_main
from dpone.contracts.airflow_deployment import deployment_id as compute_deployment_id
from dpone.contracts.airflow_deployment import release_id as compute_release_id

TEST_RETENTION_OPERATION_ID = "sha256:" + "f" * 64


def _report_promotion_lock(cache_root: str, connection: Connection) -> None:
    from dpone.runtime.deployment_cache_common import promotion_lock

    with promotion_lock(Path(cache_root)):
        connection.send("acquired")


def _run_cli(args: list[str], capsys: pytest.CaptureFixture[str]) -> tuple[int, str, str]:
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code or 0), captured.out, captured.err


def _write_deployment(
    root: Path,
    deployment_id: str,
    *,
    complete: bool = True,
    environment: str = "dev",
    release_id: str = "sha256:" + "a" * 64,
    index_release_id: str | None = None,
) -> Path:
    dag_id = "DAG__fixture__cache__refresh"
    dag_path = "dags/cache_fixture.dag-spec.json"
    dag_bytes = json.dumps({"dag_id": dag_id}, sort_keys=True).encode("utf-8")
    dag_sha256 = "sha256:" + hashlib.sha256(dag_bytes).hexdigest()
    release_payload = {
        "schema": "dpone.release-set.v1",
        "release_id": "",
        "artifacts": {
            "dag_specs": [
                {
                    "id": dag_id,
                    "path": dag_path,
                    "sha256": dag_sha256,
                }
            ],
            "workload_packs": [],
            "canonical_schemas": [],
        },
        "fixture_identity": release_id,
    }
    computed_release_id = compute_release_id(release_payload)
    release_payload["release_id"] = computed_release_id
    release_dir = root / "releases" / computed_release_id.replace(":", "-")
    release_dir.mkdir(parents=True, exist_ok=True)
    release_set_path = release_dir / "release-set.json"
    if not release_set_path.exists():
        release_set_path.write_text(json.dumps(release_payload), encoding="utf-8")
        artifact_path = release_dir / dag_path
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_bytes(dag_bytes)
    runtime_delivery = {"mode": "local_preview"}
    deployment_payload = {
        "schema": "dpone.deployment-set.v1",
        "deployment_id": "",
        "deployment_type": "preview",
        "runnable": False,
        "environment": environment,
        "release_ref": computed_release_id,
        "binding_set_ref": None,
        "connection_registry_ref": None,
        "credential_runtime_ref": None,
        "runtime_image_digest": None,
        "airflow_bundle_ref": None,
        "runtime_artifact_delivery": runtime_delivery,
        "fixture_identity": deployment_id,
    }
    computed_deployment_id = compute_deployment_id(deployment_payload)
    deployment_payload["deployment_id"] = computed_deployment_id
    deployment = root / "deployments" / environment / computed_deployment_id.replace(":", "-")
    deployment.mkdir(parents=True)
    (deployment / "deployment.json").write_text(
        json.dumps(deployment_payload),
        encoding="utf-8",
    )
    (deployment / "airflow-index.json").write_text(
        json.dumps(
            {
                "schema": "dpone.airflow-deployment-index.v1",
                "release_id": index_release_id or computed_release_id,
                "deployment_id": computed_deployment_id,
                "dag_specs": [
                    {
                        "id": dag_id,
                        "artifact_ref": f"cache://releases/{computed_release_id.replace(':', '-')}/{dag_path}",
                        "sha256": dag_sha256,
                        "bytes": len(dag_bytes),
                    }
                ],
                "workload_packs": [],
                "binding_set_ref": None,
                "connection_registry_ref": None,
                "credential_runtime_ref": None,
                "runtime_image_digest": None,
                "airflow_bundle_ref": None,
                "runtime_artifact_delivery": runtime_delivery,
            }
        ),
        encoding="utf-8",
    )
    if complete:
        (deployment / "_SUCCESS").write_text("ok\n", encoding="utf-8")
    return deployment


def _deployment_id(path: Path) -> str:
    return str(json.loads((path / "deployment.json").read_text(encoding="utf-8"))["deployment_id"])


def test_promotion_postcommit_action_finishes_before_lock_release(tmp_path: Path) -> None:
    pytest.importorskip("fcntl")
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "a" * 64)
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    contender = context.Process(target=_report_promotion_lock, args=(cache_root.as_posix(), child))
    observed: list[str | None] = []

    def commit_control_records(current) -> None:
        observed.append(current.activation_id)
        contender.start()
        assert not parent.poll(0.5), "promotion lock was released before durable control records committed"

    try:
        promoted = DeploymentCacheMaterializer(cache_root).promote(
            deployment,
            environment="dev",
            postcommit_action=commit_control_records,
        )
        assert parent.poll(5), "competing promotion did not resume after postcommit"
        assert parent.recv() == "acquired"
    finally:
        if contender.pid is not None:
            contender.join(timeout=5)
            if contender.is_alive():
                contender.terminate()
                contender.join(timeout=2)

    assert observed == [promoted.activation_id]
    assert contender.exitcode == 0


def _retention_authorization(
    cache_root: Path,
    *,
    protected_deployment_ids: tuple[str, ...] = (),
) -> dict[str, object]:
    from dpone.adapters.airflow_desired_state_checkpoint import FileDesiredStateCheckpointStore
    from dpone.contracts.airflow_desired_state import DesiredStateRevision
    from dpone.contracts.airflow_desired_state_reconcile import DesiredStateCheckpoint
    from dpone.contracts.airflow_loader_ack import AirflowLoaderAck
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionPlanner

    index_path = cache_root / "current" / "airflow-index.json"
    index = json.loads(index_path.read_text(encoding="utf-8"))
    pointer = json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))
    plan = DeploymentCacheRetentionPlanner(cache_root).plan(
        environment="dev",
        protected_deployment_ids=protected_deployment_ids,
    )
    dag_ids = tuple(sorted(item["id"] for item in index["dag_specs"]))
    index_sha256 = "sha256:" + hashlib.sha256(index_path.read_bytes()).hexdigest()
    checkpoint_store = FileDesiredStateCheckpointStore(
        cache_root / "status" / "desired-state-checkpoint.json",
        root=cache_root,
    )
    checkpoint_store.commit(
        DesiredStateCheckpoint(
            environment="dev",
            observed_revision=DesiredStateRevision("fixture-revision"),
            desired_state_sha256="sha256:" + "1" * 64,
            registry_scope_id="sha256:" + "2" * 64,
            source_project="tests/airflow-cache",
            source_ref="master",
            release_id=pointer["release_id"],
            deployment_id=pointer["deployment_id"],
            occurrence_id=pointer["activation_id"],
            source_git_sha="3" * 40,
            airflow_index_sha256=index_sha256,
            runtime_image_digest="sha256:" + "4" * 64,
            expected_dag_ids=dag_ids,
            activation_id=pointer["activation_id"],
        )
    )
    return {
        "expected_plan_sha256": plan.plan_sha256,
        "review_id": "00000000-0000-4000-8000-000000000001",
        "checkpoint_reader": checkpoint_store,
        "loader_ack_reader": lambda: AirflowLoaderAck(
            release_id=pointer["release_id"],
            deployment_id=pointer["deployment_id"],
            airflow_index_sha256=index_sha256,
            activation_id=pointer["activation_id"],
            loaded_dag_ids=dag_ids,
            skipped_dag_ids=(),
            error_codes=(),
            fatal=False,
            acknowledged_at="2026-08-03T00:00:00+00:00",
        ),
    }


def _retention_cli_authorization(
    cache_root: Path,
    output_root: Path,
    *,
    protected_deployment_ids: tuple[str, ...] = (),
) -> list[str]:
    authorization = _retention_authorization(
        cache_root,
        protected_deployment_ids=protected_deployment_ids,
    )
    ack_reader = authorization["loader_ack_reader"]
    assert callable(ack_reader)
    loader_ack = ack_reader()
    ack_path = output_root / "loader-ack.json"
    ack_path.write_text(json.dumps(loader_ack.to_dict()), encoding="utf-8")
    return [
        "--expected-plan-sha256",
        str(authorization["expected_plan_sha256"]),
        "--review-id",
        str(authorization["review_id"]),
        "--loader-ack-file",
        str(ack_path),
    ]


def _retention_transaction(path: Path, deployment_id: str, *, phase: str) -> dict[str, object]:
    identity = path.stat()
    cache_root = path.parents[2]
    environment = path.parent.name
    return {
        "deployment_id": deployment_id,
        "environment": environment,
        "phase": phase,
        "original_path": path.as_posix(),
        "detached_path": (cache_root / ".retention-trash" / f"{path.name}.fixture").as_posix(),
        "activation_path": (cache_root / "activations" / environment / deployment_id.replace(":", "-", 1)).as_posix(),
        "expected_device": identity.st_dev,
        "expected_inode": identity.st_ino,
    }


def test_deployment_cache_retention_uses_reconcile_then_promotion_lock_order(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    current = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    transitions: list[str] = []

    @contextmanager
    def observed_reconcile_lock(path: Path):
        transitions.append("enter:reconcile")
        yield
        transitions.append("exit:reconcile")

    @contextmanager
    def observed_promotion_lock(path: Path):
        transitions.append("enter:promotion")
        yield
        transitions.append("exit:promotion")

    monkeypatch.setattr(retention_applier, "reconcile_lock", observed_reconcile_lock)
    monkeypatch.setattr(retention_applier, "promotion_lock", observed_promotion_lock)

    build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
    )

    assert transitions == ["enter:reconcile", "enter:promotion", "exit:promotion", "exit:reconcile"]


def test_cache_materializer_switches_current_only_for_complete_deployment(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)

    result = DeploymentCacheMaterializer(cache_root).promote(deployment, environment="dev")

    assert result.deployment_id == deployment_id
    assert (cache_root / "current" / "airflow-index.json").exists()
    pointer = json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))
    assert pointer["schema"] == "dpone.current-pointer.v1"
    assert pointer["deployment_id"] == deployment_id
    assert pointer["environment"] == "dev"


def test_cache_materializer_writes_schema_compliant_current_pointer_with_cas_metadata(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    first_id = "sha256:" + "b" * 64
    second_id = "sha256:" + "c" * 64
    first = _write_deployment(cache_root, first_id, complete=True)
    second = _write_deployment(cache_root, second_id, complete=True)
    first_id = _deployment_id(first)
    second_id = _deployment_id(second)
    materializer = DeploymentCacheMaterializer(
        cache_root,
        clock=lambda: datetime(2026, 7, 12, 9, 30, tzinfo=UTC),
    )

    materializer.promote(first, environment="dev", promoted_by="ci://github-actions/first")
    current = materializer.promote(
        second,
        environment="dev",
        promoted_by="ci://github-actions/second",
        expected_current_deployment_id=first_id,
        source_commit="7ac31f2",
        attestation_ref="gha://attestations/123",
        workspace_authority_connection_ref="dpone_control",
    )

    pointer = json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))
    pointer_schema = json.loads(Path("docs/schemas/gitops/current-pointer.schema.json").read_text(encoding="utf-8"))
    jsonschema.validate(pointer, pointer_schema)
    assert current.to_dict()["schema"] == "dpone.current-pointer.v1"
    assert pointer["deployment_id"] == second_id
    assert pointer["previous_deployment_id"] == first_id
    assert pointer["promoted_by"] == "ci://github-actions/second"
    assert pointer["promoted_at"] == "2026-07-12T09:30:00+00:00"
    assert pointer["source_commit"] == "7ac31f2"
    assert pointer["attestation_ref"] == "gha://attestations/123"
    assert pointer["workspace_authority_connection_ref"] == "dpone_control"
    assert current.workspace_authority_connection_ref == "dpone_control"

    with pytest.raises(DeploymentCacheError) as exc:
        materializer.promote(
            first,
            environment="dev",
            promoted_by="ci://github-actions/stale",
            expected_current_deployment_id="sha256:" + "9" * 64,
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_CAS_MISMATCH"
    assert json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))["deployment_id"] == second_id


def test_cache_materializer_rejects_unknown_promoter_when_policy_is_configured(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    materializer = DeploymentCacheMaterializer(
        cache_root,
        allowed_promoters=("ci://github-actions/dpone-airflow",),
    )

    with pytest.raises(DeploymentCacheError) as exc:
        materializer.promote(
            deployment,
            environment="dev",
            promoted_by="local://unknown-publisher",
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED"
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_cache_sync_result_rejects_empty_promoter_allowlist_before_mutation(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)

    result = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="ci://github-actions/dpone-airflow",
        allowed_promoters=(),
        confirm_promote=True,
    )

    payload = result.to_dict()
    assert result.exit_code == 4
    assert payload["errors"][0]["code"] == "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED"
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current-promotion-audit.jsonl").exists()
    assert not (cache_root / "current").exists()


def test_cache_sync_result_rejects_missing_promoter_before_mutation(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)

    result = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="",
        allowed_promoters=("ci://github-actions/dpone-airflow",),
        confirm_promote=True,
    )

    payload = result.to_dict()
    assert result.exit_code == 4
    assert payload["errors"][0]["code"] == "DPONE_CURRENT_POINTER_PROMOTER_MISSING"
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current-promotion-audit.jsonl").exists()
    assert not (cache_root / "current").exists()


def test_local_cache_sync_result_uses_exact_actor_policy(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import local_cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)

    result = local_cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="local://dpone-test-preview",
    )

    pointer = json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))
    assert result.passed is True
    assert pointer["promoted_by"] == "local://dpone-test-preview"
    assert pointer["previous_deployment_id"] is None


def test_cache_sync_promotion_precondition_fails_before_pointer_mutation(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import (
        PromotionPrecondition,
        local_cache_sync_result,
    )

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)

    result = local_cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="local://dpone-test-preview",
        promotion_precondition=PromotionPrecondition(
            check=lambda: False,
            code="DPONE_SELECTION_STATE_CHANGED",
            message="Selection changed before promotion.",
        ),
    )

    assert not result.passed
    assert result.errors[0]["code"] == "DPONE_SELECTION_STATE_CHANGED"
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_local_cache_sync_result_rejects_concurrent_stale_promotion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness import airflow_self_service_cache_sync
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    winner = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)
    winner_id = _deployment_id(winner)
    original_cache_sync_result = airflow_self_service_cache_sync.cache_sync_result

    def race_then_sync(**kwargs: object):
        DeploymentCacheMaterializer(
            cache_root,
            allowed_promoters=("local://concurrent-winner",),
        ).promote(
            winner,
            environment="dev",
            promoted_by="local://concurrent-winner",
            expect_current_absent=True,
        )
        return original_cache_sync_result(**kwargs)

    monkeypatch.setattr(airflow_self_service_cache_sync, "cache_sync_result", race_then_sync)

    result = airflow_self_service_cache_sync.local_cache_sync_result(
        cache_root=cache_root,
        deployment_dir=stale,
        environment="dev",
        promoted_by="local://dpone-test-preview",
    )

    pointer = json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))
    assert result.exit_code == 4
    assert result.errors[0]["code"] == "DPONE_CURRENT_POINTER_CAS_MISMATCH"
    assert pointer["deployment_id"] == winner_id


def test_cache_materializer_rejects_incomplete_deployment_without_touching_current(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    complete = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    materializer = DeploymentCacheMaterializer(cache_root)
    materializer.promote(complete, environment="dev")
    original_pointer = (cache_root / "current-pointer.json").read_text(encoding="utf-8")
    incomplete = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=False)

    with pytest.raises(DeploymentCacheError) as exc:
        materializer.promote(incomplete, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_INCOMPLETE"
    assert (cache_root / "current-pointer.json").read_text(encoding="utf-8") == original_pointer


def test_cache_materializer_rejects_deployment_environment_mismatch(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True, environment="prod")

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(deployment, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH"
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_cache_materializer_rejects_release_mismatch_between_deployment_and_index(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(
        cache_root,
        "sha256:" + "b" * 64,
        complete=True,
        release_id="sha256:" + "a" * 64,
        index_release_id="sha256:" + "d" * 64,
    )

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(deployment, environment="dev")

    assert exc.value.code == "DPONE_RELEASE_ID_MISMATCH"
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_sync_result_reports_deployment_id_mismatch_with_index_path(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    payload = json.loads((deployment / "airflow-index.json").read_text(encoding="utf-8"))
    payload["deployment_id"] = "sha256:" + "c" * 64
    (deployment / "airflow-index.json").write_text(json.dumps(payload), encoding="utf-8")

    result = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="ci://github-actions/dpone-airflow",
        allowed_promoters=("ci://github-actions/dpone-airflow",),
        confirm_promote=True,
    )

    output = result.to_dict()
    assert result.exit_code == 4
    assert output["passed"] is False
    assert output["errors"][0]["code"] == "DPONE_DEPLOYMENT_ID_MISMATCH"
    assert output["errors"][0]["stage"] == "cache_sync"
    assert output["errors"][0]["path"] == str(deployment / "airflow-index.json")
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_sync_result_reports_missing_release_with_deployment_path(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_payload = json.loads((deployment / "deployment.json").read_text(encoding="utf-8"))
    deployment_payload.pop("release_ref")
    (deployment / "deployment.json").write_text(json.dumps(deployment_payload), encoding="utf-8")
    index_payload = json.loads((deployment / "airflow-index.json").read_text(encoding="utf-8"))
    index_payload.pop("release_id")
    (deployment / "airflow-index.json").write_text(json.dumps(index_payload), encoding="utf-8")

    result = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="ci://github-actions/dpone-airflow",
        allowed_promoters=("ci://github-actions/dpone-airflow",),
        confirm_promote=True,
    )

    output = result.to_dict()
    assert result.exit_code == 4
    assert output["passed"] is False
    assert output["errors"][0]["code"] == "DPONE_RELEASE_NOT_FOUND"
    assert output["errors"][0]["stage"] == "cache_sync"
    assert output["errors"][0]["path"] == str(deployment / "deployment.json")
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_sync_result_reports_cas_mismatch_with_current_pointer_path(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    first_id = "sha256:" + "b" * 64
    second_id = "sha256:" + "c" * 64
    first = _write_deployment(cache_root, first_id, complete=True)
    second = _write_deployment(cache_root, second_id, complete=True)
    first_id = _deployment_id(first)
    second_id = _deployment_id(second)
    DeploymentCacheMaterializer(cache_root).promote(first, environment="dev")

    result = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=second,
        environment="dev",
        promoted_by="ci://github-actions/dpone-airflow",
        allowed_promoters=("ci://github-actions/dpone-airflow",),
        expected_current_deployment_id="sha256:" + "9" * 64,
        confirm_promote=True,
    )

    output = result.to_dict()
    assert result.exit_code == 4
    assert output["passed"] is False
    assert output["errors"][0]["code"] == "DPONE_CURRENT_POINTER_CAS_MISMATCH"
    assert output["errors"][0]["stage"] == "cache_sync"
    assert output["errors"][0]["path"] == str(cache_root / "current-pointer.json")
    assert output["state_may_have_changed"] is True
    assert output["recovery_required"] is True
    assert json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))["deployment_id"] == first_id
    assert (cache_root / "activations" / "dev" / second_id.replace(":", "-", 1)).is_dir()


def test_airflow_cache_sync_reports_staged_validation_failure_as_mutation_uncertain(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result
    from dpone.runtime.deployment_cache import DeploymentCacheError
    from dpone.runtime.deployment_cache_projection_validator import (
        DeploymentCacheProjectionValidator,
    )

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)

    def reject_staged_projection(*args: object, **kwargs: object) -> None:
        raise DeploymentCacheError(
            "DPONE_DEPLOYMENT_ID_MISMATCH",
            "synthetic staged projection mismatch",
        )

    monkeypatch.setattr(
        DeploymentCacheProjectionValidator,
        "validate_staged_activation_details",
        reject_staged_projection,
    )

    output = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="ci://github-actions/dpone-airflow",
        allowed_promoters=("ci://github-actions/dpone-airflow",),
        expect_current_absent=True,
        confirm_promote=True,
    ).to_dict()

    assert output["passed"] is False
    assert output["errors"][0]["code"] == "DPONE_DEPLOYMENT_ID_MISMATCH"
    assert output["state_may_have_changed"] is True
    assert output["recovery_required"] is True
    assert not (cache_root / "current").exists()
    assert not (cache_root / "current-pointer.json").exists()


def test_cache_materializer_rejects_deployment_outside_cache_root(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    outside_root = tmp_path / "outside-cache"
    deployment = _write_deployment(outside_root, "sha256:" + "b" * 64, complete=True)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(deployment, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_PATH_OUTSIDE_CACHE_ROOT"
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_cache_materializer_rejects_noncanonical_deployment_path_inside_cache_root(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheError, DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root / "staging-area", "sha256:" + "b" * 64, complete=True)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheMaterializer(cache_root).promote(deployment, environment="dev")

    assert exc.value.code == "DPONE_DEPLOYMENT_PATH_INVALID"
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_deployment_cache_retention_plan_protects_current_and_marks_stale_deployments(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer, DeploymentCacheRetentionPlanner

    cache_root = tmp_path / ".dpone-cache"
    stale_id = "sha256:" + "a" * 64
    current_id = "sha256:" + "b" * 64
    stale = _write_deployment(cache_root, stale_id, complete=True)
    current = _write_deployment(cache_root, current_id, complete=True)
    stale_id = _deployment_id(stale)
    current_id = _deployment_id(current)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")

    plan = DeploymentCacheRetentionPlanner(cache_root).plan(environment="dev")

    assert plan.to_dict()["schema"] == "dpone.deployment-cache-retention-plan.v1"
    actions = {item.deployment_id: item.action for item in plan.items}
    reasons = {item.deployment_id: item.reason for item in plan.items}
    assert actions[current_id] == "protect"
    assert reasons[current_id] == "current"
    assert actions[stale_id] == "delete"
    assert reasons[stale_id] == "unreferenced"
    assert sorted(plan.delete_candidates) == [stale_id]


def test_deployment_cache_retention_plan_flags_incomplete_deployments(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionPlanner

    cache_root = tmp_path / ".dpone-cache"
    incomplete_id = "sha256:" + "c" * 64
    incomplete = _write_deployment(cache_root, incomplete_id, complete=False)
    incomplete_id = _deployment_id(incomplete)

    plan = DeploymentCacheRetentionPlanner(cache_root).plan(environment="dev")

    assert plan.items[0].deployment_id == incomplete_id
    assert plan.items[0].action == "quarantine"
    assert plan.items[0].reason == "incomplete"
    assert plan.items[0].error_code == "DPONE_DEPLOYMENT_INCOMPLETE"
    assert plan.delete_candidates == ()


def test_deployment_cache_retention_quarantines_projection_missing_airflow_index(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionPlanner

    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    (deployment / "airflow-index.json").unlink()

    plan = DeploymentCacheRetentionPlanner(cache_root).plan(environment="dev")

    assert plan.delete_candidates == ()
    assert plan.items[0].action == "quarantine"
    assert plan.items[0].reason == "invalid"
    assert plan.items[0].error_code == "DPONE_AIRFLOW_INDEX_NOT_FOUND"


def test_deployment_cache_retention_quarantines_symlink_candidate(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionPlanner

    cache_root = tmp_path / ".dpone-cache"
    target = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    alias = target.parent / ("sha256-" + "b" * 64)
    alias.symlink_to(target.name, target_is_directory=True)

    plan = DeploymentCacheRetentionPlanner(cache_root).plan(environment="dev")
    alias_item = next(item for item in plan.items if item.path == alias.as_posix())

    assert alias_item.action == "quarantine"
    assert alias_item.error_code == "DPONE_DEPLOYMENT_PATH_INVALID"


def test_deployment_cache_retention_quarantine_with_invalid_directory_id_is_schema_valid(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionPlanner

    cache_root = tmp_path / ".dpone-cache"
    invalid = cache_root / "deployments" / "dev" / "not-a-digest"
    invalid.mkdir(parents=True)

    payload = DeploymentCacheRetentionPlanner(cache_root).plan(environment="dev").to_dict()
    schema = json.loads(
        Path("docs/schemas/gitops/deployment-cache-retention-plan.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, schema)
    assert payload["items"][0]["deployment_id"] is None
    assert payload["items"][0]["action"] == "quarantine"


def test_deployment_cache_retention_plan_protects_retention_evidence_refs(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionPlanner

    cache_root = tmp_path / ".dpone-cache"
    protected_id = "sha256:" + "d" * 64
    protected = _write_deployment(cache_root, protected_id, complete=True)
    protected_id = _deployment_id(protected)

    plan = DeploymentCacheRetentionPlanner(cache_root).plan(
        environment="dev",
        protected_deployment_ids=(protected_id,),
    )

    assert plan.items[0].deployment_id == protected_id
    assert plan.items[0].action == "protect"
    assert plan.items[0].reason == "retention_evidence"
    assert plan.protected_deployment_ids == (protected_id,)
    assert plan.to_dict()["protected_deployment_ids"] == [protected_id]
    assert plan.delete_candidates == ()


def test_deployment_cache_retention_apply_requires_explicit_confirmation(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionApplyError

    cache_root = tmp_path / ".dpone-cache"
    stale_id = "sha256:" + "a" * 64
    stale = _write_deployment(cache_root, stale_id, complete=True)
    stale_id = _deployment_id(stale)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root).apply(
            environment="dev", confirm_delete=False, promoted_by="ci://retention"
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_CONFIRMATION_REQUIRED"
    assert stale.exists()


def test_deployment_cache_retention_apply_requires_allowed_actor(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionApplyError

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://trusted",)).apply(
            environment="dev", confirm_delete=True, promoted_by="ci://untrusted"
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED"
    assert stale.exists()


def test_deployment_cache_retention_apply_deletes_only_unreferenced_complete_deployments(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale_id = "sha256:" + "a" * 64
    current_id = "sha256:" + "b" * 64
    incomplete_id = "sha256:" + "c" * 64
    protected_id = "sha256:" + "d" * 64
    stale = _write_deployment(cache_root, stale_id, complete=True)
    current = _write_deployment(cache_root, current_id, complete=True)
    incomplete = _write_deployment(cache_root, incomplete_id, complete=False)
    protected = _write_deployment(cache_root, protected_id, complete=True)
    stale_id = _deployment_id(stale)
    current_id = _deployment_id(current)
    incomplete_id = _deployment_id(incomplete)
    protected_id = _deployment_id(protected)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")

    report = build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        protected_deployment_ids=(protected_id,),
        **_retention_authorization(cache_root, protected_deployment_ids=(protected_id,)),
    )

    compatibility_payload = report.to_dict()
    v2_payload = report.to_v2_dict()
    payload = report.to_v3_dict()
    assert compatibility_payload["schema"] == "dpone.deployment-cache-retention-apply.v1"
    assert "activation_history_revision" not in compatibility_payload
    assert "operation_id" not in compatibility_payload
    assert "receipt_revision" not in compatibility_payload
    assert "transaction_status" not in compatibility_payload
    assert v2_payload["schema"] == "dpone.deployment-cache-retention-apply.v2"
    assert "operation_id" not in v2_payload
    assert "receipt_revision" not in v2_payload
    assert "transaction_status" not in v2_payload
    assert payload["schema"] == "dpone.deployment-cache-retention-apply.v3"
    assert payload["promoted_by"] == "ci://retention"
    assert payload["deleted_deployment_ids"] == [stale_id]
    assert payload["skipped_deployment_ids"] == sorted([current_id, incomplete_id, protected_id])
    assert payload["operation_id"].startswith("sha256:")
    assert payload["review_id"] == _retention_authorization(cache_root)["review_id"]
    assert payload["receipt_revision"].startswith("sha256:")
    assert payload["transaction_status"] == "committed"
    assert not stale.exists()
    assert current.exists()
    assert incomplete.exists()
    assert protected.exists()
    history = json.loads((cache_root / ".retention-activation-history.v2.json").read_text(encoding="utf-8"))
    assert payload["activation_history_revision"] == history["revision"]
    assert list(history["entries"]) == [next(iter(history["entries"].values()))["activation_id"]]


def test_deployment_cache_retention_blocks_before_delete_when_activation_history_is_not_durable(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.runtime.deployment_cache_activation_history as activation_history
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")

    def fail_history_write(path: Path, payload: dict[str, object]) -> None:
        raise OSError("simulated fsync failure")

    monkeypatch.setattr(activation_history, "atomic_write_json", fail_history_write)

    applier = build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",))
    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **_retention_authorization(cache_root),
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_FAILED"
    assert stale.exists()
    assert current.exists()


def test_deployment_cache_retention_migrates_legacy_history_as_diagnostic_hashes(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    legacy_secret = "must-not-be-copied"
    (cache_root / ".retention-activation-history.v1.json").write_text(
        json.dumps(
            {
                "schema": "dpone.deployment-cache-activation-history.v1",
                "entries": [{"legacy": legacy_secret}],
            }
        ),
        encoding="utf-8",
    )

    build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        **_retention_authorization(cache_root),
    )

    history_text = (cache_root / ".retention-activation-history.v2.json").read_text(encoding="utf-8")
    history = json.loads(history_text)
    assert history["legacy_v1_diagnostics"][0]["entry_sha256"].startswith("sha256:")
    assert legacy_secret not in history_text
    assert not stale.exists()


def test_deployment_cache_retention_reads_loader_ack_after_lock_acquisition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.contracts.airflow_loader_ack import AirflowLoaderAck
    from dpone.readiness.airflow_cache_retention import AirflowCacheRetentionError, AirflowCacheRetentionService
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    ack_reader = authorization["loader_ack_reader"]
    assert callable(ack_reader)
    successful_ack = ack_reader()
    ack_path = tmp_path / "loader-ack.json"
    ack_path.write_text(json.dumps(successful_ack.to_dict()), encoding="utf-8")
    original_lock = retention_applier.promotion_lock

    @contextmanager
    def replace_ack_before_lock(cache_path: Path):
        fatal_ack = AirflowLoaderAck(
            release_id=successful_ack.release_id,
            deployment_id=successful_ack.deployment_id,
            airflow_index_sha256=successful_ack.airflow_index_sha256,
            activation_id=successful_ack.activation_id,
            loaded_dag_ids=(),
            skipped_dag_ids=(),
            error_codes=("DPONE_AIRFLOW_DAG_LOAD_FAILED",),
            fatal=True,
            acknowledged_at="2026-08-03T00:01:00+00:00",
        )
        ack_path.write_text(json.dumps(fatal_ack.to_dict()), encoding="utf-8")
        with original_lock(cache_path):
            yield

    monkeypatch.setattr(retention_applier, "promotion_lock", replace_ack_before_lock)

    with pytest.raises(AirflowCacheRetentionError) as exc:
        AirflowCacheRetentionService(cache_root=cache_root).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            allowed_promoters=("ci://retention",),
            expected_plan_sha256=str(authorization["expected_plan_sha256"]),
            loader_ack_file=ack_path,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID"
    assert stale.exists()
    assert current.exists()


def test_deployment_cache_retention_rejects_unknown_evidence_version_before_mutation(tmp_path: Path) -> None:
    from dpone.readiness.airflow_cache_retention import AirflowCacheRetentionError, AirflowCacheRetentionService
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    ack_reader = authorization["loader_ack_reader"]
    assert callable(ack_reader)
    ack_path = tmp_path / "loader-ack.json"
    ack_path.write_text(json.dumps(ack_reader().to_dict()), encoding="utf-8")

    with pytest.raises(AirflowCacheRetentionError) as exc:
        AirflowCacheRetentionService(cache_root=cache_root).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            allowed_promoters=("ci://retention",),
            expected_plan_sha256=str(authorization["expected_plan_sha256"]),
            loader_ack_file=ack_path,
            evidence_version="future",
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_EVIDENCE_VERSION_INVALID"
    assert stale.exists()
    assert current.exists()
    assert not (cache_root / ".retention-activation-history.v2.json").exists()


def test_public_retention_apply_old_call_shape_returns_domain_error_instead_of_type_error(tmp_path: Path) -> None:
    from dpone.readiness.airflow_cache_retention import AirflowCacheRetentionError, AirflowCacheRetentionService

    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()

    with pytest.raises(AirflowCacheRetentionError) as exc:
        AirflowCacheRetentionService(cache_root=cache_root).apply(
            environment="dev",
            confirm_delete=False,
            promoted_by="ci://legacy-caller",
            allowed_promoters=(),
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_CONFIRMATION_REQUIRED"


def test_deployment_cache_retention_refuses_a_changed_plan_without_deleting(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    late = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"
    assert stale.exists()
    assert current.exists()
    assert late.exists()


def test_deployment_cache_retention_refuses_a_rogue_loader_ack_without_deleting(tmp_path: Path) -> None:
    from dataclasses import replace

    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    ack_reader = authorization["loader_ack_reader"]
    assert callable(ack_reader)
    rogue_ack = replace(
        ack_reader(),
        activation_id="4f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    )
    authorization["loader_ack_reader"] = lambda: rogue_ack

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID"
    assert stale.exists()
    assert current.exists()


def test_deployment_cache_retention_requires_desired_checkpoint_before_delete(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    (cache_root / "status" / "desired-state-checkpoint.json").unlink()

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_DESIRED_CHECKPOINT_REQUIRED"
    assert stale.exists()
    assert current.exists()


def test_deployment_cache_retention_rejects_desired_checkpoint_identity_drift(tmp_path: Path) -> None:
    from dataclasses import replace

    from dpone.adapters.airflow_desired_state_checkpoint import FileDesiredStateCheckpointStore
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    checkpoint_path = cache_root / "status" / "desired-state-checkpoint.json"
    store = FileDesiredStateCheckpointStore(checkpoint_path, root=cache_root)
    checkpoint = store.read()
    assert checkpoint is not None
    store.commit(replace(checkpoint, release_id="sha256:" + "9" * 64))

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_DESIRED_CHECKPOINT_MISMATCH"
    assert stale.exists()
    assert current.exists()


def test_deployment_cache_retention_refuses_skipped_dag_ack_without_deleting(tmp_path: Path) -> None:
    from dataclasses import replace

    from dpone.contracts.airflow_loader_ack import AirflowLoaderAck
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    ack_reader = authorization["loader_ack_reader"]
    assert callable(ack_reader)
    acknowledgement = ack_reader()
    assert isinstance(acknowledgement, AirflowLoaderAck)
    skipped_ack = replace(
        acknowledgement,
        loaded_dag_ids=(),
        skipped_dag_ids=("DAG__skipped",),
    )
    authorization["loader_ack_reader"] = lambda: skipped_ack

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID"
    assert stale.exists()
    assert current.exists()


@pytest.mark.parametrize(
    "ack_override",
    [
        {"error_codes": ("DPONE_AIRFLOW_DAG_SPEC_INVALID",)},
        {"fatal": True},
    ],
)
def test_deployment_cache_retention_refuses_blocking_loader_ack_without_deleting(
    tmp_path: Path,
    ack_override: dict[str, object],
) -> None:
    from dataclasses import replace

    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    ack_reader = authorization["loader_ack_reader"]
    assert callable(ack_reader)
    invalid_ack = replace(ack_reader(), **ack_override)
    authorization["loader_ack_reader"] = lambda: invalid_ack

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID"
    assert stale.exists()
    assert current.exists()


def test_deployment_cache_retention_never_deletes_path_swapped_after_validation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import shutil

    from dpone.runtime import deployment_cache_retention_transaction as retention_transaction
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    validated_copy = stale.with_name(stale.name + ".validated")
    original_detach = retention_transaction.detach_validated_directory

    def swap_before_detach(path: Path, **kwargs):
        path.rename(validated_copy)
        shutil.copytree(validated_copy, path)
        (path / "replacement-marker").write_text("not reviewed\n", encoding="utf-8")
        return original_detach(path, **kwargs)

    monkeypatch.setattr(retention_transaction, "detach_validated_directory", swap_before_detach)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PATH_CHANGED"
    assert exc.value.details["deleted_deployment_ids"] == []
    assert validated_copy.exists()
    assert (stale / "replacement-marker").read_text(encoding="utf-8") == "not reviewed\n"
    assert current.exists()


def test_deployment_cache_retention_fsync_failure_never_reports_missing_quarantine(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.runtime.deployment_cache_retention_deletion as deletion
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError

    cache_root = tmp_path / ".dpone-cache"
    candidate = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)

    def fail_directory_fsync(path: Path) -> None:
        raise OSError(f"simulated fsync failure for {path}")

    monkeypatch.setattr(deletion, "fsync_directory", fail_directory_fsync)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        deletion.detach_validated_directory(
            candidate,
            expected_identity=deletion.directory_identity(candidate),
            trash_root=cache_root / ".retention-trash",
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PATH_CHANGED"
    assert exc.value.details["state_may_have_changed"] is True
    assert exc.value.details["restored"] is False
    assert exc.value.details["original_present"] is True
    assert exc.value.details["restore_durability_uncertain"] is True
    assert exc.value.details["quarantined_path"] is None
    assert candidate.exists()
    assert tuple((cache_root / ".retention-trash").iterdir()) == ()


def test_deployment_cache_retention_recovers_interrupted_detach_before_replay(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )
    from dpone.runtime.deployment_cache_retention_deletion import (
        detach_validated_directory,
        directory_identity,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    detach_validated_directory(
        stale,
        expected_identity=directory_identity(stale),
        trash_root=cache_root / ".retention-trash",
    )

    applier = build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",))
    with pytest.raises(DeploymentCacheRetentionApplyError) as recovery_exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )

    assert recovery_exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
    assert recovery_exc.value.details["restored_deployment_ids"] == [_deployment_id(stale)]
    assert stale.exists()
    with pytest.raises(DeploymentCacheRetentionApplyError) as replay_exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **authorization,
        )
    assert replay_exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED"


def test_deployment_cache_retention_journals_partial_recovery_before_failing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.runtime.deployment_cache_retention_deletion as deletion
    import dpone.runtime.deployment_cache_retention_transaction as transaction
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = [
        _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True),
        _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True),
    ]
    current = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    stale_ids = [_deployment_id(path) for path in stale]
    detached = [
        deletion.detach_validated_directory(
            path,
            expected_identity=deletion.directory_identity(path),
            trash_root=cache_root / ".retention-trash",
        )
        for path in stale
    ]
    original_restore = deletion.restore_detached_directory
    calls = 0

    def fail_second_restore(candidate: deletion.DetachedDeployment) -> bool:
        nonlocal calls
        calls += 1
        return original_restore(candidate) if calls == 1 else False

    monkeypatch.setattr(transaction, "restore_detached_directory", fail_second_restore)

    applier = build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",))
    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
        )

    ordered_ids = [
        deployment_id
        for _, deployment_id in sorted(
            zip((item.detached_path.name for item in detached), stale_ids, strict=True),
            key=lambda item: item[0],
        )
    ]
    first_id, second_id = ordered_ids
    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
    assert exc.value.details["state_may_have_changed"] is True
    assert exc.value.details["restored_deployment_ids"] == [first_id]
    assert exc.value.details["pending_deployment_ids"] == [second_id]
    quarantined_path = next(
        item.detached_path.as_posix()
        for item, deployment_id in zip(detached, stale_ids, strict=True)
        if deployment_id == second_id
    )
    assert exc.value.details["quarantined_path"] == quarantined_path
    journal = json.loads((cache_root / ".retention-recovery.json").read_text(encoding="utf-8"))
    assert journal["status"] == "blocked"
    assert journal["restored_deployment_ids"] == [first_id]
    assert journal["pending_deployment_ids"] == [second_id]
    restored_path = next(
        path for path, deployment_id in zip(stale, stale_ids, strict=True) if deployment_id == first_id
    )
    assert restored_path.exists()
    assert Path(quarantined_path).exists()

    monkeypatch.setattr(transaction, "restore_detached_directory", original_restore)
    with pytest.raises(DeploymentCacheRetentionApplyError) as replay_exc:
        applier.apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
        )

    assert replay_exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED"
    assert replay_exc.value.details["restored_deployment_ids"] == sorted([first_id, second_id])
    assert all(path.exists() for path in stale)
    recovered_journal = json.loads((cache_root / ".retention-recovery.json").read_text(encoding="utf-8"))
    assert recovered_journal["status"] == "recovered"
    assert recovered_journal["pending_deployment_ids"] == []


def test_deployment_cache_retention_restarts_after_durable_block_transition(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.runtime.deployment_cache_retention_transaction as transaction_module
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
    from dpone.runtime.deployment_cache_retention_deletion import (
        detach_validated_directory,
        directory_identity,
    )
    from dpone.runtime.deployment_cache_retention_transaction import (
        DeploymentCacheRetentionTransactionCoordinator,
    )

    class FatalCrash(BaseException):
        pass

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    stale_id = _deployment_id(stale)
    detached = detach_validated_directory(
        stale,
        expected_identity=directory_identity(stale),
        trash_root=cache_root / ".retention-trash",
    )
    coordinator = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    )
    coordinator._journal.commit(
        {
            "deployment_id": stale_id,
            "environment": "dev",
            "phase": "detached",
            "original_path": stale.as_posix(),
            "detached_path": detached.detached_path.as_posix(),
            "activation_path": (cache_root / "activations" / "dev" / stale_id.replace(":", "-", 1)).as_posix(),
            "expected_device": detached.identity.device,
            "expected_inode": detached.identity.inode,
            "operation_id": TEST_RETENTION_OPERATION_ID,
        }
    )
    original_block = coordinator._journal.block

    monkeypatch.setattr(transaction_module, "restore_detached_directory", lambda _candidate: False)

    def crash_after_block(transaction: dict[str, object]) -> bool:
        original_block(transaction)
        raise FatalCrash("simulated death after durable blocked transition")

    monkeypatch.setattr(coordinator._journal, "block", crash_after_block)
    with pytest.raises(FatalCrash):
        coordinator.recover(environment="dev")

    blocked = json.loads((cache_root / ".retention-recovery.json").read_text(encoding="utf-8"))
    assert blocked["status"] == "blocked"
    assert next(iter(blocked["transactions"].values()))["phase"] == "blocked"
    monkeypatch.undo()

    recovered = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    ).recover(environment="dev")

    assert tuple(item.deployment_id for item in recovered) == (stale_id,)
    assert stale.exists()
    state = json.loads((cache_root / ".retention-recovery.json").read_text(encoding="utf-8"))
    assert state["status"] == "recovered"
    assert state["pending_deployment_ids"] == []


def test_deployment_cache_retention_keeps_last_restore_pending_until_acknowledged(tmp_path: Path) -> None:
    from dpone.contracts.deployment_cache_retention_state import build_retention_recovery
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
    from dpone.runtime.deployment_cache_retention_transaction import (
        DeploymentCacheRetentionTransactionCoordinator,
    )

    cache_root = tmp_path / ".dpone-cache"
    restored_path = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    restored_id = _deployment_id(restored_path)
    trash_root = cache_root / ".retention-trash"
    trash_root.mkdir(parents=True)
    journal_path = cache_root / ".retention-recovery.json"
    transaction = _retention_transaction(restored_path, restored_id, phase="restored")
    journal_path.write_text(
        json.dumps(
            build_retention_recovery(
                status="recovered",
                restored_deployment_ids=[restored_id],
                pending_deployment_ids=[],
                transactions={restored_id: transaction},
            )
        ),
        encoding="utf-8",
    )

    recovered = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    ).recover(environment="dev")

    assert tuple(item.deployment_id for item in recovered) == (restored_id,)
    assert restored_path.exists()
    assert tuple(trash_root.iterdir()) == ()
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    assert journal["status"] == "recovered"
    assert journal["restored_deployment_ids"] == [restored_id]
    assert journal["pending_deployment_ids"] == []


def test_deployment_cache_retention_closes_recovery_when_pending_path_was_restored(tmp_path: Path) -> None:
    from dpone.contracts.deployment_cache_retention_state import build_retention_recovery
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
    from dpone.runtime.deployment_cache_retention_transaction import (
        DeploymentCacheRetentionTransactionCoordinator,
    )

    cache_root = tmp_path / ".dpone-cache"
    restored_path = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    restored_id = _deployment_id(restored_path)
    journal_path = cache_root / ".retention-recovery.json"
    transaction = _retention_transaction(restored_path, restored_id, phase="prepared")
    journal_path.write_text(
        json.dumps(
            build_retention_recovery(
                status="recovering",
                restored_deployment_ids=[],
                pending_deployment_ids=[restored_id],
                transactions={restored_id: transaction},
            )
        ),
        encoding="utf-8",
    )

    recovered = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    ).recover(environment="dev")

    assert tuple(item.deployment_id for item in recovered) == (restored_id,)
    journal = json.loads(journal_path.read_text(encoding="utf-8"))
    assert journal["status"] == "recovered"
    assert journal["restored_deployment_ids"] == [restored_id]
    assert journal["pending_deployment_ids"] == []


def test_retention_restores_never_activated_candidate_after_delete_kill_point(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
    from dpone.runtime.deployment_cache_retention_deletion import directory_identity
    from dpone.runtime.deployment_cache_retention_transaction import (
        DeploymentCacheRetentionTransactionCoordinator,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    stale_id = _deployment_id(stale)
    coordinator = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    )
    original_advance = coordinator._journal.advance

    def kill_after_tree_delete(transaction: dict[str, object], phase: str) -> None:
        if phase == "deployment_deleted":
            raise KeyboardInterrupt("simulated process kill")
        original_advance(transaction, phase)

    monkeypatch.setattr(coordinator._journal, "advance", kill_after_tree_delete)

    with pytest.raises(KeyboardInterrupt, match="simulated process kill"):
        coordinator.delete(
            stale,
            expected_identity=directory_identity(stale),
            deployment_id=stale_id,
            environment="dev",
            operation_id=TEST_RETENTION_OPERATION_ID,
        )

    assert not stale.exists()
    recovered = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    ).recover(environment="dev")

    assert tuple(item.deployment_id for item in recovered) == (stale_id,)
    assert stale.exists()


def test_retention_recovers_journaled_and_unjournaled_detaches_together(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
    from dpone.runtime.deployment_cache_retention_deletion import (
        detach_validated_directory,
        directory_identity,
    )
    from dpone.runtime.deployment_cache_retention_transaction import (
        DeploymentCacheRetentionTransactionCoordinator,
    )

    cache_root = tmp_path / ".dpone-cache"
    paths = [
        _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True),
        _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True),
    ]
    deployment_ids = [_deployment_id(path) for path in paths]
    detached = [
        detach_validated_directory(
            path,
            expected_identity=directory_identity(path),
            trash_root=cache_root / ".retention-trash",
        )
        for path in paths
    ]
    coordinator = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    )
    first = detached[0]
    coordinator._journal.commit(
        {
            "deployment_id": deployment_ids[0],
            "environment": "dev",
            "phase": "detached",
            "original_path": first.original_path.as_posix(),
            "detached_path": first.detached_path.as_posix(),
            "activation_path": (cache_root / "activations" / "dev" / deployment_ids[0].replace(":", "-", 1)).as_posix(),
            "expected_device": first.identity.device,
            "expected_inode": first.identity.inode,
        }
    )

    recovered = coordinator.recover(environment="dev")

    assert {item.deployment_id for item in recovered} == set(deployment_ids)
    assert all(path.exists() for path in paths)
    assert tuple((cache_root / ".retention-trash").iterdir()) == ()


def test_retention_forward_recovery_reports_success_after_transient_activation_cleanup_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.runtime.deployment_cache_retention_transaction as transaction_module
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
    from dpone.runtime.deployment_cache_retention_deletion import directory_identity
    from dpone.runtime.deployment_cache_retention_transaction import (
        DeploymentCacheRetentionTransactionCoordinator,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    stale_id = _deployment_id(stale)
    original_remove = transaction_module.remove_sealed_directory
    calls = 0

    def fail_once(path: Path) -> None:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise OSError("simulated activation cleanup failure")
        original_remove(path)

    monkeypatch.setattr(transaction_module, "remove_sealed_directory", fail_once)
    coordinator = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    )

    coordinator.delete(
        stale,
        expected_identity=directory_identity(stale),
        deployment_id=stale_id,
        environment="dev",
        operation_id=TEST_RETENTION_OPERATION_ID,
    )

    journal = json.loads((cache_root / ".retention-recovery.json").read_text(encoding="utf-8"))
    assert next(iter(journal["transactions"].values()))["phase"] == "committed"
    assert not stale.exists()
    assert calls == 2


def test_retention_forward_recovery_survives_one_terminal_journal_write_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
    from dpone.runtime.deployment_cache_retention_deletion import directory_identity
    from dpone.runtime.deployment_cache_retention_transaction import (
        DeploymentCacheRetentionTransactionCoordinator,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    stale_id = _deployment_id(stale)
    coordinator = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    )
    original_commit = coordinator._journal.commit
    failed = False

    def fail_terminal_once(transaction: dict[str, object]) -> None:
        nonlocal failed
        if transaction["phase"] == "committed" and not failed:
            failed = True
            raise OSError("simulated terminal journal write failure")
        original_commit(transaction)

    monkeypatch.setattr(coordinator._journal, "commit", fail_terminal_once)

    coordinator.delete(
        stale,
        expected_identity=directory_identity(stale),
        deployment_id=stale_id,
        environment="dev",
        operation_id=TEST_RETENTION_OPERATION_ID,
    )

    journal = json.loads((cache_root / ".retention-recovery.json").read_text(encoding="utf-8"))
    assert next(iter(journal["transactions"].values()))["phase"] == "committed"
    assert not stale.exists()


def test_retention_snapshot_failure_is_structured_and_leaves_candidate_intact(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache_common import DeploymentCacheError
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError
    from dpone.runtime.deployment_cache_retention_deletion import directory_identity
    from dpone.runtime.deployment_cache_retention_transaction import (
        DeploymentCacheRetentionTransactionCoordinator,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    stale_id = _deployment_id(stale)
    coordinator = DeploymentCacheRetentionTransactionCoordinator(
        cache_root,
        validator=DeploymentCacheProjectionValidator(cache_root),
    )

    def fail_snapshot(*_args: object, **_kwargs: object) -> object:
        raise DeploymentCacheError("DPONE_TEST_SNAPSHOT_FAILED", "snapshot failed")

    monkeypatch.setattr(coordinator._activation_snapshotter, "prepare", fail_snapshot)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        coordinator.delete(
            stale,
            expected_identity=directory_identity(stale),
            deployment_id=stale_id,
            environment="dev",
            operation_id=TEST_RETENTION_OPERATION_ID,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_ACTIVATION_SNAPSHOT_FAILED"
    assert exc.value.details["cause_code"] == "DPONE_TEST_SNAPSHOT_FAILED"
    assert exc.value.details["restored"] is True
    assert stale.exists()


def test_activation_restore_cleanup_failure_preserves_primary_structured_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import shutil

    import dpone.runtime.deployment_cache_activation_restore as restore_module
    from dpone.runtime.deployment_cache_activation import DeploymentCacheActivationSnapshotter
    from dpone.runtime.deployment_cache_activation_restore import DeploymentCacheActivationRestorer
    from dpone.runtime.deployment_cache_common import DeploymentCacheError
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator

    cache_root = tmp_path / ".dpone-cache"
    original = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    deployment_id = _deployment_id(original)
    validator = DeploymentCacheProjectionValidator(cache_root)
    activation = DeploymentCacheActivationSnapshotter(cache_root, validator=validator).prepare(
        original,
        environment="dev",
    )
    shutil.rmtree(original)

    def fail_copy(*_args: object, **_kwargs: object) -> None:
        raise DeploymentCacheError("DPONE_PRIMARY_RESTORE_FAILED", "primary restore failure")

    def fail_cleanup(_path: Path) -> None:
        raise OSError("cleanup failure")

    monkeypatch.setattr(restore_module, "copy_regular_activation_tree", fail_copy)
    monkeypatch.setattr(restore_module, "remove_sealed_activation", fail_cleanup)

    with pytest.raises(DeploymentCacheError) as exc:
        DeploymentCacheActivationRestorer(cache_root, validator=validator).restore(
            activation.path,
            original,
            deployment_id=deployment_id,
            environment="dev",
        )

    assert exc.value.code == "DPONE_PRIMARY_RESTORE_FAILED"
    assert exc.value.details["cleanup_failed_paths"]
    assert exc.value.details["recovery_required"] is True


def test_deployment_cache_retention_repeated_apply_replays_committed_receipt(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    stale_id = _deployment_id(stale)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization = _retention_authorization(cache_root)
    applier = build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",))

    first = applier.apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        **authorization,
    )
    second = applier.apply(
        environment="dev",
        confirm_delete=True,
        promoted_by="ci://retention",
        **authorization,
    )

    assert first.deleted_deployment_ids == (stale_id,)
    assert second.deleted_deployment_ids == (stale_id,)
    assert second.operation_id == first.operation_id
    assert second.receipt_revision == first.receipt_revision
    assert second.transaction_status == "committed"
    assert current.exists()


def test_deployment_cache_retention_refuses_split_current_state_before_delete(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    (cache_root / "current-pointer.json").unlink()

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev", confirm_delete=True, promoted_by="ci://retention"
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"
    assert stale.exists()
    assert current.exists()
    assert (cache_root / "current").exists()


def test_deployment_cache_retention_refuses_pointer_current_identity_mismatch(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    first = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    second = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)
    materializer = DeploymentCacheMaterializer(cache_root)
    first_result = materializer.promote(first, environment="dev")
    first_pointer = (cache_root / "current-pointer.json").read_bytes()
    materializer.promote(
        second,
        environment="dev",
        expected_current_deployment_id=first_result.deployment_id,
    )
    (cache_root / "current-pointer.json").write_bytes(first_pointer)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"
    assert stale.exists()
    assert first.exists()
    assert second.exists()
    assert (cache_root / "current").resolve().name == _deployment_id(second).replace(":", "-")


def test_deployment_cache_retention_refuses_corrupt_active_projection_before_delete(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache import (
        DeploymentCacheError,
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
        DeploymentCacheRetentionPlanner,
    )

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    active_deployment_path = cache_root / "current" / "deployment.json"
    active_deployment_path.chmod(0o600)
    active_deployment = json.loads(active_deployment_path.read_text(encoding="utf-8"))
    release_ref = str(active_deployment["release_ref"])
    active_deployment["release_ref"] = "sha256:" + release_ref.split(":", 1)[1].upper()
    active_deployment_path.write_text(json.dumps(active_deployment), encoding="utf-8")

    with pytest.raises(DeploymentCacheError) as plan_exc:
        DeploymentCacheRetentionPlanner(cache_root).plan(environment="dev")

    assert plan_exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"
    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",)).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED"
    assert stale.exists()
    assert current.exists()


def test_deployment_cache_retention_apply_rechecks_current_before_delete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import dpone.runtime.deployment_cache as deployment_cache
    from dpone.runtime.deployment_cache import (
        DeploymentCacheMaterializer,
        DeploymentCacheRetentionApplyError,
        DeploymentRetentionPlan,
        DeploymentRetentionPlanItem,
    )

    cache_root = tmp_path / ".dpone-cache"
    delete_candidate_id = "sha256:" + "a" * 64
    initial_current_id = "sha256:" + "b" * 64
    delete_candidate = _write_deployment(cache_root, delete_candidate_id, complete=True)
    initial_current = _write_deployment(cache_root, initial_current_id, complete=True)
    delete_candidate_id = _deployment_id(delete_candidate)
    initial_current_id = _deployment_id(initial_current)
    materializer = DeploymentCacheMaterializer(cache_root)
    materializer.promote(initial_current, environment="dev")
    authorization = _retention_authorization(cache_root)
    reviewed_ack_reader = authorization["loader_ack_reader"]

    def stale_plan(
        self: deployment_cache.DeploymentCacheRetentionPlanner,
        *,
        environment: str,
        protected_deployment_ids: tuple[str, ...] = (),
    ) -> DeploymentRetentionPlan:
        materializer.promote(delete_candidate, environment=environment)
        return DeploymentRetentionPlan(
            environment=environment,
            current_deployment_id=initial_current_id,
            protected_deployment_ids=protected_deployment_ids,
            items=(
                DeploymentRetentionPlanItem(
                    deployment_id=delete_candidate_id,
                    action="delete",
                    reason="unreferenced",
                    path=delete_candidate.as_posix(),
                ),
            ),
        )

    monkeypatch.setattr(deployment_cache.DeploymentCacheRetentionPlanner, "plan", stale_plan)
    reviewed_plan = DeploymentRetentionPlan(
        environment="dev",
        current_deployment_id=initial_current_id,
        protected_deployment_ids=(),
        items=(
            DeploymentRetentionPlanItem(
                deployment_id=delete_candidate_id,
                action="delete",
                reason="unreferenced",
                path=delete_candidate.as_posix(),
            ),
        ),
    )

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        build_deployment_cache_retention_applier(
            cache_root,
            allowed_promoters=("ci://retention",),
        ).apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            expected_plan_sha256=reviewed_plan.plan_sha256,
            loader_ack_reader=reviewed_ack_reader,
            checkpoint_reader=authorization["checkpoint_reader"],
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID"
    assert delete_candidate.exists()
    assert (
        json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))["deployment_id"]
        == delete_candidate_id
    )


def test_deployment_cache_recovery_plan_reports_missing_current_with_candidate(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)
    (cache_root / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "dev",
                "deployment_id": deployment_id,
                "release_id": "sha256:" + "a" * 64,
                "current_path": "current",
                "promoted_by": "ci://fixture",
                "promoted_at": "2026-07-13T12:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    report = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev")
    payload = report.to_dict()

    assert payload["schema"] == "dpone.deployment-cache-recovery-plan.v1"
    assert payload["status"] == "repairable"
    assert payload["issues"][0]["code"] == "DPONE_CURRENT_PATH_MISSING"
    assert payload["repair_candidates"][0]["deployment_id"] == deployment_id
    assert payload["preferred_repair_deployment_id"] == deployment_id


def test_deployment_cache_recovery_plan_reports_current_path_escape(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)
    outside_current = tmp_path / "outside-current"
    outside_current.mkdir()
    (outside_current / "deployment.json").write_text(
        json.dumps({"schema": "dpone.deployment-set.v1", "deployment_id": deployment_id}),
        encoding="utf-8",
    )
    (cache_root / "current").symlink_to(outside_current, target_is_directory=True)
    (cache_root / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "dev",
                "deployment_id": deployment_id,
                "release_id": "sha256:" + "a" * 64,
                "current_path": "current",
                "promoted_by": "ci://fixture",
                "promoted_at": "2026-07-13T12:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    payload = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").to_dict()

    assert payload["status"] == "blocked"
    assert payload["preferred_repair_deployment_id"] == deployment_id
    issues = {issue["code"]: issue for issue in payload["issues"]}
    assert issues["DPONE_CURRENT_PATH_UNSAFE"]["path"] == str(cache_root / "current")
    assert issues["DPONE_CURRENT_PATH_ID_UNAVAILABLE"]["path"] == str(cache_root / "current")


def test_deployment_cache_recovery_plan_reports_incomplete_deployment_blocker(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    incomplete_id = "sha256:" + "c" * 64
    incomplete_path = _write_deployment(cache_root, incomplete_id, complete=False)

    payload = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").to_dict()

    assert payload["status"] == "blocked"
    assert payload["preferred_repair_deployment_id"] is None
    assert payload["repair_candidates"] == []
    issues = {issue["code"]: issue for issue in payload["issues"]}
    assert issues["DPONE_DEPLOYMENT_INCOMPLETE"]["path"] == str(incomplete_path / "_SUCCESS")


def test_deployment_cache_recovery_plan_reports_invalid_deployment_blocker(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    broken_path = cache_root / "deployments" / "dev" / ("sha256-" + "d" * 64)
    broken_path.mkdir(parents=True)
    (broken_path / "deployment.json").write_text("{", encoding="utf-8")
    (broken_path / "_SUCCESS").write_text("ok\n", encoding="utf-8")

    payload = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").to_dict()

    assert payload["status"] == "blocked"
    assert payload["preferred_repair_deployment_id"] is None
    assert payload["repair_candidates"] == []
    issues = {issue["code"]: issue for issue in payload["issues"]}
    assert issues["DPONE_DEPLOYMENT_INVALID"]["path"] == str(broken_path / "deployment.json")


def test_deployment_cache_recovery_apply_requires_explicit_confirmation(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_recovery import (
        DeploymentCacheRecoveryApplier,
        DeploymentCacheRecoveryApplyError,
    )

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)

    with pytest.raises(DeploymentCacheRecoveryApplyError) as exc:
        DeploymentCacheRecoveryApplier(cache_root).apply(
            environment="dev",
            deployment_id=deployment_id,
            confirm_repair=False,
            promoted_by="ci://recovery",
            expected_current_deployment_id=None,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_RECOVERY_CONFIRMATION_REQUIRED"
    assert not (cache_root / "current").exists()


def test_deployment_cache_recovery_apply_promotes_selected_complete_deployment(tmp_path: Path) -> None:
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryApplier

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)

    report = DeploymentCacheRecoveryApplier(cache_root).apply(
        environment="dev",
        deployment_id=deployment_id,
        confirm_repair=True,
        promoted_by="ci://recovery",
        expected_current_deployment_id=None,
    )

    payload = report.to_dict()
    assert payload["schema"] == "dpone.deployment-cache-recovery-apply.v1"
    assert payload["recovered_deployment_id"] == deployment_id
    assert (cache_root / "current" / "airflow-index.json").exists()
    pointer = json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))
    assert pointer["deployment_id"] == deployment_id


def test_airflow_cache_retention_plan_cli_reports_without_deleting(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale_id = "sha256:" + "a" * 64
    current_id = "sha256:" + "b" * 64
    stale = _write_deployment(cache_root, stale_id, complete=True)
    current = _write_deployment(cache_root, current_id, complete=True)
    stale_id = _deployment_id(stale)
    current_id = _deployment_id(current)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-plan",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is True
    assert payload["schema"] == "dpone.deployment-cache-retention-plan.v1"
    assert payload["current_deployment_id"] == current_id
    assert payload["delete_candidates"] == [stale_id]
    assert stale.exists()


def test_airflow_cache_retention_plan_cli_protects_explicit_evidence_refs(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    protected_id = "sha256:" + "d" * 64
    protected = _write_deployment(cache_root, protected_id, complete=True)
    protected_id = _deployment_id(protected)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-plan",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--protect-deployment-id",
            protected_id,
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["delete_candidates"] == []
    assert payload["items"][0]["action"] == "protect"
    assert payload["items"][0]["reason"] == "retention_evidence"


def test_airflow_cache_retention_apply_cli_deletes_only_after_confirmation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale_id = "sha256:" + "a" * 64
    current_id = "sha256:" + "b" * 64
    stale = _write_deployment(cache_root, stale_id, complete=True)
    current = _write_deployment(cache_root, current_id, complete=True)
    stale_id = _deployment_id(stale)
    current_id = _deployment_id(current)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://retention",
            "--allowed-promoter",
            "ci://retention",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    rejected = json.loads(stdout)
    assert rejected["passed"] is False
    assert rejected["errors"][0]["code"] == "DPONE_DEPLOYMENT_CACHE_GC_CONFIRMATION_REQUIRED"
    assert stale.exists()

    authorization_args = _retention_cli_authorization(cache_root, tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://retention",
            "--allowed-promoter",
            "ci://retention",
            "--confirm-delete",
            *authorization_args,
            "--evidence-version",
            "v3",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is True
    assert payload["schema"] == "dpone.deployment-cache-retention-apply.v3"
    assert payload["deleted_deployment_ids"] == [stale_id]
    assert not stale.exists()
    assert current.exists()

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://retention",
            "--allowed-promoter",
            "ci://retention",
            "--confirm-delete",
            "--format",
            "json",
        ],
        capsys,
    )
    assert code == 0, stderr
    compatibility_payload = json.loads(stdout)
    assert compatibility_payload["schema"] == "dpone.deployment-cache-retention-apply.v1"
    assert "activation_history_revision" not in compatibility_payload


def test_airflow_cache_retention_plan_text_is_actionable_without_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale_id = "sha256:" + "a" * 64
    current_id = "sha256:" + "b" * 64
    stale = _write_deployment(cache_root, stale_id, complete=True)
    current = _write_deployment(cache_root, current_id, complete=True)
    stale_id = _deployment_id(stale)
    current_id = _deployment_id(current)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-plan",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone airflow cache retention: NEEDS_CLEANUP" in stdout
    assert "- environment: dev" in stdout
    assert f"- current deployment: {current_id}" in stdout
    assert "- delete candidates: 1" in stdout
    assert f"- delete candidate: {stale_id} (unreferenced)" in stdout
    assert f"- protected: {current_id} (current)" in stdout
    assert (
        '- action: dpone airflow cache-retention-apply --cache-root "${DPONE_CACHE_ROOT:?set DPONE_CACHE_ROOT}" '
        "--environment dev" in stdout
    )
    assert "--expected-plan-sha256 sha256:" in stdout
    assert '--loader-ack-file "${DPONE_LOADER_ACK_FILE:?set DPONE_LOADER_ACK_FILE}"' in stdout
    assert '--promoted-by "${DPONE_RETENTION_ACTOR:?set DPONE_RETENTION_ACTOR}"' in stdout
    assert '--allowed-promoter "${DPONE_RETENTION_ACTOR:?set DPONE_RETENTION_ACTOR}" --confirm-delete' in stdout
    assert "- details: rerun with --format json for the full cache-retention plan" in stdout
    assert str(cache_root) not in stdout
    assert str(stale) not in stdout
    assert str(current) not in stdout
    assert "dpone self-service: OK" not in stdout


def test_airflow_cache_retention_text_reports_unidentified_quarantine(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    invalid = cache_root / "deployments" / "dev" / "not-a-digest"
    invalid.mkdir(parents=True)

    plan_code, plan_stdout, plan_stderr = _run_cli(
        [
            "airflow",
            "cache-retention-plan",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
        ],
        capsys,
    )
    apply_code, apply_stdout, apply_stderr = _run_cli(
        [
            "airflow",
            "cache-retention-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://retention",
            "--allowed-promoter",
            "ci://retention",
            "--confirm-delete",
        ],
        capsys,
    )

    assert plan_code == 0, plan_stderr
    assert "dpone airflow cache retention: NEEDS_ATTENTION" in plan_stdout
    assert "- quarantined entries: 1" in plan_stdout
    assert "- quarantine: unidentified cache entry (incomplete; DPONE_DEPLOYMENT_INCOMPLETE)" in plan_stdout
    assert "- action: inspect quarantine diagnostics before running cache-retention-apply" in plan_stdout
    assert apply_code == 0, apply_stderr
    assert "dpone airflow cache retention apply: NEEDS_ATTENTION" in apply_stdout
    assert "- skipped deployments: 1" in apply_stdout
    assert "- quarantined entries: 1" in apply_stdout
    assert "- skipped: unidentified cache entry (incomplete; DPONE_DEPLOYMENT_INCOMPLETE)" in apply_stdout
    assert str(cache_root) not in plan_stdout
    assert str(cache_root) not in apply_stdout


def test_airflow_cache_retention_plan_action_preserves_effective_protection_ids(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    delete_candidate = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    protected = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    protected_id = _deployment_id(protected)
    evidence = tmp_path / "run-evidence.json"
    evidence.write_text(json.dumps({"deployment_id": protected_id}), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-plan",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--evidence-file",
            str(evidence),
        ],
        capsys,
    )

    assert code == 0, stderr
    assert f"--protect-deployment-id {protected_id}" in stdout
    assert "--evidence-file" not in stdout
    assert f"- protected: {protected_id} (retention_evidence)" in stdout
    assert f"- delete candidate: {_deployment_id(delete_candidate)} (unreferenced)" in stdout


@pytest.mark.parametrize("command", ["cache-retention-plan", "cache-retention-apply"])
@pytest.mark.parametrize("protection_id", ["not-a-digest", "sha256:" + "A" * 64])
def test_airflow_cache_retention_rejects_malformed_explicit_protection_id(
    tmp_path: Path,
    command: str,
    protection_id: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    arguments = [
        "airflow",
        command,
        "--cache-root",
        str(cache_root),
        "--environment",
        "dev",
        "--protect-deployment-id",
        protection_id,
    ]
    if command == "cache-retention-apply":
        arguments.extend(
            [
                "--promoted-by",
                "ci://retention",
                "--allowed-promoter",
                "ci://retention",
                "--confirm-delete",
            ]
        )
    arguments.extend(["--format", "json"])

    code, stdout, stderr = _run_cli(arguments, capsys)

    assert code == 2, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_ID_INVALID"
    assert deployment.exists()


def test_airflow_cache_retention_restores_intact_tree_after_transient_delete_failure(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.runtime import deployment_cache_retention_transaction as retention_transaction

    cache_root = tmp_path / ".dpone-cache"
    deployments = [
        _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True),
        _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True),
    ]
    current = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    deployments.sort(key=_deployment_id)
    deployment_ids = [_deployment_id(path) for path in deployments]
    original_remove = retention_transaction.remove_path
    failed_dir_prefix = deployment_ids[1].replace(":", "-", 1) + "."

    def fail_second_delete(path: Path) -> None:
        if path.name.startswith(failed_dir_prefix):
            raise OSError("simulated delete failure")
        original_remove(path)

    monkeypatch.setattr(retention_transaction, "remove_path", fail_second_delete)
    authorization_args = _retention_cli_authorization(cache_root, tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://retention",
            "--allowed-promoter",
            "ci://retention",
            "--confirm-delete",
            *authorization_args,
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_CACHE_GC_DELETE_FAILED"
    assert payload["failed_step"] == "deletion_started"
    assert payload["state_may_have_changed"] is True
    assert payload["deleted_deployment_ids"] == [deployment_ids[0]]
    assert payload["deleted_count"] == 1
    assert payload["failed_paths"] == ["$ABSOLUTE_PATH"]
    assert payload["retention_incomplete"] is True
    assert not deployments[0].exists()
    assert deployments[1].exists()
    assert payload["restored"] is True
    assert payload["restored_deployment_ids"] == [deployment_ids[1]]
    assert payload["quarantined_path"] is None
    assert payload["quarantined_paths"] == []


def test_retention_partial_rmtree_restores_only_from_intact_activation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime import deployment_cache_retention_transaction as transaction
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentCacheRetentionApplyError
    from dpone.runtime.deployment_cache_retention_deletion import directory_identity

    cache_root = tmp_path / ".dpone-cache"
    stale = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    materializer = DeploymentCacheMaterializer(cache_root)
    stale_activation = materializer.promote(stale, environment="dev")
    materializer.promote(current, environment="dev", expected_current_deployment_id=stale_activation.deployment_id)
    original_remove = transaction.remove_path
    first_call = True

    def partial_then_fail(path: Path) -> None:
        nonlocal first_call
        if first_call:
            first_call = False
            path.chmod(0o755)
            (path / "_SUCCESS").unlink()
            raise OSError("simulated partial recursive deletion")
        original_remove(path)

    monkeypatch.setattr(transaction, "remove_path", partial_then_fail)
    validator = DeploymentCacheProjectionValidator(cache_root)
    coordinator = transaction.DeploymentCacheRetentionTransactionCoordinator(cache_root, validator=validator)

    with pytest.raises(DeploymentCacheRetentionApplyError) as exc:
        coordinator.delete(
            stale,
            expected_identity=directory_identity(stale),
            deployment_id=stale_activation.deployment_id,
            environment="dev",
            operation_id=TEST_RETENTION_OPERATION_ID,
        )

    assert exc.value.code == "DPONE_DEPLOYMENT_CACHE_GC_DELETE_FAILED"
    assert exc.value.details["restored"] is True, exc.value.details
    assert validator.validate_details(stale, environment="dev").deployment_id == stale_activation.deployment_id
    activation_path = cache_root / "activations" / "dev" / stale_activation.deployment_id.replace(":", "-", 1)
    assert (
        validator.validate_activation_details(
            activation_path,
            environment="dev",
        ).deployment_id
        == stale_activation.deployment_id
    )


def test_activation_restore_replay_reseals_after_rename_kill_point(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer
    from dpone.runtime.deployment_cache_activation_restore import DeploymentCacheActivationRestorer
    from dpone.runtime.deployment_cache_common import remove_path
    from dpone.runtime.deployment_cache_projection_validator import DeploymentCacheProjectionValidator

    cache_root = tmp_path / ".dpone-cache"
    original = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    current = DeploymentCacheMaterializer(cache_root).promote(original, environment="dev")
    activation = cache_root / "activations" / "dev" / current.deployment_id.replace(":", "-", 1)
    remove_path(original)
    validator = DeploymentCacheProjectionValidator(cache_root)
    restorer = DeploymentCacheActivationRestorer(cache_root, validator=validator)
    original_seal = restorer._seal_root
    interrupted = False

    def fail_once_after_rename(path: Path) -> None:
        nonlocal interrupted
        if path == original and not interrupted:
            interrupted = True
            raise OSError("simulated kill after restore rename")
        original_seal(path)

    monkeypatch.setattr(restorer, "_seal_root", fail_once_after_rename)

    with pytest.raises(OSError, match="simulated kill"):
        restorer.restore(
            activation,
            original,
            deployment_id=current.deployment_id,
            environment="dev",
        )

    assert stat.S_IMODE(original.lstat().st_mode) == 0o755
    restorer.restore(
        activation,
        original,
        deployment_id=current.deployment_id,
        environment="dev",
    )
    assert stat.S_IMODE(original.lstat().st_mode) == 0o555
    assert validator.validate_details(original, environment="dev").deployment_id == current.deployment_id
    assert validator.validate_activation_details(activation, environment="dev").deployment_id == current.deployment_id
    retention_trash = cache_root / ".retention-trash"
    assert not retention_trash.exists() or tuple(retention_trash.iterdir()) == ()
    assert not (original.parent / f".{original.name}.restore").exists()


def test_airflow_cache_retention_prevalidates_all_candidates_before_deletion(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.runtime.deployment_cache_retention_candidate import DeploymentCacheRetentionCandidateValidator
    from dpone.runtime.deployment_cache_retention_contracts import DeploymentRetentionPlanItem

    cache_root = tmp_path / ".dpone-cache"
    deployments = [
        _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True),
        _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True),
    ]
    current = _write_deployment(cache_root, "sha256:" + "c" * 64, complete=True)
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    deployments.sort(key=_deployment_id)
    second_id = _deployment_id(deployments[1])
    original_validate = DeploymentCacheRetentionCandidateValidator.validate

    def fail_second_validation(
        self: DeploymentCacheRetentionCandidateValidator,
        item: DeploymentRetentionPlanItem,
        *,
        environment: str,
    ) -> tuple[Path, object]:
        if item.deployment_id == second_id:
            (Path(item.path) / "airflow-index.json").write_text("{", encoding="utf-8")
        return original_validate(self, item, environment=environment)

    monkeypatch.setattr(DeploymentCacheRetentionCandidateValidator, "validate", fail_second_validation)
    authorization_args = _retention_cli_authorization(cache_root, tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://retention",
            "--allowed-promoter",
            "ci://retention",
            "--confirm-delete",
            *authorization_args,
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_CACHE_GC_VALIDATION_FAILED"
    assert payload["failed_step"] == "validate_candidates"
    assert payload["state_may_have_changed"] is False
    assert payload["deleted_deployment_ids"] == []
    assert payload["failed_deployment_id"] == second_id
    assert payload["cause_code"] == "DPONE_AIRFLOW_INDEX_INVALID"
    assert all(path.exists() for path in deployments)
    assert not (cache_root / ".retention-activation-history.v2.json").exists()


def test_airflow_cache_retention_apply_text_summarizes_deleted_and_skipped_deployments(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    stale_id = "sha256:" + "a" * 64
    current_id = "sha256:" + "b" * 64
    stale = _write_deployment(cache_root, stale_id, complete=True)
    current = _write_deployment(cache_root, current_id, complete=True)
    stale_id = _deployment_id(stale)
    current_id = _deployment_id(current)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    authorization_args = _retention_cli_authorization(cache_root, tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://retention",
            "--allowed-promoter",
            "ci://retention",
            "--confirm-delete",
            *authorization_args,
        ],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone airflow cache retention apply: OK" in stdout
    assert "- environment: dev" in stdout
    assert f"- current deployment: {current_id}" in stdout
    assert "- deleted deployments: 1" in stdout
    assert f"- deleted: {stale_id} (unreferenced)" in stdout
    assert "- skipped deployments: 1" in stdout
    assert f"- skipped: {current_id} (current)" in stdout
    assert "- action: run dpone airflow cache-retention-plan to verify cache health" in stdout
    assert "- details: rerun with --format json for deleted/skipped deployment paths" in stdout
    assert str(stale) not in stdout
    assert str(current) not in stdout
    assert "dpone self-service: OK" not in stdout
    assert not stale.exists()
    assert current.exists()


def test_airflow_cache_sync_cli_promotes_complete_deployment_only_after_confirmation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-sync",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--deployment-dir",
            str(deployment),
            "--promoted-by",
            "ci://github-actions/dpone-airflow",
            "--allowed-promoter",
            "ci://github-actions/dpone-airflow",
            "--expect-current-absent",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    rejected = json.loads(stdout)
    assert rejected["passed"] is False
    assert rejected["errors"][0]["code"] == "DPONE_DEPLOYMENT_CACHE_SYNC_CONFIRMATION_REQUIRED"
    assert not (cache_root / "current").exists()

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-sync",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--deployment-dir",
            str(deployment),
            "--promoted-by",
            "ci://github-actions/dpone-airflow",
            "--allowed-promoter",
            "ci://github-actions/dpone-airflow",
            "--expect-current-absent",
            "--source-commit",
            "7ac31f2",
            "--confirm-promote",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is True
    assert payload["schema"] == "dpone.current-pointer.v1"
    assert payload["deployment_id"] == deployment_id
    assert payload["environment"] == "dev"
    assert payload["promoted_by"] == "ci://github-actions/dpone-airflow"
    assert payload["source_commit"] == "7ac31f2"
    assert payload["current_path"] == "$ABSOLUTE_PATH"
    assert (cache_root / "current" / "airflow-index.json").exists()


def test_airflow_cache_sync_cli_requires_explicit_platform_allowlist(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-sync",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--deployment-dir",
            str(deployment),
            "--promoted-by",
            "ci://self-asserted",
            "--expect-current-absent",
            "--confirm-promote",
        ],
        capsys,
    )

    assert code == 2
    assert stdout == ""
    assert "--allowed-promoter" in stderr
    assert not (cache_root / "current").exists()


@pytest.mark.parametrize(
    "arguments",
    [
        [
            "airflow",
            "cache-recovery-apply",
            "--deployment-id",
            "sha256:" + "a" * 64,
            "--promoted-by",
            "ci://recovery",
            "--expect-current-absent",
            "--confirm-repair",
        ],
        [
            "airflow",
            "cache-retention-apply",
            "--promoted-by",
            "ci://retention",
            "--confirm-delete",
        ],
    ],
)
def test_recovery_and_retention_cli_require_explicit_platform_allowlist(
    arguments: list[str],
    capsys: pytest.CaptureFixture[str],
) -> None:
    code, stdout, stderr = _run_cli(arguments, capsys)

    assert code == 2
    assert stdout == ""
    assert "--allowed-promoter" in stderr


@pytest.mark.parametrize("command", ["cache-recovery-apply", "cache-retention-apply"])
def test_recovery_and_retention_cli_reject_actor_outside_allowlist(
    tmp_path: Path,
    command: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    deployment_id = _deployment_id(deployment)
    if command == "cache-recovery-apply":
        command_arguments = [
            "--deployment-id",
            deployment_id,
            "--promoted-by",
            "ci://intruder",
            "--allowed-promoter",
            "ci://recovery",
            "--expect-current-absent",
            "--confirm-repair",
        ]
    else:
        command_arguments = [
            "--promoted-by",
            "ci://intruder",
            "--allowed-promoter",
            "ci://retention",
            "--confirm-delete",
        ]

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            command,
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            *command_arguments,
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED"
    assert deployment.exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_sync_text_summarizes_current_pointer_promotion(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    cache_root = tmp_path / ".dpone-cache"
    previous_id = "sha256:" + "a" * 64
    deployment_id = "sha256:" + "b" * 64
    previous = _write_deployment(cache_root, previous_id, complete=True)
    deployment = _write_deployment(
        cache_root,
        deployment_id,
        release_id="sha256:" + "c" * 64,
        complete=True,
    )
    previous_id = _deployment_id(previous)
    deployment_id = _deployment_id(deployment)
    release_id = json.loads((deployment / "deployment.json").read_text(encoding="utf-8"))["release_ref"]
    DeploymentCacheMaterializer(cache_root).promote(previous, environment="dev")

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-sync",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--deployment-dir",
            str(deployment),
            "--promoted-by",
            "ci://github-actions/dpone-airflow",
            "--allowed-promoter",
            "ci://github-actions/dpone-airflow",
            "--expected-current-deployment-id",
            previous_id,
            "--source-commit",
            "7ac31f2",
            "--attestation-ref",
            "attestation://dpone/cache-sync/1",
            "--confirm-promote",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone airflow cache sync: OK" in stdout
    assert "- environment: dev" in stdout
    assert f"- promoted deployment: {deployment_id}" in stdout
    assert f"- previous deployment: {previous_id}" in stdout
    assert f"- release: {release_id}" in stdout
    assert "- promoted by: ci://github-actions/dpone-airflow" in stdout
    assert "- source commit: 7ac31f2" in stdout
    assert "- attestation: attestation://dpone/cache-sync/1" in stdout
    assert "- current pointer: updated" in stdout
    assert "- action: run dpone airflow cache-recovery-plan to verify cache health" in stdout
    assert "- details: rerun with --format json for current/pointer paths and audit metadata" in stdout
    assert str(cache_root / "current") not in stdout
    assert str(cache_root / "current-pointer.json") not in stdout
    assert "dpone self-service: OK" not in stdout
    assert (cache_root / "current" / "airflow-index.json").exists()


def test_airflow_cache_sync_cli_rejects_promoter_outside_allowlist(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-sync",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--deployment-dir",
            str(deployment),
            "--promoted-by",
            "local://unknown-publisher",
            "--allowed-promoter",
            "ci://github-actions/dpone-airflow",
            "--expect-current-absent",
            "--confirm-promote",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED"
    assert payload["errors"][0]["stage"] == "cache_sync"
    assert payload["errors"][0]["path"] == "$ABSOLUTE_PATH"
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_sync_result_reports_corrupt_deployment_json_without_traceback(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    (deployment / "deployment.json").write_text("{", encoding="utf-8")

    try:
        result = cache_sync_result(
            cache_root=cache_root,
            deployment_dir=deployment,
            environment="dev",
            promoted_by="ci://github-actions/dpone-airflow",
            allowed_promoters=("ci://github-actions/dpone-airflow",),
            confirm_promote=True,
        )
    except Exception as exc:  # noqa: BLE001 - this test proves cache sync does not leak raw tracebacks.
        pytest.fail(f"cache sync should return a structured error instead of raising {exc!r}")

    payload = result.to_dict()
    assert result.exit_code == 4
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_INVALID"
    assert payload["errors"][0]["stage"] == "cache_sync"
    assert payload["errors"][0]["path"] == str(deployment / "deployment.json")
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_sync_result_reports_non_object_deployment_json_with_file_path(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    (deployment / "deployment.json").write_text("[]", encoding="utf-8")

    result = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="ci://github-actions/dpone-airflow",
        allowed_promoters=("ci://github-actions/dpone-airflow",),
        confirm_promote=True,
    )

    payload = result.to_dict()
    assert result.exit_code == 4
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_INVALID"
    assert payload["errors"][0]["stage"] == "cache_sync"
    assert payload["errors"][0]["path"] == str(deployment / "deployment.json")
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_sync_result_reports_deployment_schema_error_with_file_path(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    payload = json.loads((deployment / "deployment.json").read_text(encoding="utf-8"))
    payload["schema"] = "wrong.schema"
    (deployment / "deployment.json").write_text(json.dumps(payload), encoding="utf-8")

    result = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="ci://github-actions/dpone-airflow",
        allowed_promoters=("ci://github-actions/dpone-airflow",),
        confirm_promote=True,
    )

    output = result.to_dict()
    assert result.exit_code == 4
    assert output["passed"] is False
    assert output["errors"][0]["code"] == "DPONE_DEPLOYMENT_SCHEMA_INVALID"
    assert output["errors"][0]["stage"] == "cache_sync"
    assert output["errors"][0]["path"] == str(deployment / "deployment.json")
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_sync_result_reports_corrupt_airflow_index_without_traceback(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    (deployment / "airflow-index.json").write_text("{", encoding="utf-8")

    try:
        result = cache_sync_result(
            cache_root=cache_root,
            deployment_dir=deployment,
            environment="dev",
            promoted_by="ci://github-actions/dpone-airflow",
            allowed_promoters=("ci://github-actions/dpone-airflow",),
            confirm_promote=True,
        )
    except Exception as exc:  # noqa: BLE001 - this test proves cache sync does not leak raw tracebacks.
        pytest.fail(f"cache sync should return a structured error instead of raising {exc!r}")

    payload = result.to_dict()
    assert result.exit_code == 4
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_AIRFLOW_INDEX_INVALID"
    assert payload["errors"][0]["stage"] == "cache_sync"
    assert payload["errors"][0]["path"] == str(deployment / "airflow-index.json")
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_sync_result_reports_airflow_index_schema_error_with_file_path(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache_sync import cache_sync_result

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    payload = json.loads((deployment / "airflow-index.json").read_text(encoding="utf-8"))
    payload["schema"] = "wrong.schema"
    (deployment / "airflow-index.json").write_text(json.dumps(payload), encoding="utf-8")

    result = cache_sync_result(
        cache_root=cache_root,
        deployment_dir=deployment,
        environment="dev",
        promoted_by="ci://github-actions/dpone-airflow",
        allowed_promoters=("ci://github-actions/dpone-airflow",),
        confirm_promote=True,
    )

    output = result.to_dict()
    assert result.exit_code == 4
    assert output["passed"] is False
    assert output["errors"][0]["code"] == "DPONE_AIRFLOW_INDEX_SCHEMA_INVALID"
    assert output["errors"][0]["stage"] == "cache_sync"
    assert output["errors"][0]["path"] == str(deployment / "airflow-index.json")
    assert not (cache_root / "current-pointer.json").exists()
    assert not (cache_root / "current").exists()


def test_airflow_cache_recovery_cli_plans_and_repairs_only_after_confirmation(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)
    (cache_root / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "dev",
                "deployment_id": deployment_id,
                "release_id": "sha256:" + "a" * 64,
                "current_path": "current",
                "promoted_by": "ci://fixture",
                "promoted_at": "2026-07-13T12:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-recovery-plan",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    plan = json.loads(stdout)
    assert plan["passed"] is True
    assert plan["status"] == "repairable"
    assert plan["preferred_repair_deployment_id"] == deployment_id

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-recovery-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--deployment-id",
            deployment_id,
            "--promoted-by",
            "ci://recovery",
            "--allowed-promoter",
            "ci://recovery",
            "--expect-current-absent",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 4, stderr
    rejected = json.loads(stdout)
    assert rejected["errors"][0]["code"] == "DPONE_DEPLOYMENT_CACHE_RECOVERY_CONFIRMATION_REQUIRED"

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-recovery-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--deployment-id",
            deployment_id,
            "--promoted-by",
            "ci://recovery",
            "--allowed-promoter",
            "ci://recovery",
            "--expect-current-absent",
            "--confirm-repair",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is True
    assert payload["recovered_deployment_id"] == deployment_id
    assert (cache_root / "current" / "airflow-index.json").exists()


def test_airflow_cache_recovery_plan_text_is_actionable_without_json(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)
    (cache_root / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "dev",
                "deployment_id": deployment_id,
                "release_id": "sha256:" + "a" * 64,
                "current_path": "current",
                "promoted_by": "ci://fixture",
                "promoted_at": "2026-07-13T12:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-recovery-plan",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone airflow cache recovery: REPAIRABLE" in stdout
    assert "- environment: dev" in stdout
    assert f"- pointer deployment: {deployment_id}" in stdout
    assert "- active deployment: absent" in stdout
    assert f"- preferred repair: {deployment_id}" in stdout
    assert "- issue: DPONE_CURRENT_PATH_MISSING: current deployment path is missing" in stdout
    assert f"- repair candidate: {deployment_id} (current_state)" in stdout
    assert (
        '- action: dpone airflow cache-recovery-apply --cache-root "${DPONE_CACHE_ROOT:?set DPONE_CACHE_ROOT}" '
        "--environment dev "
        f"--deployment-id {deployment_id} "
        '--promoted-by "${DPONE_RECOVERY_ACTOR:?set DPONE_RECOVERY_ACTOR}" '
        '--allowed-promoter "${DPONE_RECOVERY_ACTOR:?set DPONE_RECOVERY_ACTOR}" '
        "--expect-current-absent --confirm-repair"
    ) in stdout
    assert "- details: rerun with --format json for the full cache-recovery plan" in stdout
    assert str(cache_root) not in stdout
    assert "dpone self-service: OK" not in stdout


def test_airflow_cache_recovery_apply_text_summarizes_repaired_current_pointer(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(
        cache_root,
        deployment_id,
        release_id="sha256:" + "a" * 64,
        complete=True,
    )
    deployment_id = _deployment_id(deployment)
    release_id = json.loads((deployment / "deployment.json").read_text(encoding="utf-8"))["release_ref"]
    (cache_root / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "dev",
                "deployment_id": deployment_id,
                "release_id": release_id,
                "current_path": "current",
                "promoted_by": "ci://fixture",
                "promoted_at": "2026-07-13T12:00:00+00:00",
            }
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-recovery-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--deployment-id",
            deployment_id,
            "--promoted-by",
            "ci://recovery",
            "--allowed-promoter",
            "ci://recovery",
            "--expect-current-absent",
            "--confirm-repair",
        ],
        capsys,
    )

    assert code == 0, stderr
    assert "dpone airflow cache recovery apply: OK" in stdout
    assert "- environment: dev" in stdout
    assert f"- recovered deployment: {deployment_id}" in stdout
    assert f"- release: {release_id}" in stdout
    assert "- current pointer: repaired" in stdout
    assert "- action: run dpone airflow cache-recovery-plan to verify cache health" in stdout
    assert "- details: rerun with --format json for current/pointer paths" in stdout
    assert str(cache_root / "current") not in stdout
    assert str(cache_root / "current-pointer.json") not in stdout
    assert "dpone self-service: OK" not in stdout
    assert (cache_root / "current" / "airflow-index.json").exists()


def test_airflow_cache_recovery_apply_partial_write_failure_is_truthful() -> None:
    from dpone.commands.airflow_cache_recovery_rendering import self_service_cache_recovery_apply_text

    output = self_service_cache_recovery_apply_text(
        {
            "passed": False,
            "environment": "prod",
            "errors": [
                {
                    "code": "DPONE_CACHE_PROMOTION_WRITE_FAILED",
                    "message": "promotion audit event could not be written",
                }
            ],
        }
    )

    assert "- environment: prod" in output
    assert "- issue: DPONE_CACHE_PROMOTION_WRITE_FAILED: promotion audit event could not be written" in output
    assert "- current pointer: recovery required" in output
    assert "- action: run dpone airflow cache-recovery-plan before retrying recovery apply" in output
    assert "current pointer: not changed" not in output


def test_airflow_cache_recovery_apply_cli_reports_partial_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    from dpone.runtime import deployment_cache_commit

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True, environment="prod")
    deployment_id = _deployment_id(deployment)

    def fail_pointer_write(path: Path, payload: dict[str, object]) -> None:
        del path, payload
        raise PermissionError("pointer storage unavailable")

    monkeypatch.setattr(deployment_cache_commit, "atomic_write_json", fail_pointer_write)

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-recovery-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "prod",
            "--deployment-id",
            deployment_id,
            "--promoted-by",
            "ci://recovery",
            "--allowed-promoter",
            "ci://recovery",
            "--expect-current-absent",
            "--confirm-repair",
        ],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    assert "- environment: prod" in stdout
    assert "- issue: DPONE_CACHE_PROMOTION_WRITE_FAILED:" in stdout
    assert "- current pointer: recovery required" in stdout
    assert not (cache_root / "current").exists()
    assert not (cache_root / "current-pointer.json").exists()

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-recovery-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "prod",
            "--deployment-id",
            deployment_id,
            "--promoted-by",
            "ci://recovery",
            "--allowed-promoter",
            "ci://recovery",
            "--expect-current-absent",
            "--confirm-repair",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["failed_step"] == "prepare_pointer"
    assert payload["state_may_have_changed"] is True
    assert payload["recovery_required"] is True


def test_airflow_cache_recovery_apply_failure_preserves_environment_in_json(tmp_path: Path) -> None:
    from dpone.readiness.airflow_self_service_cache import cache_recovery_apply_result

    payload = cache_recovery_apply_result(
        cache_root=tmp_path / ".dpone-cache",
        environment="prod",
        deployment_id="sha256:" + "a" * 64,
        confirm_repair=True,
        promoted_by="ci://recovery",
        expected_current_deployment_id=None,
    ).to_dict()

    assert payload["passed"] is False
    assert payload["environment"] == "prod"
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_NOT_FOUND"


def test_airflow_cache_retention_plan_cli_protects_evidence_file_deployment_refs(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    first_id = "sha256:" + "d" * 64
    second_id = "sha256:" + "e" * 64
    first = _write_deployment(cache_root, first_id, complete=True)
    second = _write_deployment(cache_root, second_id, complete=True)
    first_id = _deployment_id(first)
    second_id = _deployment_id(second)
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "artifacts": {"deployment_id": first_id},
                "runs": [{"deployment_id": second_id}],
            }
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-plan",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--evidence-file",
            str(evidence_path),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["delete_candidates"] == []
    reasons = {item["deployment_id"]: item["reason"] for item in payload["items"]}
    assert reasons == {first_id: "retention_evidence", second_id: "retention_evidence"}


def test_airflow_cache_retention_plan_cli_reports_bad_evidence_file(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    evidence_path = tmp_path / "broken.json"
    evidence_path.write_text("{", encoding="utf-8")

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-plan",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--evidence-file",
            str(evidence_path),
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_EVIDENCE_INVALID"
    assert payload["errors"][0]["path"] == "$ABSOLUTE_PATH"


def test_airflow_cache_retention_apply_rejects_evidence_file_without_deployment_id(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    stale_id = "sha256:" + "a" * 64
    stale = _write_deployment(cache_root, stale_id, complete=True)
    evidence_path = tmp_path / "evidence.json"
    evidence_path.write_text(json.dumps({"schema": "dpone.safe-sample-runtime-run.v1"}), encoding="utf-8")

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://retention",
            "--allowed-promoter",
            "ci://retention",
            "--evidence-file",
            str(evidence_path),
            "--confirm-delete",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_EVIDENCE_DEPLOYMENT_ID_NOT_FOUND"
    assert payload["errors"][0]["path"] == "$ABSOLUTE_PATH"
    assert stale.exists()


def test_airflow_cache_retention_apply_rejects_mixed_malformed_evidence_ids(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    cache_root = tmp_path / ".dpone-cache"
    intended = _write_deployment(cache_root, "sha256:" + "a" * 64, complete=True)
    valid = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    evidence_path = tmp_path / "mixed-evidence.json"
    evidence_path.write_text(
        json.dumps(
            {
                "runs": [
                    {"deployment_id": _deployment_id(valid)},
                    {"deployment_id": "not-a-digest", "intended": _deployment_id(intended)},
                ]
            }
        ),
        encoding="utf-8",
    )

    code, stdout, stderr = _run_cli(
        [
            "airflow",
            "cache-retention-apply",
            "--cache-root",
            str(cache_root),
            "--environment",
            "dev",
            "--promoted-by",
            "ci://retention",
            "--allowed-promoter",
            "ci://retention",
            "--evidence-file",
            str(evidence_path),
            "--confirm-delete",
            "--format",
            "json",
        ],
        capsys,
    )

    assert code == 1, stderr
    payload = json.loads(stdout)
    assert payload["errors"][0]["code"] == "DPONE_DEPLOYMENT_EVIDENCE_DEPLOYMENT_ID_INVALID"
    assert intended.exists()
    assert valid.exists()


def test_deployment_cache_retention_plan_schema_validates_payload(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.runtime.deployment_cache import DeploymentCacheRetentionPlanner

    cache_root = tmp_path / ".dpone-cache"
    protected_id = "sha256:" + "d" * 64
    protected = _write_deployment(cache_root, protected_id, complete=True)
    protected_id = _deployment_id(protected)
    payload = (
        DeploymentCacheRetentionPlanner(cache_root)
        .plan(
            environment="dev",
            protected_deployment_ids=(protected_id,),
        )
        .to_dict()
    )
    schema = json.loads(
        Path("docs/schemas/gitops/deployment-cache-retention-plan.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, schema)


def test_deployment_cache_recovery_plan_schema_validates_payload(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)
    payload = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").to_dict()
    schema = json.loads(
        Path("docs/schemas/gitops/deployment-cache-recovery-plan.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, schema)


def test_deployment_cache_recovery_plan_with_malformed_pointer_is_schema_valid(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryPlanner

    cache_root = tmp_path / ".dpone-cache"
    cache_root.mkdir()
    (cache_root / "current-pointer.json").write_text(
        json.dumps(
            {
                "schema": "dpone.current-pointer.v1",
                "environment": "dev",
                "deployment_id": "not-a-digest",
                "release_id": "sha256:" + "a" * 64,
                "promoted_by": "ci://airflow",
                "promoted_at": "2026-07-14T00:00:00Z",
            }
        ),
        encoding="utf-8",
    )

    payload = DeploymentCacheRecoveryPlanner(cache_root).plan(environment="dev").to_dict()
    schema = json.loads(
        Path("docs/schemas/gitops/deployment-cache-recovery-plan.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, schema)
    assert payload["current_deployment_id"] is None
    assert "DPONE_CURRENT_POINTER_INVALID" in {issue["code"] for issue in payload["issues"]}


def test_deployment_cache_recovery_apply_schema_validates_payload(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")
    from dpone.runtime.deployment_cache_recovery import DeploymentCacheRecoveryApplier

    cache_root = tmp_path / ".dpone-cache"
    deployment_id = "sha256:" + "b" * 64
    deployment = _write_deployment(cache_root, deployment_id, complete=True)
    deployment_id = _deployment_id(deployment)
    payload = (
        DeploymentCacheRecoveryApplier(cache_root)
        .apply(
            environment="dev",
            deployment_id=deployment_id,
            confirm_repair=True,
            promoted_by="ci://recovery",
            expected_current_deployment_id=None,
        )
        .to_dict()
    )
    schema = json.loads(
        Path("docs/schemas/gitops/deployment-cache-recovery-apply.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, schema)


def test_deployment_cache_retention_apply_schema_validates_payload(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")

    cache_root = tmp_path / ".dpone-cache"
    stale_id = "sha256:" + "a" * 64
    _write_deployment(cache_root, stale_id, complete=True)
    from dpone.runtime.deployment_cache import DeploymentCacheMaterializer

    current = _write_deployment(cache_root, "sha256:" + "b" * 64, complete=True)
    DeploymentCacheMaterializer(cache_root).promote(current, environment="dev")
    payload = (
        build_deployment_cache_retention_applier(cache_root, allowed_promoters=("ci://retention",))
        .apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
            **_retention_authorization(cache_root),
        )
        .to_v3_dict()
    )
    schema = json.loads(
        Path("docs/schemas/gitops/deployment-cache-retention-apply-v3.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, schema)


def test_deployment_cache_retention_apply_with_invalid_quarantine_is_schema_valid(tmp_path: Path) -> None:
    jsonschema = pytest.importorskip("jsonschema")

    cache_root = tmp_path / ".dpone-cache"
    invalid = cache_root / "deployments" / "dev" / "not-a-digest"
    invalid.mkdir(parents=True)
    payload = (
        build_deployment_cache_retention_applier(
            cache_root,
            allowed_promoters=("ci://retention",),
        )
        .apply(
            environment="dev",
            confirm_delete=True,
            promoted_by="ci://retention",
        )
        .to_v2_dict()
    )
    schema = json.loads(
        Path("docs/schemas/gitops/deployment-cache-retention-apply-v2.schema.json").read_text(encoding="utf-8")
    )

    jsonschema.validate(payload, schema)
    assert payload["items"][0]["deployment_id"] is None
    assert payload["items"][0]["action"] == "skipped"
    assert payload["skipped_deployment_ids"] == []

    for invalid_promoted_by in (None, ""):
        invalid = dict(payload)
        if invalid_promoted_by is None:
            invalid.pop("promoted_by")
        else:
            invalid["promoted_by"] = invalid_promoted_by
        with pytest.raises(jsonschema.ValidationError):
            jsonschema.validate(invalid, schema)
