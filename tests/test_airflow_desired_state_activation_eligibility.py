from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment
from dpone.ports.airflow_desired_state import DesiredStateReconcilePortError
from dpone.readiness.airflow_desired_state_reconcile import (
    DeploymentCacheDesiredDeploymentActivator,
)
from tests.support.airflow_artifact_projection import write_exact_test_projection

_NON_V4_OCCURRENCE = "11111111-1111-1111-8111-111111111111"
_V4_OCCURRENCE_A = "123e4567-e89b-42d3-a456-426614174000"
_V4_OCCURRENCE_B = "223e4567-e89b-42d3-a456-426614174000"
_DIGEST = "sha256:" + "e" * 64


def test_schema_valid_non_v4_occurrence_is_rejected_before_cache_mutation(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "cache"
    release_id, deployment_id = write_exact_test_projection(cache_root)
    deployment_dir = cache_root / "deployments" / "dev" / deployment_id.replace(":", "-", 1)
    index_bytes = (deployment_dir / "airflow-index.json").read_bytes()
    index = json.loads(index_bytes)
    desired = _desired(
        release_id=release_id,
        deployment_id=deployment_id,
        index_bytes=index_bytes,
        runtime_image_digest=str(index["runtime_image_digest"]),
        expected_dag_ids=tuple(sorted(str(item["id"]) for item in index["dag_specs"])),
        occurrence_id=_NON_V4_OCCURRENCE,
    )
    release_dir = cache_root / "releases" / release_id.replace(":", "-", 1)
    modes = _tree_modes(release_dir)

    with pytest.raises(DesiredStateReconcilePortError) as exc:
        DeploymentCacheDesiredDeploymentActivator(
            cache_root=cache_root,
            promoted_by="airflow-pack-watcher",
        ).activate(
            desired,
            expected_current_deployment_id=None,
            remote_precondition=lambda: True,
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_ACTIVATION_ID_INVALID"
    assert exc.value.state_may_have_changed is False
    assert not (cache_root / "activations").exists()
    assert not (cache_root / "current").exists()
    assert _tree_modes(release_dir) == modes


def test_superseded_cold_activation_reports_possible_snapshot_mutation(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "cache"
    desired, deployment_id = _desired_for_cache(cache_root, occurrence_id=_V4_OCCURRENCE_A)

    with pytest.raises(DesiredStateReconcilePortError) as exc:
        _activator(cache_root).activate(
            desired,
            expected_current_deployment_id=None,
            remote_precondition=lambda: False,
        )

    assert exc.value.code == "DPONE_AIRFLOW_DESIRED_STATE_SUPERSEDED"
    assert exc.value.state_may_have_changed is True
    assert (cache_root / "activations" / "dev" / deployment_id.replace(":", "-", 1)).is_dir()
    assert not (cache_root / "current").exists()
    assert not (cache_root / "current-pointer.json").exists()


def test_local_cas_mismatch_reports_possible_snapshot_mutation(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "cache"
    desired, deployment_id = _desired_for_cache(cache_root, occurrence_id=_V4_OCCURRENCE_A)

    with pytest.raises(DesiredStateReconcilePortError) as exc:
        _activator(cache_root).activate(
            desired,
            expected_current_deployment_id=_DIGEST,
            remote_precondition=lambda: True,
        )

    assert exc.value.code == "DPONE_CURRENT_POINTER_CAS_MISMATCH"
    assert exc.value.state_may_have_changed is True
    assert (cache_root / "activations" / "dev" / deployment_id.replace(":", "-", 1)).is_dir()
    assert not (cache_root / "current").exists()
    assert not (cache_root / "current-pointer.json").exists()


def test_superseded_same_deployment_reactivation_keeps_current_occurrence(
    tmp_path: Path,
) -> None:
    cache_root = tmp_path / "cache"
    desired_a, deployment_id = _desired_for_cache(cache_root, occurrence_id=_V4_OCCURRENCE_A)
    current = _activator(cache_root).activate(
        desired_a,
        expected_current_deployment_id=None,
        remote_precondition=lambda: True,
    )
    desired_b = _desired_from_existing_cache(
        cache_root,
        deployment_id=deployment_id,
        occurrence_id=_V4_OCCURRENCE_B,
    )

    with pytest.raises(DesiredStateReconcilePortError) as exc:
        _activator(cache_root).activate(
            desired_b,
            expected_current_deployment_id=deployment_id,
            remote_precondition=lambda: False,
        )

    pointer = json.loads((cache_root / "current-pointer.json").read_text(encoding="utf-8"))
    assert exc.value.code == "DPONE_AIRFLOW_DESIRED_STATE_SUPERSEDED"
    assert exc.value.state_may_have_changed is True
    assert pointer["activation_id"] == current.activation_id == _V4_OCCURRENCE_A


def _desired(
    *,
    release_id: str,
    deployment_id: str,
    index_bytes: bytes,
    runtime_image_digest: str,
    expected_dag_ids: tuple[str, ...],
    occurrence_id: str,
) -> AirflowDesiredDeployment:
    return AirflowDesiredDeployment.from_mapping(
        {
            "schema": "dpone.airflow-desired-deployment.v1",
            "environment": "dev",
            "source": {
                "project": "group/repository",
                "ref": "master",
                "pipeline_id": "1",
                "job_id": "2",
                "occurrence_id": occurrence_id,
                "git_sha": "7" * 40,
            },
            "promotion": {
                "registry_scope_id": _DIGEST,
                "release_id": release_id,
                "deployment_id": deployment_id,
                "airflow_index_sha256": "sha256:" + hashlib.sha256(index_bytes).hexdigest(),
                "runtime_image_digest": runtime_image_digest,
                "expected_dag_ids": list(expected_dag_ids),
                "publication_evidence_sha256": _DIGEST,
            },
            "previous": {"revision": None, "deployment_id": None},
            "promoted_at": "2026-08-01T21:00:00Z",
        }
    )


def _desired_for_cache(
    cache_root: Path,
    *,
    occurrence_id: str,
) -> tuple[AirflowDesiredDeployment, str]:
    _, deployment_id = write_exact_test_projection(cache_root)
    return (
        _desired_from_existing_cache(
            cache_root,
            deployment_id=deployment_id,
            occurrence_id=occurrence_id,
        ),
        deployment_id,
    )


def _desired_from_existing_cache(
    cache_root: Path,
    *,
    deployment_id: str,
    occurrence_id: str,
) -> AirflowDesiredDeployment:
    deployment_dir = cache_root / "deployments" / "dev" / deployment_id.replace(":", "-", 1)
    index_bytes = (deployment_dir / "airflow-index.json").read_bytes()
    index = json.loads(index_bytes)
    return _desired(
        release_id=str(index["release_id"]),
        deployment_id=deployment_id,
        index_bytes=index_bytes,
        runtime_image_digest=str(index["runtime_image_digest"]),
        expected_dag_ids=tuple(sorted(str(item["id"]) for item in index["dag_specs"])),
        occurrence_id=occurrence_id,
    )


def _activator(cache_root: Path) -> DeploymentCacheDesiredDeploymentActivator:
    return DeploymentCacheDesiredDeploymentActivator(
        cache_root=cache_root,
        promoted_by="airflow-pack-watcher",
    )


def _tree_modes(root: Path) -> dict[str, int]:
    return {path.relative_to(root).as_posix(): path.stat().st_mode for path in (root, *root.rglob("*"))}
