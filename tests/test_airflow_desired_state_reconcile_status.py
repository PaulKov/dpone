from __future__ import annotations

import json
from dataclasses import dataclass, field

import pytest

from dpone.contracts.airflow_desired_state import DesiredStateRevision
from dpone.contracts.airflow_desired_state_reconcile import (
    DesiredStateCheckpoint,
)
from dpone.ports.airflow_desired_state import (
    DesiredStateReconcilePortError,
)
from dpone.readiness.airflow_desired_state_reconcile import (
    _commit_running_status,
    _commit_success_status,
)
from dpone.services.airflow_desired_state_reconcile import (
    DesiredStateReconcileError,
    unchanged_evidence,
)

_DIGESTS = tuple("sha256:" + char * 64 for char in "abcde")


@dataclass
class _StatusStore:
    bodies: list[bytes] = field(default_factory=list)
    fail_next: bool = False
    fail_with_port_error: bool = False

    def commit_status(self, body: bytes) -> None:
        if self.fail_with_port_error:
            raise DesiredStateReconcilePortError(
                "DPONE_CACHE_PATH_ESCAPE",
                "status path is unsafe",
            )
        if self.fail_next:
            self.fail_next = False
            raise OSError("disk full")
        self.bodies.append(body)


def _evidence():
    checkpoint = DesiredStateCheckpoint(
        environment="dev",
        observed_revision=DesiredStateRevision('"etag"'),
        desired_state_sha256=_DIGESTS[0],
        registry_scope_id=_DIGESTS[1],
        source_project="group/repository",
        source_ref="master",
        release_id=_DIGESTS[2],
        deployment_id=_DIGESTS[3],
        occurrence_id="123e4567-e89b-42d3-a456-426614174000",
        source_git_sha="1" * 40,
        airflow_index_sha256=_DIGESTS[4],
        runtime_image_digest=_DIGESTS[0],
        expected_dag_ids=("DAG__platform__smoke__run",),
        activation_id="123e4567-e89b-42d3-a456-426614174001",
    )
    return unchanged_evidence(checkpoint)


def test_running_status_replaces_stale_success_before_reconcile() -> None:
    store = _StatusStore()

    _commit_running_status(store)

    status = json.loads(store.bodies[-1])
    assert status["passed"] is False
    assert status["errors"][0]["code"] == "DPONE_AIRFLOW_DESIRED_STATE_RECONCILE_IN_PROGRESS"


def test_failed_final_status_leaves_machine_readable_failure() -> None:
    store = _StatusStore()
    _commit_running_status(store)
    store.fail_next = True

    with pytest.raises(DesiredStateReconcileError) as failed:
        _commit_success_status(store, _evidence())

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_STATUS_FAILED"
    status = json.loads(store.bodies[-1])
    assert status["passed"] is False
    assert status["errors"][0]["code"] == "DPONE_AIRFLOW_DESIRED_STATE_STATUS_FAILED"


def test_unsafe_status_path_is_normalized_to_typed_status_failure() -> None:
    store = _StatusStore(fail_with_port_error=True)

    with pytest.raises(DesiredStateReconcileError) as failed:
        _commit_running_status(store)

    assert failed.value.code == "DPONE_AIRFLOW_DESIRED_STATE_STATUS_FAILED"
