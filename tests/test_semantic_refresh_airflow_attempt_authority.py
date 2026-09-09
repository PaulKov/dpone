from __future__ import annotations

from types import SimpleNamespace

import pytest

from dpone.adapters import semantic_refresh_airflow_attempt_authority as subject
from dpone.adapters.semantic_refresh_airflow_attempt_authority import (
    AirflowKubernetesWorkerAttemptAuthority,
    SemanticRefreshAirflowAttemptAuthorityError,
)


def _digest(character: str) -> str:
    return "sha256:" + character * 64


def _context(*, run_id: str = "scheduled__2026-08-09", task_id: str = "semantic_refresh__dbt_build_test"):
    return {
        "dag_run": SimpleNamespace(run_id=run_id),
        "task_instance": SimpleNamespace(task_id=task_id, try_number=2),
    }


def _authority(tmp_path, monkeypatch: pytest.MonkeyPatch, *, context=None):
    (tmp_path / "uid").write_text("12345678-1234-5678-1234-567812345678\n", encoding="ascii")
    sdk = SimpleNamespace(get_current_context=lambda: context or _context())
    monkeypatch.setattr(subject, "import_module", lambda name: sdk if name == "airflow.sdk" else None)
    return AirflowKubernetesWorkerAttemptAuthority(
        pod_uid_reader=lambda: (tmp_path / "uid").read_bytes(),
    )


def test_worker_attempt_authority_binds_actual_dagrun_task_and_pod(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    authority = _authority(tmp_path, monkeypatch)

    result = authority.load_attempts(
        plan_bundle_sha256=_digest("a"),
        workflow_execution_id="scheduled__2026-08-09",
        operation_ids=(_digest("1"), _digest("2")),
    )

    assert tuple(item.operation_id for item in result.attempts) == (_digest("1"), _digest("2"))
    assert {item.task_id for item in result.attempts} == {"semantic_refresh__dbt_build_test"}
    assert {item.try_number for item in result.attempts} == {2}
    assert {item.pod_uid for item in result.attempts} == {"12345678-1234-5678-1234-567812345678"}


@pytest.mark.parametrize(
    ("context", "message"),
    [
        (_context(run_id="scheduled__other"), "DagRun/task differs"),
        (_context(task_id="caller_task"), "DagRun/task differs"),
    ],
)
def test_worker_attempt_authority_rejects_foreign_scheduler_context(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    context,
    message: str,
) -> None:
    authority = _authority(tmp_path, monkeypatch, context=context)

    with pytest.raises(SemanticRefreshAirflowAttemptAuthorityError, match=message):
        authority.load_attempts(
            plan_bundle_sha256=_digest("a"),
            workflow_execution_id="scheduled__2026-08-09",
            operation_ids=(_digest("1"),),
        )


def test_worker_attempt_authority_rejects_invalid_pod_identity(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    sdk = SimpleNamespace(get_current_context=lambda: _context())
    monkeypatch.setattr(subject, "import_module", lambda _name: sdk)
    authority = AirflowKubernetesWorkerAttemptAuthority(
        pod_uid_reader=lambda: b"caller-value",
    )

    with pytest.raises(SemanticRefreshAirflowAttemptAuthorityError, match="pod UID is invalid"):
        authority.load_attempts(
            plan_bundle_sha256=_digest("a"),
            workflow_execution_id="scheduled__2026-08-09",
            operation_ids=(_digest("1"),),
        )
