"""A restart retains the original immutable recovery plan without credential output."""

from __future__ import annotations

import os

import pytest

from dpone.app.prepared_recovery_plan_store import read_plan, write_plan
from tests.test_clickhouse_prepared_recovery import _case, _execution_case, _plan


def test_plan_artifact_survives_restart_with_private_permissions(tmp_path) -> None:
    catalog, authority, ddl = _case()
    plan = _plan(catalog, authority, ddl)
    path = tmp_path / "recovery-plan.json"
    write_plan(plan, path)
    restored = read_plan(path)
    assert restored == plan
    assert path.stat().st_mode & 0o777 == 0o600
    assert "password" not in path.read_text(encoding="utf-8")


def test_plan_artifact_cannot_overwrite_existing_file(tmp_path) -> None:
    catalog, authority, ddl = _case()
    plan = _plan(catalog, authority, ddl)
    path = tmp_path / "recovery-plan.json"
    write_plan(plan, path)
    with pytest.raises(FileExistsError):
        write_plan(plan, path)


def test_plan_artifact_rejects_world_readable_file(tmp_path) -> None:
    catalog, authority, ddl = _case()
    plan = _plan(catalog, authority, ddl)
    path = tmp_path / "recovery-plan.json"
    write_plan(plan, path)
    os.chmod(path, 0o644)
    with pytest.raises(ValueError, match="private"):
        read_plan(path)


def test_saved_plan_replays_completed_operation_without_second_ddl(tmp_path) -> None:
    plan, service, _authority, ddl = _execution_case()
    path = tmp_path / "recovery-plan.json"
    write_plan(plan, path)
    service.execute_prepared_recovery(read_plan(path), confirmation_digest=plan.plan_digest)
    service.execute_prepared_recovery(read_plan(path), confirmation_digest=plan.plan_digest)
    assert ddl.dispatches == 1
