"""Project-scoped serialization contracts for self-service authoring."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from threading import Event

import pytest

from dpone.adapters import project_authoring_lock as lock_module
from dpone.adapters.project_authoring_lock import ProjectAuthoringLockError, project_authoring_lock


def test_sibling_projects_do_not_share_an_authoring_lock(tmp_path: Path) -> None:
    project_a = tmp_path / "a"
    project_b = tmp_path / "b"
    project_a.mkdir()
    project_b.mkdir()
    acquired = Event()

    def lock_sibling() -> None:
        with project_authoring_lock(project_b):
            acquired.set()

    with ThreadPoolExecutor(max_workers=1) as executor:
        with project_authoring_lock(project_a):
            future = executor.submit(lock_sibling)
            assert acquired.wait(timeout=1)
        future.result(timeout=1)


def test_same_project_lock_wait_is_bounded(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    project = tmp_path / "project"
    project.mkdir()
    monkeypatch.setattr(lock_module, "_LOCK_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(lock_module, "_LOCK_POLL_SECONDS", 0.005)

    def contend() -> str:
        try:
            with project_authoring_lock(project):
                return "acquired"
        except ProjectAuthoringLockError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with project_authoring_lock(project):
            outcome = executor.submit(contend).result(timeout=1)

    assert outcome == "Timed out waiting for the project authoring lock."


def test_symbolic_link_project_root_is_rejected(tmp_path: Path) -> None:
    project = tmp_path / "project"
    alias = tmp_path / "alias"
    project.mkdir()
    alias.symlink_to(project, target_is_directory=True)

    with pytest.raises(ProjectAuthoringLockError, match="opened safely"):
        with project_authoring_lock(alias):
            pass


def test_nonexistent_project_root_uses_stable_external_lock_without_creating_root(tmp_path: Path) -> None:
    project = tmp_path / "new-project"

    with project_authoring_lock(project):
        assert not project.exists()


def test_symlinked_parent_alias_uses_the_same_project_lock(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    actual_parent = tmp_path / "actual"
    project = actual_parent / "project"
    project.mkdir(parents=True)
    alias_parent = tmp_path / "alias"
    alias_parent.symlink_to(actual_parent, target_is_directory=True)
    alias = alias_parent / "project"
    monkeypatch.setattr(lock_module, "_LOCK_TIMEOUT_SECONDS", 0.05)
    monkeypatch.setattr(lock_module, "_LOCK_POLL_SECONDS", 0.005)

    def contend() -> str:
        try:
            with project_authoring_lock(alias):
                return "acquired"
        except ProjectAuthoringLockError as exc:
            return str(exc)

    with ThreadPoolExecutor(max_workers=1) as executor:
        with project_authoring_lock(project):
            outcome = executor.submit(contend).result(timeout=1)

    assert outcome == "Timed out waiting for the project authoring lock."
