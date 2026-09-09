"""Fail-closed process-group quiescence classification contracts."""

from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.backfill import process_lane_group

_PROCESS_GROUP_ID = 812_347


class _ExitedProcess:
    pid = _PROCESS_GROUP_ID
    exitcode = 1

    @staticmethod
    def is_alive() -> bool:
        return False


def _lane() -> SimpleNamespace:
    return SimpleNamespace(
        process=_ExitedProcess(),
        ready=SimpleNamespace(
            pid=_PROCESS_GROUP_ID,
            process_group_id=_PROCESS_GROUP_ID,
        ),
    )


def _enable_linux_procfs(monkeypatch: pytest.MonkeyPatch, proc_root: Path) -> None:
    monkeypatch.setattr(process_lane_group, "_IS_LINUX", True, raising=False)
    monkeypatch.setattr(process_lane_group, "_LINUX_PROC_ROOT", proc_root, raising=False)
    monkeypatch.setattr(process_lane_group.os, "killpg", lambda _group, _signal: None)


def _write_stat(proc_root: Path, *, pid: int, state: str, process_group: int) -> None:
    process_dir = proc_root / str(pid)
    process_dir.mkdir()
    (process_dir / "stat").write_text(
        f"{pid} (lane worker) {state} 1 {process_group} {process_group}\n",
        encoding="utf-8",
    )


def test_only_zombie_and_dead_group_members_are_quiesced(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _enable_linux_procfs(monkeypatch, tmp_path)
    _write_stat(
        tmp_path,
        pid=_PROCESS_GROUP_ID,
        state="Z",
        process_group=_PROCESS_GROUP_ID,
    )
    _write_stat(
        tmp_path,
        pid=_PROCESS_GROUP_ID + 1,
        state="X",
        process_group=_PROCESS_GROUP_ID,
    )
    _write_stat(
        tmp_path,
        pid=_PROCESS_GROUP_ID + 2,
        state="R",
        process_group=_PROCESS_GROUP_ID + 2,
    )

    assert not process_lane_group.lane_or_group_is_alive(_lane())


def test_one_live_group_member_blocks_quiescence(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    _enable_linux_procfs(monkeypatch, tmp_path)
    _write_stat(
        tmp_path,
        pid=_PROCESS_GROUP_ID,
        state="Z",
        process_group=_PROCESS_GROUP_ID,
    )
    _write_stat(
        tmp_path,
        pid=_PROCESS_GROUP_ID + 1,
        state="S",
        process_group=_PROCESS_GROUP_ID,
    )

    assert process_lane_group.lane_or_group_is_alive(_lane())


@pytest.mark.parametrize("evidence", ["unreadable", "malformed"])
def test_ambiguous_proc_evidence_fails_closed(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    evidence: str,
) -> None:
    _enable_linux_procfs(monkeypatch, tmp_path)
    process_dir = tmp_path / str(_PROCESS_GROUP_ID)
    process_dir.mkdir()
    stat_path = process_dir / "stat"
    if evidence == "unreadable":
        stat_path.mkdir()
    else:
        stat_path.write_text("malformed", encoding="utf-8")

    assert process_lane_group.lane_or_group_is_alive(_lane())
