"""Public continuity is independently testable; unavailable history never passes."""

from __future__ import annotations

import subprocess
from pathlib import Path

import pytest
from tools.agent_policy.public_snapshot_history import (
    PUBLIC_ROOT,
    HistoricalCheck,
    assert_public_snapshot_continuity,
    evaluate_history,
)

ROOT = Path(__file__).resolve().parents[2]
FROZEN = "test_artifacts/agent-policy/dpone-ci-shadow-pr3b-spec.yml"


def test_exact_public_snapshot_continuity() -> None:
    assert_public_snapshot_continuity(ROOT, (FROZEN,))


@pytest.mark.parametrize("kind", ["missing", "foreign", "changed", "committed", "restored", "symlink"])
def test_public_snapshot_rejects_unproven_continuity(tmp_path: Path, kind: str) -> None:
    def git(*args: str) -> None:
        subprocess.run(["git", *args], cwd=tmp_path, check=True, capture_output=True)

    git("init", "--quiet")
    if kind == "foreign":
        git("fetch", "--quiet", "--depth=1", str(ROOT), PUBLIC_ROOT)
        git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "--allow-empty", "-m", "foreign")
    elif kind in {"changed", "committed", "restored", "symlink"}:
        git("fetch", "--quiet", "--depth=1", str(ROOT), PUBLIC_ROOT)
        git("checkout", "--quiet", "--detach", "FETCH_HEAD")
        path = tmp_path / FROZEN
        original = path.read_bytes()
        if kind in {"changed", "committed", "restored"}:
            path.write_bytes(path.read_bytes() + b"\n# changed\n")
            if kind in {"committed", "restored"}:
                git("add", FROZEN)
                git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "changed")
                if kind == "restored":
                    path.write_bytes(original)
                    git("add", FROZEN)
                    git("-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-m", "restore")
        else:
            target = tmp_path / "copied.yml"
            target.write_bytes(path.read_bytes())
            path.unlink()
            path.symlink_to(target)
    with pytest.raises((AssertionError, ValueError, subprocess.CalledProcessError)):
        assert_public_snapshot_continuity(tmp_path, (FROZEN,))


def test_historical_outcomes_are_distinct_and_fail_closed() -> None:
    invoked: list[str] = []

    def failure() -> None:
        raise AssertionError("historical assertion failed")

    checks = (
        HistoricalCheck("present", (PUBLIC_ROOT,), lambda: invoked.append("present")),
        HistoricalCheck("missing", ("f" * 40,), lambda: invoked.append("missing")),
        HistoricalCheck("mismatch", (PUBLIC_ROOT,), failure),
    )
    report = evaluate_history(ROOT, checks)
    assert report["status"] == "FAIL"
    assert [item["status"] for item in report["checks"]] == ["PASS", "UNVERIFIED", "FAIL"]
    assert invoked == ["present"]
    assert evaluate_history(ROOT, checks[:1])["status"] == "PASS"
    assert evaluate_history(ROOT, checks[1:2])["status"] == "UNVERIFIED"


def test_missing_anchor_cannot_certify_history(tmp_path: Path) -> None:
    invoked: list[str] = []
    report = evaluate_history(tmp_path, (HistoricalCheck("unused", (), lambda: invoked.append("bad")),))
    assert report["status"] == "FAIL"
    assert invoked == []


def test_fixed_tree_mismatch_is_rejected(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr("tools.agent_policy.public_snapshot_history.PUBLIC_TREE", "f" * 40)
    with pytest.raises(AssertionError):
        assert_public_snapshot_continuity(ROOT, (FROZEN,))


@pytest.mark.parametrize("path", ["../escape", "/absolute", ""])
def test_unconfined_frozen_paths_are_rejected(path: str) -> None:
    with pytest.raises(ValueError):
        assert_public_snapshot_continuity(ROOT, (path,))


@pytest.mark.parametrize("status, code", [("PASS", 0), ("FAIL", 1), ("UNVERIFIED", 2)])
def test_historical_cli_exit_statuses(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str], status: str, code: int
) -> None:
    from tools.agent_policy import public_snapshot_history

    monkeypatch.setattr(public_snapshot_history, "_load_checks", lambda root: ())
    monkeypatch.setattr(public_snapshot_history, "evaluate_history", lambda root, checks: {"status": status})
    assert public_snapshot_history.main() == code
    assert f'"status": "{status}"' in capsys.readouterr().out


def test_empty_history_selection_is_not_a_pass() -> None:
    assert evaluate_history(ROOT, ())["status"] == "UNVERIFIED"


def test_all_seven_original_historical_checks_remain_callable() -> None:
    from tools.agent_policy.public_snapshot_history import _load_checks

    checks = _load_checks(ROOT)
    assert len(checks) == 7
    assert len({check.name for check in checks}) == 7
    assert all(check.required_commits and callable(check.verify) for check in checks)


def test_optimized_python_cannot_report_historical_pass():
    import json
    import sys

    result = subprocess.run(
        [sys.executable, "-O", "-m", "tools.agent_policy.public_snapshot_history"],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 1
    assert json.loads(result.stdout)["status"] == "FAIL"
