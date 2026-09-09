"""Contracts for the Linux containment-aware CI process-tree supervisor."""

from __future__ import annotations

import importlib.util
import os
import subprocess
import sys
import time
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
TOOL = ROOT / "tools/ci/run_bounded_process_tree.py"
SPEC = importlib.util.spec_from_file_location("run_bounded_process_tree", TOOL)
assert SPEC is not None and SPEC.loader is not None
supervisor = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = supervisor
SPEC.loader.exec_module(supervisor)

_SIGTERM_IGNORING_SETSID_PROGRAM = """
import os
import signal
import sys
import time
from pathlib import Path

os.setsid()
signal.signal(signal.SIGTERM, signal.SIG_IGN)
raw = Path('/proc/self/stat').read_text(encoding='utf-8')
starttime = raw[raw.rfind(')') + 2:].split()[19]
Path(sys.argv[1]).write_text(f'{os.getpid()} {starttime}', encoding='utf-8')
while True:
    time.sleep(1)
"""


def _require_linux_procfs() -> None:
    if sys.platform != "linux" or not Path("/proc/self/stat").is_file() or not hasattr(os, "setsid"):
        pytest.skip("process-tree containment requires Linux procfs")


def _run_tool(*arguments: str, timeout: float = 15.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        (sys.executable, TOOL.as_posix(), *arguments),
        check=False,
        capture_output=True,
        text=True,
        timeout=timeout,
    )


def test_proc_stat_parser_uses_last_parenthesis_and_starttime_identity() -> None:
    fields = ["S", "20", "30", "40"] + ["0"] * 15 + ["987654"] + ["0"] * 4

    observed = supervisor.parse_process_stat(f"10 (worker ) lane) {' '.join(fields)}")

    assert observed.identity == supervisor.ProcessIdentity(pid=10, starttime=987654)
    assert observed.state == "S"
    assert observed.ppid == 20
    assert observed.process_group == 30
    assert observed.session == 40


def test_timeout_and_unproven_quiescence_have_distinct_exit_codes() -> None:
    assert supervisor.EXIT_TIMEOUT == 124
    assert supervisor.EXIT_CONTAINMENT_FAILURE == 125
    assert supervisor.EXIT_TIMEOUT != supervisor.EXIT_CONTAINMENT_FAILURE


def test_process_tracker_rejects_pid_reuse() -> None:
    owner = supervisor.ProcessIdentity(pid=1, starttime=10)
    original = supervisor.ProcessStat(supervisor.ProcessIdentity(20, 30), "S", 1, 20, 20)
    reused = supervisor.ProcessStat(supervisor.ProcessIdentity(20, 31), "S", 99, 20, 20)
    tracker = supervisor.ProcessTracker(owner, frozenset())

    tracker.discover({1: supervisor.ProcessStat(owner, "S", 0, 1, 1), 20: original})

    assert tracker.current({20: reused}) == ()


def test_command_exit_code_is_preserved_after_tree_quiesces() -> None:
    _require_linux_procfs()

    result = _run_tool(
        "--timeout-seconds",
        "5",
        "--",
        sys.executable,
        "-c",
        "raise SystemExit(23)",
    )

    assert result.returncode == 23, result.stderr
    assert result.stdout == ""
    assert result.stderr == ""


def test_start_failure_does_not_disclose_command_or_environment(monkeypatch: pytest.MonkeyPatch) -> None:
    _require_linux_procfs()
    marker = "credential-marker-must-not-be-logged"
    monkeypatch.setenv("DPONE_TEST_SECRET", marker)

    result = _run_tool(
        "--timeout-seconds",
        "1",
        "--",
        f"missing-{marker}",
    )

    assert result.returncode == supervisor.EXIT_NOT_FOUND
    assert marker not in result.stdout
    assert marker not in result.stderr
    assert "command could not be started" in result.stderr


def test_parent_exit_reaps_setsid_descendant_and_preserves_exit_code(tmp_path: Path) -> None:
    _require_linux_procfs()
    nested_identity = tmp_path / "nested.identity"
    parent_program = """
import subprocess
import sys
import time
from pathlib import Path

subprocess.Popen((sys.executable, '-c', sys.argv[2], sys.argv[1]))
deadline = time.monotonic() + 5
while not Path(sys.argv[1]).is_file():
    if time.monotonic() >= deadline:
        raise SystemExit(91)
    time.sleep(0.01)
raise SystemExit(23)
"""

    started = time.monotonic()
    result = _run_tool(
        "--timeout-seconds",
        "20",
        "--term-grace-seconds",
        "0.2",
        "--kill-grace-seconds",
        "2",
        "--poll-interval-seconds",
        "0.01",
        "--",
        sys.executable,
        "-c",
        parent_program,
        nested_identity.as_posix(),
        _SIGTERM_IGNORING_SETSID_PROGRAM,
    )

    assert result.returncode == 23, result.stderr
    assert time.monotonic() - started < 5
    pid, _ = (int(value) for value in nested_identity.read_text(encoding="utf-8").split())
    assert not (Path("/proc") / str(pid)).exists(), f"setsid descendant survived parent exit: pid={pid}"


def test_timeout_reaps_parent_and_nested_setsid_child(tmp_path: Path) -> None:
    _require_linux_procfs()
    parent_identity = tmp_path / "parent.identity"
    nested_identity = tmp_path / "nested.identity"
    parent_program = """
import os
import signal
import subprocess
import sys
import time
from pathlib import Path

signal.signal(signal.SIGTERM, signal.SIG_IGN)
raw = Path('/proc/self/stat').read_text(encoding='utf-8')
starttime = raw[raw.rfind(')') + 2:].split()[19]
Path(sys.argv[1]).write_text(f'{os.getpid()} {starttime}', encoding='utf-8')
subprocess.Popen((sys.executable, '-c', sys.argv[3], sys.argv[2]))
deadline = time.monotonic() + 5
while not Path(sys.argv[2]).is_file():
    if time.monotonic() >= deadline:
        raise SystemExit(91)
    time.sleep(0.01)
while True:
    time.sleep(1)
"""

    result = _run_tool(
        "--timeout-seconds",
        "1",
        "--term-grace-seconds",
        "0.2",
        "--kill-grace-seconds",
        "2",
        "--poll-interval-seconds",
        "0.01",
        "--",
        sys.executable,
        "-c",
        parent_program,
        parent_identity.as_posix(),
        nested_identity.as_posix(),
        _SIGTERM_IGNORING_SETSID_PROGRAM,
    )

    assert result.returncode == supervisor.EXIT_TIMEOUT, result.stderr
    assert "quiescence was proven" in result.stderr
    for identity_path in (parent_identity, nested_identity):
        pid, starttime = (int(value) for value in identity_path.read_text(encoding="utf-8").split())
        stat_path = Path("/proc") / str(pid) / "stat"
        assert not stat_path.exists(), f"process survived bounded reap: pid={pid}, starttime={starttime}"
