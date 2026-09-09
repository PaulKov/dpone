from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md"
COMMAND_MARKER = "The only Pages recovery uses the versioned workflow-dispatch REST endpoint"


def _recovery_shell() -> str:
    tail = SPEC.read_text(encoding="utf-8").split(COMMAND_MARKER, maxsplit=1)[1]
    return tail.split("```bash\n", maxsplit=1)[1].split("\n```", maxsplit=1)[0]


def _write_fake_gh(path: Path, body: str) -> None:
    path.write_text("#!/bin/sh\n" + body, encoding="utf-8")
    path.chmod(0o755)


@pytest.mark.parametrize("prior", (None, "0", "-1", "1.5", "true"))
def test_recovery_rejects_invalid_prior_before_any_gh_call(tmp_path: Path, prior: str | None) -> None:
    marker = tmp_path / "gh-was-called"
    _write_fake_gh(tmp_path / "gh", f"printf called > {marker}\nexit 99\n")
    env = {"PATH": str(tmp_path)}
    if prior is not None:
        env["PRIOR_RUN_ID"] = prior

    result = subprocess.run(("/bin/bash", "-s"), input=_recovery_shell(), text=True, capture_output=True, env=env)

    assert result.returncode != 0
    assert result.stdout == ""
    assert not marker.exists()


def test_recovery_binds_every_api_call_to_github_com_despite_foreign_ambient_host(tmp_path: Path) -> None:
    call_log = tmp_path / "post-call"
    fake_body = """\
case " $* " in *" --hostname github.com "*) ;; *) exit 88 ;; esac
case "$1 $2" in "auth status") exit 0 ;; esac
case "$*" in
  *"/actions/workflows/pages.yml"*)
    printf '%s' '{"path":".github/workflows/pages.yml","state":"active","id":288538896}' ;;
  *"/git/ref/heads/master"*) printf '%040d' 0 | tr 0 a ;;
  *"--method POST"*)
    printf '%s\n' "$GH_HOST|$*" > "$CALL_LOG"
    printf '%s' '{"workflow_run_id":101,"run_url":"https://api.github.com/repos/PaulKov/dpone/actions/runs/101","html_url":"https://github.com/PaulKov/dpone/actions/runs/101"}' ;;
  *) exit 89 ;;
esac
"""
    _write_fake_gh(tmp_path / "gh", fake_body)
    env = os.environ | {
        "PATH": f"{tmp_path}:{os.environ['PATH']}",
        "GH_HOST": "attacker.example",
        "CALL_LOG": str(call_log),
        "PRIOR_RUN_ID": "100",
    }

    result = subprocess.run(("/bin/bash", "-s"), input=_recovery_shell(), text=True, capture_output=True, env=env)

    assert result.returncode == 0, result.stderr
    assert result.stdout.splitlines() == [
        "PAGES_RECOVERY_RUN_ID=101",
        "PAGES_RECOVERY_EXPECTED_SHA=" + "a" * 40,
    ]
    assert call_log.read_text(encoding="utf-8").startswith("attacker.example|api --hostname github.com --method POST ")
