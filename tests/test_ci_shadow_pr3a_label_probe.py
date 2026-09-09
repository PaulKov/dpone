from __future__ import annotations

import json
import os
import subprocess
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md"
EVIDENCE = ROOT / "test_artifacts/ci-shadow-pr3a-live/label-readback-evidence.json"
EVIDENCE_TOOL = ROOT / "tools/ci/pr3a_label_readback_evidence.py"
MARKER = "The copyable read-only check is:\n\n```bash\n"


def _label_probe() -> str:
    text = SPEC.read_text(encoding="utf-8")
    assert text.count(MARKER) == 1
    return text.split(MARKER, maxsplit=1)[1].split("\n```", maxsplit=1)[0]


def _fake_gh(tmp_path: Path) -> tuple[Path, Path]:
    executable = tmp_path / "gh"
    log = tmp_path / "api.log"
    executable.write_text(
        """#!/usr/bin/env python3
import os
from pathlib import Path
import sys

mode = os.environ.get("PR3A_FAKE_MODE", "exact")
args = sys.argv[1:]
if args[:2] == ["auth", "status"]:
    if args != ["auth", "status", "--active", "--hostname", "github.com"]:
        raise SystemExit(70)
    print("active-account-noise")
    raise SystemExit(1 if mode == "auth-fail" else 0)
if not args or args[0] != "api":
    raise SystemExit(70)

if args[1:3] != ["--hostname", "github.com"]:
    raise SystemExit(70)
endpoint = args[3]
label = endpoint.rsplit("/", 1)[-1]
if args != ["api", "--hostname", "github.com", endpoint, "--jq", ".name"]:
    raise SystemExit(70)
if endpoint != f"repos/PaulKov/dpone/labels/{label}":
    raise SystemExit(70)
Path(os.environ["PR3A_FAKE_LOG"]).open("a", encoding="utf-8").write(label + "\\n")
order = ["dependencies", "python%3Auv", "github-actions"]
if mode.startswith("fail-") and label == order[int(mode.removeprefix("fail-"))]:
    raise SystemExit(1)
values = {
    "dependencies": "dependencies",
    "python%3Auv": "python:uv",
    "github-actions": "github-actions",
}
value = values[label]
if mode == "case" and label == "github-actions":
    value = "GitHub-Actions"
elif mode == "missing" and label == "python%3Auv":
    value = ""
elif mode == "extra" and label == "github-actions":
    value += "\\nextra"
print(value)
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return executable, log


def _run_probe(
    tmp_path: Path,
    mode: str,
    script: str | None = None,
) -> tuple[subprocess.CompletedProcess[str], list[str]]:
    _fake_gh(tmp_path)
    log = tmp_path / "api.log"
    env = os.environ.copy()
    env.update(
        {
            "PATH": f"{tmp_path}:{env['PATH']}",
            "GH_HOST": "attacker.example",
            "PR3A_FAKE_LOG": str(log),
            "PR3A_FAKE_MODE": mode,
        }
    )
    result = subprocess.run(
        ["bash", "-c", script or _label_probe()],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )
    calls = log.read_text(encoding="utf-8").splitlines() if log.exists() else []
    return result, calls


def test_label_probe_success_has_exact_output_and_order(tmp_path: Path) -> None:
    result, calls = _run_probe(tmp_path, "exact")

    assert result.returncode == 0
    assert result.stdout == "dependencies\npython:uv\ngithub-actions\n"
    assert result.stderr == ""
    assert calls == ["dependencies", "python%3Auv", "github-actions"]


def test_label_probe_auth_failure_is_silent_and_makes_no_api_call(tmp_path: Path) -> None:
    result, calls = _run_probe(tmp_path, "auth-fail")

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "UNVERIFIED: active github.com authentication is unavailable.\n"
    assert calls == []


@pytest.mark.parametrize(
    ("mode", "expected_calls"),
    [
        ("fail-0", ["dependencies"]),
        ("fail-1", ["dependencies", "python%3Auv"]),
        ("fail-2", ["dependencies", "python%3Auv", "github-actions"]),
    ],
)
def test_label_probe_fails_each_api_call(
    tmp_path: Path,
    mode: str,
    expected_calls: list[str],
) -> None:
    result, calls = _run_probe(tmp_path, mode)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "UNVERIFIED: exact GitHub label readback failed.\n"
    assert calls == expected_calls


@pytest.mark.parametrize("mode", ["case", "missing", "extra"])
def test_label_probe_rejects_nonexact_name_evidence(tmp_path: Path, mode: str) -> None:
    result, calls = _run_probe(tmp_path, mode)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == ("UNVERIFIED: GitHub label names or ordering differ from the approved contract.\n")
    assert calls == ["dependencies", "python%3Auv", "github-actions"]


def test_label_probe_rejects_foreign_repository_endpoint(tmp_path: Path) -> None:
    foreign_script = _label_probe().replace("repos/PaulKov/dpone", "repos/Attacker/fork")

    result, calls = _run_probe(tmp_path, "exact", foreign_script)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "UNVERIFIED: exact GitHub label readback failed.\n"
    assert calls == []


def test_label_probe_rejects_ambient_host_fallback(tmp_path: Path) -> None:
    unbound_script = _label_probe().replace("gh api --hostname github.com", "gh api")

    result, calls = _run_probe(tmp_path, "exact", unbound_script)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr == "UNVERIFIED: exact GitHub label readback failed.\n"
    assert calls == []


def test_label_readback_evidence_binds_provider_observation() -> None:
    evidence_text = EVIDENCE.read_text(encoding="utf-8")
    payload = json.loads(evidence_text)

    result = subprocess.run(
        [os.fspath(EVIDENCE_TOOL), "verify", "--input", os.fspath(EVIDENCE)],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""

    assert payload["schema_version"] == 3
    assert payload["evidence_type"] == "dpone.ci-shadow-pr3a-label-readback.v3"
    assert payload["receipt_status"] == "VERIFIED"
    assert payload["label_readiness"] == "PASS"
    assert payload["authority"] == {
        "host": "github.com",
        "labels": ["dependencies", "python:uv", "github-actions"],
        "repository": "PaulKov/dpone",
    }
    assert payload["subject"] == {
        "implementation_base": "bc3d56093b4adb2eae94a475f20d5e3e1721b1ec",
        "pull_request": 531,
    }
    assert payload["source"] == "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md#dependabot"
    assert payload["collector"]["path"] == "tools/ci/pr3a_label_readback_evidence.py"
    assert payload["collector"]["ambient_gh_host"] == "attacker.example"
    assert payload["collector"]["operation_class"] == "repository metadata GETs only"
    provider = payload["diagnostics"]["provider_responses"]
    assert [(item["method"], item["request_path"], item["name"]) for item in provider] == [
        ("GET", "repos/PaulKov/dpone/labels/dependencies", "dependencies"),
        ("GET", "repos/PaulKov/dpone/labels/python%3Auv", "python:uv"),
        ("GET", "repos/PaulKov/dpone/labels/github-actions", "github-actions"),
    ]

    for forbidden in ("authorization", "token", "secret", "x-oauth-scopes"):
        assert forbidden not in evidence_text.lower()
