from __future__ import annotations

import base64
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
COLLECTOR = ROOT / "tools/ci/pr3a_label_readback_evidence.py"
BASE = "dd45dd85b3c807a439f5917ad09fe147a9a8bce7"
EXPECTED = ("dependencies", "python:uv", "github-actions")


def _fake_gh(tmp_path: Path) -> Path:
    executable = tmp_path / "gh"
    log = tmp_path / "gh-calls.jsonl"
    executable.write_text(
        """#!/usr/bin/env python3
import json
import os
import sys

args = sys.argv[1:]
with open(os.environ["PR3A_GH_LOG"], "a", encoding="utf-8") as stream:
    stream.write(json.dumps({"args": args, "gh_host": os.environ.get("GH_HOST")}) + "\\n")
if args == ["auth", "status", "--active", "--hostname", "github.com"]:
    raise SystemExit(0)
if args == ["version"]:
    if os.environ.get("PR3A_VERSION_FAIL") == "1":
        raise SystemExit(1)
    print(os.environ.get("PR3A_GH_VERSION", "gh version 2.93.0 (2026-05-27)"))
    raise SystemExit(0)
prefix = ["api", "--hostname", "github.com"]
if args[:3] != prefix or len(args) != 4:
    raise SystemExit(70)
encoded = args[3].rsplit("/", 1)[-1]
order = ["dependencies", "python%3Auv", "github-actions"]
if encoded not in order:
    raise SystemExit(70)
fail_at = os.environ.get("PR3A_FAIL_AT")
if fail_at == encoded:
    raise SystemExit(1)
names = {"dependencies": "dependencies", "python%3Auv": "python:uv", "github-actions": "github-actions"}
name = names[encoded]
if os.environ.get("PR3A_CASE_DRIFT") == encoded:
    name = name.title()
identities = {
    "dependencies": [11110857664, "NODE_dependencies"],
    "python%3Auv": [11110857678, "NODE_python_uv"],
    "github-actions": [11791145750, "NODE_github_actions"],
}
metadata_version = int(os.environ.get("PR3A_METADATA_VERSION", "1"))
payload = {
    "color": "blue" if metadata_version == 1 else "green",
    "default": False,
    "description": "original" if metadata_version == 1 else "changed diagnostics",
    "id": identities[encoded][0] + metadata_version - 1,
    "name": name,
    "node_id": identities[encoded][1] + ("" if metadata_version == 1 else "_new"),
    "url": "https://api.github.com/repos/PaulKov/dpone/labels/" + name,
}
if metadata_version == 2:
    payload = dict(reversed(list(payload.items())))
sys.stdout.write(json.dumps(payload, separators=(",", ":")))
""",
        encoding="utf-8",
    )
    executable.chmod(0o755)
    return log


def _run(tmp_path: Path, *arguments: str, extra_env: dict[str, str] | None = None) -> subprocess.CompletedProcess[str]:
    log = _fake_gh(tmp_path)
    env = os.environ.copy()
    env.update(
        {
            "GH_HOST": "attacker.example",
            "PATH": f"{tmp_path}:{env['PATH']}",
            "PR3A_GH_LOG": os.fspath(log),
        }
    )
    if extra_env:
        env.update(extra_env)
    return subprocess.run(
        [os.fspath(COLLECTOR), *arguments],
        check=False,
        capture_output=True,
        env=env,
        text=True,
    )


def _collect(tmp_path: Path, *, extra_env: dict[str, str] | None = None) -> Path:
    output = tmp_path / "evidence.json"
    result = _run(
        tmp_path,
        "collect",
        "--output",
        os.fspath(output),
        "--pull-request",
        "531",
        "--implementation-base",
        BASE,
        extra_env=extra_env,
    )
    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout == f"wrote verified PR3A label evidence: {output}\n"
    return output


def _load(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _write(path: Path, payload: object) -> None:
    path.write_text(json.dumps(payload, allow_nan=False) + "\n", encoding="utf-8")


def test_collector_binds_host_and_preserves_diagnostic_bytes(tmp_path: Path) -> None:
    output = _collect(tmp_path)
    payload = _load(output)

    assert payload["receipt_status"] == "VERIFIED"
    assert payload["label_readiness"] == "PASS"
    assert payload["authority"] == {
        "host": "github.com",
        "labels": list(EXPECTED),
        "repository": "PaulKov/dpone",
    }
    for response in payload["diagnostics"]["provider_responses"]:
        raw = base64.b64decode(response["body_base64"], validate=True)
        assert response["body_bytes"] == len(raw)
        assert response["body_sha256"] == "sha256:" + hashlib.sha256(raw).hexdigest()

    calls = [json.loads(line) for line in (tmp_path / "gh-calls.jsonl").read_text().splitlines()]
    assert [call["gh_host"] for call in calls] == ["attacker.example"] * 5
    assert all(call["args"][:3] == ["api", "--hostname", "github.com"] for call in calls[2:])


@pytest.mark.parametrize(
    "extra_env",
    (
        {"PR3A_METADATA_VERSION": "2"},
        {"PR3A_GH_VERSION": "gh version 2.94.0 (2026-08-01)"},
        {"PR3A_GH_VERSION": "arbitrary diagnostic output"},
        {"PR3A_VERSION_FAIL": "1"},
    ),
)
def test_live_readiness_ignores_coherent_diagnostic_drift(tmp_path: Path, extra_env: dict[str, str]) -> None:
    output = _collect(tmp_path)
    before = output.read_bytes()

    result = _run(tmp_path, "verify-live", "--input", os.fspath(output), extra_env=extra_env)

    assert result.returncode == 0, result.stderr
    assert result.stderr == ""
    assert result.stdout == f"verified live GitHub label names: {output}\n"
    assert output.read_bytes() == before


@pytest.mark.parametrize(
    ("extra_env", "expected"),
    (
        ({"PR3A_GH_VERSION": "arbitrary diagnostic output"}, "arbitrary diagnostic output"),
        ({"PR3A_VERSION_FAIL": "1"}, None),
    ),
)
def test_collection_treats_version_as_best_effort_diagnostic(
    tmp_path: Path,
    extra_env: dict[str, str],
    expected: str | None,
) -> None:
    output = _collect(tmp_path, extra_env=extra_env)

    assert _load(output)["diagnostics"]["gh_version_first_line"] == expected
    verified = _run(tmp_path, "verify", "--input", os.fspath(output), extra_env=extra_env)
    assert verified.returncode == 0, verified.stderr


@pytest.mark.parametrize(
    ("command", "mutation"),
    (
        ("verify", ("observed_at", None)),
        ("verify", ("observed_at", 1)),
        ("verify-live", ("collector", [])),
        ("verify-live", ("provider_responses", {})),
    ),
)
def test_malformed_evidence_is_controlled(
    tmp_path: Path,
    command: str,
    mutation: tuple[str, object],
) -> None:
    output = _collect(tmp_path)
    payload = _load(output)
    if mutation[0] == "provider_responses":
        payload["diagnostics"][mutation[0]] = mutation[1]
    else:
        payload[mutation[0]] = mutation[1]
    _write(output, payload)
    log = tmp_path / "gh-calls.jsonl"
    log.write_text("", encoding="utf-8")

    result = _run(tmp_path, command, "--input", os.fspath(output))

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.startswith("UNVERIFIED: ")
    assert "Traceback" not in result.stderr
    if command == "verify-live":
        assert log.read_text(encoding="utf-8") == ""


def test_deep_json_is_controlled_without_traceback(tmp_path: Path) -> None:
    output = tmp_path / "deep.json"
    output.write_text('{"x":' * 10_000 + "null" + "}" * 10_000, encoding="utf-8")

    result = _run(tmp_path, "verify", "--input", os.fspath(output))

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.startswith("UNVERIFIED: ")
    assert "Traceback" not in result.stderr


@pytest.mark.parametrize(
    "extra_env",
    (
        {"PR3A_FAIL_AT": "python%3Auv"},
        {"PR3A_CASE_DRIFT": "dependencies"},
    ),
)
def test_live_readiness_rejects_missing_or_non_exact_names(tmp_path: Path, extra_env: dict[str, str]) -> None:
    output = _collect(tmp_path)

    result = _run(tmp_path, "verify-live", "--input", os.fspath(output), extra_env=extra_env)

    assert result.returncode == 1
    assert result.stdout == ""
    assert result.stderr.startswith("UNVERIFIED: ")
    assert "Traceback" not in result.stderr
