#!/usr/bin/env python3
"""Collect and verify names-only PR3A GitHub label readiness evidence."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote

EVIDENCE_TYPE = "dpone.ci-shadow-pr3a-label-readback.v3"
REPOSITORY = "PaulKov/dpone"
PROVIDER_HOST = "github.com"
EXPECTED_LABELS = ("dependencies", "python:uv", "github-actions")
MAX_EVIDENCE_BYTES = 1_048_576
MAX_RESPONSE_BYTES = 65_536
AMBIENT_HOST_SENTINEL = "attacker.example"
SOURCE = "docs/feature-design-ci-shadow-pr3a-ci-hygiene.md#dependabot"
COLLECTOR = "tools/ci/pr3a_label_readback_evidence.py"


class EvidenceError(ValueError):
    """Raised when collection or verification cannot prove readiness."""


def _strict_object(raw_bytes: bytes, *, source: str, max_bytes: int) -> dict[str, Any]:
    if not raw_bytes or len(raw_bytes) > max_bytes:
        raise EvidenceError(f"{source} byte length is outside 1..{max_bytes}")

    def reject_duplicate(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in pairs:
            if key in result:
                raise EvidenceError(f"{source} contains duplicate key {key!r}")
            result[key] = value
        return result

    def reject_constant(value: str) -> None:
        raise EvidenceError(f"{source} contains non-finite value {value}")

    try:
        parsed = json.loads(
            raw_bytes,
            object_pairs_hook=reject_duplicate,
            parse_constant=reject_constant,
        )
    except EvidenceError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError, TypeError) as exc:
        raise EvidenceError(f"{source} is not bounded strict UTF-8 JSON") from exc
    if not isinstance(parsed, dict):
        raise EvidenceError(f"{source} root must be an object")
    return parsed


def _sha256(raw_bytes: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw_bytes).hexdigest()


def _rfc3339(value: object, *, field: str) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise EvidenceError(f"{field} must be an RFC3339 UTC string")
    try:
        parsed = datetime.fromisoformat(value.removesuffix("Z") + "+00:00")
    except ValueError as exc:
        raise EvidenceError(f"{field} must be an RFC3339 UTC string") from exc
    if parsed.tzinfo != timezone.utc:  # noqa: UP017 -- Python 3.10 lacks datetime.UTC typing.
        raise EvidenceError(f"{field} must use UTC")
    return parsed


def _object(value: object, *, field: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise EvidenceError(f"{field} must be an object")
    return value


def _exact_keys(payload: dict[str, Any], expected: set[str], *, source: str) -> None:
    actual = set(payload)
    if actual != expected:
        raise EvidenceError(
            f"{source} keys differ: missing={sorted(expected - actual)!r}, extra={sorted(actual - expected)!r}"
        )


def _run_gh(args: list[str], *, env: dict[str, str]) -> bytes:
    command = ["gh", *args]
    result = subprocess.run(command, check=False, capture_output=True, env=env)
    if result.returncode != 0:
        raise EvidenceError(f"GitHub CLI command failed ({result.returncode}): {' '.join(command)}")
    return result.stdout


def _diagnostic_version(env: dict[str, str]) -> str | None:
    try:
        raw = _run_gh(["version"], env=env)
    except EvidenceError:
        return None
    try:
        line = raw.decode("utf-8").splitlines()[0]
    except (UnicodeDecodeError, IndexError):
        return None
    return line if len(line) <= 256 else None


def _collect_response(label: str, *, env: dict[str, str]) -> dict[str, Any]:
    request_path = f"repos/{REPOSITORY}/labels/{quote(label, safe='')}"
    raw_body = _run_gh(["api", "--hostname", PROVIDER_HOST, request_path], env=env)
    body = _strict_object(raw_body, source=f"response for {label!r}", max_bytes=MAX_RESPONSE_BYTES)
    if body.get("name") != label:
        raise EvidenceError(f"response for {label!r} has a non-exact name")
    return {
        "body_base64": base64.b64encode(raw_body).decode("ascii"),
        "body_bytes": len(raw_body),
        "body_sha256": _sha256(raw_body),
        "echoed_url": body.get("url"),
        "label_id": body.get("id"),
        "method": "GET",
        "name": label,
        "node_id": body.get("node_id"),
        "request_path": request_path,
    }


def collect(*, pull_request: int, implementation_base: str) -> dict[str, Any]:
    if type(pull_request) is not int or pull_request <= 0:
        raise EvidenceError("pull request must be positive")
    if len(implementation_base) != 40 or any(char not in "0123456789abcdef" for char in implementation_base):
        raise EvidenceError("implementation base must be a lowercase 40-hex SHA")
    env = os.environ.copy()
    env["GH_HOST"] = AMBIENT_HOST_SENTINEL
    _run_gh(["auth", "status", "--active", "--hostname", PROVIDER_HOST], env=env)
    version = _diagnostic_version(env)
    responses = [_collect_response(label, env=env) for label in EXPECTED_LABELS]
    stdout = ("\n".join(EXPECTED_LABELS) + "\n").encode()
    observed_at = (
        datetime.now(timezone.utc)  # noqa: UP017 -- Python 3.10 lacks datetime.UTC typing.
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z")
    )
    return {
        "authority": {
            "host": PROVIDER_HOST,
            "labels": list(EXPECTED_LABELS),
            "repository": REPOSITORY,
        },
        "collector": {
            "ambient_gh_host": AMBIENT_HOST_SENTINEL,
            "command": (
                f"uv run python {COLLECTOR} collect --output <path> "
                f"--pull-request {pull_request} --implementation-base {implementation_base}"
            ),
            "operation_class": "repository metadata GETs only",
            "path": COLLECTOR,
            "tool": "gh",
        },
        "diagnostics": {
            "gh_version_first_line": version,
            "provider_responses": responses,
            "stdout_bytes": len(stdout),
            "stdout_sha256": _sha256(stdout),
            "stdout_utf8": stdout.decode(),
        },
        "evidence_type": EVIDENCE_TYPE,
        "label_readiness": "PASS",
        "observed_at": observed_at,
        "receipt_status": "VERIFIED",
        "schema_version": 3,
        "source": SOURCE,
        "subject": {"implementation_base": implementation_base, "pull_request": pull_request},
    }


def _verify_response(response: object, label: str) -> None:
    item = _object(response, field=f"provider response for {label!r}")
    _exact_keys(
        item,
        {
            "body_base64",
            "body_bytes",
            "body_sha256",
            "echoed_url",
            "label_id",
            "method",
            "name",
            "node_id",
            "request_path",
        },
        source=f"provider response for {label!r}",
    )
    try:
        raw = base64.b64decode(item["body_base64"], validate=True)
    except (TypeError, ValueError) as exc:
        raise EvidenceError(f"provider response for {label!r} has invalid base64") from exc
    if item["body_bytes"] != len(raw) or item["body_sha256"] != _sha256(raw):
        raise EvidenceError(f"provider response for {label!r} has invalid byte binding")
    body = _strict_object(raw, source=f"provider response for {label!r}", max_bytes=MAX_RESPONSE_BYTES)
    request_path = f"repos/{REPOSITORY}/labels/{quote(label, safe='')}"
    if item["method"] != "GET" or item["request_path"] != request_path:
        raise EvidenceError(f"provider request for {label!r} is foreign")
    if item["name"] != label or body.get("name") != label:
        raise EvidenceError(f"provider name for {label!r} is non-exact")
    if body.get("id") != item["label_id"] or body.get("node_id") != item["node_id"]:
        raise EvidenceError(f"diagnostic identity for {label!r} is inconsistent")
    if body.get("url") != item["echoed_url"]:
        raise EvidenceError(f"diagnostic URL for {label!r} is inconsistent")


def verify(payload: dict[str, Any]) -> None:
    _exact_keys(
        payload,
        {
            "authority",
            "collector",
            "diagnostics",
            "evidence_type",
            "label_readiness",
            "observed_at",
            "receipt_status",
            "schema_version",
            "source",
            "subject",
        },
        source="evidence",
    )
    if payload["schema_version"] != 3 or payload["evidence_type"] != EVIDENCE_TYPE:
        raise EvidenceError("unsupported evidence schema or type")
    if payload["receipt_status"] != "VERIFIED" or payload["label_readiness"] != "PASS":
        raise EvidenceError("evidence is not a verified PASS receipt")
    if payload["source"] != SOURCE:
        raise EvidenceError("evidence source is foreign")
    _rfc3339(payload["observed_at"], field="observed_at")

    authority = _object(payload["authority"], field="authority")
    if authority != {"host": PROVIDER_HOST, "labels": list(EXPECTED_LABELS), "repository": REPOSITORY}:
        raise EvidenceError("authority projection is foreign")
    subject = _object(payload["subject"], field="subject")
    _exact_keys(subject, {"implementation_base", "pull_request"}, source="subject")
    if type(subject["pull_request"]) is not int or subject["pull_request"] <= 0:
        raise EvidenceError("subject pull_request must be positive")
    base = subject["implementation_base"]
    if not isinstance(base, str) or len(base) != 40 or any(char not in "0123456789abcdef" for char in base):
        raise EvidenceError("subject implementation_base must be lowercase 40-hex")

    collector = _object(payload["collector"], field="collector")
    _exact_keys(collector, {"ambient_gh_host", "command", "operation_class", "path", "tool"}, source="collector")
    expected_command = (
        f"uv run python {COLLECTOR} collect --output <path> "
        f"--pull-request {subject['pull_request']} --implementation-base {base}"
    )
    if collector != {
        "ambient_gh_host": AMBIENT_HOST_SENTINEL,
        "command": expected_command,
        "operation_class": "repository metadata GETs only",
        "path": COLLECTOR,
        "tool": "gh",
    }:
        raise EvidenceError("collector identity is non-canonical")

    diagnostics = _object(payload["diagnostics"], field="diagnostics")
    _exact_keys(
        diagnostics,
        {"gh_version_first_line", "provider_responses", "stdout_bytes", "stdout_sha256", "stdout_utf8"},
        source="diagnostics",
    )
    version = diagnostics["gh_version_first_line"]
    if version is not None and (not isinstance(version, str) or len(version) > 256):
        raise EvidenceError("diagnostic gh version must be null or a bounded string")
    stdout = ("\n".join(EXPECTED_LABELS) + "\n").encode()
    if diagnostics["stdout_utf8"] != stdout.decode():
        raise EvidenceError("stdout projection is non-exact")
    if diagnostics["stdout_bytes"] != len(stdout) or diagnostics["stdout_sha256"] != _sha256(stdout):
        raise EvidenceError("stdout byte binding is invalid")
    responses = diagnostics["provider_responses"]
    if not isinstance(responses, list) or len(responses) != len(EXPECTED_LABELS):
        raise EvidenceError("provider_responses must contain exactly three items")
    for label, response in zip(EXPECTED_LABELS, responses, strict=True):
        _verify_response(response, label)


def load_and_verify(path: Path) -> dict[str, Any]:
    payload = _strict_object(path.read_bytes(), source=str(path), max_bytes=MAX_EVIDENCE_BYTES)
    verify(payload)
    return payload


def verify_live(payload: dict[str, Any]) -> None:
    verify(payload)
    env = os.environ.copy()
    env["GH_HOST"] = AMBIENT_HOST_SENTINEL
    _run_gh(["auth", "status", "--active", "--hostname", PROVIDER_HOST], env=env)
    _diagnostic_version(env)
    current_names = [_collect_response(label, env=env)["name"] for label in EXPECTED_LABELS]
    if current_names != list(EXPECTED_LABELS):
        raise EvidenceError("current label names differ from the approved order")


def _write_verified(path: Path, payload: dict[str, Any]) -> None:
    if path.is_symlink():
        raise EvidenceError(f"refusing to replace symlink {path}")
    path.parent.mkdir(parents=True, exist_ok=True)
    raw = (json.dumps(payload, indent=2, sort_keys=True, allow_nan=False) + "\n").encode()
    verify(_strict_object(raw, source="generated evidence", max_bytes=MAX_EVIDENCE_BYTES))
    descriptor, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    temporary_path = Path(temporary)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary_path, path)
        directory = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        temporary_path.unlink(missing_ok=True)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    collect_parser = commands.add_parser("collect", help="collect and atomically write evidence")
    collect_parser.add_argument("--output", required=True, type=Path)
    collect_parser.add_argument("--pull-request", required=True, type=int)
    collect_parser.add_argument("--implementation-base", required=True)
    for name, help_text in (("verify", "verify receipt integrity"), ("verify-live", "verify current exact names")):
        command = commands.add_parser(name, help=help_text)
        command.add_argument("--input", required=True, type=Path)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "collect":
            _write_verified(
                args.output,
                collect(pull_request=args.pull_request, implementation_base=args.implementation_base),
            )
            print(f"wrote verified PR3A label evidence: {args.output}")
        else:
            payload = load_and_verify(args.input)
            if args.command == "verify-live":
                verify_live(payload)
                print(f"verified live GitHub label names: {args.input}")
            else:
                print(f"verified PR3A label evidence integrity: {args.input}")
    except (EvidenceError, OSError, TypeError, AttributeError, RecursionError) as exc:
        print(f"UNVERIFIED: {exc}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
