"""Produce an additive acceptance plan for two exact Git commit trees.

This tool records required work, not successful execution. Existing workflow,
security, packaging, and release gates remain authoritative and unconditional.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import subprocess
import sys
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.agent_policy.acceptance_impact import CLASSIFIER_VERSION, MAX_DOCUMENT_BYTES, schedule_only
from tools.agent_policy.select_checks import plan

SCHEDULE_COMMAND = (
    "uv run pytest tests/test_airflow_dag_schedule.py tests/test_airflow_dag_spec_contract.py "
    "tests/test_airflow_interval_contract.py "
    "tests/test_run_interval_context.py -q"
)


def _git(repo: Path, *args: str) -> bytes:
    try:
        return subprocess.run(
            ("git", "--no-replace-objects", "-C", str(repo), *args), check=True, capture_output=True, timeout=30
        ).stdout
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired, OSError) as exc:
        # Do not include repository content or command stderr in evidence.
        raise ValueError("Unable to inspect requested Git revision or object.") from exc


def _revision(repo: Path, reference: str) -> str:
    value = _git(repo, "rev-parse", "--verify", "--end-of-options", f"{reference}^{{commit}}").decode().strip()
    if not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", value):
        raise ValueError("Git revision did not resolve to a full commit identity.")
    return value


def _changes(repo: Path, base: str, head: str) -> list[dict[str, str]]:
    raw = _git(
        repo,
        "diff",
        "--raw",
        "-z",
        "--no-abbrev",
        "--no-ext-diff",
        "--no-textconv",
        "--find-renames",
        "--ignore-submodules=none",
        base,
        head,
        "--",
    )
    fields = raw.split(b"\0")
    changes = []
    index = 0
    try:
        while index < len(fields) - 1:
            old_mode, new_mode, old_blob, new_blob, status = fields[index].decode("ascii").split()
            path = fields[index + 1].decode("utf-8")
            index += 2
            previous_path = path
            if status.startswith(("R", "C")):
                path = fields[index].decode("utf-8")
                index += 1
            changes.append(
                {
                    "path": path,
                    "previous_path": previous_path,
                    "status": status,
                    "old_mode": old_mode.removeprefix(":"),
                    "new_mode": new_mode,
                    "old_blob": old_blob,
                    "new_blob": new_blob,
                }
            )
    except (ValueError, IndexError, UnicodeError) as exc:
        raise ValueError("Malformed or unsupported Git diff input.") from exc
    return changes


def _classify_file(repo: Path, change: dict[str, str]) -> tuple[bool, str]:
    if change["status"] != "M" or change["old_mode"] != "100644" or change["new_mode"] != "100644":
        return False, "Only modified regular non-executable files are eligible."
    contents = []
    for identity in (change["old_blob"], change["new_blob"]):
        size = int(_git(repo, "cat-file", "-s", identity))
        if size > MAX_DOCUMENT_BYTES:
            return False, "Changed blob exceeds semantic policy byte limit."
        contents.append(_git(repo, "cat-file", "blob", identity))
    return schedule_only(change["path"], *contents)


def _check(identity: str, reason: str, command: str | None = None) -> dict[str, Any]:
    return {"id": identity, "reason": reason, "command": command, "status": "UNVERIFIED", "artifacts": []}


def build_plan(repo: Path, base_ref: str, head_ref: str) -> dict[str, Any]:
    """Bind conservative decisions to immutable trees; ignore working-tree edits."""
    base, head = _revision(repo, base_ref), _revision(repo, head_ref)
    changes = _changes(repo, base, head)
    files = []
    for change in changes:
        eligible, reason = _classify_file(repo, change)
        files.append({**change, "schedule_only": eligible, "reason": reason})
    classification = (
        "no_changes" if not files else "schedule_only" if all(f["schedule_only"] for f in files) else "broad"
    )
    paths = sorted({value for change in changes for value in (change["path"], change["previous_path"])})
    checks = [
        _check(
            "existing_required_gates",
            "Retain every existing mandatory non-live, security, packaging, and release gate.",
        )
    ]
    if files:
        checks.append(
            _check(
                "scheduling_contracts",
                "Verify parsing, serialization, scheduling, and interval contracts.",
                SCHEDULE_COMMAND,
            )
        )
    if classification == "broad":
        checks.extend(
            [
                _check(
                    "full_non_live",
                    "Mixed, runtime, or unproven changes require broad contracts.",
                    'uv run pytest -m "not integration_live" -n auto --dist loadfile',
                ),
                _check(
                    "synthetic_container_smoke",
                    "Run a scoped synthetic Docker smoke for affected supported routes; "
                    "record justified N/A if no route is affected. Missing infrastructure is SKIP/UNVERIFIED, never PASS.",
                ),
            ]
        )
    return {
        "schema_version": 1,
        "classifier_version": CLASSIFIER_VERSION,
        "base_sha": base,
        "head_sha": head,
        "classification": classification,
        "files": files,
        "legacy_validation": plan(paths),
        "preserve_existing_required_checks": True,
        "required_checks": checks,
        "performance_soak": {"policy": "opt_in", "release_requirements_unchanged": True},
        "note": "This additive plan is not execution evidence or authorization to skip workflow gates. "
        "Working-tree changes are outside the exact commit pair. Candidate policy runs only in unprivileged CI.",
    }


def verify_junit(path: Path, expected_files: list[str], head_sha: str) -> dict[str, Any]:
    """Require real passing test cases for every requested file, with zero skips."""
    try:
        raw = path.read_bytes()
        text = raw.decode("utf-8")
        if len(raw) > 16_777_216 or "<!DOCTYPE" in text.upper() or "<!ENTITY" in text.upper():
            raise ValueError("Unsupported JUnit document.")
        root = ET.fromstring(text)
        if root.tag not in {"testsuite", "testsuites"}:
            raise ValueError("Unsupported JUnit root.")
        suites = [root] if root.tag == "testsuite" else list(root.findall("testsuite"))
        cases = []
        if not suites or any(node.tag in {"skipped", "failure", "error"} for node in root.iter()):
            raise ValueError("JUnit contains missing, skipped, or failed tests.")
        for suite in suites:
            children = suite.findall("testcase")
            if int(suite.attrib["tests"]) != len(children) or not children:
                raise ValueError("JUnit test counts are empty or inconsistent.")
            if any(int(suite.attrib[key]) != 0 for key in ("failures", "errors", "skipped")):
                raise ValueError("JUnit reports incomplete execution.")
            cases.extend(children)
        identities = [(case.attrib["classname"], case.attrib["name"]) for case in cases]
        if len(set(identities)) != len(identities) or not all(all(item) for item in identities):
            raise ValueError("JUnit test identities are missing or duplicated.")
        if not expected_files or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", head_sha):
            raise ValueError("Expected files and exact commit identity are required.")
        counts = {}
        for filename in expected_files:
            if not filename.startswith("tests/") or not filename.endswith(".py") or ".." in Path(filename).parts:
                raise ValueError("Expected test files must be repository-relative Python tests.")
            module = filename[:-3].replace("/", ".")
            counts[filename] = sum(name == module or name.startswith(module + ".") for name, _ in identities)
        if not all(counts.values()):
            raise ValueError("JUnit does not contain executed tests for every required file.")
    except (OSError, ET.ParseError, KeyError, TypeError, UnicodeError) as exc:
        raise ValueError("Unable to verify JUnit execution evidence.") from exc
    return {
        "schema_version": 1,
        "kind": "acceptance_test_receipt",
        "head_sha": head_sha,
        "status": "PASS",
        "tests": len(cases),
        "files": counts,
        "artifact": str(path),
        "artifact_sha256": hashlib.sha256(raw).hexdigest(),
    }


def require_clean_candidate(repo: Path, head_ref: str) -> str:
    """Prevent local modifications from being certified as committed source."""
    head = _revision(repo, head_ref)
    if (
        _revision(repo, "HEAD") != head
        or _git(repo, "status", "--porcelain=v1", "-z", "--untracked-files=no")
        or _git(repo, "ls-files", "--others", "--exclude-standard", "-z", "--", ".", ":(exclude)test_artifacts/**")
    ):
        raise ValueError("Execution receipt requires the exact candidate checkout without uncommitted source changes.")
    return head


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base-ref")
    parser.add_argument("--head-ref", required=True)
    parser.add_argument("--repo", type=Path, default=Path("."))
    parser.add_argument("--output", type=Path)
    parser.add_argument("--verify-junit", type=Path)
    parser.add_argument("--require-test-file", action="append", default=[])
    args = parser.parse_args()
    try:
        if args.verify_junit:
            if args.base_ref:
                parser.error("--base-ref is not used for execution receipts")
            payload = verify_junit(
                args.verify_junit, args.require_test_file, require_clean_candidate(args.repo, args.head_ref)
            )
        else:
            if not args.base_ref or args.require_test_file:
                parser.error("Planning requires --base-ref; --require-test-file requires --verify-junit")
            payload = build_plan(args.repo, args.base_ref, args.head_ref)
    except ValueError as exc:
        parser.exit(2, f"Acceptance planning failed: {exc}\n")
    serialized = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized, encoding="utf-8")
    else:
        print(serialized, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
