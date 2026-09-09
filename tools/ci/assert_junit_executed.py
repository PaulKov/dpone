"""Fail-closed gate: intentional CI must not treat skipped junit as PASS.

Used by route-live-wide-certification.yml after pytest. Exit 0 only when the
junit report shows at least ``min_passed`` passed tests and satisfies the
optional maximum skipped-test budget.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import xml.etree.ElementTree as ET
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True, slots=True)
class JunitTotals:
    tests: int
    passed: int
    skipped: int
    failures: int
    errors: int


@dataclass(frozen=True, slots=True)
class JunitCase:
    """One observed test outcome from the exact JUnit document."""

    node_id: str
    status: str


def _int_attr(node: ET.Element, name: str, default: int = 0) -> int:
    raw = node.attrib.get(name)
    if raw is None or raw == "":
        return default
    return int(raw)


def summarize_junit(path: Path) -> JunitTotals:
    """Aggregate pytest-style junit XML (testsuites or single testsuite)."""
    root = ET.parse(path).getroot()
    suites: list[ET.Element]
    if root.tag == "testsuites":
        suites = list(root.findall("testsuite"))
        if not suites and root.attrib:
            suites = [root]
    elif root.tag == "testsuite":
        suites = [root]
    else:
        raise ValueError(f"unsupported junit root tag: {root.tag!r}")

    tests = skipped = failures = errors = 0
    for suite in suites:
        tests += _int_attr(suite, "tests")
        skipped += _int_attr(suite, "skipped")
        failures += _int_attr(suite, "failures")
        errors += _int_attr(suite, "errors")

    # Prefer suite attributes; fall back to counting testcase outcomes when
    # attributes are missing/zero but cases exist (some emitters omit totals).
    if tests == 0:
        cases = root.findall(".//testcase")
        if cases:
            tests = len(cases)
            skipped = sum(1 for case in cases if case.find("skipped") is not None)
            failures = sum(1 for case in cases if case.find("failure") is not None)
            errors = sum(1 for case in cases if case.find("error") is not None)

    passed = max(tests - skipped - failures - errors, 0)
    return JunitTotals(
        tests=tests,
        passed=passed,
        skipped=skipped,
        failures=failures,
        errors=errors,
    )


def evaluate_junit(
    path: Path,
    *,
    min_passed: int = 1,
    max_skipped: int | None = None,
) -> tuple[bool, str]:
    """Return a fail-closed result for executed and skipped-test budgets."""
    if not path.is_file():
        return False, f"junit file missing: {path}"

    totals = summarize_junit(path)
    summary = (
        f"tests={totals.tests} passed={totals.passed} skipped={totals.skipped} "
        f"failures={totals.failures} errors={totals.errors}"
    )
    if totals.tests <= 0:
        return False, f"SKIP≠PASS: no tests executed ({summary})"
    if totals.failures > 0 or totals.errors > 0:
        return False, f"FAIL≠PASS: failures or errors reported ({summary})"
    if totals.skipped == totals.tests and totals.failures == 0 and totals.errors == 0:
        return False, f"SKIP≠PASS: all tests skipped ({summary})"
    if totals.passed < min_passed:
        return False, f"SKIP≠PASS: passed < {min_passed} ({summary})"
    if max_skipped is not None and totals.skipped > max_skipped:
        return False, f"SKIP≠PASS: skipped > {max_skipped} ({summary})"
    return True, f"junit gate ok ({summary})"


def junit_cases(path: Path) -> tuple[JunitCase, ...]:
    """Return deterministic per-case evidence without inventing outcomes."""

    root = ET.parse(path).getroot()
    cases: list[JunitCase] = []
    for case in root.findall(".//testcase"):
        classname = case.attrib.get("classname", "")
        name = case.attrib.get("name", "")
        node_id = f"{classname}::{name}" if classname else name
        if case.find("error") is not None:
            status = "error"
        elif case.find("failure") is not None:
            status = "failed"
        elif case.find("skipped") is not None:
            status = "skipped"
        else:
            status = "passed"
        cases.append(JunitCase(node_id=node_id, status=status))
    return tuple(cases)


def _write_evidence(
    path: Path,
    *,
    junit: Path,
    profile: str,
    commit_sha: str,
    min_passed: int,
    max_skipped: int | None,
    ok: bool,
    message: str,
    totals: JunitTotals,
    cases: tuple[JunitCase, ...],
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "schema_version": 1,
        "status": "PASS" if ok else "FAIL",
        "profile": profile,
        "commit_sha": commit_sha,
        "junit": str(junit),
        "gate": {
            "min_passed": min_passed,
            "max_skipped": max_skipped,
        },
        "totals": asdict(totals),
        "cases": [asdict(case) for case in cases],
        "message": message,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _append_summary(
    path: Path,
    *,
    profile: str,
    ok: bool,
    totals: JunitTotals,
    cases: tuple[JunitCase, ...],
) -> None:
    lines = [
        f"### JUnit evidence: `{profile}`",
        "",
        f"- Status: `{'PASS' if ok else 'FAIL'}`",
        f"- Totals: `{totals.passed} passed`, `{totals.failures} failed`, "
        f"`{totals.errors} errors`, `{totals.skipped} skipped`",
        "",
        "| test | status |",
        "|---|---|",
    ]
    lines.extend(f"| `{case.node_id}` | `{case.status}` |" for case in cases)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write("\n".join(lines) + "\n")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--junit", type=Path, required=True, help="Path to junit XML")
    parser.add_argument(
        "--min-passed",
        type=int,
        default=1,
        help="Minimum passed tests required (default: 1)",
    )
    parser.add_argument(
        "--max-skipped",
        type=int,
        help="Maximum skipped tests allowed; omitted means no partial-skip limit",
    )
    parser.add_argument("--evidence-json", type=Path, help="Optional derived JSON evidence output")
    parser.add_argument("--profile", help="Certification profile recorded with JSON evidence")
    parser.add_argument("--commit-sha", help="Exact commit recorded with JSON evidence")
    parser.add_argument("--summary-md", type=Path, help="Optional Markdown summary to append")
    args = parser.parse_args(argv)
    if args.min_passed < 0 or (args.max_skipped is not None and args.max_skipped < 0):
        parser.error("junit count budgets must be non-negative")
    if args.evidence_json is not None:
        if not args.profile:
            parser.error("--profile is required with --evidence-json")
        if not args.commit_sha or re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", args.commit_sha) is None:
            parser.error("--commit-sha must be an exact 40- or 64-character hexadecimal SHA")
    try:
        ok, message = evaluate_junit(
            args.junit,
            min_passed=args.min_passed,
            max_skipped=args.max_skipped,
        )
        totals = summarize_junit(args.junit) if args.junit.is_file() else JunitTotals(0, 0, 0, 0, 0)
        cases = junit_cases(args.junit) if args.junit.is_file() else ()
    except (ET.ParseError, OSError, ValueError) as exc:
        ok = False
        message = f"invalid junit evidence: {exc}"
        totals = JunitTotals(0, 0, 0, 0, 0)
        cases = ()
    if args.evidence_json is not None:
        _write_evidence(
            args.evidence_json,
            junit=args.junit,
            profile=args.profile,
            commit_sha=args.commit_sha,
            min_passed=args.min_passed,
            max_skipped=args.max_skipped,
            ok=ok,
            message=message,
            totals=totals,
            cases=cases,
        )
    if args.summary_md is not None:
        _append_summary(
            args.summary_md,
            profile=args.profile or "unspecified",
            ok=ok,
            totals=totals,
            cases=cases,
        )
    stream = sys.stdout if ok else sys.stderr
    print(message, file=stream)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
