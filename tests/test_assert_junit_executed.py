"""Unit tests for tools/ci/assert_junit_executed.py (SKIP≠PASS junit gate)."""

from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
HELPER = ROOT / "tools" / "ci" / "assert_junit_executed.py"


def _load_helper():
    spec = importlib.util.spec_from_file_location("assert_junit_executed", HELPER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


helper = _load_helper()


def _write(path: Path, body: str) -> Path:
    path.write_text(body, encoding="utf-8")
    return path


def test_junit_gate_passes_when_at_least_one_passed(tmp_path: Path) -> None:
    junit = _write(
        tmp_path / "ok.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite" tests="3" skipped="1" failures="0" errors="0">
          <testcase classname="t" name="a"/><testcase classname="t" name="b"/>
          <testcase classname="t" name="c"><skipped/></testcase>
        </testsuite>
        """,
    )
    ok, message = helper.evaluate_junit(junit, min_passed=1)
    assert ok is True
    assert "passed=2" in message


def test_junit_gate_enforces_optional_partial_skip_budget(tmp_path: Path) -> None:
    junit = _write(
        tmp_path / "partial-skip.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite" tests="3" skipped="1" failures="0" errors="0">
          <testcase classname="t" name="a"/><testcase classname="t" name="b"/>
          <testcase classname="t" name="c"><skipped/></testcase>
        </testsuite>
        """,
    )

    default_ok, _ = helper.evaluate_junit(junit)
    strict_ok, strict_message = helper.evaluate_junit(junit, max_skipped=0)

    assert default_ok is True
    assert strict_ok is False
    assert "skipped > 0" in strict_message


def test_junit_gate_fails_when_all_skipped(tmp_path: Path) -> None:
    junit = _write(
        tmp_path / "skip.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite" tests="2" skipped="2" failures="0" errors="0">
          <testcase classname="t" name="a"><skipped message="env"/></testcase>
          <testcase classname="t" name="b"><skipped message="env"/></testcase>
        </testsuite>
        """,
    )
    ok, message = helper.evaluate_junit(junit)
    assert ok is False
    assert "all tests skipped" in message


def test_junit_gate_fails_when_any_case_failed(tmp_path: Path) -> None:
    junit = _write(
        tmp_path / "mixed-failure.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite" tests="2" skipped="0" failures="1" errors="0">
          <testcase classname="t" name="passed"/>
          <testcase classname="t" name="failed"><failure message="boom"/></testcase>
        </testsuite>
        """,
    )

    ok, message = helper.evaluate_junit(junit, min_passed=1)

    assert ok is False
    assert "FAIL≠PASS" in message
    assert "failures=1" in message


def test_junit_gate_fails_when_any_case_errored(tmp_path: Path) -> None:
    junit = _write(
        tmp_path / "mixed-error.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite" tests="2" skipped="0" failures="0" errors="1">
          <testcase classname="t" name="passed"/>
          <testcase classname="t" name="errored"><error message="boom"/></testcase>
        </testsuite>
        """,
    )

    ok, message = helper.evaluate_junit(junit, min_passed=1)

    assert ok is False
    assert "FAIL≠PASS" in message
    assert "errors=1" in message


def test_junit_gate_fails_when_empty_or_missing(tmp_path: Path) -> None:
    empty = _write(
        tmp_path / "empty.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite" tests="0" skipped="0" failures="0" errors="0"/>
        """,
    )
    ok_empty, msg_empty = helper.evaluate_junit(empty)
    assert ok_empty is False
    assert "no tests executed" in msg_empty

    ok_missing, msg_missing = helper.evaluate_junit(tmp_path / "missing.xml")
    assert ok_missing is False
    assert "missing" in msg_missing


def test_junit_gate_cli_exit_codes(tmp_path: Path) -> None:
    junit = _write(
        tmp_path / "cli.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuites>
          <testsuite name="suite" tests="1" skipped="0" failures="0" errors="0">
            <testcase classname="t" name="a"/>
          </testsuite>
        </testsuites>
        """,
    )
    assert helper.main(["--junit", str(junit), "--min-passed", "1"]) == 0
    skipped = _write(
        tmp_path / "cli-skip.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite" tests="1" skipped="1" failures="0" errors="0">
          <testcase classname="t" name="a"><skipped/></testcase>
        </testsuite>
        """,
    )
    assert helper.main(["--junit", str(skipped)]) == 1
    assert helper.main(["--junit", str(junit), "--max-skipped", "0"]) == 0


def test_junit_gate_counts_cases_when_suite_attrs_missing(tmp_path: Path) -> None:
    junit = _write(
        tmp_path / "cases.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite">
          <testcase classname="t" name="a"><skipped/></testcase>
          <testcase classname="t" name="b"><skipped/></testcase>
        </testsuite>
        """,
    )
    ok, message = helper.evaluate_junit(junit)
    assert ok is False
    assert "all tests skipped" in message


def test_junit_gate_cli_writes_derived_evidence_and_summary(tmp_path: Path) -> None:
    junit = _write(
        tmp_path / "routes.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite" tests="2" skipped="0" failures="0" errors="0">
          <testcase classname="integration.mysql.mssql" name="test_full_refresh"/>
          <testcase classname="integration.mysql.mssql" name="test_incremental"/>
        </testsuite>
        """,
    )
    evidence = tmp_path / "mysql_local_route_cells.json"
    summary = tmp_path / "step-summary.md"

    result = helper.main(
        [
            "--junit",
            str(junit),
            "--min-passed",
            "2",
            "--max-skipped",
            "0",
            "--evidence-json",
            str(evidence),
            "--profile",
            "real_local",
            "--commit-sha",
            "a" * 40,
            "--summary-md",
            str(summary),
        ]
    )

    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert result == 0
    assert payload["status"] == "PASS"
    assert payload["profile"] == "real_local"
    assert payload["commit_sha"] == "a" * 40
    assert payload["totals"] == {"errors": 0, "failures": 0, "passed": 2, "skipped": 0, "tests": 2}
    assert [case["node_id"] for case in payload["cases"]] == [
        "integration.mysql.mssql::test_full_refresh",
        "integration.mysql.mssql::test_incremental",
    ]
    assert all(case["status"] == "passed" for case in payload["cases"])
    assert "2 passed" in summary.read_text(encoding="utf-8")


def test_junit_gate_failure_evidence_cannot_claim_pass(tmp_path: Path) -> None:
    junit = _write(
        tmp_path / "skipped.xml",
        """<?xml version="1.0" encoding="utf-8"?>
        <testsuite name="suite" tests="1" skipped="1" failures="0" errors="0">
          <testcase classname="integration.mysql" name="test_route"><skipped message="unavailable"/></testcase>
        </testsuite>
        """,
    )
    evidence = tmp_path / "failed.json"

    result = helper.main(
        [
            "--junit",
            str(junit),
            "--max-skipped",
            "0",
            "--evidence-json",
            str(evidence),
            "--profile",
            "local_live",
            "--commit-sha",
            "b" * 40,
        ]
    )

    payload = json.loads(evidence.read_text(encoding="utf-8"))
    assert result == 1
    assert payload["status"] == "FAIL"
    assert payload["totals"]["skipped"] == 1
    assert payload["cases"][0]["status"] == "skipped"


def test_junit_gate_writes_fail_evidence_for_missing_or_truncated_xml(tmp_path: Path) -> None:
    for name, junit in (
        ("missing", tmp_path / "missing.xml"),
        ("truncated", _write(tmp_path / "truncated.xml", "<testsuite><testcase>")),
    ):
        evidence = tmp_path / f"{name}.json"

        result = helper.main(
            [
                "--junit",
                str(junit),
                "--evidence-json",
                str(evidence),
                "--profile",
                "postgres_to_mssql_real_local",
                "--commit-sha",
                "c" * 40,
            ]
        )

        payload = json.loads(evidence.read_text(encoding="utf-8"))
        assert result == 1
        assert payload["status"] == "FAIL"
        assert payload["totals"] == {
            "errors": 0,
            "failures": 0,
            "passed": 0,
            "skipped": 0,
            "tests": 0,
        }
