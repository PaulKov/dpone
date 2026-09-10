"""Verify attribution and completeness of the frozen direct PostgreSQL campaign.

The real assertions are executed by pytest and retained as individual database
observations. This receipt verifies their successful completion and common source
identity; it does not expand their scope into general route certification.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent


def verify(directory: Path, expected_cases: int = 40) -> dict:
    receipt = json.loads((directory / "run-receipt.json").read_text())
    environment = json.loads((directory / "environment.json").read_text())
    assert receipt["exit_code"] == 0 and receipt["unchanged_source_and_tests"]
    digest = receipt["start_source_and_test_sha256"]
    assert digest == receipt["end_source_and_test_sha256"] == environment["source_and_test_sha256"]
    files = sorted(directory.glob("test_*.json"))
    assert len(files) == expected_cases
    passed = [line for line in (directory / "pytest.log").read_text().splitlines() if line.startswith("PASSED ")]
    assert len(passed) == expected_cases
    allowed_modules = {
        "PASSED tests/integration/postgres/test_postgres_strategy_preservation_live.py",
        "PASSED tests/integration/postgres/test_postgres_snapshot_metrics_live.py",
    }
    assert all(line.split("::", 1)[0] in allowed_modules for line in passed)
    names = [path.name.split("-dp_preserve_", 1)[0] for path in files]
    assert len(set(names)) == expected_cases
    assert set(names) == {line.split("::", 1)[1] for line in passed}
    versions = set()
    for path in files:
        payload = json.loads(path.read_text())
        identity = payload["identity"]
        assert identity["git_head"] == environment["source_head"]
        assert identity["source_tree_sha256"] == digest
        assert payload["events"] and (payload["observations"] or payload["loads"])
        test_name = path.name.split("-dp_preserve_", 1)[0]
        assert sum(line.endswith("::" + test_name) for line in passed) == 1
        versions.add(json.dumps(identity["postgres_version"], sort_keys=True))
    return {
        "status": "PASS",
        "scope": f"{expected_cases} direct PostgresSink/legacy adapter regression cases on real PostgreSQL; not every route certified",
        "source_commit": environment["source_head"],
        "source_and_test_sha256": digest,
        "case_count": expected_cases,
        "environment": environment,
        "postgres_versions": sorted(versions),
        "evidence_files": {
            str(path.relative_to(ROOT)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted(directory.iterdir())
            if path.is_file()
        },
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("directory", nargs="?", default="live-direct-1ff8387")
    parser.add_argument("--expected-cases", type=int, default=40)
    parser.add_argument("--output", type=Path, default=ROOT / "verification-direct.json")
    args = parser.parse_args()
    report = verify(ROOT / args.directory, args.expected_cases)
    args.output.write_text(json.dumps(report, indent=2) + "\n")
    print(f"PASS: {report['case_count']} cases, source {report['source_commit']}")
