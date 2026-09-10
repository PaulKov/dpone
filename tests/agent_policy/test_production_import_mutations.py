"""Keep the production source policy in the ordinary non-live test suite."""

from pathlib import Path

from tools.agent_policy.import_mutation_gate import scan


def test_production_roots_have_no_detected_import_mutations() -> None:
    report = scan(Path(__file__).resolve().parents[2])

    assert report["files_scanned"] > 0
    assert report["status"] == "PASS", report
