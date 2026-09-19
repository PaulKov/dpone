from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

from tools.clickhouse_external_publication_evidence import SCENARIOS, produce_receipt


def test_evidence_producer_binds_commit_fixture_and_independent_scenarios(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    test_module = tmp_path / "tests/test_clickhouse_external_replication_runtime.py"
    test_module.parent.mkdir()
    test_module.write_text("synthetic fixture\n", encoding="utf-8")
    monkeypatch.setattr(
        "tools.clickhouse_external_publication_evidence._git",
        lambda root, *args: "a" * 40,
    )
    invoked: list[str] = []

    def runner(command: tuple[str, ...], root: Path) -> int:
        del root
        invoked.append(command[-2])
        return 0

    receipt = produce_receipt(tmp_path, runner=runner)

    assert receipt["source_commit"] == "a" * 40
    assert receipt["status"] == "PASS"
    assert receipt["evidence_scope"] == "mocked_in_process"
    assert receipt["local_synthetic_certification"] == "UNVERIFIED"
    assert receipt["live_external_certification"] == "UNVERIFIED"
    assert [item["scenario"] for item in receipt["scenarios"]] == [name for name, _ in SCENARIOS]
    assert invoked == [node for _, node in SCENARIOS]
    assert receipt["fixture_digest"] != hashlib.sha256(b"").hexdigest()


def test_evidence_producer_records_failure_without_leaking_runner_output(
    tmp_path: Path,
    monkeypatch: Any,
) -> None:
    test_module = tmp_path / "tests/test_clickhouse_external_replication_runtime.py"
    test_module.parent.mkdir()
    test_module.write_text("synthetic fixture\n", encoding="utf-8")
    monkeypatch.setattr(
        "tools.clickhouse_external_publication_evidence._git",
        lambda root, *args: "b" * 40,
    )

    receipt = produce_receipt(tmp_path, runner=lambda command, root: int("lost_member_stage_ack" in command[-2]))
    serialized = json.dumps(receipt)

    assert receipt["status"] == "FAIL"
    assert {item["status"] for item in receipt["scenarios"]} == {"PASS", "FAIL"}
    for forbidden in ("stdout", "stderr", "hostname", "endpoint", "credential", "row_value", str(tmp_path)):
        assert forbidden not in serialized
