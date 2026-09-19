"""Offline contracts for the external-publication performance producer."""

from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from tests.integration.clickhouse_cluster import external_replication_performance_evidence as evidence
from tests.integration.clickhouse_cluster.external_replication_performance_evidence import (
    EXTERNAL_PERFORMANCE_BUDGET,
    EXTERNAL_PERFORMANCE_COMMAND,
    EXTERNAL_PERFORMANCE_TEST_NODEID,
    validate_external_performance_receipt,
    write_external_performance_receipt,
)
from tests.integration.clickhouse_cluster.test_clickhouse_external_replication_performance_live import (
    _benchmark_canonical_digest,
)


def test_external_performance_budget_is_bounded_and_internally_consistent() -> None:
    budget = json.loads(EXTERNAL_PERFORMANCE_BUDGET.read_text(encoding="utf-8"))

    assert budget["schema_version"] == "dpone.clickhouse.external-publication-benchmark-config.v1"
    assert budget["canonical_digest"] == {
        "rows": 100_000,
        "columns": 3,
        "warmups": 1,
        "trials": 5,
        "max_trial_seconds": 2.0,
    }
    fanout = budget["member_fanout"]
    assert fanout["members"] == 2
    assert fanout["logical_rows"] == 10_000
    assert fanout["physical_rows"] == 20_000
    assert fanout["columns"] == 1
    assert fanout["warmups"] == 1
    assert fanout["trials"] == 3
    assert fanout["max_trial_seconds"] == 30.0
    assert fanout["max_source_bytes"] == 1_048_576


def test_canonical_benchmark_producer_reports_stable_digest_and_timings() -> None:
    result = _benchmark_canonical_digest(
        {
            "rows": 100,
            "columns": 3,
            "warmups": 1,
            "trials": 2,
            "max_trial_seconds": 2.0,
        }
    )

    assert result["status"] == "PASS"
    assert result["stable_across_trials"] is True
    assert len(result["digest"]) == 64
    assert len(result["trial_seconds"]) == 2


def test_canonical_benchmark_reports_budget_breach_and_rejects_zero_trials() -> None:
    breached = _benchmark_canonical_digest(
        {
            "rows": 100,
            "columns": 3,
            "warmups": 0,
            "trials": 1,
            "max_trial_seconds": 0.000000000001,
        }
    )

    assert breached["status"] == "FAIL"
    with pytest.raises(ValueError, match="trials"):
        _benchmark_canonical_digest(
            {
                "rows": 100,
                "columns": 3,
                "warmups": 0,
                "trials": 0,
                "max_trial_seconds": 2.0,
            }
        )


def test_performance_receipt_is_exact_bound_and_never_overwritten(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    budget_path = tmp_path / "budget.json"
    budget_path.write_text(EXTERNAL_PERFORMANCE_BUDGET.read_text(encoding="utf-8"), encoding="utf-8")
    receipt_path = tmp_path / "receipt.json"
    monkeypatch.setattr(evidence, "EXTERNAL_PERFORMANCE_BUDGET", budget_path)
    monkeypatch.setattr(evidence, "EXTERNAL_PERFORMANCE_RECEIPT", receipt_path)
    monkeypatch.setattr(evidence, "source_binding", lambda: ("a" * 40, "b" * 40))
    monkeypatch.setattr(evidence, "fixture_digest", lambda commit: "c" * 64)

    written = write_external_performance_receipt(
        canonical_digest=_passing_sections()[0],
        member_fanout=_passing_sections()[1],
        observation=_observation(),
        server_version="24.8.14.39",
    )
    original = receipt_path.read_bytes()

    assert written["status"] == "PASS"
    assert written["source_commit"] == "a" * 40
    assert written["source_tree"] == "b" * 40
    assert written["fixture_digest"] == "c" * 64
    assert written["benchmark_config_sha256"] == hashlib.sha256(budget_path.read_bytes()).hexdigest()
    with pytest.raises(FileExistsError):
        write_external_performance_receipt(
            canonical_digest=_passing_sections()[0],
            member_fanout=_passing_sections()[1],
            observation=_observation(),
            server_version="24.8.14.39",
        )
    assert receipt_path.read_bytes() == original


def test_performance_receipt_fails_closed_and_rejects_tampering(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    budget_path = tmp_path / "budget.json"
    budget_path.write_text(EXTERNAL_PERFORMANCE_BUDGET.read_text(encoding="utf-8"), encoding="utf-8")
    receipt_path = tmp_path / "receipt.json"
    monkeypatch.setattr(evidence, "EXTERNAL_PERFORMANCE_BUDGET", budget_path)
    monkeypatch.setattr(evidence, "EXTERNAL_PERFORMANCE_RECEIPT", receipt_path)
    monkeypatch.setattr(evidence, "source_binding", lambda: ("a" * 40, "b" * 40))
    monkeypatch.setattr(evidence, "fixture_digest", lambda commit: "c" * 64)
    canonical, fanout = _passing_sections()
    canonical["status"] = "FAIL"
    canonical["stable_across_trials"] = False

    failed = write_external_performance_receipt(
        canonical_digest=canonical,
        member_fanout=fanout,
        observation=_observation(),
        server_version="24.8.14.39",
    )
    assert failed["status"] == "FAIL"

    passing_canonical, passing_fanout = _passing_sections()
    valid = deepcopy(failed)
    valid["status"] = "PASS"
    valid["canonical_digest"] = passing_canonical
    valid["member_fanout"] = passing_fanout
    _validate(valid, budget_path)
    for mutation in ("commit", "budget", "secret"):
        tampered = deepcopy(valid)
        if mutation == "commit":
            tampered["source_commit"] = "d" * 40
        elif mutation == "budget":
            tampered["member_fanout"]["logical_rows"] = 1
        else:
            tampered["canonical_digest"]["detail"] = "sensitive-row-value"
        with pytest.raises(ValueError, match="failed validation"):
            _validate(tampered, budget_path)


def _validate(payload: dict[str, Any], budget_path: Path) -> None:
    validate_external_performance_receipt(
        payload,
        source_commit="a" * 40,
        source_tree="b" * 40,
        fixture_digest="c" * 64,
        benchmark_config_sha256=hashlib.sha256(budget_path.read_bytes()).hexdigest(),
        budget=json.loads(budget_path.read_text(encoding="utf-8")),
    )


def _passing_sections() -> tuple[dict[str, Any], dict[str, Any]]:
    budget = json.loads(EXTERNAL_PERFORMANCE_BUDGET.read_text(encoding="utf-8"))
    canonical_budget = budget["canonical_digest"]
    fanout_budget = budget["member_fanout"]
    canonical = {
        "rows": canonical_budget["rows"],
        "columns": canonical_budget["columns"],
        "warmups": canonical_budget["warmups"],
        "trials": canonical_budget["trials"],
        "trial_seconds": [0.1] * canonical_budget["trials"],
        "median_seconds": 0.1,
        "max_seconds": 0.1,
        "budget_max_seconds": canonical_budget["max_trial_seconds"],
        "digest": "d" * 64,
        "stable_across_trials": True,
        "status": "PASS",
    }
    fanout = {
        "members": fanout_budget["members"],
        "logical_rows": fanout_budget["logical_rows"],
        "physical_rows": fanout_budget["physical_rows"],
        "columns": fanout_budget["columns"],
        "warmups": fanout_budget["warmups"],
        "trials": fanout_budget["trials"],
        "trial_seconds": [1.0] * fanout_budget["trials"],
        "median_seconds": 1.0,
        "max_seconds": 1.0,
        "budget_max_seconds": fanout_budget["max_trial_seconds"],
        "max_source_bytes": fanout_budget["max_source_bytes"],
        "per_member_row_counts": [fanout_budget["logical_rows"]] * fanout_budget["members"],
        "content_digests_equal": True,
        "publication_phase": "COMMITTED",
        "cleanup_proven": True,
        "status": "PASS",
    }
    return canonical, fanout


def _observation() -> dict[str, str]:
    return {
        "observation_id": "e" * 32,
        "started_at": "2026-09-19T12:00:00+00:00",
        "finished_at": "2026-09-19T12:01:00+00:00",
        "command": EXTERNAL_PERFORMANCE_COMMAND,
        "test_nodeid": EXTERNAL_PERFORMANCE_TEST_NODEID,
    }
