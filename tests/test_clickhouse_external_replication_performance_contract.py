"""Offline contracts for the external-publication performance producer."""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest

from tests.integration.clickhouse_cluster import evidence_identity
from tests.integration.clickhouse_cluster import external_replication_performance_evidence as evidence
from tests.integration.clickhouse_cluster import external_replication_performance_validation as validation
from tests.integration.clickhouse_cluster.external_replication_performance_evidence import (
    EXTERNAL_PERFORMANCE_BUDGET,
    EXTERNAL_PERFORMANCE_COMMAND,
    EXTERNAL_PERFORMANCE_TEST_NODEID,
    validate_external_performance_receipt,
    verify_external_performance_receipt,
    write_external_performance_receipt,
)
from tests.integration.clickhouse_cluster.external_replication_performance_validation import SCHEMA_V1, SCHEMA_V2
from tests.integration.clickhouse_cluster.test_clickhouse_external_replication_performance_live import (
    _benchmark_canonical_digest,
)

ROOT = Path(__file__).resolve().parents[1]


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
    assert written["schema_version"] == SCHEMA_V2
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
    for mutation in (
        "commit",
        "budget",
        "section_secret",
        "top_level_secret",
        "digest",
        "environment",
        "environment_endpoint",
        "missing_median",
        "forged_median",
        "forged_maximum",
        "not_finite",
        "boolean_number",
        "float_integer",
        "boolean_warmup",
        "float_rows",
        "boolean_columns",
        "float_physical_rows",
        "tiny_median_drift",
    ):
        tampered = deepcopy(valid)
        if mutation == "commit":
            tampered["source_commit"] = "d" * 40
        elif mutation == "budget":
            tampered["member_fanout"]["logical_rows"] = 1
        elif mutation == "section_secret":
            tampered["canonical_digest"]["detail"] = "sensitive-row-value"
        elif mutation == "top_level_secret":
            tampered["credential"] = "plaintext-secret"
        elif mutation == "digest":
            tampered["canonical_digest"]["digest"] = "not-a-sha256"
        elif mutation == "environment":
            tampered["environment"]["clickhouse_version"] = "forged"
        elif mutation == "environment_endpoint":
            tampered["environment"]["endpoint"] = "https://user:secret@example.invalid"
        elif mutation == "missing_median":
            del tampered["canonical_digest"]["median_seconds"]
        elif mutation == "forged_median":
            tampered["canonical_digest"]["median_seconds"] = 0.2
        elif mutation == "forged_maximum":
            tampered["member_fanout"]["max_seconds"] = 0.5
        elif mutation == "not_finite":
            tampered["member_fanout"]["trial_seconds"][0] = float("nan")
        elif mutation == "boolean_number":
            tampered["canonical_digest"]["rows"] = True
        elif mutation == "float_integer":
            tampered["member_fanout"]["max_source_bytes"] = 1_048_576.0
        elif mutation == "boolean_warmup":
            tampered["canonical_digest"]["warmups"] = True
        elif mutation == "float_rows":
            tampered["canonical_digest"]["rows"] = 100_000.0
        elif mutation == "boolean_columns":
            tampered["member_fanout"]["columns"] = True
        elif mutation == "float_physical_rows":
            tampered["member_fanout"]["physical_rows"] = 20_000.0
        else:
            tampered["canonical_digest"]["median_seconds"] += 0.0000005
        with pytest.raises(ValueError, match="failed validation"):
            _validate(tampered, budget_path)


def test_performance_receipt_reads_bounded_historical_v1_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    budget_path = tmp_path / "budget.json"
    budget_path.write_text(EXTERNAL_PERFORMANCE_BUDGET.read_text(encoding="utf-8"), encoding="utf-8")
    canonical, fanout = _passing_sections()
    fanout.pop("max_source_bytes")
    payload = _valid_payload(canonical=canonical, fanout=fanout)
    payload["schema_version"] = SCHEMA_V1
    payload.pop("observation")

    monkeypatch.setattr(validation, "_pinned_clickhouse_version", lambda: "future-compose-version")
    monkeypatch.setattr(
        validation,
        "canonical_rows_digest",
        lambda rows: (_ for _ in ()).throw(AssertionError("v1 must not use the future digest algorithm")),
    )
    _validate(payload, budget_path)

    mixed = deepcopy(payload)
    mixed["member_fanout"]["max_source_bytes"] = "v2-only"
    with pytest.raises(ValueError, match="failed validation"):
        _validate(mixed, budget_path)


def test_v1_fixture_digest_uses_schema_owned_file_manifest(monkeypatch: pytest.MonkeyPatch) -> None:
    requested: list[str] = []

    def git(*args: str, text: bool = True) -> bytes:
        del text
        assert args[0] == "show"
        requested.append(args[1])
        return args[1].encode()

    monkeypatch.setattr(evidence_identity, "_git", git)

    digest = evidence_identity.performance_fixture_digest(
        "a" * 40,
        SCHEMA_V1,
    )

    assert len(digest) == 64
    assert requested
    assert not any(
        path.endswith(
            (
                "evidence_identity.py",
                "external_replication_performance_evidence.py",
                "external_replication_performance_validation.py",
            )
        )
        for path in requested
    )


def test_historical_v1_binding_matches_immutable_release_observation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        evidence_identity,
        "_git",
        lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("historical binding must not read Git")),
    )

    binding = evidence_identity.historical_performance_binding(
        "5451b1989796258a022c21cdd3059e442f81b1dc",
        SCHEMA_V1,
    )

    assert binding is not None
    assert binding.source_tree == "b5c90123c42fef1a7059a5bcff450ff4e42122e9"
    assert binding.fixture_digest == "a6dfa6864c7181a41666a470866f86b9058c3622ca0c681c6a2edfcd7d3f6d7c"
    assert hashlib.sha256(binding.benchmark_config).hexdigest() == (
        "5caef54d4e33c1f0b6cf2c552c1506f319c7d7c253af9e211e8e37dcb4650871"
    )


def test_historical_v1_receipt_verifies_end_to_end_against_trusted_commit() -> None:
    receipt_path = Path("tests/fixtures/release_evidence/clickhouse-external-benchmark-v1-5451b198.json")
    receipt = verify_external_performance_receipt(
        expected_source_commit="5451b1989796258a022c21cdd3059e442f81b1dc",
        receipt_path=receipt_path,
    )

    assert receipt["schema_version"] == SCHEMA_V1
    assert receipt["status"] == "PASS"
    current_commit = subprocess.run(
        ("git", "rev-parse", "HEAD"),
        check=True,
        capture_output=True,
        text=True,
    ).stdout.strip()
    with pytest.raises(ValueError, match="failed validation"):
        verify_external_performance_receipt(
            expected_source_commit=current_commit,
            receipt_path=receipt_path,
        )


def test_historical_v1_binding_rejects_tampered_budget_bytes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding_path = Path("tests/fixtures/release_evidence/clickhouse-external-benchmark-v1-source-binding-5451b198.json")
    payload = json.loads(binding_path.read_text(encoding="utf-8"))
    payload["benchmark_config_base64"] = payload["benchmark_config_base64"][:-4] + "AAAA"
    tampered_path = tmp_path / "binding.json"
    tampered_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(evidence_identity, "_PERFORMANCE_V1_SOURCE_BINDING", tampered_path)

    with pytest.raises(ValueError, match="digest does not match"):
        evidence_identity.historical_performance_binding(
            "5451b1989796258a022c21cdd3059e442f81b1dc",
            SCHEMA_V1,
        )


@pytest.mark.parametrize(("field", "tampered_value"), (("sha256", "f" * 64), ("size", 1)))
@pytest.mark.parametrize("fixture_index", (0, 1, 2, 3, 5, 6, 7, 8, 9))
def test_historical_v1_binding_rejects_tampered_non_budget_inventory(
    fixture_index: int,
    field: str,
    tampered_value: str | int,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    binding_path = Path("tests/fixtures/release_evidence/clickhouse-external-benchmark-v1-source-binding-5451b198.json")
    payload = json.loads(binding_path.read_text(encoding="utf-8"))
    payload["fixture_files"][fixture_index][field] = tampered_value
    tampered_path = tmp_path / "binding.json"
    tampered_path.write_text(json.dumps(payload), encoding="utf-8")
    monkeypatch.setattr(evidence_identity, "_PERFORMANCE_V1_SOURCE_BINDING", tampered_path)

    with pytest.raises(ValueError, match="reviewed source identities"):
        evidence_identity.historical_performance_binding(
            "5451b1989796258a022c21cdd3059e442f81b1dc",
            SCHEMA_V1,
        )


def test_historical_v1_verification_passes_in_branch_only_clone(tmp_path: Path) -> None:
    bundle_path = tmp_path / "candidate.bundle"
    clone_path = tmp_path / "candidate"
    subprocess.run(
        ("git", "bundle", "create", str(bundle_path), "HEAD"),
        cwd=ROOT,
        check=True,
        capture_output=True,
        text=True,
    )
    subprocess.run(
        ("git", "clone", "--quiet", str(bundle_path), str(clone_path)),
        check=True,
        capture_output=True,
        text=True,
    )
    orphan = subprocess.run(
        ("git", "cat-file", "-e", "5451b1989796258a022c21cdd3059e442f81b1dc^{commit}"),
        cwd=clone_path,
        check=False,
        capture_output=True,
        text=True,
    )
    assert orphan.returncode != 0
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join((str(clone_path / "src"), str(clone_path)))
    completed = subprocess.run(
        (
            sys.executable,
            "-m",
            "pytest",
            "tests/test_clickhouse_external_replication_performance_contract.py::test_historical_v1_binding_matches_immutable_release_observation",
            "tests/test_clickhouse_external_replication_performance_contract.py::test_historical_v1_receipt_verifies_end_to_end_against_trusted_commit",
            "-q",
        ),
        cwd=clone_path,
        env=environment,
        check=False,
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert completed.returncode == 0, completed.stdout + completed.stderr


@pytest.mark.parametrize(
    "mutable_or_noncanonical_ref",
    (
        "HEAD",
        "5451b198",
        "5451B1989796258A022C21CDD3059E442F81B1DC",
        "5451b1989796258a022c21cdd3059e442f81b1dc^{commit}",
    ),
)
def test_historical_receipt_rejects_mutable_or_noncanonical_source_refs(
    mutable_or_noncanonical_ref: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def unexpected_git(*args: str, text: bool = True) -> str:
        del args, text
        raise AssertionError("invalid commit identity must fail before invoking git")

    monkeypatch.setattr(evidence_identity, "_git", unexpected_git)

    with pytest.raises(ValueError, match="full lowercase 40-character SHA-1"):
        verify_external_performance_receipt(
            expected_source_commit=mutable_or_noncanonical_ref,
            receipt_path=Path("must-not-be-read.json"),
        )


def test_historical_receipt_rejects_nonexistent_full_source_sha() -> None:
    with pytest.raises(ValueError, match="resolve to an existing commit"):
        verify_external_performance_receipt(
            expected_source_commit="f" * 40,
            receipt_path=Path("must-not-be-read.json"),
        )


def test_historical_source_reads_ignore_git_replace_refs(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def git(*args: str) -> str:
        return subprocess.run(
            ("git", *args),
            cwd=tmp_path,
            check=True,
            capture_output=True,
            text=True,
        ).stdout.strip()

    git("init", "--quiet")
    git("config", "user.name", "dpone test")
    git("config", "user.email", "dpone-test@example.invalid")
    fixture = tmp_path / "fixture.txt"
    fixture.write_text("original\n", encoding="utf-8")
    git("add", "fixture.txt")
    git("commit", "--quiet", "-m", "original")
    original_commit = git("rev-parse", "HEAD")
    original_tree = git("rev-parse", "HEAD^{tree}")

    fixture.write_text("replacement\n", encoding="utf-8")
    git("commit", "--quiet", "-am", "replacement")
    replacement_commit = git("rev-parse", "HEAD")
    replacement_tree = git("rev-parse", "HEAD^{tree}")
    git("replace", original_commit, replacement_commit)

    assert replacement_tree != original_tree
    assert git("rev-parse", f"{original_commit}^{{tree}}") == replacement_tree
    monkeypatch.chdir(tmp_path)
    assert evidence_identity.tracked_source_tree(original_commit) == original_tree
    assert evidence_identity.tracked_file_bytes(original_commit, Path("fixture.txt")) == b"original\n"


def test_performance_receipt_v2_requires_observation_and_source_budget(tmp_path: Path) -> None:
    budget_path = tmp_path / "budget.json"
    budget_path.write_text(EXTERNAL_PERFORMANCE_BUDGET.read_text(encoding="utf-8"), encoding="utf-8")
    canonical, fanout = _passing_sections()
    payload = _valid_payload(canonical=canonical, fanout=fanout)

    for field, section in (("observation", payload), ("max_source_bytes", payload["member_fanout"])):
        tampered = deepcopy(payload)
        target = tampered if section is payload else tampered["member_fanout"]
        del target[field]
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
        "digest": "c4370bf4ec0e4f1ca8df2645c7785a79d7ee4a8970b5fb20977c3dcbf8aa4655",
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


def _valid_payload(*, canonical: dict[str, Any], fanout: dict[str, Any]) -> dict[str, Any]:
    budget_bytes = EXTERNAL_PERFORMANCE_BUDGET.read_bytes()
    return {
        "schema_version": SCHEMA_V2,
        "status": "PASS",
        "evidence_scope": "local_synthetic",
        "production_certification": "UNVERIFIED",
        "source_commit": "a" * 40,
        "source_tree": "b" * 40,
        "tracked_tree_status": "clean",
        "fixture_digest": "c" * 64,
        "benchmark_config_sha256": hashlib.sha256(budget_bytes).hexdigest(),
        "environment": {
            "clickhouse_version": "24.8.14.39",
            "python_version": "3.13.7",
            "platform": "test-platform",
            "cpu_count": 8,
        },
        "observation": _observation(),
        "canonical_digest": canonical,
        "member_fanout": fanout,
    }


def _observation() -> dict[str, str]:
    return {
        "observation_id": "e" * 32,
        "started_at": "2026-09-19T12:00:00+00:00",
        "finished_at": "2026-09-19T12:01:00+00:00",
        "command": EXTERNAL_PERFORMANCE_COMMAND,
        "test_nodeid": EXTERNAL_PERFORMANCE_TEST_NODEID,
    }
