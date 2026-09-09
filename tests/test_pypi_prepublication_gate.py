from __future__ import annotations

import hashlib
import json
from copy import deepcopy
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from tests.pypi_prepublication_test_support import (
    NAMES,
    VERSION,
)
from tests.pypi_prepublication_test_support import (
    github_environment as _github_environment,
)
from tests.pypi_prepublication_test_support import (
    load_gate as _load_module,
)
from tests.pypi_prepublication_test_support import (
    public_fetcher as _fetcher,
)
from tests.pypi_prepublication_test_support import (
    public_row as _row,
)
from tests.pypi_prepublication_test_support import (
    publication_context as _context,
)
from tests.pypi_prepublication_test_support import (
    write_inventory as _write_inventory,
)


def test_fresh_public_state_passes_with_deterministic_closed_receipt(tmp_path: Path) -> None:
    module = _load_module()
    inventory, dist, _ = _write_inventory(tmp_path)

    first = module.evaluate_prepublication(
        inventory, dist, expected_version=VERSION, context=_context(module), fetcher=lambda *_: None
    )
    second = module.evaluate_prepublication(
        inventory, dist, expected_version=VERSION, context=_context(module), fetcher=lambda *_: None
    )

    payload = first.to_payload()
    assert module.canonical_bytes(payload) == module.canonical_bytes(second.to_payload())
    assert set(payload) == {
        "artifacts",
        "blockers",
        "candidate_inventory_sha256",
        "decision",
        "expected_version",
        "index_url",
        "observations",
        "publication",
        "public_observation_sha256",
        "schema_version",
        "status",
        "summary",
    }
    assert payload["summary"] == {
        "candidate_count": 8,
        "existing_exact_count": 0,
        "pending_upload_count": 8,
        "publication_mode": "fresh",
    }
    assert payload["publication"] == _context(module).to_payload()
    assert payload["candidate_inventory_sha256"] == hashlib.sha256(inventory.read_bytes()).hexdigest()
    assert [item["release_state"] for item in payload["observations"]] == ["VERSION_ABSENT"] * 4
    assert [item["package"] for item in payload["observations"]] == sorted(
        {"apache-airflow-providers-dpone", "dpone", "dpone-airflow-pack", "dpone-native-accel"}
    )
    assert all(
        item["endpoint_url"] == f"https://pypi.org/pypi/{item['package']}/{VERSION}/json"
        and item["published_filenames"] == []
        for item in payload["observations"]
    )
    assert (
        payload["public_observation_sha256"]
        == hashlib.sha256(
            module.canonical_bytes({"artifacts": payload["artifacts"], "observations": payload["observations"]})
        ).hexdigest()
    )
    assert {row["classification"] for row in payload["artifacts"]} == {"PENDING_UPLOAD"}


def test_exact_subset_and_complete_set_are_safe_resume_states(tmp_path: Path) -> None:
    module = _load_module()
    inventory, dist, artifacts = _write_inventory(tmp_path)

    subset = module.evaluate_prepublication(
        inventory,
        dist,
        expected_version=VERSION,
        context=_context(module),
        fetcher=_fetcher(artifacts, set(NAMES[:3])),
    ).to_payload()
    complete = module.evaluate_prepublication(
        inventory,
        dist,
        expected_version=VERSION,
        context=_context(module),
        fetcher=_fetcher(artifacts, set(NAMES)),
    ).to_payload()

    assert subset["summary"] == {
        "candidate_count": 8,
        "existing_exact_count": 3,
        "pending_upload_count": 5,
        "publication_mode": "resume",
    }
    assert complete["summary"]["publication_mode"] == "idempotent"
    assert complete["summary"]["existing_exact_count"] == 8


class _StabilityClock:
    def __init__(self) -> None:
        self.monotonic = 0.0
        self.wall = datetime(2026, 8, 23, tzinfo=UTC)

    def clock(self) -> float:
        return self.monotonic

    def now(self) -> datetime:
        return self.wall + timedelta(seconds=self.monotonic)

    def sleep(self, seconds: float) -> None:
        self.monotonic += seconds


class _RoundFetcher:
    def __init__(self, fetchers: list[Any], *, packages_per_round: int = 4) -> None:
        self.fetchers = fetchers
        self.packages_per_round = packages_per_round
        self.calls = 0

    def __call__(self, package: str, version: str) -> Any:
        round_index = min(self.calls // self.packages_per_round, len(self.fetchers) - 1)
        self.calls += 1
        return self.fetchers[round_index](package, version)


def test_postpublication_observer_waits_for_two_identical_exact_subsets(tmp_path: Path) -> None:
    module = _load_module()
    inventory, dist, artifacts = _write_inventory(tmp_path)
    clock = _StabilityClock()
    subset = _fetcher(artifacts, set(NAMES[:3]))
    fetcher = _RoundFetcher([lambda *_: None, subset, subset])

    report, evidence = module.observe_stable_prepublication(
        inventory,
        dist,
        expected_version=VERSION,
        context=_context(module),
        required_stable_observations=2,
        timeout_seconds=300,
        poll_interval_seconds=15,
        fetcher=fetcher,
        clock=clock.clock,
        sleeper=clock.sleep,
        utcnow=clock.now,
    )

    assert report.to_payload()["summary"]["existing_exact_count"] == 3
    assert evidence.attempts == 3
    assert evidence.stable_count == evidence.required_stable_observations == 2
    assert evidence.elapsed_seconds == 30
    assert evidence.started_at == "2026-08-23T00:00:00+00:00"
    assert evidence.ended_at == "2026-08-23T00:00:30+00:00"
    assert len(evidence.public_observation_sha256_timeline) == 3
    assert evidence.public_observation_sha256_timeline[-2:] == (
        evidence.public_observation_sha256,
        evidence.public_observation_sha256,
    )


def test_postpublication_observer_fails_closed_when_exact_subset_never_stabilizes(tmp_path: Path) -> None:
    module = _load_module()
    inventory, dist, artifacts = _write_inventory(tmp_path)
    clock = _StabilityClock()
    subset = _fetcher(artifacts, set(NAMES[:3]))
    fetcher = _RoundFetcher([lambda *_: None, subset, lambda *_: None])

    with pytest.raises(module.PrepublicationStabilityError, match="PUBLIC_STATE_UNSTABLE") as captured:
        module.observe_stable_prepublication(
            inventory,
            dist,
            expected_version=VERSION,
            context=_context(module),
            required_stable_observations=2,
            timeout_seconds=30,
            poll_interval_seconds=15,
            fetcher=fetcher,
            clock=clock.clock,
            sleeper=clock.sleep,
            utcnow=clock.now,
        )

    assert captured.value.evidence.attempts == 3
    assert captured.value.evidence.stable_count == 1
    assert captured.value.evidence.elapsed_seconds == 30
    assert len(captured.value.evidence.public_observation_sha256_timeline) == 3
    assert captured.value.last_safe_report is not None
    assert captured.value.last_safe_report.to_payload()["summary"]["existing_exact_count"] == 0


@pytest.mark.parametrize(
    ("mutation", "error"),
    [
        ({"digests": {"sha256": "0" * 64}}, "PUBLIC_DIGEST_MISMATCH"),
        ({"size": 1}, "PUBLIC_SIZE_INVALID"),
        ({"yanked": True}, "PUBLIC_YANKED_OR_UNKNOWN"),
        ({"yanked": None}, "PUBLIC_YANKED_OR_UNKNOWN"),
    ],
)
def test_conflicting_public_identity_fails_before_upload(
    tmp_path: Path,
    mutation: dict[str, Any],
    error: str,
) -> None:
    module = _load_module()
    inventory, dist, artifacts = _write_inventory(tmp_path)
    target = NAMES[0]

    with pytest.raises(module.PrepublicationGateError, match=error):
        module.evaluate_prepublication(
            inventory,
            dist,
            expected_version=VERSION,
            context=_context(module),
            fetcher=_fetcher(artifacts, {target}, mutations={target: mutation}),
        )


def test_unexpected_or_duplicate_public_filename_fails_closed(tmp_path: Path) -> None:
    module = _load_module()
    inventory, dist, artifacts = _write_inventory(tmp_path)
    package = artifacts[0]["package"]
    expected_row = _row(artifacts[0])
    unexpected = deepcopy(expected_row)
    unexpected["filename"] = "foreign-0.74.0.tar.gz"

    for rows, error in (
        ([unexpected], "PUBLIC_FILE_UNEXPECTED"),
        ([expected_row, deepcopy(expected_row)], "PUBLIC_FILENAME_INVALID"),
    ):
        with pytest.raises(module.PrepublicationGateError, match=error):
            module.evaluate_prepublication(
                inventory,
                dist,
                expected_version=VERSION,
                context=_context(module),
                fetcher=lambda current, _version, rows=rows: (
                    {"info": {"name": package, "version": VERSION}, "urls": rows} if current == package else None
                ),
            )


@pytest.mark.parametrize(
    ("payload", "error"),
    [
        ({"info": {"name": "dpone", "version": VERSION}, "urls": []}, "PUBLIC_RELEASE_INVALID"),
        ({"info": {"name": "foreign", "version": VERSION}, "urls": [{}]}, "PUBLIC_PROJECT_MISMATCH"),
        ({"info": {"name": "dpone", "version": VERSION}, "urls": ["invalid"]}, "PUBLIC_FILE_SHAPE_INVALID"),
    ],
)
def test_malformed_public_release_fails_closed(tmp_path: Path, payload: dict[str, Any], error: str) -> None:
    module = _load_module()
    inventory, dist, _ = _write_inventory(tmp_path)

    with pytest.raises(module.PrepublicationGateError, match=error):
        module.evaluate_prepublication(
            inventory,
            dist,
            expected_version=VERSION,
            context=_context(module),
            fetcher=lambda package, _version: payload if package == "dpone" else None,
        )


def test_publication_context_requires_exact_github_identity() -> None:
    module = _load_module()
    arguments = {
        "repository": "PaulKov/dpone",
        "commit_sha": "a" * 40,
        "release": f"v{VERSION}",
        "workflow_path": ".github/workflows/release.yml",
        "workflow_run_id": "123",
        "workflow_run_attempt": "1",
        "expected_version": VERSION,
    }

    assert module.publication_context(**arguments, environment=_github_environment()) == _context(module)
    for field, value in (("release", "v0.74.1"), ("workflow_run_id", "0"), ("commit_sha", "a" * 64)):
        with pytest.raises(module.PrepublicationGateError, match="CONTEXT_INVALID"):
            module.publication_context(**(arguments | {field: value}), environment=_github_environment())
    mismatched = _github_environment() | {"GITHUB_RUN_ATTEMPT": "2"}
    with pytest.raises(module.PrepublicationGateError, match="GITHUB_CONTEXT_MISMATCH"):
        module.publication_context(**arguments, environment=mismatched)


def test_cli_failure_writes_closed_receipt_without_untrusted_filename(tmp_path: Path, capsys, monkeypatch) -> None:
    module = _load_module()
    inventory, dist, _ = _write_inventory(tmp_path)
    (dist / NAMES[0]).write_bytes(b"tampered")
    receipt = tmp_path / "receipt.json"
    for key, value in _github_environment().items():
        monkeypatch.setenv(key, value)

    assert (
        module.main(
            [
                "--candidate-inventory",
                str(inventory),
                "--dist-dir",
                str(dist),
                "--expected-version",
                VERSION,
                "--repository",
                "PaulKov/dpone",
                "--commit-sha",
                "a" * 40,
                "--release",
                f"v{VERSION}",
                "--workflow-path",
                ".github/workflows/release.yml",
                "--workflow-run-id",
                "123",
                "--workflow-run-attempt",
                "1",
                "--output",
                str(receipt),
            ]
        )
        == 1
    )
    payload = json.loads(receipt.read_text(encoding="ascii"))
    assert payload["status"] == "FAIL"
    assert payload["decision"] == "NO-GO"
    assert payload["summary"]["publication_mode"] == "blocked"
    assert NAMES[0] not in capsys.readouterr().err
    assert "filename_ref=sha256:" in payload["blockers"][0]
