from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.adapters.airflow_cache_status_files import PosixAirflowCacheStatusFileStorage
from dpone.adapters.deployment_cache_files import AtomicJsonWriteError
from dpone.cli import main as cli_main
from dpone.contracts.deployment_cache_retention_state import canonical_digest
from dpone.gitops.airflow_cache_status_schema_validator import (
    GitOpsAirflowCacheStatusSchemaValidator,
)
from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.services.airflow_cache_status_publication import (
    CACHE_STATUS_MAX_BYTES,
    AirflowCacheStatusPublicationRequest,
    AirflowCacheStatusPublisher,
)

_NOW = "2026-08-03T12:00:00Z"


def _publisher(
    storage: PosixAirflowCacheStatusFileStorage | None = None,
) -> AirflowCacheStatusPublisher:
    return AirflowCacheStatusPublisher(
        validator=GitOpsAirflowCacheStatusSchemaValidator(),
        storage=storage or PosixAirflowCacheStatusFileStorage(),
        now=lambda: _NOW,
    )


def _plan_payload() -> dict[str, object]:
    digest = "sha256:" + "a" * 64
    payload: dict[str, object] = {
        "schema": "dpone.deployment-cache-retention-plan.v1",
        "environment": "dev",
        "current_deployment_id": digest,
        "recovery_revision": "sha256:" + "c" * 64,
        "protected_deployment_ids": [digest],
        "items": [],
        "delete_candidates": [],
    }
    payload["plan_sha256"] = canonical_digest(payload)
    return payload


def _root(tmp_path: Path) -> Path:
    root = tmp_path / "status"
    root.mkdir(mode=0o700)
    return root


def _write_source(root: Path, name: str, value: object) -> Path:
    path = root / name
    if isinstance(value, bytes):
        path.write_bytes(value)
    else:
        path.write_text(json.dumps(value), encoding="utf-8")
    path.chmod(0o600)
    return path


def _request(root: Path) -> AirflowCacheStatusPublicationRequest:
    return AirflowCacheStatusPublicationRequest(
        status_root=root,
        source_name=".plan.tmp",
        target_name="last-retention-plan.json",
        failure_marker_name="last-retention-plan-publication-failure.json",
        expected_schema="dpone.deployment-cache-retention-plan.v1",
    )


def test_valid_status_atomically_replaces_target_and_clears_failure_marker(tmp_path: Path) -> None:
    root = _root(tmp_path)
    source = _write_source(root, ".plan.tmp", _plan_payload())
    marker = _write_source(root, "last-retention-plan-publication-failure.json", {"stale": True})

    report = _publisher().publish(_request(root))

    assert report.passed is True
    assert report.status == "published"
    assert report.exit_code == 0
    assert report.source_sha256.startswith("sha256:")
    assert report.target_sha256.startswith("sha256:")
    assert json.loads((root / "last-retention-plan.json").read_text(encoding="utf-8")) == _plan_payload()
    assert not marker.exists()
    assert source.exists()
    assert (
        GitOpsSchemaValidator().validate(
            report.to_dict(),
            expected_kind="dpone.airflow-cache-status-publication.v1",
        )
        == ()
    )


def test_malformed_status_preserves_last_known_good_and_writes_failure_marker(tmp_path: Path) -> None:
    root = _root(tmp_path)
    target = _write_source(root, "last-retention-plan.json", _plan_payload())
    before = target.read_bytes()
    _write_source(root, ".plan.tmp", b'{"schema":')

    report = _publisher().publish(_request(root))

    assert report.passed is False
    assert report.status == "rejected"
    assert report.exit_code == 1
    assert report.error_code == "DPONE_AIRFLOW_CACHE_STATUS_JSON_INVALID"
    assert target.read_bytes() == before
    marker = json.loads((root / "last-retention-plan-publication-failure.json").read_text(encoding="utf-8"))
    assert marker == {
        "attempted_at": _NOW,
        "error_code": "DPONE_AIRFLOW_CACHE_STATUS_JSON_INVALID",
        "expected_schema": "dpone.deployment-cache-retention-plan.v1",
        "schema": "dpone.airflow-cache-status-publication-failure.v1",
        "source": ".plan.tmp",
        "status": "rejected",
        "target": "last-retention-plan.json",
    }
    assert "payload" not in marker
    assert "message" not in marker
    assert (
        GitOpsSchemaValidator().validate(
            marker,
            expected_kind="dpone.airflow-cache-status-publication-failure.v1",
        )
        == ()
    )


def test_excessive_json_nesting_is_rejected_without_replacing_target(tmp_path: Path) -> None:
    root = _root(tmp_path)
    target = _write_source(root, "last-retention-plan.json", _plan_payload())
    before = target.read_bytes()
    nested = b'{"payload":' + (b"[" * 10_000) + b"0" + (b"]" * 10_000) + b"}"
    _write_source(root, ".plan.tmp", nested)

    report = _publisher().publish(_request(root))

    assert report.passed is False
    assert report.error_code == "DPONE_AIRFLOW_CACHE_STATUS_JSON_INVALID"
    assert target.read_bytes() == before


@pytest.mark.parametrize(
    ("source_factory", "expected_code"),
    [
        (
            lambda root: (root / ".plan.tmp").symlink_to(root / "outside.json"),
            "DPONE_AIRFLOW_CACHE_STATUS_SOURCE_UNSAFE",
        ),
        (
            lambda root: _write_source(root, ".plan.tmp", b"{" + b" " * CACHE_STATUS_MAX_BYTES + b"}"),
            "DPONE_AIRFLOW_CACHE_STATUS_SOURCE_OVERSIZED",
        ),
    ],
)
def test_unsafe_or_oversized_source_never_replaces_target(
    tmp_path: Path,
    source_factory: object,
    expected_code: str,
) -> None:
    root = _root(tmp_path)
    outside = tmp_path / "outside.json"
    outside.write_text(json.dumps(_plan_payload()), encoding="utf-8")
    target = _write_source(root, "last-retention-plan.json", _plan_payload())
    before = target.read_bytes()
    source_factory(root)  # type: ignore[operator]

    report = _publisher().publish(_request(root))

    assert report.passed is False
    assert report.exit_code == 4
    assert report.error_code == expected_code
    assert target.read_bytes() == before


def test_wrong_registered_schema_is_rejected_without_replacing_target(tmp_path: Path) -> None:
    root = _root(tmp_path)
    target = _write_source(root, "last-retention-plan.json", _plan_payload())
    before = target.read_bytes()
    source = _plan_payload()
    source["schema"] = "dpone.deployment-cache-retention-apply.v1"
    _write_source(root, ".plan.tmp", source)

    report = _publisher().publish(_request(root))

    assert report.passed is False
    assert report.exit_code == 1
    assert report.error_code == "DPONE_AIRFLOW_CACHE_STATUS_SCHEMA_MISMATCH"
    assert target.read_bytes() == before
    marker = json.loads((root / "last-retention-plan-publication-failure.json").read_text(encoding="utf-8"))
    assert marker["error_code"] == "DPONE_AIRFLOW_CACHE_STATUS_SCHEMA_MISMATCH"


def test_group_readable_source_is_rejected_without_replacing_target(tmp_path: Path) -> None:
    root = _root(tmp_path)
    target = _write_source(root, "last-retention-plan.json", _plan_payload())
    before = target.read_bytes()
    source = _write_source(root, ".plan.tmp", _plan_payload())
    source.chmod(0o640)

    report = _publisher().publish(_request(root))

    assert report.passed is False
    assert report.exit_code == 4
    assert report.error_code == "DPONE_AIRFLOW_CACHE_STATUS_SOURCE_UNSAFE"
    assert target.read_bytes() == before


def test_cli_publishes_bounded_schema_versioned_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    root = _root(tmp_path)
    _write_source(root, ".plan.tmp", _plan_payload())

    with pytest.raises(SystemExit) as exit_info:
        cli_main.main(
            [
                "airflow",
                "cache-status-publish",
                "--status-root",
                str(root),
                "--source",
                ".plan.tmp",
                "--target",
                "last-retention-plan.json",
                "--failure-marker",
                "last-retention-plan-publication-failure.json",
                "--expected-schema",
                "dpone.deployment-cache-retention-plan.v1",
                "--format",
                "json",
            ]
        )

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert exit_info.value.code == 0
    assert captured.err == ""
    assert payload["schema"] == "dpone.airflow-cache-status-publication.v1"
    assert payload["passed"] is True
    assert payload["target"] == "last-retention-plan.json"


def test_post_replace_failure_reports_commit_unknown(
    tmp_path: Path,
) -> None:
    from dpone.adapters.deployment_cache_files import atomic_write_json as real_atomic_write_json

    root = _root(tmp_path)
    _write_source(root, ".plan.tmp", _plan_payload())

    def fail_after_replace(path: Path, payload: dict[str, object]) -> None:
        real_atomic_write_json(path, payload)
        if path.name == "last-retention-plan.json":
            raise AtomicJsonWriteError("directory fsync failed", state_may_have_changed=True)

    report = _publisher(PosixAirflowCacheStatusFileStorage(write_json=fail_after_replace)).publish(_request(root))

    assert report.passed is False
    assert report.status == "commit_unknown"
    assert report.exit_code == 4
    assert report.error_code == "DPONE_AIRFLOW_CACHE_STATUS_COMMIT_UNKNOWN"
    assert (root / "last-retention-plan.json").exists()
    marker = json.loads((root / "last-retention-plan-publication-failure.json").read_text(encoding="utf-8"))
    assert marker["status"] == "commit_unknown"


def test_lock_release_failure_reports_commit_unknown_after_publication(tmp_path: Path) -> None:
    root = _root(tmp_path)
    _write_source(root, ".plan.tmp", _plan_payload())

    class BrokenReleaseLock:
        LOCK_EX = 1
        LOCK_NB = 2
        LOCK_UN = 4

        @staticmethod
        def flock(_descriptor: int, operation: int) -> None:
            if operation == BrokenReleaseLock.LOCK_UN:
                raise OSError("simulated lock release failure")

    storage = PosixAirflowCacheStatusFileStorage(module_importer=lambda _name: BrokenReleaseLock)
    report = _publisher(storage).publish(_request(root))

    assert report.passed is False
    assert report.status == "commit_unknown"
    assert report.exit_code == 4
    assert report.error_code == "DPONE_AIRFLOW_CACHE_STATUS_LOCK_RELEASE_UNAVAILABLE"
    assert (root / "last-retention-plan.json").is_file()


def test_publication_lock_timeout_is_bounded(tmp_path: Path) -> None:
    class BusyLock:
        LOCK_EX = 1
        LOCK_NB = 2
        LOCK_UN = 8

        @staticmethod
        def flock(_descriptor: int, operation: int) -> None:
            if operation != BusyLock.LOCK_UN:
                raise BlockingIOError("busy")

    ticks = iter((0.0, 0.0, 1.0))
    root = _root(tmp_path)
    _write_source(root, ".plan.tmp", _plan_payload())
    publisher = AirflowCacheStatusPublisher(
        validator=GitOpsAirflowCacheStatusSchemaValidator(),
        storage=PosixAirflowCacheStatusFileStorage(
            module_importer=lambda _name: BusyLock,
            monotonic=lambda: next(ticks),
            sleep=lambda _seconds: None,
        ),
        now=lambda: _NOW,
        lock_timeout_seconds=0.1,
    )

    report = publisher.publish(_request(root))

    assert report.passed is False
    assert report.error_code == "DPONE_AIRFLOW_CACHE_STATUS_LOCK_UNAVAILABLE"
    assert report.exit_code == 4


def test_marker_failure_does_not_hide_commit_unknown(
    tmp_path: Path,
) -> None:
    from dpone.adapters.deployment_cache_files import atomic_write_json as real_atomic_write_json

    root = _root(tmp_path)
    _write_source(root, ".plan.tmp", _plan_payload())

    def fail_target_and_marker(path: Path, payload: dict[str, object]) -> None:
        if path.name == "last-retention-plan.json":
            real_atomic_write_json(path, payload)
            raise AtomicJsonWriteError("directory fsync failed", state_may_have_changed=True)
        raise AtomicJsonWriteError("marker unavailable", state_may_have_changed=False)

    report = _publisher(PosixAirflowCacheStatusFileStorage(write_json=fail_target_and_marker)).publish(_request(root))

    assert report.status == "commit_unknown"
    assert report.error_code == "DPONE_AIRFLOW_CACHE_STATUS_COMMIT_UNKNOWN"
    assert report.diagnostic_error_code == "DPONE_AIRFLOW_CACHE_STATUS_FAILURE_MARKER_UNAVAILABLE"
    assert report.source_sha256 is not None
    assert report.target_sha256 is not None
