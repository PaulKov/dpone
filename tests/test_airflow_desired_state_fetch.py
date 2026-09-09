from __future__ import annotations

from pathlib import Path

import pytest

from dpone.adapters.airflow_desired_state_snapshot import AtomicAirflowDesiredStateSnapshotWriter
from dpone.contracts.airflow_desired_state import AirflowDesiredDeployment, DesiredStateRevision
from dpone.ports.airflow_desired_state import DesiredStateReadResult, DesiredStateReadUnavailable
from dpone.services.airflow_desired_state_fetch import (
    AirflowDesiredStateFetcher,
    DesiredStateFetchError,
    DesiredStateFetchRequest,
)

_DIGESTS = tuple("sha256:" + char * 64 for char in "abcde")


def _body(environment: str = "dev") -> bytes:
    return AirflowDesiredDeployment.from_mapping(
        {
            "schema": "dpone.airflow-desired-deployment.v1",
            "environment": environment,
            "source": {
                "project": "group/repository",
                "ref": "master",
                "pipeline_id": "1",
                "job_id": "2",
                "occurrence_id": "123e4567-e89b-42d3-a456-426614174000",
                "git_sha": "1" * 40,
            },
            "promotion": {
                "registry_scope_id": _DIGESTS[4],
                "release_id": _DIGESTS[0],
                "deployment_id": _DIGESTS[1],
                "airflow_index_sha256": _DIGESTS[2],
                "runtime_image_digest": _DIGESTS[3],
                "expected_dag_ids": ["DAG__platform__smoke__run"],
                "publication_evidence_sha256": _DIGESTS[4],
            },
            "previous": {"revision": None, "deployment_id": None},
            "promoted_at": "2026-07-28T10:00:00Z",
        }
    ).to_json_bytes()


class _Reader:
    def __init__(self, *results: DesiredStateReadResult | Exception) -> None:
        self.results = list(results)
        self.revisions: list[DesiredStateRevision | None] = []

    def read(
        self,
        *,
        if_changed_from: DesiredStateRevision | None = None,
        **_: object,
    ) -> DesiredStateReadResult:
        self.revisions.append(if_changed_from)
        result = self.results.pop(0)
        if isinstance(result, Exception):
            raise result
        return result


def test_fetch_validates_and_atomically_commits_snapshot(tmp_path: Path) -> None:
    output = tmp_path / "status" / "desired.json"
    revision = DesiredStateRevision('"etag"')
    service = AirflowDesiredStateFetcher(
        reader=_Reader(DesiredStateReadResult.present(_body(), revision)),
        snapshot_writer=AtomicAirflowDesiredStateSnapshotWriter(output),
    )

    evidence = service.fetch(DesiredStateFetchRequest(environment="dev"))

    assert output.read_bytes() == _body()
    assert evidence.status == "fetched"
    assert evidence.observed_revision == revision
    assert evidence.deployment_id == _DIGESTS[1]


def test_fetch_rejects_wrong_environment_without_mutating_snapshot(tmp_path: Path) -> None:
    output = tmp_path / "desired.json"
    output.write_bytes(b"last-known-good")
    service = AirflowDesiredStateFetcher(
        reader=_Reader(
            DesiredStateReadResult.present(
                _body("prod"),
                DesiredStateRevision('"etag"'),
            )
        ),
        snapshot_writer=AtomicAirflowDesiredStateSnapshotWriter(output),
    )

    with pytest.raises(DesiredStateFetchError) as mismatch:
        service.fetch(DesiredStateFetchRequest(environment="dev"))

    assert mismatch.value.code == "DPONE_AIRFLOW_DESIRED_STATE_ENVIRONMENT_MISMATCH"
    assert output.read_bytes() == b"last-known-good"


def test_fetch_preserves_snapshot_on_remote_or_integrity_failure(tmp_path: Path) -> None:
    output = tmp_path / "desired.json"
    output.write_bytes(b"last-known-good")
    for result, code in (
        (DesiredStateReadUnavailable("offline"), "DPONE_AIRFLOW_DESIRED_STATE_UNAVAILABLE"),
        (
            DesiredStateReadResult.present(b"{}", DesiredStateRevision('"etag"')),
            "DPONE_AIRFLOW_DESIRED_STATE_INTEGRITY",
        ),
        (DesiredStateReadResult.absent(), "DPONE_AIRFLOW_DESIRED_STATE_NOT_FOUND"),
    ):
        service = AirflowDesiredStateFetcher(
            reader=_Reader(result),
            snapshot_writer=AtomicAirflowDesiredStateSnapshotWriter(output),
        )
        with pytest.raises(DesiredStateFetchError) as failed:
            service.fetch(DesiredStateFetchRequest(environment="dev"))
        assert failed.value.code == code
        assert output.read_bytes() == b"last-known-good"


def test_unchanged_fetch_revalidates_and_rewrites_the_verified_snapshot(
    tmp_path: Path,
) -> None:
    output = tmp_path / "desired.json"
    output.write_bytes(b"last-known-good")
    revision = DesiredStateRevision('"etag"')
    service = AirflowDesiredStateFetcher(
        reader=_Reader(
            DesiredStateReadResult.unchanged(revision),
            DesiredStateReadResult.present(_body(), revision),
        ),
        snapshot_writer=AtomicAirflowDesiredStateSnapshotWriter(output),
    )

    evidence = service.fetch(DesiredStateFetchRequest(environment="dev", previous_revision=revision))

    assert evidence.status == "fetched"
    assert output.read_bytes() == _body()
