from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace
from datetime import UTC, datetime
from pathlib import Path

import pytest

from dpone.adapters.airflow_desired_state_publish_intent import (
    FileDesiredStatePublishPreparationStore,
)
from dpone.readiness.airflow_desired_state_authority import (
    AirflowDesiredStateAuthority,
)
from dpone.readiness.airflow_desired_state_store import PromotionInput
from dpone.services.airflow_desired_state_preparation import (
    AirflowDesiredStatePreparationService,
)

_DIGESTS = tuple("sha256:" + char * 64 for char in "abcde")
_OCCURRENCE = "123e4567-e89b-42d3-a456-426614174000"


def _authority(
    *,
    desired_state_uri: str = "s3://bucket/control/dev/desired.json",
    watcher_identity: str = "airflow-pack-watcher",
) -> AirflowDesiredStateAuthority:
    return AirflowDesiredStateAuthority(
        environment="dev",
        desired_state_uri=desired_state_uri,
        certified_s3_endpoint_url="https://storage.example.test",
        artifact_registry_uri="s3://bucket/dpone-artifacts/dev/repository",
        artifact_registry_ref="dpone-artifacts-dev",
        watcher_identity=watcher_identity,
        source_project="group/repository",
        source_ref="master",
    )


def _promotion(authority: AirflowDesiredStateAuthority) -> PromotionInput:
    return PromotionInput(
        environment="dev",
        registry_scope_id=authority.registry_scope_id,
        release_id=_DIGESTS[0],
        deployment_id=_DIGESTS[1],
        source_git_sha="1" * 40,
        runtime_image_digest=_DIGESTS[2],
        airflow_index_sha256=_DIGESTS[3],
        expected_dag_ids=("DAG__platform__smoke__run",),
        evidence_sha256=_DIGESTS[4],
    )


def _service() -> AirflowDesiredStatePreparationService:
    return AirflowDesiredStatePreparationService(
        clock=lambda: datetime(2026, 7, 29, 9, 0, tzinfo=UTC),
        occurrence_id_factory=lambda: _OCCURRENCE,
    )


def test_authority_identity_binds_desired_key_endpoint_and_watcher() -> None:
    baseline = _authority()
    another_key = _authority(desired_state_uri="s3://bucket/control/dev/another.json")
    another_watcher = _authority(watcher_identity="another-watcher")

    assert (
        len(
            {
                baseline.publish_authority_sha256,
                another_key.publish_authority_sha256,
                another_watcher.publish_authority_sha256,
            }
        )
        == 3
    )


def test_validation_rejects_authority_drift_and_records_both_jobs() -> None:
    authority = _authority()
    service = _service()
    candidate = service.candidate(
        authority=authority,
        promotion=_promotion(authority),
        pipeline_id="10",
        preparation_job_id="11",
        expected_revision=None,
    )
    preparation = service.preparation(candidate)

    request = service.validate(
        preparation=preparation,
        authority=authority,
        promotion=_promotion(authority),
        pipeline_id="10",
        publisher_job_id="22",
    )

    assert request.candidate.job_id == "11"
    assert request.publisher_job_id == "22"
    with pytest.raises(ValueError, match="trusted candidate"):
        drifted = _authority(desired_state_uri="s3://bucket/control/dev/another.json")
        service.validate(
            preparation=preparation,
            authority=drifted,
            promotion=_promotion(drifted),
            pipeline_id="10",
            publisher_job_id="22",
        )


def test_file_preparation_is_create_once_and_candidate_safe(tmp_path: Path) -> None:
    authority = _authority()
    service = _service()
    candidate = service.candidate(
        authority=authority,
        promotion=_promotion(authority),
        pipeline_id="10",
        preparation_job_id="11",
        expected_revision=None,
    )
    store = FileDesiredStatePublishPreparationStore(tmp_path / "preparation.json")
    factory_calls = 0

    def create():
        nonlocal factory_calls
        factory_calls += 1
        return service.preparation(candidate)

    with ThreadPoolExecutor(max_workers=4) as pool:
        resolved = tuple(
            pool.map(
                lambda _: store.resolve(
                    candidate=candidate,
                    preparation_factory=create,
                ),
                range(8),
            )
        )

    assert len(set(resolved)) == 1
    assert factory_calls == 1
    with pytest.raises(ValueError, match="another candidate"):
        store.resolve(
            candidate=replace(candidate, deployment_id=_DIGESTS[4]),
            preparation_factory=create,
        )
