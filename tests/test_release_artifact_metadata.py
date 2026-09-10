"""Preserve legacy checksum spelling and adapter-specific metadata failures."""

from pathlib import PurePosixPath

import pytest

from dpone.readiness.airflow_deployment_projection_errors import AirflowDeploymentProjectionError
from dpone.readiness.airflow_release_artifact_index import ReleaseArtifactRules, index_release_artifacts
from dpone.runtime.airflow_artifact_delivery_models import AirflowArtifactDeliveryError
from dpone.runtime.airflow_artifact_inventory import declared_release_artifacts


def release_with(item):
    return {
        "schema": "dpone.release-set.v1",
        "artifacts": {"dag_specs": [item], "workload_packs": [], "canonical_schemas": []},
    }


@pytest.mark.parametrize("digest", ["sha256:" + "a" * 64, "sha256:" + "A" * 64])
def test_publication_preserves_legacy_checksum_spelling(digest):
    assert declared_release_artifacts(release_with({"id": "dag", "path": "dags/main.json", "sha256": digest})) == (
        (PurePosixPath("dags/main.json"), digest),
    )


@pytest.mark.parametrize(
    ("path", "digest", "expected_code"),
    [
        ("../escape", "invalid", "DPONE_RELEASE_ARTIFACT_PATH_INVALID"),
        ("dags/main.json", "invalid", "DPONE_DEPLOYMENT_DIGEST_INVALID"),
    ],
)
def test_projection_metadata_failure_precedes_any_file_read(tmp_path, path, digest, expected_code):
    def forbidden_read(*args, **kwargs):
        pytest.fail("invalid metadata must fail before acquisition")

    with pytest.raises(AirflowDeploymentProjectionError) as error:
        index_release_artifacts(
            release_with({"id": "dag", "path": path, "sha256": digest}),
            release_id="sha256:" + "0" * 64,
            cache_root=tmp_path,
            section="dag_specs",
            rules=ReleaseArtifactRules(True, True, True),
            reader=forbidden_read,
        )
    assert error.value.code == expected_code


def test_publication_path_failure_precedes_invalid_checksum():
    with pytest.raises(AirflowArtifactDeliveryError) as error:
        declared_release_artifacts(release_with({"id": "dag", "path": "../escape", "sha256": "invalid"}))
    assert error.value.code == "DPONE_RELEASE_ARTIFACTS_INVALID"
    assert "sha256" not in str(error.value)
