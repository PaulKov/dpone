from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from dpone.contracts.airflow_artifact_attestation import AirflowArtifactAttestation
from dpone.gitops.schema_validation import GitOpsSchemaValidator

_SHA = "sha256:" + "a" * 64


def test_attestation_schema_and_runtime_accept_same_valid_claims() -> None:
    statement = AirflowArtifactAttestation.create(_claims()).to_dict()
    internal, standard = _schema_errors(statement)

    assert internal == standard == ()
    assert AirflowArtifactAttestation.create(statement["claims"])


@pytest.mark.parametrize(
    ("path", "value"),
    [
        (("claims", "source", "project"), "dwh/\x00airflow-dags"),
        (("claims", "source", "project"), "dwh/\nairflow-dags"),
        (("claims", "source", "git_sha"), "1" * 40 + "\n"),
        (("claims", "source", "git_sha"), 1),
        (("claims", "issued_at"), "2" * 41),
        (("claims", "subject", "environment"), "p" * 64),
    ],
)
def test_attestation_schema_and_runtime_reject_same_invalid_claim(
    path: tuple[str, ...],
    value: object,
) -> None:
    statement = AirflowArtifactAttestation.create(_claims()).to_dict()
    _set_path(statement, path, value)
    internal, standard = _schema_errors(statement)

    assert internal
    assert standard
    with pytest.raises((TypeError, ValueError)):
        AirflowArtifactAttestation.create(statement["claims"])


def _schema_errors(
    payload: dict[str, Any],
) -> tuple[tuple[object, ...], tuple[object, ...]]:
    internal = GitOpsSchemaValidator().validate(
        payload,
        expected_kind="dpone.airflow-artifact-attestation.v1",
    )
    schema_path = Path(__file__).parents[1] / "docs/schemas/gitops/airflow-artifact-attestation.schema.json"
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    standard = tuple(Draft202012Validator(schema).iter_errors(payload))
    return internal, standard


def _set_path(payload: dict[str, Any], path: tuple[str, ...], value: object) -> None:
    current: dict[str, Any] = payload
    for part in path[:-1]:
        current = current[part]
    current[path[-1]] = value


def _claims() -> dict[str, Any]:
    return copy.deepcopy(
        {
            "subject": {
                "release_id": _SHA,
                "deployment_id": _SHA,
                "environment": "prod",
                "artifact_registry_ref": "dpone_prod",
                "registry_scope_id": _SHA,
                "release_set_sha256": _SHA,
                "deployment_sha256": _SHA,
                "airflow_index_sha256": _SHA,
                "runtime_image_digest": _SHA,
            },
            "source": {
                "project": "platform/example-workloads",
                "ref": "refs/heads/master",
                "git_sha": "1" * 40,
            },
            "publication": {
                "evidence_sha256": _SHA,
                "verification_mode": "remote_readback_sha256",
            },
            "issued_at": "2026-07-29T10:00:00+00:00",
        }
    )
