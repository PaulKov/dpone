from __future__ import annotations

import json
from pathlib import Path

import pytest

from dpone.adapters.object_storage_artifact_registry import (
    object_storage_registry_authority_scope_id,
    parse_artifact_registry_root,
)
from dpone.readiness.airflow_desired_state_authority import (
    AUTHORITY_FILE_ENV,
    load_airflow_desired_state_authority,
)


def _payload() -> dict[str, str]:
    return {
        "schema": "dpone.airflow-desired-state-authority.v1",
        "environment": "dev",
        "desired_state_uri": ("s3://example-data-bucket/dpone-artifacts/prod/example-workloads/environments/dev/desired-state.json"),
        "certified_s3_endpoint_url": "https://storage.yandexcloud.net",
        "artifact_registry_uri": ("s3://example-data-bucket/dpone-artifacts/prod/example-workloads/immutable"),
        "artifact_registry_ref": "dpone-artifacts-dev",
        "watcher_identity": "airflow-example-dev/dpone-pack-watcher",
        "source_project": "platform/example-workloads",
        "source_ref": "master",
    }


def test_authority_loads_only_from_protected_file_and_derives_registry_scope(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
) -> None:
    path = tmp_path / "authority.json"
    path.write_text(json.dumps(_payload()), encoding="utf-8")
    monkeypatch.setenv(AUTHORITY_FILE_ENV, str(path))

    authority = load_airflow_desired_state_authority()

    assert authority.environment == "dev"
    assert authority.watcher_identity == "airflow-example-dev/dpone-pack-watcher"
    assert authority.registry_scope_id == object_storage_registry_authority_scope_id(
        parse_artifact_registry_root(_payload()["artifact_registry_uri"]),
        endpoint_authority=_payload()["certified_s3_endpoint_url"],
    )


def test_v2_authority_projects_workspace_control_binding_without_secrets(tmp_path: Path) -> None:
    payload = {
        **_payload(),
        "schema": "dpone.airflow-desired-state-authority.v2",
        "workspace_authority_connection_ref": "dpone_control",
    }
    path = tmp_path / "authority-v2.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    authority = load_airflow_desired_state_authority(path)

    assert authority.workspace_authority_connection_ref == "dpone_control"


def test_v1_cannot_smuggle_workspace_control_binding(tmp_path: Path) -> None:
    payload = {**_payload(), "workspace_authority_connection_ref": "dpone_control"}
    path = tmp_path / "authority-v1-extra.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError, match="fields"):
        load_airflow_desired_state_authority(path)


@pytest.mark.parametrize(
    "mutation",
    [
        lambda value: value.update({"extra": "value"}),
        lambda value: value.update({"environment": "PROD"}),
        lambda value: value.update({"artifact_registry_uri": "s3://another-bucket/dpone-artifacts/prod"}),
        lambda value: value.update({"artifact_registry_uri": "s3://example-data-bucket/dpone-artifacts/prod/example-workloads"}),
    ],
)
def test_authority_rejects_ambiguous_or_cross_authority_configuration(
    mutation: object,
    tmp_path: Path,
) -> None:
    payload = _payload()
    mutation(payload)  # type: ignore[operator]
    path = tmp_path / "authority.json"
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(ValueError):
        load_airflow_desired_state_authority(path)
