"""Closed metadata contract negatives; no secret reads or external systems."""

from __future__ import annotations

from copy import deepcopy

import pytest
from dpone_airflow_pack.credential_projection_contract import (
    CredentialProjectionError,
    canonical_projection_bytes,
    parse_credential_projection,
    projection_descriptor,
)

from dpone.gitops.schema_validation import GitOpsSchemaValidator
from dpone.readiness.airflow_credential_projection import build_credential_projection
from tests.test_airflow_credential_projection import projection_case


def _decode(raw):
    body = canonical_projection_bytes(raw)
    return parse_credential_projection(
        body,
        descriptor=projection_descriptor(body),
        environment="prod",
        release_id=raw["release_id"],
        binding_set_sha256=raw["binding_set_sha256"],
        runtime_registry_sha256=raw["runtime_registry_sha256"],
    )


def test_schema_and_reader_accept_real_producer():
    result = build_credential_projection(**projection_case())
    raw = result.to_dict()
    assert _decode(raw) == result
    assert GitOpsSchemaValidator().validate(raw, expected_kind=raw["schema"]) == ()


@pytest.mark.parametrize(
    "mutation",
    [
        lambda raw: raw.update(password="forbidden-value"),
        lambda raw: raw["sources"][0].update(secret_key="AIRFLOW_CONN_CONTROL_RUNTIME"),
        lambda raw: raw["sources"][0].update(filename="../uri"),
        lambda raw: raw["sources"][0].update(mount_path="/run/secrets/dpone/airflow-connections"),
        lambda raw: raw["sources"][0].update(registry_ref="../escape"),
        lambda raw: raw["sources"].reverse(),
        lambda raw: raw["sources"].append(deepcopy(raw["sources"][0])),
        lambda raw: raw["workloads"][0]["connections"].clear(),
        lambda raw: raw["workloads"][0]["connections"][1].update(connection_ref="other"),
        lambda raw: raw["workloads"][0]["connections"].extend([raw["workloads"][0]["connections"][0]] * 256),
    ],
)
def test_closed_reader_rejects_resealed_invalid_mapping(mutation):
    raw = build_credential_projection(**projection_case()).to_dict()
    mutation(raw)
    with pytest.raises(CredentialProjectionError) as error:
        _decode(raw)
    assert "forbidden-value" not in str(error.value)


def test_authority_and_workload_parity_fail_closed():
    result = build_credential_projection(**projection_case())
    with pytest.raises(CredentialProjectionError):
        result.require_authority(control_ref="other")
    with pytest.raises(CredentialProjectionError):
        result.require_authority(control_ref="workspace_control", authority_sha256="sha256:" + "b" * 64)
    with pytest.raises(CredentialProjectionError):
        result.membership("unselected")
