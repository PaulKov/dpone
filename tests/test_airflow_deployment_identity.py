from __future__ import annotations

import json

import pytest

from dpone.contracts.airflow_run_identity import (
    AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA,
    AirflowDeploymentIdentity,
    AirflowDeploymentIdentityError,
    parse_airflow_deployment_identity_json,
)
from dpone.gitops.schema_contracts import get_gitops_schema_contract


def _payload() -> dict[str, str]:
    return {
        "schema": AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA,
        "release_id": "sha256:" + "a" * 64,
        "deployment_id": "sha256:" + "b" * 64,
        "activation_id": "3f60628e-ef48-48b0-84c3-a9e27a82a7f2",
    }


def test_deployment_identity_round_trips_and_matches_schema() -> None:
    jsonschema = pytest.importorskip("jsonschema")
    parsed = parse_airflow_deployment_identity_json(json.dumps(_payload()))
    contract = get_gitops_schema_contract(AIRFLOW_DEPLOYMENT_IDENTITY_SCHEMA)

    assert parsed == AirflowDeploymentIdentity.from_mapping(_payload())
    assert parsed.to_dict() == _payload()
    assert contract is not None
    jsonschema.validate(parsed.to_dict(), contract.schema, format_checker=jsonschema.FormatChecker())


@pytest.mark.parametrize(
    "field,value",
    [
        ("activation_id", "latest"),
        ("activation_id", " 3f60628e-ef48-48b0-84c3-a9e27a82a7f2 "),
        ("release_id", "latest"),
        ("release_id", " sha256:" + "a" * 64),
        ("schema", "dpone.airflow-deployment-identity.v2"),
    ],
)
def test_deployment_identity_rejects_malformed_values(field: str, value: str) -> None:
    with pytest.raises(AirflowDeploymentIdentityError):
        AirflowDeploymentIdentity.from_mapping({**_payload(), field: value})
