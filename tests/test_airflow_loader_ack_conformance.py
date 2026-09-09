from __future__ import annotations

import json

import pytest
from dpone_airflow_pack.loader_ack import (
    LoaderAcknowledgementError,
    parse_loader_ack_json,
)

from dpone.contracts.airflow_loader_ack import (
    AirflowLoaderAckError,
    parse_airflow_loader_ack,
)

_RELEASE_ID = "sha256:" + "a" * 64
_DEPLOYMENT_ID = "sha256:" + "b" * 64
_INDEX_SHA256 = "sha256:" + "c" * 64
_ACTIVATION_ID = "12345678-1234-4234-9234-123456789abc"


def _canonical_ack_payload() -> dict[str, object]:
    return {
        "schema": "dpone.airflow_loader_ack.v2",
        "release_id": _RELEASE_ID,
        "deployment_id": _DEPLOYMENT_ID,
        "airflow_index_sha256": _INDEX_SHA256,
        "activation_id": _ACTIVATION_ID,
        "loaded_dag_ids": ["DAG__demo"],
        "skipped_dag_ids": [],
        "error_codes": [],
        "fatal": False,
        "acknowledged_at": "2026-08-03T00:00:00+00:00",
    }


@pytest.mark.parametrize(
    ("override", "accepted"),
    [
        ({}, True),
        ({"loaded_dag_ids": ["DAG__z", "DAG__a"]}, False),
        ({"skipped_dag_ids": ["DAG__demo"]}, False),
        ({"release_id": "sha256:" + "A" * 64}, False),
        ({"fatal": "false"}, False),
        ({"error_codes": ["bad-code"]}, False),
        ({"acknowledged_at": "2026-08-03T00:00:00"}, False),
    ],
)
def test_runtime_and_lightweight_loader_ack_parsers_share_conformance_vectors(
    override: dict[str, object],
    accepted: bool,
) -> None:
    raw = json.dumps({**_canonical_ack_payload(), **override}).encode("utf-8")
    if accepted:
        assert parse_airflow_loader_ack(raw).to_dict() == parse_loader_ack_json(raw).to_jsonable()
        return
    with pytest.raises(AirflowLoaderAckError):
        parse_airflow_loader_ack(raw)
    with pytest.raises(LoaderAcknowledgementError):
        parse_loader_ack_json(raw)
