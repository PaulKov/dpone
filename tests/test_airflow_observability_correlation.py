from __future__ import annotations

import json
from uuid import UUID

import pytest
from jsonschema import Draft202012Validator

from dpone.contracts.airflow_correlation import (
    AirflowCorrelationError,
    build_airflow_correlation,
    parse_airflow_correlation,
)
from dpone.contracts.airflow_run_identity import AirflowRunIdentity
from dpone.gitops.schema_contracts import get_gitops_schema_contract


def _digest(char: str) -> str:
    return "sha256:" + char * 64


def _run_identity() -> AirflowRunIdentity:
    return AirflowRunIdentity.from_mapping(
        {
            "schema": "dpone.airflow-run-identity.v1",
            "release_id": _digest("a"),
            "deployment_id": _digest("b"),
            "dag_spec": {"id": "orders_daily", "sha256": _digest("c")},
            "workload_pack": {"id": "load_orders", "sha256": _digest("d")},
            "runtime_image_digest": _digest("e"),
            "binding_set_ref": _digest("1"),
            "connection_registry_ref": _digest("2"),
            "credential_runtime_ref": _digest("3"),
            "airflow_bundle": None,
        }
    )


def _attempt(*, try_number: int = 1) -> dict[str, object]:
    return {
        "dag_id": "orders_daily",
        "task_id": "load_orders",
        "run_id": "scheduled__2026-07-17T00:00:00+00:00",
        "try_number": try_number,
        "map_index": -1,
    }


def _pod(*, image_digest: str | None = None) -> dict[str, object]:
    return {
        "name": "load-orders-x7f9",
        "uid": "pod-uid-123",
        "namespace": "airflow-example",
        "image_digest": image_digest or _digest("e"),
    }


def test_complete_correlation_is_deterministic_and_round_trips() -> None:
    first = build_airflow_correlation(
        run_identity=_run_identity(),
        attempt=_attempt(),
        dpone_run_id="orders-20260717T000000Z",
        dpone_process="orders",
        runtime_evidence_sha256=_digest("f"),
        pod=_pod(),
    )
    second = build_airflow_correlation(
        run_identity=_run_identity(),
        attempt=dict(reversed(tuple(_attempt().items()))),
        dpone_run_id="orders-20260717T000000Z",
        dpone_process="orders",
        runtime_evidence_sha256=_digest("f"),
        pod=_pod(),
    )

    assert first.complete
    assert first.missing_fields == ()
    assert first.correlation_id == second.correlation_id
    assert first.attempt_ref == first.correlation_id
    assert parse_airflow_correlation(json.loads(first.to_json())) == first
    assert UUID(first.openlineage_run_id).version == 5


def test_late_observations_do_not_change_attempt_correlation_id() -> None:
    first = build_airflow_correlation(
        run_identity=_run_identity(),
        attempt=_attempt(),
        dpone_run_id="run-one",
        dpone_process="orders",
        runtime_evidence_sha256=_digest("f"),
        pod=_pod(),
    )
    replaced_pod = build_airflow_correlation(
        run_identity=_run_identity(),
        attempt=_attempt(),
        dpone_run_id="run-one",
        dpone_process="orders",
        runtime_evidence_sha256=_digest("9"),
        pod={**_pod(), "name": "load-orders-replacement", "uid": "pod-uid-456"},
    )

    assert first.correlation_id == replaced_pod.correlation_id
    assert first.pod != replaced_pod.pod


def test_retry_has_a_distinct_correlation_id() -> None:
    first = build_airflow_correlation(run_identity=_run_identity(), attempt=_attempt())
    retry = build_airflow_correlation(run_identity=_run_identity(), attempt=_attempt(try_number=2))

    assert not first.complete
    assert first.correlation_id != retry.correlation_id
    assert set(first.missing_fields) == {
        "dpone.process",
        "dpone.run_id",
        "artifacts.runtime_evidence_sha256",
        "pod.image_digest",
        "pod.name",
        "pod.namespace",
        "pod.uid",
    }


def test_correlation_blocks_dag_and_runtime_image_mismatch() -> None:
    with pytest.raises(AirflowCorrelationError, match="DPONE_AIRFLOW_CORRELATION_MISMATCH"):
        build_airflow_correlation(
            run_identity=_run_identity(),
            attempt={**_attempt(), "dag_id": "other_daily"},
        )

    with pytest.raises(AirflowCorrelationError, match="DPONE_AIRFLOW_CORRELATION_MISMATCH"):
        build_airflow_correlation(
            run_identity=_run_identity(),
            attempt=_attempt(),
            pod=_pod(image_digest=_digest("9")),
        )


def test_parser_rejects_tampering_unknown_fields_and_oversize() -> None:
    correlation = build_airflow_correlation(run_identity=_run_identity(), attempt=_attempt())
    tampered = correlation.to_dict()
    tampered["correlation_id"] = _digest("0")
    with pytest.raises(AirflowCorrelationError, match="DPONE_AIRFLOW_CORRELATION_MISMATCH"):
        parse_airflow_correlation(tampered)

    unknown = correlation.to_dict()
    unknown["password"] = "must-not-leak"
    with pytest.raises(AirflowCorrelationError, match="DPONE_AIRFLOW_CORRELATION_INVALID") as error:
        parse_airflow_correlation(unknown)
    assert "must-not-leak" not in str(error.value)

    oversized = correlation.to_dict()
    oversized["airflow"]["run_id"] = "x" * (33 * 1024)  # type: ignore[index]
    with pytest.raises(AirflowCorrelationError, match="DPONE_AIRFLOW_CORRELATION_INVALID"):
        parse_airflow_correlation(oversized)


def test_public_schema_accepts_complete_and_partial_correlation() -> None:
    contract = get_gitops_schema_contract("dpone.airflow-correlation.v1")
    assert contract is not None
    validator = Draft202012Validator(contract.schema)
    complete = build_airflow_correlation(
        run_identity=_run_identity(),
        attempt=_attempt(),
        dpone_run_id="orders-20260717T000000Z",
        dpone_process="orders",
        runtime_evidence_sha256=_digest("f"),
        pod=_pod(),
    )
    partial = build_airflow_correlation(run_identity=_run_identity(), attempt=_attempt())

    validator.validate(complete.to_dict())
    validator.validate(partial.to_dict())
