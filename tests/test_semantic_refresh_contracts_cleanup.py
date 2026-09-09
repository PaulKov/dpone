"""Closed durable cleanup evidence contracts for semantic refresh V2."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import cast

import pytest
from jsonschema import Draft202012Validator, ValidationError

from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.contracts.semantic_refresh_schema_cleanup import (
    CLICKHOUSE_FAILED_SCRATCH_CLEANUP_RECEIPT_SCHEMA,
    MSSQL_FAILED_PRECOMMIT_CLEANUP_ACK_SCHEMA,
)
from dpone.contracts.semantic_refresh_schemas import (
    render_semantic_refresh_schema,
    semantic_refresh_contract_schemas,
)
from dpone.ports.semantic_refresh_clickhouse_resources import (
    ClickHouseFailedScratchCleanupReceipt,
    ClickHouseScratchRelationAbsence,
)
from dpone.ports.semantic_refresh_mssql_cleanup_ack import MssqlFailedPrecommitCleanupAck
from dpone.ports.semantic_refresh_mssql_resources import (
    MSSQL_RESOURCE_KINDS,
    MssqlProtectedResourceAllocation,
    MssqlProtectedResourceAllocationClosure,
    mssql_resource_allocation_id,
)

_WORKFLOW_EXECUTION_BINDING_SHA256 = "sha256:" + "a" * 64
_OPERATION_ID = "sha256:" + "b" * 64
_OPERATION_PLAN_SHA256 = "sha256:" + "c" * 64
_ATTEMPT_BINDING_SHA256 = "sha256:" + "d" * 64
_WORKFLOW_EXECUTION_ID = "scheduled__2026-08-07T00:00:00+00:00"
_TARGET_UUID = "0198f11c-6956-74f2-984b-4cfcb1653b87"
_RESERVATION_ID = "reservation-cleanup-1"


def _mapping(value: object) -> dict[str, object]:
    assert isinstance(value, Mapping)
    assert all(isinstance(key, str) for key in value)
    return dict(cast(Mapping[str, object], value))


def _mapping_list(value: object) -> list[dict[str, object]]:
    assert isinstance(value, list)
    return [_mapping(item) for item in value]


def _scratch_cleanup_receipt() -> ClickHouseFailedScratchCleanupReceipt:
    return ClickHouseFailedScratchCleanupReceipt(
        workflow_execution_id=_WORKFLOW_EXECUTION_ID,
        workflow_execution_binding_sha256=_WORKFLOW_EXECUTION_BINDING_SHA256,
        operation_id=_OPERATION_ID,
        operation_plan_sha256=_OPERATION_PLAN_SHA256,
        attempt_binding_sha256=_ATTEMPT_BINDING_SHA256,
        fencing_epoch=7,
        target_uuid=_TARGET_UUID,
        relations=(
            ClickHouseScratchRelationAbsence(
                kind="shadow",
                name="events__shadow_failed",
                expected_uuid="0298f11c-6956-74f2-984b-4cfcb1653b87",
            ),
            ClickHouseScratchRelationAbsence(
                kind="staging",
                name="events__staging_failed",
                expected_uuid="0398f11c-6956-74f2-984b-4cfcb1653b87",
            ),
        ),
    )


def _released_resource_closure() -> MssqlProtectedResourceAllocationClosure:
    allocations = tuple(
        MssqlProtectedResourceAllocation(
            allocation_id=mssql_resource_allocation_id(_RESERVATION_ID, _OPERATION_ID, resource_kind),
            reservation_id=_RESERVATION_ID,
            workflow_execution_binding_sha256=_WORKFLOW_EXECUTION_BINDING_SHA256,
            operation_id=_OPERATION_ID,
            resource_kind=resource_kind,
            amount=ordinal,
            status="RELEASED",
        )
        for ordinal, resource_kind in enumerate(MSSQL_RESOURCE_KINDS, start=1)
    )
    return MssqlProtectedResourceAllocationClosure(
        workflow_execution_binding_sha256=_WORKFLOW_EXECUTION_BINDING_SHA256,
        operation_id=_OPERATION_ID,
        reservation_id=_RESERVATION_ID,
        allocations=allocations,
    )


def _cleanup_ack() -> MssqlFailedPrecommitCleanupAck:
    return MssqlFailedPrecommitCleanupAck.build(
        scratch=_scratch_cleanup_receipt(),
        resources=_released_resource_closure(),
    )


def cleanup_golden_documents() -> dict[str, dict[str, object]]:
    """Produce deterministic cleanup evidence vectors from the frozen DTOs."""

    scratch = _scratch_cleanup_receipt().to_mapping()
    acknowledgement = _cleanup_ack().to_mapping()
    return {
        CLICKHOUSE_FAILED_SCRATCH_CLEANUP_RECEIPT_SCHEMA: scratch,
        MSSQL_FAILED_PRECOMMIT_CLEANUP_ACK_SCHEMA: acknowledgement,
    }


def test_cleanup_documents_are_schema_valid_and_digest_bound() -> None:
    schemas = semantic_refresh_contract_schemas()
    documents = cleanup_golden_documents()

    for schema_id, document in documents.items():
        assert document["schema"] == schema_id
        Draft202012Validator(schemas[schema_id]).validate(document)

    scratch = documents[CLICKHOUSE_FAILED_SCRATCH_CLEANUP_RECEIPT_SCHEMA]
    scratch_unsigned = {key: value for key, value in scratch.items() if key != "scratch_absence_evidence_sha256"}
    assert scratch["scratch_absence_evidence_sha256"] == semantic_refresh_sha256(scratch_unsigned)

    acknowledgement = documents[MSSQL_FAILED_PRECOMMIT_CLEANUP_ACK_SCHEMA]
    acknowledgement_unsigned = {key: value for key, value in acknowledgement.items() if key != "cleanup_receipt_sha256"}
    assert acknowledgement["cleanup_receipt_sha256"] == semantic_refresh_sha256(acknowledgement_unsigned)
    scratch_evidence = _mapping(acknowledgement["scratch_absence_evidence"])
    assert acknowledgement["scratch_absence_evidence_sha256"] == scratch_evidence["scratch_absence_evidence_sha256"]
    assert acknowledgement["resource_allocation_closure_sha256"] == semantic_refresh_sha256(
        _mapping(acknowledgement["resource_allocation_closure"])
    )


def test_clickhouse_cleanup_schema_rejects_open_or_incomplete_relation_closure() -> None:
    schema = semantic_refresh_contract_schemas()[CLICKHOUSE_FAILED_SCRATCH_CLEANUP_RECEIPT_SCHEMA]
    receipt = _scratch_cleanup_receipt().to_mapping()
    relations = _mapping_list(receipt["relations"])

    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate({**receipt, "airflow_success": True})

    missing_relation = dict(receipt)
    missing_relation["relations"] = relations[:1]
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(missing_relation)

    reversed_relations = dict(receipt)
    reversed_relations["relations"] = list(reversed(relations))
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(reversed_relations)

    open_relation = dict(receipt)
    first_relation = {**relations[0], "drop_query_succeeded": True}
    open_relation["relations"] = [first_relation, relations[1]]
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(open_relation)


def test_mssql_cleanup_ack_schema_requires_complete_released_five_kind_closure() -> None:
    schema = semantic_refresh_contract_schemas()[MSSQL_FAILED_PRECOMMIT_CLEANUP_ACK_SCHEMA]
    acknowledgement = _cleanup_ack().to_mapping()

    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate({**acknowledgement, "task_state": "success"})

    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate({**acknowledgement, "status": "INCOMPLETE"})

    incomplete = dict(acknowledgement)
    resource_closure = _mapping(acknowledgement["resource_allocation_closure"])
    resource_allocations = _mapping_list(resource_closure["allocations"])
    closure = dict(resource_closure)
    closure["allocations"] = resource_allocations[:-1]
    incomplete["resource_allocation_closure"] = closure
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(incomplete)

    reserved = dict(acknowledgement)
    closure = dict(resource_closure)
    allocations = [dict(item) for item in resource_allocations]
    allocations[0]["status"] = "RESERVED"
    closure["allocations"] = allocations
    reserved["resource_allocation_closure"] = closure
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(reserved)

    open_allocation = dict(acknowledgement)
    closure = dict(resource_closure)
    allocations = [dict(item) for item in resource_allocations]
    allocations[0]["release_task_state"] = "success"
    closure["allocations"] = allocations
    open_allocation["resource_allocation_closure"] = closure
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(open_allocation)

    wrong_kind_order = dict(acknowledgement)
    closure = dict(resource_closure)
    closure["allocations"] = list(reversed(resource_allocations))
    wrong_kind_order["resource_allocation_closure"] = closure
    with pytest.raises(ValidationError):
        Draft202012Validator(schema).validate(wrong_kind_order)


def test_cleanup_schema_bytes_and_golden_vectors_are_stable() -> None:
    documents = cleanup_golden_documents()
    for schema_id in documents:
        checked = Path("docs/schemas/dbt", f"{schema_id}.schema.json")
        assert checked.read_bytes() == render_semantic_refresh_schema(schema_id)

    expected = json.loads(
        Path("tests/fixtures/semantic-refresh-v2/contracts/cleanup-golden-v1.json").read_text(encoding="utf-8")
    )
    assert documents == expected
