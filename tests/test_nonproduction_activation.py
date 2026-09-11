"""Exact scoped request identity; these offline claims are not admission authority."""

import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import (
    CompositionActivationOccurrence,
    CompositionActivationReceipt,
    CompositionActivationRequest,
    CompositionAdmissionError,
    CompositionOccurrenceContext,
    CompositionPhysicalResource,
    CompositionWorkloadAdmission,
)
from dpone.contracts.composition_attempt import CompositionAttemptIdentity, require_composition_attempt_scope
from dpone.contracts.composition_persistence import (
    decode_activation_request,
    decode_attempt_identity,
    encode_activation_request,
    encode_attempt_identity,
)
from dpone.contracts.nonproduction_activation import NonproductionCompositionActivationRequest
from dpone.contracts.nonproduction_grants import NonproductionExecutionGrant
from dpone.contracts.nonproduction_scope import MAX_DOCUMENT_BYTES, NonproductionAuthorityError
from dpone.contracts.strict_json import canonical_json_bytes
from tests.nonproduction_authority_helpers import digest, execution, identifier, limits, qualification
from tests.test_composition_activation_contract import request as legacy_request

CELLS = ("sqlserver_dbt_v1", "mssql_clickhouse_full_refresh_v1", "postgres_mssql_full_refresh_v1")
ERRORS = (NonproductionAuthorityError, CompositionAdmissionError)


def resources(workloads: tuple[CompositionWorkloadAdmission, ...]) -> tuple[CompositionPhysicalResource, ...]:
    result = []
    for connector in ("mssql", "clickhouse"):
        physical = digest(connector + " database continuity")
        service = identifier(20 if connector == "mssql" else 21)
        guard = canonical_fingerprint(
            {
                "schema": "dpone.composition-physical-domain.v1",
                "connector": connector,
                "service_id": service,
                "physical_subject_sha256": physical,
            }
        )
        writes = tuple(
            sorted(
                subject
                for row in workloads
                for subject in row.write_subjects
                if (row.execution_cell == CELLS[1]) == (connector == "clickhouse")
            )
        )
        if writes:
            result.append(CompositionPhysicalResource(guard, connector, service, physical, digest("catalog"), writes))
    return tuple(sorted(result, key=lambda row: row.guard_id))


def request(grant: NonproductionExecutionGrant | None = None) -> NonproductionCompositionActivationRequest:
    grant = execution() if grant is None else grant
    workloads = tuple(
        CompositionWorkloadAdmission(
            row.workload_id, row.constituent_id, row.pack_sha256, CELLS[index], (digest(row.workload_id + " write"),)
        )
        for index, row in enumerate(grant.workloads)
    )
    return NonproductionCompositionActivationRequest(
        context=CompositionOccurrenceContext(
            grant.activation_id,
            "synthetic",
            grant.parent_release_id,
            grant.deployment_id,
            None,
            digest("unchanged runtime context"),
        ),
        source_subject_sha256=digest("source closure"),
        workloads=workloads,
        resources=resources(workloads),
        execution_grant_sha256=grant.grant_sha256,
    )


def test_exact_new_wire_roundtrip_and_grant_comparison() -> None:
    value = request()
    raw = value.to_bytes()
    assert raw == canonical_json_bytes(value.to_dict())
    assert value.to_dict()["schema"] == "dpone.composition-activation-request.nonproduction.v1"
    assert value.request_sha256 == "sha256:" + hashlib.sha256(raw).hexdigest()
    assert NonproductionCompositionActivationRequest.from_bytes(raw) == value
    value.require_execution_grant(execution())
    assert isinstance(value, CompositionActivationRequest)


def test_legacy_family_retains_original_bytes_hash_and_closed_reader() -> None:
    legacy = legacy_request()
    raw = encode_activation_request(legacy)
    assert decode_activation_request(raw, legacy.request_sha256) == legacy
    assert legacy.request_sha256 == canonical_fingerprint(legacy.to_dict())
    with pytest.raises(NonproductionAuthorityError):
        NonproductionCompositionActivationRequest.from_bytes(raw)
    scoped = request()
    assert encode_activation_request(scoped) == scoped.to_bytes()
    with pytest.raises(CompositionAdmissionError):
        decode_activation_request(scoped.to_bytes(), scoped.request_sha256)


def test_grant_is_mandatory_and_does_not_change_runtime_context_or_physical_guards() -> None:
    value = request()
    changed = replace(value, execution_grant_sha256=digest("different execution grant"))
    assert changed.request_sha256 != value.request_sha256
    assert changed.context == value.context and changed.resources == value.resources
    with pytest.raises(TypeError):
        NonproductionCompositionActivationRequest(
            value.context, value.source_subject_sha256, value.workloads, value.resources
        )  # type: ignore[call-arg]


@pytest.mark.parametrize("bad", [None, "", "SHA256:" + "a" * 64, "sha256:" + "A" * 64, 1, True])
def test_noncanonical_grant_digest_is_rejected(bad: Any) -> None:
    with pytest.raises(NonproductionAuthorityError):
        replace(request(), execution_grant_sha256=bad)


def test_slash_and_backslash_have_distinct_exact_request_identity() -> None:
    value = request()
    variants = [
        replace(
            value, workloads=(replace(value.workloads[0], workload_id="a" + separator + "native"), *value.workloads[1:])
        )
        for separator in ("/", "\\")
    ]
    assert variants[0].to_bytes() != variants[1].to_bytes()
    assert variants[0].request_sha256 != variants[1].request_sha256


def test_request_and_detached_document_cannot_mutate_identity() -> None:
    value = request()
    original = value.request_sha256
    with pytest.raises(FrozenInstanceError):
        value.execution_grant_sha256 = digest("mutation")  # type: ignore[misc]
    value.to_dict()["workloads"][0]["workload_id"] = "mutated"
    assert value.request_sha256 == original


@pytest.mark.parametrize("section", [None, "context", "workloads", "resources"])
@pytest.mark.parametrize("change", ["unknown", "missing"])
def test_closed_fields_at_every_document_level(section: str | None, change: str) -> None:
    body = json.loads(request().to_bytes())
    target = body if section is None else body[section]
    if isinstance(target, list):
        target = target[0]
    if change == "unknown":
        target["caller_authority"] = "not trusted"
    else:
        target.pop(next(iter(target)))
    with pytest.raises(NonproductionAuthorityError):
        NonproductionCompositionActivationRequest.from_bytes(canonical_json_bytes(body))


@pytest.mark.parametrize(
    "change",
    [
        "whitespace",
        "duplicate",
        "wrong_schema",
        "old_schema",
        "grant_missing",
        "nan",
        "bad_utf8",
        "oversized",
        "empty",
        "array",
        "nested_write_string",
    ],
)
def test_noncanonical_or_wrong_family_documents_fail_safely(change: str) -> None:
    raw = request().to_bytes()
    body = json.loads(raw)
    if change == "whitespace":
        raw += b"\n"
    elif change == "duplicate":
        raw = raw[:-1] + b',"execution_grant_sha256":"' + execution().grant_sha256.encode() + b'"}'
    elif change in {"wrong_schema", "old_schema"}:
        body["schema"] = "unknown" if change == "wrong_schema" else "dpone.composition-activation-request.v1"
        raw = canonical_json_bytes(body)
    elif change == "grant_missing":
        del body["execution_grant_sha256"]
        raw = canonical_json_bytes(body)
    elif change == "nested_write_string":
        body["resources"][0]["write_subjects"] = body["resources"][0]["write_subjects"][0]
        raw = canonical_json_bytes(body)
    else:
        raw = {
            "nan": b'{"schema":NaN}',
            "bad_utf8": b"\xff",
            "oversized": b" " * (MAX_DOCUMENT_BYTES + 1),
            "empty": b"",
            "array": b"[]",
        }[change]
    with pytest.raises(NonproductionAuthorityError):
        NonproductionCompositionActivationRequest.from_bytes(raw)


@pytest.mark.parametrize("section", ["context", "workloads", "resources"])
@pytest.mark.parametrize("bad", [None, True, "claim", 3, {}])
def test_wrong_nested_shapes_are_sanitized(section: str, bad: Any) -> None:
    body = json.loads(request().to_bytes())
    body[section] = bad
    with pytest.raises(NonproductionAuthorityError):
        NonproductionCompositionActivationRequest.from_bytes(canonical_json_bytes(body))


@pytest.mark.parametrize("index", range(3))
def test_each_execution_cell_is_required(index: int) -> None:
    value = request()
    rows = tuple(row for position, row in enumerate(value.workloads) if position != index)
    with pytest.raises(ERRORS):
        replace(value, workloads=rows, resources=resources(rows))


@pytest.mark.parametrize("index", range(3))
def test_execution_cells_cannot_move_between_constituents(index: int) -> None:
    value = request()
    rows = list(value.workloads)
    rows[index] = replace(
        rows[index], constituent_id="standalone" if rows[index].constituent_id == "native" else "native"
    )
    with pytest.raises(ERRORS):
        replace(value, workloads=tuple(rows))


@pytest.mark.parametrize("count", [64, 65])
def test_complete_parent_workload_ceiling(count: int) -> None:
    value = request()
    rows = tuple(
        replace(value.workloads[index % 3], workload_id=f"w{index:03}", write_subjects=(digest(f"write {index}"),))
        for index in range(count)
    )
    if count == 64:
        scoped = replace(value, workloads=rows, resources=resources(rows))
        assert NonproductionCompositionActivationRequest.from_bytes(scoped.to_bytes()) == scoped
    else:
        with pytest.raises(NonproductionAuthorityError):
            replace(value, workloads=rows, resources=resources(rows))


@pytest.mark.parametrize(
    "change", ["duplicate_write", "missing_resource", "order", "mutable_workloads", "mutable_resources"]
)
def test_existing_exact_partition_and_immutable_membership_remain_required(change: str) -> None:
    value = request()
    changes: dict[str, Any] = {}
    if change == "duplicate_write":
        changes["workloads"] = (
            value.workloads[0],
            replace(value.workloads[1], write_subjects=value.workloads[0].write_subjects),
            value.workloads[2],
        )
    elif change == "missing_resource":
        changes["resources"] = value.resources[:1]
    elif change == "order":
        changes["workloads"] = value.workloads[::-1]
    else:
        name = change.removeprefix("mutable_")
        changes[name] = list(getattr(value, name))
    with pytest.raises(ERRORS):
        replace(value, **changes)


@pytest.mark.parametrize(
    "change", ["parent", "deployment", "activation", "pack", "member", "constituent", "lower_limit"]
)
def test_refingerprinted_execution_grant_cannot_move_scoped_subjects(change: str) -> None:
    grant = execution()
    if change in {"parent", "deployment", "activation"}:
        field = {"parent": "parent_release_id", "deployment": "deployment_id", "activation": "activation_id"}[change]
        changed = replace(grant, **{field: identifier(30) if change == "activation" else digest("foreign")})
    elif change == "lower_limit":
        changed = replace(grant, workloads=grant.workloads[1:], limits=limits(max_workloads=2))
    else:
        field = {"pack": "pack_sha256", "member": "workload_id", "constituent": "constituent_id"}[change]
        alternate = {"pack": digest("foreign pack"), "member": "a_foreign", "constituent": "standalone"}[change]
        changed = replace(grant, workloads=(replace(grant.workloads[0], **{field: alternate}), *grant.workloads[1:]))
    value = replace(request(), execution_grant_sha256=changed.grant_sha256)
    with pytest.raises(NonproductionAuthorityError):
        value.require_execution_grant(changed)


@pytest.mark.parametrize("grant", [qualification(), None, object(), replace(execution(), grant_id=identifier(31))])
def test_other_phase_untyped_or_foreign_grant_is_never_execution_permission(grant: Any) -> None:
    with pytest.raises(NonproductionAuthorityError):
        request().require_execution_grant(grant)


def test_structural_comparison_does_not_claim_clock_or_signature_verification() -> None:
    # Past, structurally valid claims still compare. Trusted authentication checks time separately.
    grant = replace(execution(), not_before="2020-01-01T00:00:00Z", expires_at="2020-01-01T01:00:00Z")
    request(grant).require_execution_grant(grant)


def test_existing_receipt_attempt_and_scope_shapes_bind_new_exact_request_digest() -> None:
    value = request()
    epochs = tuple((row.guard_id, 1) for row in value.resources)
    occurrence = CompositionActivationOccurrence(
        value, CompositionActivationReceipt(value.request_sha256, "RETIRING", epochs)
    )
    workload = value.workloads[0]
    selected = tuple(
        (row.guard_id, 1) for row in value.resources if set(row.write_subjects).intersection(workload.write_subjects)
    )
    attempt = CompositionAttemptIdentity(
        value.request_sha256,
        workload.workload_id,
        workload.constituent_id,
        workload.pack_sha256,
        digest("plan"),
        "run",
        "task",
        1,
        -1,
        selected,
    )
    assert decode_attempt_identity(encode_attempt_identity(attempt), attempt.attempt_sha256) == attempt
    assert require_composition_attempt_scope(occurrence, attempt) == frozenset(guard for guard, _ in selected)
    for changed in (
        replace(attempt, activation_request_sha256=digest("foreign parent")),
        replace(attempt, guard_epochs=tuple((guard, epoch + 1) for guard, epoch in selected)),
    ):
        with pytest.raises(CompositionAdmissionError):
            require_composition_attempt_scope(occurrence, changed)
