"""Strict original qualification bytes and structural owner binding only."""

import hashlib
import json
from dataclasses import FrozenInstanceError, replace
from typing import Any, Literal

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_ownership import CompositionOwnerReference, CompositionPhysicalClaim
from dpone.contracts.composition_persistence import (
    decode_activation_request,
    decode_attempt_identity,
    decode_attempt_proof,
)
from dpone.contracts.composition_qualification_operation import (
    CompositionQualificationOperation,
    CompositionQualificationOwner,
)
from dpone.contracts.strict_json import canonical_json_bytes

RUN = "11111111-1111-4111-8111-111111111111"
NEXT_RUN = "22222222-2222-4222-8222-222222222222"
UUID1 = "11111111-1111-1111-8111-111111111111"
DIGEST = "sha256:" + "a" * 64
OTHER = "sha256:" + "b" * 64
OWNER_SCHEMA = "dpone.composition-qualification-owner.v1"
OPERATION_SCHEMA = "dpone.composition-qualification-operation.v1"


def sha(raw: bytes) -> str:
    return "sha256:" + hashlib.sha256(raw).hexdigest()


def owner(**changes: Any) -> CompositionQualificationOwner:
    connectors: tuple[Literal["mssql", "clickhouse", "postgres"], ...] = ("mssql", "clickhouse", "postgres")
    claims = tuple(
        sorted(
            (
                CompositionPhysicalClaim(
                    connector, RUN, DIGEST, OTHER, "mutation_and_retained_source", (DIGEST,), (OTHER,)
                )
                for connector in connectors
            ),
            key=lambda row: row.guard_id,
        )
    )
    return CompositionQualificationOwner(
        **dict(
            dict(
                qualification_run_id=RUN,
                consumption_subject_sha256=DIGEST,
                grant_sha256=DIGEST,
                fixture_plan_sha256=DIGEST,
                qualification_plan_sha256=DIGEST,
                scope_sha256=DIGEST,
                environment_id=RUN,
                campaign_id=RUN,
                claims=claims,
            ),
            **changes,
        )
    )


def operation(**changes: Any) -> CompositionQualificationOperation:
    original = owner()
    return CompositionQualificationOperation(
        **dict(
            dict(
                owner_key=original.owner_key,
                owner_subject_sha256=original.subject_sha256,
                qualification_run_id=RUN,
                grant_sha256=DIGEST,
                fixture_plan_sha256=DIGEST,
                qualification_plan_sha256=DIGEST,
                work_item_id="route/α\\fixture",
                work_item_sha256=DIGEST,
                action="route_qualification",
                runner_invocation_id=RUN,
                try_number=1,
                guard_epochs=tuple((row.guard_id, 1) for row in original.claims),
            ),
            **changes,
        )
    )


@pytest.mark.parametrize(
    "construct,schema,hash_property",
    [(owner, OWNER_SCHEMA, "subject_sha256"), (operation, OPERATION_SCHEMA, "operation_key")],
)
def test_exact_original_document_roundtrip(construct: Any, schema: str, hash_property: str) -> None:
    value = construct()
    body = value.to_dict()
    assert body["schema"] == schema
    raw = value.to_bytes()
    assert raw == json.dumps(body, sort_keys=True, ensure_ascii=False, separators=(",", ":"), allow_nan=False).encode()
    expected = sha(raw)
    assert getattr(value, hash_property) == expected
    assert type(value).from_bytes(raw, expected_sha256=expected) == value
    assert hash(value) == hash(type(value).from_bytes(raw, expected_sha256=expected))
    with pytest.raises(TypeError):
        type(value).from_bytes(raw)
    with pytest.raises(CompositionAdmissionError):
        type(value).from_bytes(raw, expected_sha256=OTHER)
    assert not hasattr(value, "__dict__")
    with pytest.raises(FrozenInstanceError):
        value.qualification_run_id = NEXT_RUN


def test_owner_reference_and_complete_original_claims_are_bound() -> None:
    original = owner()
    assert original.owner_reference == CompositionOwnerReference("qualification", RUN)
    assert original.owner_key == original.owner_reference.owner_key
    body = original.to_dict()
    assert isinstance(body["claims"], list)
    body["claims"].clear()
    assert len(original.claims) == 3
    operation().require_owner(original)
    changes: dict[str, Any] = {
        field: OTHER
        for field in (
            "consumption_subject_sha256",
            "grant_sha256",
            "fixture_plan_sha256",
            "qualification_plan_sha256",
            "scope_sha256",
        )
    }
    changes.update(environment_id=NEXT_RUN, campaign_id=NEXT_RUN)
    for field, value in changes.items():
        changed = replace(original, **{field: value})
        assert changed.owner_key == original.owner_key
        assert changed.subject_sha256 != original.subject_sha256
        with pytest.raises(CompositionAdmissionError):
            operation().require_owner(changed)
    changed_claims = (replace(original.claims[0], observation_sha256=DIGEST), *original.claims[1:])
    with pytest.raises(CompositionAdmissionError):
        operation().require_owner(replace(original, claims=changed_claims))
    assert owner(qualification_run_id=NEXT_RUN).owner_key != original.owner_key


@pytest.mark.parametrize(
    "field", ["owner_subject_sha256", "grant_sha256", "fixture_plan_sha256", "qualification_plan_sha256"]
)
def test_operation_rejects_changed_original_owner_bindings(field: str) -> None:
    changed = operation(**{field: OTHER})
    assert changed.operation_key != operation().operation_key
    with pytest.raises(CompositionAdmissionError):
        changed.require_owner(owner())


def test_operation_scope_is_a_structural_subset_and_detached() -> None:
    value = operation(guard_epochs=(operation().guard_epochs[0],))
    value.require_owner(owner())
    detached = value.to_dict()
    epochs = detached["guard_epochs"]
    assert isinstance(epochs, list)
    epochs[0][1] = 99
    assert value.guard_epochs[0][1] == 1
    with pytest.raises(CompositionAdmissionError):
        operation(guard_epochs=((OTHER, 1),)).require_owner(owner())
    wrong_owners: tuple[Any, ...] = (None, {}, CompositionOwnerReference("qualification", RUN))
    for wrong_owner in wrong_owners:
        with pytest.raises(CompositionAdmissionError):
            value.require_owner(wrong_owner)


@pytest.mark.parametrize(
    "changes",
    [
        {"action": "fixture_seed"},
        {"action": "source_seal"},
        {"runner_invocation_id": NEXT_RUN},
        {"try_number": 2},
        {"work_item_sha256": OTHER},
        {"work_item_id": "route/α/fixture"},
        {"work_item_id": "route/é"},
        {"work_item_id": "route/e\u0301"},
    ],
)
def test_exact_operation_changes_have_distinct_keys(changes: dict[str, Any]) -> None:
    assert operation(**changes).operation_key != operation().operation_key


def test_run_and_epoch_changes_are_bound() -> None:
    value = operation()
    changed = replace(value, qualification_run_id=NEXT_RUN, owner_key=owner(qualification_run_id=NEXT_RUN).owner_key)
    assert changed.operation_key != value.operation_key
    with pytest.raises(CompositionAdmissionError):
        changed.require_owner(owner())
    assert (
        replace(value, guard_epochs=tuple((guard, 2) for guard, _ in value.guard_epochs)).operation_key
        != value.operation_key
    )
    assert operation(work_item_id="route/é").operation_key != operation(work_item_id="route/e\u0301").operation_key


@pytest.mark.parametrize(
    "changes",
    [
        {"owner_key": OTHER},
        {"owner_key": CompositionOwnerReference("execution", RUN).owner_key},
        {"qualification_run_id": UUID1},
        {"runner_invocation_id": UUID1},
        {"action": "execute"},
        {"action": []},
        {"work_item_id": " a"},
        {"work_item_id": "a "},
        {"work_item_id": "a\nb"},
        {"work_item_id": ""},
        {"work_item_id": "a" * 513},
        {"work_item_id": 3},
        {"work_item_id": "\ud800"},
        {"try_number": True},
        {"try_number": 0},
        {"try_number": -1},
        {"try_number": 2**63},
        {"try_number": 1.0},
        {"guard_epochs": ()},
        {"guard_epochs": []},
        {"guard_epochs": ((DIGEST, True),)},
        {"guard_epochs": ((DIGEST, 0),)},
        {"guard_epochs": ((DIGEST, 2**63),)},
        {"guard_epochs": ([DIGEST, 1],)},
        {"guard_epochs": ((DIGEST, 1, 2),)},
        {"guard_epochs": ((OTHER, 1), (DIGEST, 2))},
        {"guard_epochs": ((DIGEST, 1), (DIGEST, 2))},
        {"guard_epochs": ((None, 1),)},
    ],
)
def test_invalid_operations_are_value_free_composition_errors(changes: dict[str, Any]) -> None:
    with pytest.raises(CompositionAdmissionError) as caught:
        operation(**changes)
    assert caught.value.__suppress_context__ or caught.value.__context__ is None


def test_numeric_text_and_guard_count_boundaries() -> None:
    value = operation(
        try_number=2**63 - 1,
        work_item_id="x" * 512,
        guard_epochs=tuple((f"sha256:{n:064x}", 2**63 - 1) for n in range(256)),
    )
    assert CompositionQualificationOperation.from_bytes(value.to_bytes(), expected_sha256=value.operation_key) == value
    with pytest.raises(CompositionAdmissionError):
        replace(value, guard_epochs=value.guard_epochs + ((OTHER, 1),))


@pytest.mark.parametrize(
    "changes",
    [
        {"qualification_run_id": UUID1},
        {"campaign_id": UUID1},
        {"environment_id": "00000000-0000-0000-0000-000000000000"},
        {"claims": ()},
        {"claims": []},
        {"claims": (None,)},
        {"grant_sha256": DIGEST.upper()},
        {"scope_sha256": 1},
    ],
)
def test_invalid_owners(changes: dict[str, Any]) -> None:
    with pytest.raises(CompositionAdmissionError):
        owner(**changes)


def test_owner_claims_require_complete_unique_sorted_guard_partition() -> None:
    claims = owner().claims
    assert owner(environment_id=UUID1).environment_id == UUID1
    for bad in (
        claims[:-1],
        tuple(reversed(claims)),
        (claims[0], *claims),
        (replace(claims[0], observation_sha256=DIGEST), *claims),
    ):
        with pytest.raises(CompositionAdmissionError):
            owner(claims=bad)


def test_owner_partition_count_and_total_subject_budget() -> None:
    base = owner().claims
    extras = tuple(replace(base[0], physical_subject_sha256=f"sha256:{n:064x}") for n in range(254))
    maximum = tuple(sorted(base + extras[:-1], key=lambda row: row.guard_id))
    assert len(owner(claims=maximum).claims) == 256
    with pytest.raises(CompositionAdmissionError):
        owner(claims=tuple(sorted(base + extras, key=lambda row: row.guard_id)))
    subjects = tuple(f"sha256:{n:064x}" for n in range(8187))
    maximum_scope = (replace(base[0], read_subjects=(DIGEST,), write_subjects=subjects), *base[1:])
    assert owner(claims=maximum_scope).to_bytes()
    with pytest.raises(CompositionAdmissionError):
        owner(claims=(replace(maximum_scope[0], write_subjects=subjects + (DIGEST,)), *base[1:]))


@pytest.mark.parametrize("construct", [owner, operation])
def test_decoders_reject_unknown_missing_nested_and_wrong_type_fields(construct: Any) -> None:
    value = construct()
    body = value.to_dict()
    for field in body:
        invalid = {key: entry for key, entry in body.items() if key != field}
        raw = canonical_json_bytes(invalid)
        with pytest.raises(CompositionAdmissionError):
            type(value).from_bytes(raw, expected_sha256=sha(raw))
    for changes in (
        {"schema": "secret-schema"},
        {"secret-field": "secret-value"},
        {"qualification_run_id": 1},
        {"grant_sha256": None},
    ):
        raw = canonical_json_bytes(dict(body, **changes))
        with pytest.raises(CompositionAdmissionError) as caught:
            type(value).from_bytes(raw, expected_sha256=sha(raw))
        assert "secret" not in str(caught.value)
    field = "claims" if construct is owner else "guard_epochs"
    nested_values: tuple[Any, ...] = (None, {}, [], [None], [[DIGEST, 1, 2]])
    for nested_value in nested_values:
        raw = canonical_json_bytes(dict(body, **{field: nested_value}))
        with pytest.raises(CompositionAdmissionError):
            type(value).from_bytes(raw, expected_sha256=sha(raw))


@pytest.mark.parametrize("construct", [owner, operation])
def test_decoders_reject_noncanonical_ambiguous_unbounded_originals(construct: Any) -> None:
    value = construct()
    raw = value.to_bytes()
    invalids = [
        b"",
        b" " * (1024 * 1024 + 1),
        bytearray(raw),
        raw.decode(),
        b"\xff",
        b"[]",
        b"null",
        b'{"secret":NaN}',
        b'{"secret":Infinity}',
        b'{"secret":1e999}',
        b'{"secret":1,"secret":2}',
        raw + b"\n",
        b" " + raw,
        raw.replace(b'{"', b'{ "', 1),
        raw.replace(b'"schema":', b'"schema":"duplicate","schema":'),
        json.dumps(value.to_dict(), sort_keys=False).encode(),
        b'{"nested":' + b"[" * 2000 + b"]" * 2000 + b"}",
    ]
    if construct is operation:
        invalids.append(raw.replace("α".encode(), b"\\u03b1"))
    for invalid in invalids:
        with pytest.raises(CompositionAdmissionError) as caught:
            type(value).from_bytes(invalid, expected_sha256=sha(raw))
        assert "secret" not in str(caught.value)
        assert caught.value.__suppress_context__ or caught.value.__context__ is None


def test_frozen_original_identity_vectors() -> None:
    assert owner().subject_sha256 == "sha256:67b569cd9eddda76cb72f47677d7d176dea4fb24c78ca57b126b099a18df2b25"
    assert operation().operation_key == "sha256:2a7213b01ce44aee667c73c0fe84570c32b490401008e41e7c3a77150f8d9389"


@pytest.mark.parametrize("construct", [owner, operation])
@pytest.mark.parametrize("expected", [None, True, 1, "", DIGEST.upper()])
def test_expected_hash_is_a_mandatory_canonical_digest(construct: Any, expected: Any) -> None:
    value = construct()
    with pytest.raises(CompositionAdmissionError):
        type(value).from_bytes(value.to_bytes(), expected_sha256=expected)


@pytest.mark.parametrize("construct", [owner, operation])
def test_internal_families_never_enter_legacy_readers(construct: Any) -> None:
    raw = construct().to_bytes()
    for decode in (decode_activation_request, decode_attempt_identity, decode_attempt_proof):
        with pytest.raises(CompositionAdmissionError):
            decode(raw, sha(raw))
    foreign_type = CompositionQualificationOperation if construct is owner else CompositionQualificationOwner
    with pytest.raises(CompositionAdmissionError):
        foreign_type.from_bytes(raw, expected_sha256=sha(raw))
