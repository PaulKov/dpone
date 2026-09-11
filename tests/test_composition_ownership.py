"""Exact shared owner identities and complete physical claims, without authority."""

from dataclasses import FrozenInstanceError, replace
from typing import Any

import pytest

from dpone.contracts.composition_activation import CompositionAdmissionError, CompositionPhysicalResource
from dpone.contracts.composition_ownership import CompositionOwnerReference, CompositionPhysicalClaim
from dpone.contracts.composition_physical import CompositionPhysicalDomain

SERVICE = "11111111-1111-4111-8111-111111111111"
DIGEST = "sha256:" + "a" * 64
OTHER = "sha256:" + "b" * 64


def claim(**changes: Any) -> CompositionPhysicalClaim:
    values = dict(
        connector="mssql",
        service_id=SERVICE,
        physical_subject_sha256=DIGEST,
        observation_sha256=OTHER,
        role="mutation",
        read_subjects=(),
        write_subjects=(DIGEST,),
    )
    return CompositionPhysicalClaim(**dict(values, **changes))


def test_owner_kind_is_distinct_from_physical_identity() -> None:
    execution = CompositionOwnerReference("execution", SERVICE)
    qualification = CompositionOwnerReference("qualification", SERVICE)
    assert execution.owner_key == "sha256:746a522b65441758dd893e977bb7e51f5836bb39dfa32c1f2f365ac21996c4ea"
    assert qualification.owner_key == "sha256:3244c4163f7c0f980f28e58753b4ed700378894478b6a8c2b22b21f9aa730e84"
    assert execution != qualification
    assert len({execution, qualification}) == 2
    with pytest.raises(FrozenInstanceError):
        execution.owner_id = "changed"  # type: ignore[misc]
    assert not hasattr(execution, "__dict__")


@pytest.mark.parametrize("kind", ["", "activation", "Qualification", None, [], 1])
def test_owner_kind_is_closed(kind: Any) -> None:
    with pytest.raises(CompositionAdmissionError):
        CompositionOwnerReference(kind, SERVICE)


@pytest.mark.parametrize(
    "value",
    [
        None,
        1,
        SERVICE.replace("-", ""),
        "00000000-0000-0000-0000-000000000000",
        "ABCDEF11-1111-4111-8111-111111111111",
        "secret-invalid-uuid",
    ],
)
def test_canonical_nonzero_service_and_owner_ids(value: Any) -> None:
    for construct in (lambda: CompositionOwnerReference("execution", value), lambda: claim(service_id=value)):
        with pytest.raises(CompositionAdmissionError) as caught:
            construct()
        assert "secret" not in str(caught.value)
        assert caught.value.__suppress_context__ or caught.value.__context__ is None


def test_common_uuid_contract_has_no_version_restriction() -> None:
    value = "11111111-1111-1111-8111-111111111111"
    assert CompositionOwnerReference("execution", value).owner_id == value
    assert claim(service_id=value).service_id == value


@pytest.mark.parametrize(
    ("connector", "expected"),
    [
        ("mssql", "sha256:c08752823ff7ac2b6983a2e0f2bc028a3e82d9661551e8d6a73fd7a17f07f2f3"),
        ("clickhouse", "sha256:8a512f69acacb46ad3493ae97071842523403ebe077bdc08025018777ee79b94"),
        ("postgres", "sha256:91879df8135377225de21ebf67da11d07affdb9f9771222b4ccf43355efa4092"),
    ],
)
def test_exact_guard_vectors_and_legacy_equality(connector: Any, expected: str) -> None:
    value = claim(connector=connector)
    assert value.guard_id == expected
    assert (
        replace(
            value, role="retained_source", read_subjects=(OTHER,), write_subjects=(), observation_sha256=DIGEST
        ).guard_id
        == expected
    )
    if connector != "postgres":
        assert CompositionPhysicalDomain(connector, SERVICE, DIGEST).guard_id == expected
        assert CompositionPhysicalResource(expected, connector, SERVICE, DIGEST, OTHER, (DIGEST,)).guard_id == expected
    else:
        with pytest.raises(CompositionAdmissionError):
            CompositionPhysicalDomain(connector, SERVICE, DIGEST)
        with pytest.raises(CompositionAdmissionError):
            CompositionPhysicalResource(expected, connector, SERVICE, DIGEST, OTHER, (DIGEST,))
    assert replace(value, physical_subject_sha256=OTHER).guard_id != expected
    assert replace(value, service_id="22222222-2222-4222-8222-222222222222").guard_id != expected


@pytest.mark.parametrize(
    ("role", "reads", "writes"),
    [
        ("mutation", (), (DIGEST,)),
        ("mutation", (DIGEST,), (DIGEST, OTHER)),
        ("retained_source", (DIGEST,), ()),
        ("mutation_and_retained_source", (DIGEST,), (OTHER,)),
    ],
)
def test_complete_role_subjects_and_detached_roundtrip(
    role: Any, reads: tuple[str, ...], writes: tuple[str, ...]
) -> None:
    value = claim(role=role, read_subjects=reads, write_subjects=writes)
    assert value.effect_subjects == tuple(sorted(set(reads + writes)))
    encoded = value.to_dict()
    assert set(encoded) == {
        "connector",
        "service_id",
        "physical_subject_sha256",
        "observation_sha256",
        "role",
        "read_subjects",
        "write_subjects",
    }
    assert CompositionPhysicalClaim.from_dict(encoded) == value
    assert isinstance(encoded["read_subjects"], list)
    encoded["read_subjects"].append(OTHER)
    assert value.read_subjects == reads
    assert not hasattr(value, "__dict__")


@pytest.mark.parametrize(
    "changes",
    [
        {"connector": "sqlite"},
        {"connector": []},
        {"role": []},
        {"role": "writer"},
        {"write_subjects": ()},
        {"role": "retained_source"},
        {"role": "retained_source", "read_subjects": (DIGEST,)},
        {"role": "mutation_and_retained_source"},
        {"role": "mutation_and_retained_source", "read_subjects": (DIGEST,), "write_subjects": ()},
        {"read_subjects": [DIGEST]},
        {"write_subjects": [DIGEST]},
        {"read_subjects": (OTHER, DIGEST)},
        {"write_subjects": (DIGEST, DIGEST)},
        {"read_subjects": ([],)},
        {"write_subjects": (None,)},
        {"physical_subject_sha256": DIGEST.upper()},
        {"observation_sha256": True},
    ],
)
def test_invalid_claims_fail_with_composition_error(changes: dict[str, Any]) -> None:
    with pytest.raises(CompositionAdmissionError):
        claim(**changes)


def test_subject_budget_is_total_and_includes_read_write_overlap() -> None:
    subjects = tuple(f"sha256:{number:064x}" for number in range(8192))
    assert len(claim(write_subjects=subjects).effect_subjects) == 8192
    with pytest.raises(CompositionAdmissionError):
        claim(read_subjects=(subjects[0],), write_subjects=subjects)
    with pytest.raises(CompositionAdmissionError):
        claim(write_subjects=subjects + (f"sha256:{8192:064x}",))


@pytest.mark.parametrize(
    "field", ["schema", "guard_id", "effect_subjects", "owner", "phase", "campaign", "grant", "alias"]
)
def test_claim_decoder_rejects_unfrozen_fields(field: str) -> None:
    with pytest.raises(CompositionAdmissionError):
        CompositionPhysicalClaim.from_dict(dict(claim().to_dict(), **{field: DIGEST}))


def test_claim_decoder_requires_complete_ordinary_json_objects_and_arrays() -> None:
    body = claim().to_dict()
    for field in body:
        with pytest.raises(CompositionAdmissionError):
            CompositionPhysicalClaim.from_dict({key: value for key, value in body.items() if key != field})
    invalid_values: tuple[Any, ...] = (None, [], {**body, "read_subjects": ()}, {**body, "write_subjects": DIGEST})
    for value in invalid_values:
        with pytest.raises(CompositionAdmissionError):
            CompositionPhysicalClaim.from_dict(value)
