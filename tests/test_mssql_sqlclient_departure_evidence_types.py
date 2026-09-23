"""Pure helper evidence bounds and subject binding never imply producer trust."""

from dataclasses import FrozenInstanceError, replace
from enum import StrEnum
from typing import Any
from uuid import UUID

import pytest

from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    EVIDENCE_LIMITS,
    evidence_limit,
    evidence_name,
    require_payload,
)
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceKind as Kind
from dpone.contracts.mssql_sqlclient_departure_evidence_types import (
    SqlClientDepartureEvidenceObservation as Observation,
)
from dpone.contracts.mssql_sqlclient_departure_evidence_types import SqlClientDepartureEvidenceReceipt as Receipt

ERROR = "^mssql_native\\.sqlclient_departure_evidence_invalid$"
HELPER = UUID("12345678-1234-1234-1234-123456789abc")
ATTEMPT = "a" * 64
PAYLOAD_HASH = "b" * 64
CAPS = {
    "launch_intent": 131072,
    "registration": 32768,
    "credential_intent": 131072,
    "result": 65536,
    "local_exit": 16384,
    "exclusion": 16384,
}


class TextAlias(str):
    """Equal scalar subclasses must not enter an exact contract."""


class IntegerAlias(int):
    """Equal integer subclass."""


class BytesAlias(bytes):
    """Equal bytes subclass."""


class UUIDAlias(UUID):
    """Equal UUID subclass."""


class KindAlias(StrEnum):
    RESULT = "result"


def receipt(kind: Kind = Kind.RESULT, **changes: Any) -> Receipt:
    fields: dict[str, Any] = {
        "helper_id": HELPER,
        "attempt_sha256": ATTEMPT,
        "kind": kind,
        "relative_name": evidence_name(HELPER, ATTEMPT, kind, PAYLOAD_HASH),
        "payload_sha256": PAYLOAD_HASH,
        "byte_count": 1,
    }
    fields.update(changes)
    return Receipt(**fields)


def test_closed_vocabulary_and_immutable_limits() -> None:
    assert {kind.name: kind.value for kind in Kind} == {name.upper(): name for name in CAPS}
    assert dict(EVIDENCE_LIMITS) == {Kind(name): cap for name, cap in CAPS.items()}
    mutable: Any = EVIDENCE_LIMITS
    with pytest.raises(TypeError):
        mutable[Kind.RESULT] = 1


@pytest.mark.parametrize("kind", list(Kind))
def test_payload_and_count_boundaries(kind: Kind) -> None:
    cap = CAPS[kind.value]
    assert evidence_limit(kind) == cap
    # Arbitrary non-JSON bytes are valid here: parsing belongs to the codec.
    for size in (1, cap):
        require_payload(b"\xff" * size, kind)
        assert receipt(kind, byte_count=size).byte_count == size
    for size in (0, cap + 1):
        with pytest.raises(ValueError, match=ERROR):
            require_payload(b"x" * size, kind)
        with pytest.raises(ValueError, match=ERROR):
            receipt(kind, byte_count=size)


@pytest.mark.parametrize("payload", [None, True, 1, "{}", bytearray(b"{}"), memoryview(b"{}"), BytesAlias(b"{}")])
def test_payload_requires_exact_bytes(payload: Any) -> None:
    with pytest.raises(ValueError, match=ERROR):
        require_payload(payload, Kind.RESULT)


@pytest.mark.parametrize("kind", [None, True, 1, "result", TextAlias("result"), KindAlias.RESULT])
def test_kind_aliases_rejected_at_every_entry(kind: Any) -> None:
    for operation in (
        lambda: evidence_limit(kind),
        lambda: require_payload(b"x", kind),
        lambda: evidence_name(HELPER, ATTEMPT, kind, PAYLOAD_HASH),
        lambda: receipt(kind=kind, relative_name="unused"),
    ):
        with pytest.raises(ValueError, match=ERROR):
            operation()


@pytest.mark.parametrize("kind", list(Kind))
def test_independent_filename_fixture_is_bounded(kind: Kind) -> None:
    expected = (
        "tds-sqlclient-departure-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-"
        "12345678-1234-1234-1234-123456789abc-"
        + kind.value
        + "-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.json"
    )
    assert evidence_name(HELPER, ATTEMPT, kind, PAYLOAD_HASH) == expected
    assert receipt(kind).relative_name == expected
    assert len(expected.encode("utf-8")) <= 255
    assert "/" not in expected and "\\" not in expected


@pytest.mark.parametrize("helper", [None, True, 1, str(HELPER), UUID(int=0), UUIDAlias(str(HELPER))])
def test_helper_requires_exact_nonzero_uuid(helper: Any) -> None:
    for operation in (
        lambda: evidence_name(helper, ATTEMPT, Kind.RESULT, PAYLOAD_HASH),
        lambda: receipt(helper_id=helper),
        lambda: Observation(helper, ATTEMPT),
    ):
        with pytest.raises(ValueError, match=ERROR):
            operation()


@pytest.mark.parametrize(
    "value", [None, True, 1, "", "a" * 63, "a" * 65, "A" * 64, "g" * 64, "a" * 63 + "\n", TextAlias(ATTEMPT)]
)
def test_hashes_require_exact_lower_hex(value: Any) -> None:
    for operation in (
        lambda: evidence_name(HELPER, value, Kind.RESULT, PAYLOAD_HASH),
        lambda: evidence_name(HELPER, ATTEMPT, Kind.RESULT, value),
        lambda: receipt(attempt_sha256=value),
        lambda: receipt(payload_sha256=value),
        lambda: Observation(HELPER, value),
    ):
        with pytest.raises(ValueError, match=ERROR):
            operation()


@pytest.mark.parametrize("count", [None, True, False, 1.0, "1", -1, IntegerAlias(1)])
def test_counts_require_exact_integer(count: Any) -> None:
    with pytest.raises(ValueError, match=ERROR):
        receipt(byte_count=count)


@pytest.mark.parametrize("name", [None, True, 1, "", "../artifact.json", "arbitrary.json"])
def test_receipt_rejects_arbitrary_names(name: Any) -> None:
    with pytest.raises(ValueError, match=ERROR):
        receipt(relative_name=name)


def test_equal_filename_subclass_is_rejected() -> None:
    with pytest.raises(ValueError, match=ERROR):
        receipt(relative_name=TextAlias(receipt().relative_name))


@pytest.mark.parametrize(
    "field,value",
    [
        ("helper_id", UUID(int=0)),
        ("attempt_sha256", "A" * 64),
        ("kind", "result"),
        ("relative_name", "../artifact.json"),
        ("payload_sha256", "g" * 64),
        ("byte_count", True),
        ("byte_count", 65537),
    ],
)
def test_observation_revalidates_mutated_receipt(field: str, value: Any) -> None:
    mutated = receipt()
    object.__setattr__(mutated, field, value)
    with pytest.raises(ValueError, match=ERROR):
        Observation(HELPER, ATTEMPT, mutated)


@pytest.mark.parametrize("nested", [True, 1, {}, "receipt", object()])
def test_observation_rejects_wrong_receipt_type(nested: Any) -> None:
    with pytest.raises(ValueError, match=ERROR):
        Observation(HELPER, ATTEMPT, nested)


def test_observation_rejects_receipt_subclass() -> None:
    class ReceiptAlias(Receipt):
        pass

    valid = receipt()
    alias = ReceiptAlias(HELPER, ATTEMPT, Kind.RESULT, valid.relative_name, PAYLOAD_HASH, 1)
    with pytest.raises(ValueError, match=ERROR):
        Observation(HELPER, ATTEMPT, alias)


@pytest.mark.parametrize("source", range(3))
@pytest.mark.parametrize("target", range(3))
@pytest.mark.parametrize("vary", ["helper", "attempt", "both"])
def test_three_subjects_cannot_cross_bind(source: int, target: int, vary: str) -> None:
    helpers = [UUID(int=index + 1) if vary != "attempt" else HELPER for index in range(3)]
    attempts = [str(index) * 64 if vary != "helper" else ATTEMPT for index in range(3)]
    nested = receipt(
        helper_id=helpers[source],
        attempt_sha256=attempts[source],
        relative_name=evidence_name(helpers[source], attempts[source], Kind.RESULT, PAYLOAD_HASH),
    )
    if source == target:
        assert Observation(helpers[target], attempts[target], nested).receipt is nested
    else:
        with pytest.raises(ValueError, match=ERROR):
            Observation(helpers[target], attempts[target], nested)


def test_frozen_slotted_values_and_optional_receipt() -> None:
    assert Observation(HELPER, ATTEMPT).receipt is None
    for value in (receipt(), Observation(HELPER, ATTEMPT, receipt())):
        assert not hasattr(value, "__dict__")
        with pytest.raises(FrozenInstanceError):
            setattr(value, "helper_id", UUID(int=7))
    assert replace(receipt(), byte_count=2).byte_count == 2
