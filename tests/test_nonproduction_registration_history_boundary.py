"""Historical record policy and decode order; offline, never SQL certification."""

from __future__ import annotations

import hashlib
import json
from dataclasses import fields
from datetime import UTC, datetime, timedelta, timezone
from typing import Any

import pytest

from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.nonproduction_mssql_registration import MssqlNonproductionRegistrationStore, _values
from dpone.adapters.nonproduction_mssql_registration_boundary import _RegistrationBoundary
from dpone.adapters.nonproduction_mssql_trust import NonproductionTrustRevision
from dpone.contracts.nonproduction_registration import (
    NonproductionGrantRegistration,
    NonproductionRegistrationOriginals,
)
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError
from tests.nonproduction_authority_helpers import NOW, identifier
from tests.nonproduction_mssql_registration_helpers import SERVICE, setup
from tests.nonproduction_signature_helpers import github_policy, trust
from tests.test_nonproduction_registration import originals

REGISTERED = "2026-09-10T01:30:00Z"


def _record(*, revision: int = 2, registered_at: str = REGISTERED) -> NonproductionGrantRegistration:
    return NonproductionGrantRegistration(originals(), revision, registered_at)


def _assert_error(error: BaseException, reason: str) -> None:
    assert type(error) is NonproductionAuthorityError
    assert error.reason == reason
    assert str(error) == f"DPONE_NONPRODUCTION_AUTHORITY_INVALID: {reason}"


@pytest.mark.parametrize(
    ("registered_at", "revision", "accepted"),
    [
        ("2026-09-10T01:29:59Z", 1, True),
        ("2026-09-10T01:29:59Z", 2, True),
        ("2026-09-10T01:29:59Z", 3, False),
        (REGISTERED, 1, True),
        (REGISTERED, 2, True),
        (REGISTERED, 3, False),
        ("2026-09-10T01:30:01Z", 1, False),
        ("2026-09-10T01:30:01Z", 2, False),
        ("2026-09-10T01:30:01Z", 3, False),
    ],
)
def test_independent_time_and_revision_boundaries(registered_at: str, revision: int, accepted: bool) -> None:
    record = _record(revision=revision, registered_at=registered_at)
    if accepted:
        instant = record.require_historical_boundary(observed_at=NOW, maximum_revision=2)
        assert instant == (NOW - timedelta(seconds=1) if registered_at.endswith("29:59Z") else NOW)
        assert instant.tzinfo is UTC
    else:
        with pytest.raises(NonproductionAuthorityError) as caught:
            record.require_historical_boundary(observed_at=NOW, maximum_revision=2)
        _assert_error(caught.value, "registration_history")
    assert record == _record(revision=revision, registered_at=registered_at)


def test_future_time_alone_rejects() -> None:
    with pytest.raises(NonproductionAuthorityError) as caught:
        _record(revision=1).require_historical_boundary(observed_at=NOW - timedelta(seconds=1), maximum_revision=2)
    _assert_error(caught.value, "registration_history")


def test_future_revision_alone_rejects() -> None:
    with pytest.raises(NonproductionAuthorityError) as caught:
        _record(revision=3).require_historical_boundary(observed_at=NOW + timedelta(seconds=1), maximum_revision=2)
    _assert_error(caught.value, "registration_history")


def test_history_after_expiry_returns_original_utc_without_revalidation(monkeypatch: pytest.MonkeyPatch) -> None:
    record = _record()

    def forbidden(*args: object, **kwargs: object) -> None:
        pytest.fail("historical coordinates cannot revalidate originals or authenticate trust")

    monkeypatch.setattr(NonproductionGrantRegistration, "__post_init__", forbidden)
    monkeypatch.setattr(NonproductionRegistrationOriginals, "__post_init__", forbidden)
    monkeypatch.setattr(NonproductionRegistrationOriginals, "require_policy", forbidden)
    for observed_at in (NOW.astimezone(timezone(timedelta(hours=3))), NOW + timedelta(days=2)):
        instant = record.require_historical_boundary(observed_at=observed_at, maximum_revision=2)
        assert instant == NOW and instant.tzinfo is UTC


@pytest.mark.parametrize("invalid", ["2026-09-10T01:30:00+00:00", "2026-02-30T01:30:00Z", "bad", None])
def test_timestamp_parse_precedes_both_comparisons(invalid: Any) -> None:
    record = _record()
    # Tamper only in this adversarial test; the new method must not repeat construction.
    object.__setattr__(record, "registered_at", invalid)
    wrong_type: Any = None
    with pytest.raises(NonproductionAuthorityError) as caught:
        record.require_historical_boundary(observed_at=wrong_type, maximum_revision=wrong_type)
    _assert_error(caught.value, "timestamp")


@pytest.mark.parametrize(
    ("observed_at", "maximum", "message"),
    [
        (NOW.replace(tzinfo=None), "bad", "can't compare offset-naive and offset-aware datetimes"),
        (None, 0, "'>' not supported between instances of 'datetime.datetime' and 'NoneType'"),
        (NOW, "bad", "'>' not supported between instances of 'int' and 'str'"),
    ],
)
def test_comparison_type_errors_keep_their_original_order(observed_at: Any, maximum: Any, message: str) -> None:
    with pytest.raises(TypeError) as caught:
        _record().require_historical_boundary(observed_at=observed_at, maximum_revision=maximum)
    assert type(caught.value) is TypeError
    assert str(caught.value) == message


def test_future_time_short_circuits_invalid_revision_comparison() -> None:
    wrong_type: Any = "bad"
    with pytest.raises(NonproductionAuthorityError) as caught:
        _record().require_historical_boundary(observed_at=NOW - timedelta(seconds=1), maximum_revision=wrong_type)
    _assert_error(caught.value, "registration_history")


def _row_bytes(record: NonproductionGrantRegistration) -> bytes:
    row = [{"bytes_hex": value.hex()} if type(value) is bytes else value for value in _values(record)]
    return json.dumps(row, ensure_ascii=False, separators=(",", ":"), sort_keys=True).encode()


def test_original_bytes_fields_and_module_identity_are_unchanged() -> None:
    record = _record()
    before, original = _row_bytes(record), record.originals
    assert hashlib.sha256(before).hexdigest() == "99cf19869104580753444fc5ce239d0c373218b83f240121726727f3730f86d5"
    assert record.require_historical_boundary(observed_at=NOW, maximum_revision=2) == NOW
    assert _row_bytes(record) == before and record.originals is original
    assert [value.name for value in fields(record)] == ["originals", "trust_revision", "registered_at"]
    assert [value.name for value in fields(original)] == [
        "grant_bytes",
        "bundle_bytes",
        "signature_subject_bytes",
        "request_bytes",
    ]
    for cls in (NonproductionGrantRegistration, NonproductionRegistrationOriginals):
        assert cls.__module__ == "dpone.contracts.nonproduction_registration"
    assert MssqlNonproductionRegistrationStore.__module__ == "dpone.adapters.nonproduction_mssql_registration"


class _RecordingBoundary(_RegistrationBoundary):
    """Only decode's historical SELECT is supplied; no live transaction claim."""

    def __init__(
        self, ledger: CompositionMssqlLedger, current: NonproductionTrustRevision, events: list[tuple[str, object]]
    ) -> None:
        self.ledger, self.current, self.events = ledger, current, events

    def rows(self, sql: str, *parameters: object) -> tuple[tuple[Any, ...], ...]:
        self.events.append(("sql", (sql, parameters)))
        snapshot = trust()
        return (
            (
                github_policy().environment_id,
                1,
                1,
                snapshot.policy_bytes,
                snapshot.policy_sha256,
                snapshot.verifier_policy_bytes,
                snapshot.verifier_policy_sha256,
                snapshot.current_revocation_epoch,
            ),
        )


def _decode_case(
    monkeypatch: pytest.MonkeyPatch, *, other_environment: bool = False
) -> tuple[MssqlNonproductionRegistrationStore, _RecordingBoundary, list[tuple[str, object]]]:
    _, ledger, store = setup(clock=lambda: pytest.fail("decode cannot acquire another clock observation"))
    policy = github_policy(environment_id=identifier(81)) if other_environment else github_policy()
    current = NonproductionTrustRevision(SERVICE, policy.environment_id, 2, trust(policy))
    events: list[tuple[str, object]] = []
    historical, require_policy = store._historical, NonproductionRegistrationOriginals.require_policy

    def read_history(boundary: _RegistrationBoundary, revision: int) -> NonproductionTrustRevision:
        events.append(("historical", revision))
        return historical(boundary, revision)

    def policy_at(self: NonproductionRegistrationOriginals, raw: bytes, digest: str, epoch: int, now: datetime) -> Any:
        events.append(("policy", (raw, digest, epoch, now)))
        return require_policy(self, raw, digest, epoch, now)

    monkeypatch.setattr(store, "_historical", read_history)
    monkeypatch.setattr(NonproductionRegistrationOriginals, "require_policy", policy_at)
    return store, _RecordingBoundary(ledger, current, events), events


@pytest.mark.parametrize(
    ("revision", "instant", "now"),
    [
        (1, REGISTERED, NOW),
        (2, REGISTERED, NOW),
        (1, "2026-09-10T01:29:59Z", NOW),
        (1, REGISTERED, NOW + timedelta(days=2)),
    ],
)
def test_decode_passes_original_instant_to_historical_policy(
    monkeypatch: pytest.MonkeyPatch, revision: int, instant: str, now: datetime
) -> None:
    store, boundary, events = _decode_case(monkeypatch)
    record = _record(revision=revision, registered_at=instant)
    assert store._decode(boundary, _values(record), now) == record
    assert [event[0] for event in events] == (
        ["historical", "sql", "policy"] if revision == 1 else ["historical", "policy"]
    )
    assert events[0] == ("historical", revision)
    expected_time = NOW - timedelta(seconds=1) if instant.endswith("29:59Z") else NOW
    snapshot = trust()
    assert events[-1] == (
        "policy",
        (snapshot.policy_bytes, snapshot.policy_sha256, snapshot.current_revocation_epoch, expected_time),
    )
    if revision == 1:
        payload = events[1][1]
        assert isinstance(payload, tuple)
        sql, parameters = payload
        assert isinstance(sql, str)
        assert "WITH (HOLDLOCK) WHERE environment_id = ? AND revision = ?;" in sql
        assert parameters == (github_policy().environment_id, 1)
        assert sql.startswith("SELECT ")


@pytest.mark.parametrize(
    ("revision", "now"), [(1, NOW - timedelta(seconds=1)), (3, NOW), (3, NOW - timedelta(seconds=1))]
)
def test_invalid_history_stops_before_historical_sql(
    monkeypatch: pytest.MonkeyPatch, revision: int, now: datetime
) -> None:
    store, boundary, events = _decode_case(monkeypatch)
    with pytest.raises(NonproductionAuthorityError) as caught:
        store._decode(boundary, _values(_record(revision=revision)), now)
    _assert_error(caught.value, "registration_history")
    assert events == []


@pytest.mark.parametrize(
    ("corruption", "reason"),
    [
        ("arity", "registration_row"),
        ("schema", "registration_row"),
        ("original", "registration_originals"),
        ("revision", "registration_revision"),
        ("timestamp", "timestamp"),
        ("identity", "registration_identity"),
        ("environment", "registration_identity"),
    ],
)
def test_original_row_and_environment_errors_precede_history(
    monkeypatch: pytest.MonkeyPatch, corruption: str, reason: str
) -> None:
    store, boundary, events = _decode_case(monkeypatch, other_environment=corruption == "environment")
    row = list(_values(_record(revision=3)))
    if corruption == "arity":
        row.pop()
    elif corruption == "schema":
        row[6] = True
    elif corruption == "original":
        row[7] = b"{}"
    elif corruption == "revision":
        row[15] = "invalid"
    elif corruption == "timestamp":
        row[16] = "invalid"
    elif corruption == "identity":
        row[8] = "sha256:" + "0" * 64
    with pytest.raises(NonproductionAuthorityError) as caught:
        store._decode(boundary, tuple(row), NOW - timedelta(seconds=1))
    _assert_error(caught.value, reason)
    assert events == []
