"""All finite caps checked before parse; raw RESULT boundary preserves spaces."""

from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_evidence_types import EVIDENCE_LIMITS, evidence_limit, require_payload
from dpone.contracts.mssql_sqlclient_evidence_types import SqlClientEvidenceKind as Kind
from tests.mssql_sqlclient_evidence_fixtures import evidence_record


@pytest.mark.parametrize("kind", list(Kind))
def test_caps_and_preparse_bounds(kind):
    cap = 2097152 if kind in (Kind.REGISTRATION, Kind.VERIFICATION) else 16384
    assert evidence_limit(kind) == cap
    require_payload(b"x" * cap, kind)
    for payload in (b"", b"x" * (cap + 1), bytearray(b"{}")):
        with pytest.raises(ValueError):
            require_payload(payload, kind)
    with pytest.raises(ValueError):
        replace(evidence_record(kind), payload=b"x" * (cap + 1))


def test_result_exact_cap_and_plus_one():
    record = evidence_record(Kind.RESULT)
    payload = record.payload.ljust(evidence_limit(Kind.RESULT), b" ")
    assert replace(record, payload=payload).receipt.byte_count == 16384
    with pytest.raises(ValueError):
        replace(record, payload=payload + b" ")


def test_registry_immutable_and_alias_rejected():
    with pytest.raises(TypeError):
        EVIDENCE_LIMITS[Kind.RESULT] = 1
    with pytest.raises(ValueError):
        evidence_limit("result")


@pytest.mark.parametrize("character", ['"', "\\", "\uffff", "\U0010ffff"])
def test_maximum_valid_nested_records_fit_actual_encoders(character):
    from dpone.contracts.mssql_sqlclient_evidence import SqlClientEvidenceRecord
    from dpone.contracts.mssql_sqlclient_registration import decode_registration, encode_registration
    from dpone.contracts.mssql_sqlclient_writer_evidence import decode_writer_observation, encode_writer_observation
    from tests.mssql_sqlclient_evidence_fixtures import maximum_registration, maximum_writer

    registration = maximum_registration(character)
    assert len(registration.input.columns) == 100
    assert len({column.name for column in registration.input.columns}) == 100
    assert all(len(column.name.encode("utf-16le")) == 256 for column in registration.input.columns)
    # No JSON padding: every byte comes from an admitted nested field.
    raw = encode_registration(registration)
    assert decode_registration(raw) == registration
    assert len(raw) <= evidence_limit(Kind.REGISTRATION)
    assert SqlClientEvidenceRecord(
        registration.binding.attempt_sha256, Kind.REGISTRATION, raw
    ).receipt.byte_count == len(raw)
    observed = maximum_writer(character)
    payload = encode_writer_observation(observed)
    assert decode_writer_observation(payload) == observed
    assert len(payload) <= evidence_limit(Kind.WRITER_OBSERVATION)
    assert SqlClientEvidenceRecord(
        observed.binding.attempt_sha256, Kind.WRITER_OBSERVATION, payload
    ).receipt.byte_count == len(payload)
    with pytest.raises(ValueError):
        replace(
            registration.input,
            columns=registration.input.columns + (replace(registration.input.columns[0], name="extra"),),
        )
