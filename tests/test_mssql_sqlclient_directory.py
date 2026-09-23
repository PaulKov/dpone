"""SqlClient cleanup capacity is admitted before work, with legacy wire stability."""

import json
from dataclasses import replace

import pytest

from dpone.contracts.mssql_sqlclient_attempt import SqlClientAttemptEvidence
from dpone.contracts.mssql_tds_directory import (
    TdsDirectoryLimits,
    authorize_retirement,
    close_admission,
    encode_directory,
    initial_directory,
    seal_work,
)
from dpone.contracts.mssql_tds_directory_codec import decode_directory
from tests.test_mssql_tds_directory import LIMITS, PARENT, Command, H, authority, reserve, settled


def sqlclient_authority():
    return replace(
        authority(True),
        schema_version=2,
        backend="mssql_sqlclient",
        sequence=10,
        sqlclient=SqlClientAttemptEvidence(H, H, False, H, H),
    )


def test_version_two_roundtrip_preserves_full_cleanup_evidence():
    state = initial_directory(PARENT, LIMITS, schema_version=2)
    state = authorize_retirement(seal_work(state), sqlclient_authority())
    payload = encode_directory(state)
    assert json.loads(payload)["schema"] == "dpone.tds.coordinator-directory.v2"
    assert decode_directory(payload, parent=PARENT, limits=LIMITS) == state


@pytest.mark.parametrize("version", [1, 2])
def test_cross_version_retirement_is_rejected(version):
    state = seal_work(initial_directory(PARENT, LIMITS, schema_version=version))
    proof = sqlclient_authority() if version == 1 else authority(True)
    with pytest.raises(ValueError):
        authorize_retirement(state, proof)


def test_new_wire_does_not_accept_duplicate_schema_discriminant():
    payload = json.loads(encode_directory(initial_directory(PARENT, LIMITS, schema_version=2)))
    payload["schema_version"] = 1
    with pytest.raises(ValueError):
        decode_directory(json.dumps(payload).encode(), parent=PARENT, limits=LIMITS)


def test_tight_budget_reserves_sqlclient_evidence_before_work():
    from dpone.contracts import mssql_tds_directory as model

    unit = model._slot_bound()
    first = {}
    for version in (1, 2):
        for budget in range(2 * unit + 1000, 2 * unit + 10000):
            try:
                limits = TdsDirectoryLimits(8, 2, budget, 2 * unit)
                state = reserve(initial_directory(PARENT, limits, schema_version=version))
            except ValueError:
                continue
            first[version] = budget
            break
        else:
            pytest.fail("no admitted boundary found")
        state = settled(state)
        with pytest.raises(ValueError):
            reserve(state, number=2)
        state = authorize_retirement(seal_work(state), sqlclient_authority() if version == 2 else authority(True))
        state = close_admission(settled(reserve(state, Command.RETIRE, number=2), 1))
        assert len(encode_directory(state)) <= limits.max_encoded_bytes
    assert first[2] > first[1]
