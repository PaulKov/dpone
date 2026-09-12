"""Synthetic V3 transaction wire identity, lifecycle and narrow-port regressions.

The active-binding golden is preserved from the reviewed cached compatibility
vector. Integer-built UUIDs, repeated bytes and session numbers are fixtures,
not runtime connection details. Historical schema1 inventory constructors are
intentionally outside this transaction/protocol-only test contract.
"""

from __future__ import annotations

import inspect
from dataclasses import replace
from itertools import product
from uuid import UUID

import pytest

from dpone.contracts.mssql_r1_v3_identity import MssqlR1V3ContractError
from dpone.contracts.mssql_r1_v3_transaction import (
    MssqlR1SqlServerSessionIdentityV3,
    MssqlR1TransactionBindingV3,
)
from dpone.contracts.mssql_r1_v3_transaction import (
    MssqlR1TransactionStateV3 as State,
)
from dpone.ports import mssql_r1_v3 as ports

TRANSACTION_ID = UUID("90000000-0000-4000-8000-000000000001")
ALLOWED_TRANSITIONS = frozenset(
    {
        (State.NEW, State.ACTIVE),
        (State.ACTIVE, State.COMMIT_DISPATCHED),
        (State.ACTIVE, State.ROLLED_BACK),
        (State.COMMIT_DISPATCHED, State.COMMITTED),
        (State.COMMIT_DISPATCHED, State.OUTCOME_UNKNOWN),
        (State.COMMITTED, State.CLOSED),
        (State.ROLLED_BACK, State.CLOSED),
        (State.OUTCOME_UNKNOWN, State.CLOSED),
    }
)


def _binding(state: State = State.ACTIVE) -> MssqlR1TransactionBindingV3:
    session = MssqlR1SqlServerSessionIdentityV3(
        bytes.fromhex("11" * 32),
        7,
        UUID(int=201),
        UUID(int=202),
        UUID(int=203),
        51,
        TRANSACTION_ID,
    )
    return MssqlR1TransactionBindingV3(
        TRANSACTION_ID,
        b"b" * 16,
        session.canonical_bytes,
        session.digest,
        state,
    )


def test_active_transaction_canonical_roundtrip_preserves_historical_golden() -> None:
    binding = _binding()

    assert MssqlR1TransactionBindingV3.from_canonical_bytes(binding.canonical_bytes) == binding
    assert binding.digest.hex() == "869d17368fd76bc6c6adfc573c10f4fbdb1934e069ab54ced5f13e4c9052b8c4"
    binding.assert_active()


@pytest.mark.parametrize(
    ("before", "after"),
    list(product(State, repeat=2)),
    ids=[f"{before.value}-to-{after.value}" for before, after in product(State, repeat=2)],
)
def test_transaction_lifecycle_matrix_is_closed(before: State, after: State) -> None:
    binding = _binding(before)
    original_bytes = binding.canonical_bytes

    if (before, after) not in ALLOWED_TRANSITIONS:
        with pytest.raises(MssqlR1V3ContractError, match="lifecycle"):
            binding.transitioned(after)
    else:
        transitioned = binding.transitioned(after)
        assert transitioned.state is after
        assert replace(transitioned, state=before) == binding
        assert MssqlR1TransactionBindingV3.from_canonical_bytes(transitioned.canonical_bytes) == transitioned
    assert binding.canonical_bytes == original_bytes


@pytest.mark.parametrize("state", list(State), ids=lambda state: state.value)
def test_only_active_binding_allows_active_operations(state: State) -> None:
    binding = _binding(state)
    if state is State.ACTIVE:
        binding.assert_active()
    else:
        with pytest.raises(MssqlR1V3ContractError, match="ACTIVE"):
            binding.assert_active()


def test_commit_dispatch_can_close_only_through_committed_or_unknown_outcome() -> None:
    dispatched = _binding().transitioned(State.COMMIT_DISPATCHED)
    for outcome in (State.COMMITTED, State.OUTCOME_UNKNOWN):
        assert dispatched.transitioned(outcome).transitioned(State.CLOSED).state is State.CLOSED


@pytest.mark.parametrize(
    "changes",
    [{"spid": 52}, {"database_id": 8}, {"session_context_transaction_id": UUID(int=998)}],
    ids=["session-splice", "database-splice", "transaction-context-splice"],
)
def test_changed_session_bytes_cannot_reuse_another_session_digest(changes: dict[str, object]) -> None:
    binding = _binding()
    changed = replace(binding.session_identity, **changes)

    with pytest.raises(MssqlR1V3ContractError, match="session identity digest"):
        replace(binding, session_identity_bytes=changed.canonical_bytes)


def test_matching_session_digest_does_not_waive_transaction_context_identity() -> None:
    binding = _binding()
    changed = replace(binding.session_identity, session_context_transaction_id=UUID(int=998))

    with pytest.raises(MssqlR1V3ContractError, match="session context"):
        replace(binding, session_identity_bytes=changed.canonical_bytes, session_identity_digest=changed.digest)


def test_transaction_identifier_cannot_be_spliced_onto_existing_context() -> None:
    with pytest.raises(MssqlR1V3ContractError, match="session context"):
        replace(_binding(), transaction_id=UUID(int=999))


def test_transaction_decoder_rejects_trailing_bytes() -> None:
    with pytest.raises(MssqlR1V3ContractError, match="canonical"):
        MssqlR1TransactionBindingV3.from_canonical_bytes(_binding().canonical_bytes + b"x")


@pytest.mark.parametrize(
    ("protocol", "method", "parameters"),
    [
        (ports.MssqlGenerationAuthoritySetTransactionV3Port, "resolve_and_admit", ("self", "transaction", "attempt")),
        (ports.MssqlSealedStageConsumptionV3Port, "consume", ("self", "transaction", "attempt", "receipt")),
        (ports.MssqlGenerationAuthoritySetVerifierV3Port, "verify_authority_set", ("self", "command")),
        (ports.MssqlGenerationAuthoritySetImporterV3Port, "import_authority_set", ("self", "command")),
    ],
)
def test_active_protocol_parameter_names_remain_exact(protocol: type, method: str, parameters: tuple[str, ...]) -> None:
    assert tuple(inspect.signature(getattr(protocol, method)).parameters) == parameters


@pytest.mark.parametrize(
    ("owner", "name"),
    [
        (ports.MssqlGenerationAuthorityImporterPort, "import_revoked_override"),
        (ports.MssqlGenerationAuthoritySetVerifierV3Port, "verify_revoked_override"),
        (ports.MssqlGenerationAuthoritySetImporterV3Port, "import_revoked_override"),
        (ports, "MssqlRevokedRegistrationOverrideTransactionV1Port"),
    ],
)
def test_active_protocols_do_not_expose_revoked_override_surface(owner: object, name: str) -> None:
    assert not hasattr(owner, name)
