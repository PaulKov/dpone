"""Bounded retained-proof reader tests; doubles never certify SQL or producers."""

from __future__ import annotations

from dataclasses import replace
from typing import Any, cast

import pytest

from dpone.adapters import composition_mssql_terminal as terminal
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import encode_attempt_proof
from dpone.contracts.composition_proof import (
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
)
from dpone.ports.composition_sql import CompositionSqlContext
from tests.composition_mssql_store_helpers import SERVICE_ID
from tests.test_composition_activation_contract import digest
from tests.test_composition_mssql_operations import add_operation, attempt_for
from tests.test_composition_mssql_ownership import SharedSql


class ProofSql(SharedSql):
    """Only immutable proof rows are modelled, with strict SQL byte/page bounds."""

    def __init__(self) -> None:
        super().__init__()
        self.issued: list[tuple[Any, ...]] = []
        self.proofs: dict[tuple[str, str], tuple[Any, ...]] = {}
        self.redirect_tables = False

    def table(self, name: str) -> str:
        return f"[{'wrong' if self.redirect_tables else self.schema}].[composition_{name}]"

    def _select(self, sql: str, parameters: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        if "composition_issued_authorities]" in sql:
            assert "TOP (8193)" in sql
            return self.issued[:8193]
        if "composition_proofs]" in sql:
            assert "DATALENGTH(proof_document) BETWEEN 1 AND 8388608" in sql
            if "TOP (1)" in sql:
                _, kind, *after = parameters
                records = sorted(
                    (sha, *row)
                    for (stored_kind, sha), row in self.proofs.items()
                    if stored_kind == kind and (not after or sha > after[0])
                )[:1]
            else:
                assert "TOP (2)" in sql
                _, kind, sha = parameters
                row = self.proofs.get((kind, sha))
                records = [] if row is None else [row]
            return [
                (*row[:-1], None if type(row[-1]) is bytes and len(row[-1]) > 8388608 else row[-1]) for row in records
            ]
        return super()._select(sql, parameters)


def proof_setup(
    connector: str = "clickhouse", database: ProofSql | None = None
) -> tuple[ProofSql, Any, tuple[CompositionAttemptProof, ...]]:
    database = ProofSql() if database is None else database
    database.add_owner()
    identity = attempt_for(database, workload_id="c_generated_данные" if connector == "clickhouse" else "a_native")
    add_operation(database, identity)
    resource = next(row for row in database.domains.values() if row[0] == connector)
    principal = (
        "clickhouse-user:30000000-0000-4000-8000-000000000001"
        if connector == "clickhouse"
        else "mssql-sid:" + "ab" * 16
    )
    authority = CompositionProofAuthority(connector, resource[1], principal)
    database.issued = [(authority.connector, authority.service_id, authority.principal_id)]
    proofs = tuple(
        CompositionAttemptProof(
            kind,
            identity.attempt_sha256,
            identity.activation_request_sha256,
            composition_attempt_epoch_subject(identity),
            (authority,),
            digest(kind),
            "SUCCEEDED" if kind == "OUTCOME" else None,
        )
        for kind in ("CLOSED_GATES", "QUIESCENCE", "OUTCOME")
    )
    for proof in proofs:
        store_proof(database, proof)
    return database, identity, proofs


def store_proof(database: ProofSql, proof: CompositionAttemptProof) -> None:
    database.proofs[proof.kind, proof.proof_sha256] = ("execution", encode_attempt_proof(proof))


def selectors(database: ProofSql, identity: Any, outcome: str) -> tuple[str, str, str]:
    return terminal.select_terminal_hashes_in(
        cast(CompositionSqlContext, database), identity, outcome, expected_service_id=SERVICE_ID
    )


def test_selector_constructs_or_checks_canonical_tables() -> None:
    database, identity, proofs = proof_setup()
    database.redirect_tables = True
    with pytest.raises(CompositionAdmissionError, match="control_schema"):
        selectors(database, identity, proofs[2].proof_sha256)


def test_transaction_replacement_after_issuance_rejects_before_proof_queries() -> None:
    database, identity, proofs = proof_setup()

    def replace_transaction(context: ProofSql, sql: str, parameters: Any) -> None:
        if "composition_issued_authorities]" in sql:
            context.transaction = (1, 1, "Exclusive", 8)

    database.after_execute = replace_transaction
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        selectors(database, identity, proofs[2].proof_sha256)
    assert not any("composition_proofs]" in sql for sql, _ in database.statements)


def require_selected(
    database: ProofSql, identity: Any, proofs: tuple[CompositionAttemptProof, ...], state: str = "SUCCEEDED"
) -> None:
    hashes = (proofs[0].proof_sha256, proofs[1].proof_sha256, proofs[2].proof_sha256)
    terminal.require_proofs_in(
        cast(CompositionSqlContext, database),
        identity,
        {(row[0], row[1]) for row in database.issued},
        hashes,
        expected_outcome_state=state,
        expected_service_id=SERVICE_ID,
    )


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"])
def test_proposed_outcome_does_not_require_finalized_operation(state: str) -> None:
    database, identity, proofs = proof_setup()
    proposed = (*proofs[:2], replace(proofs[2], outcome_state=state))
    store_proof(database, proposed[2])
    before = database.operations.copy(), database.proofs.copy(), database.issued.copy()
    assert selectors(database, identity, proposed[2].proof_sha256) == tuple(p.proof_sha256 for p in proposed)
    require_selected(database, identity, proposed, state)
    assert database.operations[identity.attempt_sha256][6] == "RUNNING"
    assert before == (database.operations, database.proofs, database.issued)


def test_selector_consumes_all_variants_and_uses_unfiltered_first_key() -> None:
    database, identity, proofs = proof_setup()
    other = replace(proofs[0].authorities[0], principal_id="clickhouse-user:00000000-0000-0000-0000-000000000000")
    for number in range(40):
        store_proof(database, replace(proofs[0], authorities=(other,), evidence_sha256=digest(str(number))))
    assert selectors(database, identity, proofs[2].proof_sha256) == tuple(p.proof_sha256 for p in proofs)
    pages = [(sql, p) for sql, p in database.statements if "composition_proofs]" in sql]
    assert len(pages) == 46
    for kind in ("CLOSED_GATES", "QUIESCENCE", "OUTCOME"):
        first_sql, parameters = next((sql, p) for sql, p in pages if p[1] == kind)
        assert parameters == (identity.attempt_sha256, kind) and "proof_sha256>?" not in first_sql
    assert all("TOP (1)" in sql for sql, _ in pages)


@pytest.mark.parametrize("damage", ["missing", "ambiguous", "low_key", "unselected_corrupt"])
def test_selection_never_skips_missing_ambiguous_or_malformed_originals(damage: str) -> None:
    database, identity, proofs = proof_setup()
    key = (proofs[0].kind, proofs[0].proof_sha256)
    if damage == "missing":
        del database.proofs[key]
    elif damage == "ambiguous":
        store_proof(database, replace(proofs[0], evidence_sha256=digest("other")))
    elif damage == "low_key":
        database.proofs["CLOSED_GATES", "!"] = database.proofs[key]
    else:
        database.proofs["OUTCOME", digest("unselected")] = ("execution", b"{}")
    with pytest.raises(CompositionAdmissionError):
        selectors(database, identity, proofs[2].proof_sha256)


@pytest.mark.parametrize(
    "damage", ["missing", "duplicate", "family", "shape", "noncanonical", "hash", "scope", "kind", "oversized"]
)
def test_selected_proof_requires_one_complete_original(damage: str) -> None:
    database, identity, proofs = proof_setup()
    key = (proofs[0].kind, proofs[0].proof_sha256)
    row = database.proofs[key]
    if damage == "missing":
        del database.proofs[key]
    elif damage == "duplicate":
        database.after_execute = lambda db, sql, p: (
            db.results.extend(db.results[:]) if "composition_proofs]" in sql else None
        )
    elif damage == "family":
        database.proofs[key] = ("qualification", row[1])
    elif damage == "shape":
        database.proofs[key] = ("execution", "extra", row[1])
    elif damage in {"noncanonical", "oversized"}:
        database.proofs[key] = ("execution", row[1] + b" " if damage == "noncanonical" else b"x" * 8388609)
    else:
        changed = replace(
            proofs[0],
            **{
                "hash": {"evidence_sha256": digest("different")},
                "scope": {"attempt_sha256": digest("different")},
                "kind": {"kind": "QUIESCENCE"},
            }[damage],
        )
        document = encode_attempt_proof(changed)
        database.proofs[key] = ("execution", document)
        if damage != "hash":
            del database.proofs[key]
            database.proofs["CLOSED_GATES", changed.proof_sha256] = ("execution", document)
            proofs = (changed, *proofs[1:])
    with pytest.raises(CompositionAdmissionError):
        require_selected(database, identity, proofs)


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [("clickhouse",)],
        [("unknown", SERVICE_ID, "x")],
        [("clickhouse", SERVICE_ID, "clickhouse-user:broken")],
        [("clickhouse", "broken", "clickhouse-user:00000000-0000-0000-0000-000000000000")],
    ],
)
def test_issued_rows_require_supported_canonical_membership(rows: list[tuple[Any, ...]]) -> None:
    database, identity, proofs = proof_setup()
    database.issued = rows
    with pytest.raises(CompositionAdmissionError):
        selectors(database, identity, proofs[2].proof_sha256)


@pytest.mark.parametrize("count", [2, 8193])
def test_duplicate_or_overflow_issuance_cannot_be_a_subset(count: int) -> None:
    database, identity, proofs = proof_setup()
    database.issued *= count
    with pytest.raises(CompositionAdmissionError, match="terminal_issued_authorities"):
        selectors(database, identity, proofs[2].proof_sha256)


def test_full_authority_membership_and_outcome_state_are_required() -> None:
    database, identity, proofs = proof_setup()
    with pytest.raises(CompositionAdmissionError, match="terminal_outcome_state"):
        require_selected(database, identity, proofs, "FAILED")
    database.issued.append(
        ("clickhouse", database.issued[0][1], "clickhouse-user:00000000-0000-0000-0000-000000000000")
    )
    with pytest.raises(CompositionAdmissionError, match="terminal_proof_authorities"):
        require_selected(database, identity, proofs)


@pytest.mark.parametrize(
    "hashes,state",
    [
        ((), "SUCCEEDED"),
        ((None, None, None), "SUCCEEDED"),
        (("x", "x", "x"), "SUCCEEDED"),
        (("x", "x", "x"), "RUNNING"),
    ],
)
def test_malformed_selectors_have_contract_errors(hashes: Any, state: str) -> None:
    database, identity, _ = proof_setup()
    with pytest.raises(CompositionAdmissionError):
        terminal.require_proofs_in(
            cast(CompositionSqlContext, database),
            identity,
            set(),
            hashes,
            expected_outcome_state=state,
            expected_service_id=SERVICE_ID,
        )


@pytest.mark.parametrize("mode", ["select", "selected"])
@pytest.mark.parametrize("fragment", ["composition_issued_authorities]", "composition_proofs]"])
def test_each_proof_observation_rechecks_actual_transaction(mode: str, fragment: str) -> None:
    database, identity, proofs = proof_setup()

    def change(db: ProofSql, sql: str, parameters: Any) -> None:
        if fragment in sql:
            db.transaction = (1, 1, "Exclusive", 8)

    database.after_execute = change
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        if mode == "select":
            selectors(database, identity, proofs[2].proof_sha256)
        else:
            require_selected(database, identity, proofs)


def test_stream_rechecks_transaction_before_resumption_and_rejects_repeated_page() -> None:
    database, identity, proofs = proof_setup()
    stream = terminal._proofs_in(
        cast(CompositionSqlContext, database), identity, "CLOSED_GATES", expected_service_id=SERVICE_ID
    )
    assert next(stream) == proofs[0]
    database.transaction = (1, 1, "Exclusive", 8)
    count = len(database.statements)
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        next(stream)
    assert not any("composition_proofs]" in sql for sql, _ in database.statements[count:])
    database.transaction = (1, 1, "Exclusive", 7)

    def repeat_page(db: ProofSql, sql: str, parameters: Any) -> None:
        if "composition_proofs]" in sql:
            db.results = [(proofs[0].proof_sha256, "execution", encode_attempt_proof(proofs[0]))]

    database.after_execute = repeat_page
    stream = terminal._proofs_in(
        cast(CompositionSqlContext, database), identity, "CLOSED_GATES", expected_service_id=SERVICE_ID
    )
    next(stream)
    with pytest.raises(CompositionAdmissionError, match="terminal_proof_order"):
        next(stream)


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED", "COMMIT_UNKNOWN"])
def test_execution_callback_reopens_history_without_changing_retired_owner(state: str) -> None:
    from dpone.adapters.composition_mssql_operations import _occurrence, read_shared_operation_in
    from dpone.contracts.composition_attempt import CompositionAttemptReceipt

    database, identity, proofs = proof_setup()
    record = read_shared_operation_in(
        cast(CompositionSqlContext, database), identity.attempt_sha256, expected_service_id=SERVICE_ID
    )
    assert record is not None
    occurrence = _occurrence(record.owner)
    occurrence = replace(occurrence, receipt=replace(occurrence.receipt, state="RETIRED"))
    proofs = (*proofs[:2], replace(proofs[2], outcome_state=state))
    store_proof(database, proofs[2])
    receipt = CompositionAttemptReceipt(identity, state, *(proof.proof_sha256 for proof in proofs))
    terminal.require_execution_terminal_in(
        cast(CompositionSqlContext, database), occurrence, receipt, expected_service_id=SERVICE_ID
    )
    assert occurrence.receipt.state == "RETIRED" and receipt.state == state


def test_maximum_complete_issuance_and_legacy_zero_service_are_preserved() -> None:
    database, identity, proofs = proof_setup()
    authorities = tuple(
        CompositionProofAuthority(
            "clickhouse",
            "00000000-0000-0000-0000-000000000000",
            f"clickhouse-user:00000000-0000-0000-0000-{number:012x}",
        )
        for number in range(8192)
    )
    database.issued = [(a.connector, a.service_id, a.principal_id) for a in reversed(authorities)]
    proofs = tuple(replace(proof, authorities=authorities) for proof in proofs)
    database.proofs.clear()
    for proof in proofs:
        store_proof(database, proof)
    assert selectors(database, identity, proofs[2].proof_sha256) == tuple(p.proof_sha256 for p in proofs)
    require_selected(database, identity, proofs)


@pytest.mark.parametrize("transaction", [(0, 0, "NoLock", 7), (1, -1, "Exclusive", 7), (1, 1, "Shared", 7)])
def test_proof_entry_requires_live_transaction_authority(transaction: Any) -> None:
    database, identity, proofs = proof_setup()
    database.transaction = transaction
    with pytest.raises(CompositionAdmissionError):
        require_selected(database, identity, proofs)
    assert not any("composition_issued_authorities]" in sql for sql, _ in database.statements)


def test_selected_proof_rechecks_gate_callback_transaction(monkeypatch: pytest.MonkeyPatch) -> None:
    database, identity, proofs = proof_setup("mssql")

    def replace_transaction(*args: Any, **kwargs: Any) -> None:
        database.transaction = (1, 1, "Exclusive", 8)

    monkeypatch.setattr(terminal, "require_historical_mssql_gate", replace_transaction)
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        require_selected(database, identity, proofs)
