"""Offline SQL adapter faults; DB-API doubles do not certify live SQL Server."""

from copy import deepcopy
from dataclasses import replace

import pytest

from dpone.adapters.composition_mssql_schema import render_composition_mssql_schema
from dpone.adapters.composition_mssql_store import MssqlCompositionActivationStore
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_persistence import encode_activation_request
from tests.composition_mssql_store_helpers import SERVICE_ID, Database, journal_attempt
from tests.test_composition_activation_contract import digest


def test_external_schema_is_separate_from_native_v2():
    sql = render_composition_mssql_schema()
    assert "CREATE TABLE [dpone_control].[composition_activations]" in sql
    assert "varbinary(max)" in sql.lower()
    assert "dbt_workspace" not in sql
    assert "semantic_refresh" not in sql
    assert "INSERT INTO" not in sql


def test_schema_identifier_is_validated_before_connecting():
    with pytest.raises(ValueError):
        MssqlCompositionActivationStore(
            lambda: pytest.fail("must not connect"),
            expected_service_id="10000000-0000-4000-8000-000000000001",
            control_schema="control]; DROP TABLE x;--",
        )


def store(database):
    return MssqlCompositionActivationStore(database.connect, expected_service_id=SERVICE_ID)


def prepared():
    database = Database()
    adapter = store(database)
    adapter.prepare(database.request)
    return database, adapter


def retiring():
    database, adapter = prepared()
    adapter.activate(database.request)
    adapter.begin_retirement(database.request)
    return database, adapter


def test_terminal_row_cannot_promote_a_protected_failed_outcome_to_success():
    database, adapter = retiring()
    attempt = journal_attempt(database, state="FAILED")
    record = database.data["attempts"][attempt.attempt_sha256]
    database.data["attempts"][attempt.attempt_sha256] = (*record[:4], "SUCCEEDED", *record[5:])
    with pytest.raises(CompositionAdmissionError, match="terminal_outcome_state"):
        adapter.finalize_retirement(database.request)
    assert all(value[4] == database.request.activation_id for value in database.data["domains"].values())


def test_mixed_engine_complete_request_and_partition_are_durable():
    database = Database()
    result = store(database).prepare(database.request)
    assert result.request == database.request
    assert len(result.request.workloads) == 3
    assert {item.connector for item in result.request.resources} == {"mssql", "clickhouse"}
    record = database.data["activations"][database.request.activation_id]
    assert record == (database.request.request_sha256, encode_activation_request(database.request), "PREPARED")
    assert "данные".encode() in record[1]
    assert (
        tuple((guard, value[3]) for guard, value in sorted(database.data["domains"].items()))
        == result.receipt.guard_epochs
    )
    assert len(database.data["activation_domains"]) == 2
    assert all(value[4] == database.request.activation_id for value in database.data["domains"].values())


def test_every_mutation_uses_independent_readback_after_commit():
    database = Database()
    adapter = store(database)
    for method, state in (
        (adapter.prepare, "PREPARED"),
        (adapter.activate, "ACTIVE"),
        (adapter.begin_retirement, "RETIRING"),
        (adapter.finalize_retirement, "RETIRED"),
    ):
        before = len(database.connections)
        assert method(database.request).receipt.state == state
        writer, reader = database.connections[before:]
        assert writer is not reader and writer.closed and reader.closed
        assert writer.commits == 1 and reader.commits == 0 and reader.rollbacks == 1


def test_prepare_replay_preserves_exact_bytes_and_epochs():
    database, adapter = prepared()
    before = deepcopy(database.data)
    expected = adapter.read(database.request.activation_id)
    assert adapter.prepare(database.request) == expected
    assert database.data == before
    adapter.activate(database.request)
    with pytest.raises(CompositionAdmissionError, match="occurrence_state"):
        adapter.prepare(database.request)


@pytest.mark.parametrize("field", ["source", "catalog", "context"])
def test_changed_complete_request_is_rejected_without_mutation(field):
    database, adapter = prepared()
    before = deepcopy(database.data)
    value = database.request
    if field == "source":
        value = replace(value, source_subject_sha256=digest("changed"))
    elif field == "catalog":
        value = replace(
            value, resources=(replace(value.resources[0], observation_sha256=digest("changed")), *value.resources[1:])
        )
    else:
        value = replace(value, context=replace(value.context, runtime_context_sha256=digest("changed")))
    with pytest.raises(CompositionAdmissionError, match="occurrence_mismatch"):
        adapter.activate(value)
    assert database.data == before


@pytest.mark.parametrize("damage", ["document", "partition", "resource", "epoch", "owner", "physical"])
def test_damaged_durable_state_cannot_acknowledge_activation(damage):
    database, adapter = prepared()
    activation = database.request.activation_id
    guard = database.request.resources[0].guard_id
    if damage == "document":
        digest_value, document, state = database.data["activations"][activation]
        database.data["activations"][activation] = (digest_value, document + b" ", state)
    elif damage == "partition":
        del database.data["activation_domains"][activation, guard]
    elif damage == "resource":
        document, epoch = database.data["activation_domains"][activation, guard]
        database.data["activation_domains"][activation, guard] = (document + b" ", epoch)
    else:
        row = list(database.data["domains"][guard])
        row[{"epoch": 3, "owner": 4, "physical": 2}[damage]] = {
            "epoch": 2,
            "owner": "10000000-0000-4000-8000-000000000099",
            "physical": digest("reincarnated"),
        }[damage]
        database.data["domains"][guard] = tuple(row)
    before = deepcopy(database.data)
    with pytest.raises(CompositionAdmissionError):
        adapter.activate(database.request)
    assert database.data == before


@pytest.mark.parametrize("damage", ["missing_domain", "foreign_owner", "authority", "schema", "version"])
def test_admission_rejects_unprovisioned_or_conflicting_authority_before_dml(damage):
    database = Database()
    guard = database.request.resources[0].guard_id
    if damage == "missing_domain":
        guard = next(row.guard_id for row in database.request.resources if row.connector == "clickhouse")
        del database.data["domains"][guard]
    elif damage == "foreign_owner":
        row = database.data["domains"][guard]
        database.data["domains"][guard] = (*row[:3], 9, "10000000-0000-4000-8000-000000000099")
    elif damage == "authority":
        database.data["authority"] = [(1, 1, "10000000-0000-4000-8000-000000000099")]
    elif damage == "schema":
        database.data["tables"] = 7
    else:
        database.data["authority"] = [(1, 2, SERVICE_ID)]
    before = deepcopy(database.data)
    with pytest.raises(CompositionAdmissionError):
        store(database).prepare(database.request)
    assert database.data == before
    assert not any(sql.startswith(("UPDATE ", "INSERT ")) for sql, _ in database.statements)


def test_commit_ack_loss_reconciles_exact_independent_state_without_replay():
    database = Database()
    database.fail_commit = True
    assert store(database).prepare(database.request).receipt.state == "PREPARED"
    assert (
        sum(sql.startswith("INSERT INTO [dpone_control].[composition_activations]") for sql, _ in database.statements)
        == 1
    )
    assert len(database.connections) == 2


def test_commit_ack_loss_with_absent_fresh_result_is_unknown():
    database = Database()
    database.fail_commit = True
    database.commit_applies = False
    with pytest.raises(CompositionAdmissionError, match="commit_unknown"):
        store(database).prepare(database.request)
    assert len(database.connections) == 2
    assert sum(connection.commits for connection in database.connections) == 1


def test_successful_commit_with_unavailable_readback_retains_all_ownership():
    database = Database()

    def deny_second_connection(value):
        if value.connections:
            value.fail_connect = "driver password=synthetic-secret"

    database.before_connect = deny_second_connection
    with pytest.raises(CompositionAdmissionError, match="commit_unknown") as caught:
        store(database).prepare(database.request)
    assert "synthetic-secret" not in str(caught.value)
    assert all(row[4] == database.request.activation_id for row in database.data["domains"].values())


def test_changed_epoch_after_commit_cannot_report_success():
    database = Database()

    def change_second_connection(value):
        if value.connections:
            guard = value.request.resources[0].guard_id
            row = value.data["domains"][guard]
            value.data["domains"][guard] = (*row[:3], row[3] + 1, row[4])

    database.before_connect = change_second_connection
    with pytest.raises(CompositionAdmissionError, match="commit_unknown"):
        store(database).prepare(database.request)


def test_retirement_retains_historical_epochs_and_allows_explicit_successor():
    database, adapter = retiring()
    old = adapter.finalize_retirement(database.request)
    successor = replace(
        database.request,
        context=replace(database.request.context, activation_id="10000000-0000-4000-8000-000000000002"),
    )
    new = adapter.prepare(successor)
    assert all(epoch == 2 for _, epoch in new.receipt.guard_epochs)
    assert adapter.read(old.request.activation_id) == old
    assert all(row[4] == successor.activation_id for row in database.data["domains"].values())


def test_driver_failure_rolls_back_complete_union_and_is_sanitized():
    database = Database()
    database.fail_sql = "INSERT INTO [dpone_control].[composition_activation_domains]"
    before = deepcopy(database.data)
    with pytest.raises(CompositionAdmissionError, match="durable_mutation") as caught:
        store(database).prepare(database.request)
    assert "synthetic-secret" not in str(caught.value)
    assert database.data == before
    assert database.connections[0].rollbacks == 1


def test_busy_global_ledger_lock_blocks_before_dml():
    database = Database()
    database.lock_result = -1
    with pytest.raises(CompositionAdmissionError, match="ledger_lock"):
        store(database).prepare(database.request)
    assert not database.data["activations"]


@pytest.mark.parametrize("state", ["SUCCEEDED", "FAILED"])
def test_retirement_requires_and_accepts_exact_protected_terminal_triplets(state):
    database, adapter = retiring()
    journal_attempt(database, state=state)
    journal_attempt(database, workload_id="c_generated_данные", extra_principal=True)
    result = adapter.finalize_retirement(database.request)
    assert result.receipt.state == "RETIRED"
    assert all(row[4] is None for row in database.data["domains"].values())


@pytest.mark.parametrize("state", ["RUNNING", "COMMIT_UNKNOWN"])
def test_unresolved_attempt_blocks_retirement_even_with_complete_proofs(state):
    database, adapter = retiring()
    journal_attempt(database, state=state)
    before = deepcopy(database.data)
    with pytest.raises(CompositionAdmissionError, match="unresolved_attempt"):
        adapter.finalize_retirement(database.request)
    assert database.data == before


@pytest.mark.parametrize(
    "damage",
    [
        "missing_proof",
        "proof_bytes",
        "missing_issuer",
        "foreign_issuer",
        "partition",
        "epoch",
        "parent",
        "terminal_hash",
        "proof_epoch",
    ],
)
def test_terminal_hashes_cannot_substitute_for_complete_protected_proofs(damage):
    database, adapter = retiring()
    attempt = journal_attempt(database)
    key = attempt.attempt_sha256
    proof_key = next(iter(database.data["proofs"]))
    if damage == "missing_proof":
        del database.data["proofs"][proof_key]
    elif damage in {"proof_bytes", "proof_epoch"}:
        parent, epoch, document = database.data["proofs"][proof_key]
        database.data["proofs"][proof_key] = (
            parent,
            digest("wrong") if damage == "proof_epoch" else epoch,
            document + b" " if damage == "proof_bytes" else document,
        )
    elif damage == "missing_issuer":
        del database.data["issued_authorities"][key]
    elif damage == "foreign_issuer":
        connector, service_id, principal = database.data["issued_authorities"][key][0]
        database.data["issued_authorities"][key] = ((connector, service_id, principal[:-1] + "b"),)
    elif damage == "partition":
        database.data["attempt_domains"][key] = ()
    else:
        record = list(database.data["attempts"][key])
        record[{"epoch": 2, "parent": 1, "terminal_hash": 5}[damage]] = digest("wrong")
        database.data["attempts"][key] = tuple(record)
    before = deepcopy(database.data)
    reason = {
        "missing_proof": "terminal_proof_identity",
        "proof_bytes": "proof_persistence_readback",
        "missing_issuer": "terminal_issued_authorities",
        "foreign_issuer": "terminal_proof_authorities",
        "partition": "terminal_attempt_partition",
        "epoch": "terminal_attempt_identity",
        "parent": "terminal_attempt_identity",
        "terminal_hash": "terminal_proof_identity",
        "proof_epoch": "terminal_proof_identity",
    }[damage]
    with pytest.raises(CompositionAdmissionError, match=reason):
        adapter.finalize_retirement(database.request)
    assert database.data == before


def test_retirement_readback_rechecks_durable_terminal_proofs():
    database, adapter = retiring()
    journal_attempt(database)
    before = len(database.connections)

    def remove_after_commit(value):
        if len(value.connections) == before + 1:
            value.data["proofs"].clear()

    database.before_connect = remove_after_commit
    with pytest.raises(CompositionAdmissionError, match="commit_unknown"):
        adapter.finalize_retirement(database.request)


def test_missing_occurrence_read_never_reserves_or_provisions():
    database = Database()
    assert store(database).read(database.request.activation_id) is None
    assert not any(sql.startswith(("CREATE ", "ALTER ", "INSERT ", "UPDATE ")) for sql, _ in database.statements)
    assert database.connections[0].commits == 0


def test_attempt_without_domain_rows_still_blocks_retirement():
    database, adapter = retiring()
    attempt = journal_attempt(database, state="RUNNING")
    database.data["attempt_domains"].clear()
    with pytest.raises(CompositionAdmissionError, match="unresolved_attempt"):
        adapter.finalize_retirement(database.request)
    assert database.data["attempts"][attempt.attempt_sha256][4] == "RUNNING"
    assert all(row[4] == database.request.activation_id for row in database.data["domains"].values())


def test_foreign_unresolved_attempt_prevents_reacquiring_an_available_domain():
    database, adapter = retiring()
    adapter.finalize_retirement(database.request)
    journal_attempt(database, state="COMMIT_UNKNOWN")
    successor = replace(
        database.request,
        context=replace(database.request.context, activation_id="10000000-0000-4000-8000-000000000002"),
    )
    before = deepcopy(database.data)
    with pytest.raises(CompositionAdmissionError, match="unresolved_attempt"):
        adapter.prepare(successor)
    assert database.data == before


@pytest.mark.parametrize("damage", ["missing_partition", "missing_proof", "promoted_outcome"])
def test_successor_rejects_damaged_foreign_history_before_reservation(damage):
    database, adapter = retiring()
    attempt = journal_attempt(database, state="FAILED")
    adapter.finalize_retirement(database.request)
    record = database.data["attempts"][attempt.attempt_sha256]
    if damage == "missing_partition":
        database.data["attempt_domains"].clear()
        database.data["attempts"][attempt.attempt_sha256] = (*record[:4], "COMMIT_UNKNOWN", *record[5:])
    elif damage == "missing_proof":
        database.data["proofs"].clear()
    else:
        database.data["attempts"][attempt.attempt_sha256] = (*record[:4], "SUCCEEDED", *record[5:])
    successor = replace(
        database.request,
        context=replace(database.request.context, activation_id="10000000-0000-4000-8000-000000000002"),
    )
    before = deepcopy(database.data)
    with pytest.raises(CompositionAdmissionError):
        adapter.prepare(successor)
    assert database.data == before


@pytest.mark.parametrize("outcome", ["SUCCEEDED", "FAILED"])
def test_successor_can_reserve_after_complete_foreign_terminal_closure(outcome):
    database, adapter = retiring()
    journal_attempt(database, state=outcome)
    adapter.finalize_retirement(database.request)
    successor = replace(
        database.request,
        context=replace(database.request.context, activation_id="10000000-0000-4000-8000-000000000002"),
    )
    result = adapter.prepare(successor)
    assert result.receipt.state == "PREPARED"
    assert all(epoch == 2 for _, epoch in result.receipt.guard_epochs)


@pytest.mark.parametrize("phase", ["activate", "begin_retirement", "finalize_retirement"])
def test_transition_commit_ack_loss_reconciles_without_replaying(phase):
    database, adapter = prepared()
    if phase in {"begin_retirement", "finalize_retirement"}:
        adapter.activate(database.request)
    if phase == "finalize_retirement":
        adapter.begin_retirement(database.request)
        journal_attempt(database)
    database.fail_commit = True
    before = len(database.connections)
    result = getattr(adapter, phase)(database.request)
    assert (
        result.receipt.state
        == {"activate": "ACTIVE", "begin_retirement": "RETIRING", "finalize_retirement": "RETIRED"}[phase]
    )
    assert len(database.connections) == before + 2
    assert database.connections[before].commits == 1
