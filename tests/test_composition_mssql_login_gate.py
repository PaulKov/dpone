"""Offline SQL login-gate tests; these do not certify SQL Server races."""

import pytest

from dpone.adapters.composition_mssql_gate_proofs import gate_proof, observe_quiescence, persist_gate_proof
from dpone.adapters.composition_mssql_gate_schema import render_composition_mssql_login_gate
from dpone.adapters.composition_mssql_issuance import (
    MssqlIssuedCredentials,
    require_gate_policy,
    require_login,
)
from dpone.adapters.composition_mssql_login_gate import MssqlCompositionLoginGate
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import CompositionAdmissionError
from tests.composition_mssql_gate_helpers import SERVICE, Connection, Cursor, begin_steps
from tests.composition_mssql_gate_helpers import issuing as issuing
from tests.composition_mssql_gate_helpers import offline_catalog as offline_catalog


def test_external_gate_ddl_enforces_monotonic_state_and_synchronous_exact_sid_barrier():
    sql = render_composition_mssql_login_gate(control_database="SyntheticControl")
    assert "JOURNALED" in sql and "CLOSING" in sql and "CLOSED" in sql
    assert "AFTER UPDATE, DELETE" in sql
    assert "FOR LOGON" in sql
    assert "HOLDLOCK, ROWLOCK, NOWAIT" in sql
    assert "original_security_id" in sql
    trigger = sql.split("FOR LOGON", 1)[1]
    assert "sp_getapplock" not in trigger
    assert "ROLLBACK TRANSACTION" in trigger
    assert "LEFT(@canonical_name, 9)" in trigger


def test_issued_password_is_repr_redacted_and_requires_exact_sid():
    value = MssqlIssuedCredentials("dpone_v3_" + "a" * 64, b"a" * 16, "secret-value")
    assert "secret-value" not in repr(value)
    assert value.password == "secret-value"
    with pytest.raises(CompositionAdmissionError):
        MssqlIssuedCredentials("dpone_v3_" + "a" * 64, b"short", "secret-value")


@pytest.mark.parametrize("name", ["db]; DROP LOGIN x;--", "a" * 129, ""])
def test_external_provisioner_rejects_unsafe_identifiers(name):
    with pytest.raises(ValueError):
        render_composition_mssql_login_gate(control_database=name)


def gate_steps(credentials, state="JOURNALED", disabled_evidence=None):
    return [
        ("login_sid, login_name", [(credentials.login_sid, credentials.login_name, state, disabled_evidence)]),
        ("principal_id FROM", [("mssql-sid:" + credentials.login_sid.hex(),)]),
    ]


def issue_steps(credentials):
    return [
        begin_steps() + [("operation_key FROM", []), ("INSERT INTO", []), ("INSERT INTO", [])],
        begin_steps()
        + gate_steps(credentials)
        + [
            ("CREATE LOGIN", []),
            ("CREATE USER", []),
            ("SELECT u.name", [(credentials.login_name, credentials.login_sid, "S", 1, "bounded_writer")]),
            ("UPDATE", [("READY",)]),
        ],
        begin_steps()
        + gate_steps(credentials, "READY")
        + [
            ("SELECT u.name", [(credentials.login_name, credentials.login_sid, "S", 1, "bounded_writer")]),
            ("SELECT p.name", [(credentials.login_name, credentials.login_sid, "S", False, 0, 0)]),
        ],
    ]


def test_credentials_return_only_after_journal_create_ready_and_fresh_readback(issuing):
    value, credentials, _ = issuing
    cursors = [Cursor(steps) for steps in issue_steps(credentials)]
    connections = [Connection(cursor) for cursor in cursors]
    pending = iter(connections)
    gate = MssqlCompositionLoginGate(lambda: next(pending), expected_service_id=SERVICE, control_database="Control")
    observed = gate.issue_once(value)
    assert observed == credentials and all(connection.committed for connection in connections)
    assert all(not cursor.steps for cursor in cursors)
    all_calls = [call for cursor in cursors for call in cursor.calls]
    assert all(credentials.password not in sql for sql, _ in all_calls)
    login_sql, parameters = next(call for call in all_calls if "CREATE LOGIN" in call[0])
    assert parameters == (credentials.login_name, credentials.login_sid, credentials.password)
    assert "QUOTENAME(@password, '''')" in login_sql
    assert not any("PASSWORD" in sql for sql, _ in cursors[0].calls)


@pytest.mark.parametrize("failed_phase", [0, 1, 2])
def test_unknown_journal_creation_or_readback_commit_returns_no_secret_and_closes(issuing, monkeypatch, failed_phase):
    value, credentials, _ = issuing
    connections = [
        Connection(Cursor(steps), commit_error=RuntimeError(credentials.password) if index == failed_phase else None)
        for index, steps in enumerate(issue_steps(credentials))
    ]
    pending = iter(connections)
    gate = MssqlCompositionLoginGate(lambda: next(pending), expected_service_id=SERVICE, control_database="Control")
    closed = []
    monkeypatch.setattr(gate, "close", lambda attempted: closed.append(attempted))
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown") as caught:
        gate.issue_once(value)
    assert credentials.password not in str(caught.value)
    assert closed == [value]
    assert connections[failed_phase].rolled_back
    assert all(not connection.committed for connection in connections[failed_phase:])


@pytest.mark.parametrize("existing_state", ["JOURNALED", "READY", "CLOSING", "CLOSED"])
def test_replayed_issuance_never_reads_or_resets_password(issuing, existing_state):
    value, credentials, _ = issuing
    cursor = Cursor(begin_steps() + [("operation_key FROM", [(value.attempt_sha256,)])])
    gate = MssqlCompositionLoginGate(
        lambda: Connection(cursor), expected_service_id=SERVICE, control_database="Control"
    )
    with pytest.raises(CompositionAdmissionError, match="login_issuance_replay"):
        gate.issue_once(value)
    assert all(not any(token in sql for token in ("CREATE LOGIN", "ALTER LOGIN", "INSERT")) for sql, _ in cursor.calls)
    assert all(credentials.password not in parameters for _, parameters in cursor.calls)


def test_closure_winning_after_journal_prevents_credential_creation(issuing, monkeypatch):
    value, credentials, _ = issuing
    first = Connection(Cursor(issue_steps(credentials)[0]))
    second_cursor = Cursor(begin_steps() + gate_steps(credentials, "CLOSING"))
    pending = iter([first, Connection(second_cursor)])
    gate = MssqlCompositionLoginGate(lambda: next(pending), expected_service_id=SERVICE, control_database="Control")
    monkeypatch.setattr(gate, "close", lambda _: None)
    with pytest.raises(CompositionAdmissionError, match="login_gate_readback"):
        gate.issue_once(value)
    assert first.committed
    assert not any("CREATE LOGIN" in sql for sql, _ in second_cursor.calls)


@pytest.mark.parametrize(
    "rows",
    [
        [],
        [("foreign", b"a" * 16, "S", True, 0, 0)],
        [("dpone_v3_" + "a" * 64, b"a" * 16, "S", False, 0, 0)],
        [("dpone_v3_" + "a" * 64, b"a" * 16, "S", True, 1, 0)],
    ],
)
def test_disabled_proof_rejects_missing_foreign_enabled_or_privileged_principal(rows):
    ledger = CompositionMssqlLedger(Cursor([("SELECT p.name", rows)]), "dpone_control")
    with pytest.raises(CompositionAdmissionError, match="login_principal_readback"):
        require_login(ledger, "dpone_v3_" + "a" * 64, b"a" * 16, disabled=True)


@pytest.mark.parametrize("rows", [[], [(None,)], [(1,)]])
def test_quiescence_missing_ambiguous_or_nonempty_session_observation_blocks(issuing, rows):
    _, credentials, enrollment = issuing
    cursor = Cursor([("original_security_id", rows)])
    with pytest.raises(CompositionAdmissionError, match="login_sessions_not_quiescent"):
        observe_quiescence(CompositionMssqlLedger(cursor, "dpone_control"), credentials.login_sid, (enrollment,))
    assert cursor.calls[0][1] == (credentials.login_sid,)
    assert "login_time" not in cursor.calls[0][0] and "login_name" not in cursor.calls[0][0]


@pytest.mark.parametrize(
    "stage,reason", [(1, "login_transactions_not_quiescent"), (3, "login_unattributed_transactions")]
)
def test_quiescence_rejects_attributable_and_orphan_database_transactions(issuing, stage, reason):
    _, credentials, enrollment = issuing
    steps = [
        ("original_security_id", [(0,)]),
        ("dm_tran_session_transactions", [(0,)]),
        ("dm_tran_current_transaction", [(123,)]),
        ("dm_tran_database_transactions", [(0,)]),
    ]
    steps[stage] = (steps[stage][0], [(1,)])
    with pytest.raises(CompositionAdmissionError, match=reason):
        observe_quiescence(CompositionMssqlLedger(Cursor(steps), "dpone_control"), credentials.login_sid, (enrollment,))


def test_quiescence_requires_complete_sid_and_database_observations(issuing):
    _, credentials, enrollment = issuing
    cursor = Cursor(
        [
            ("original_security_id", [(0,)]),
            ("dm_tran_session_transactions", [(0,)]),
            ("dm_tran_current_transaction", [(123,)]),
            ("dm_tran_database_transactions", [(0,)]),
        ]
    )
    observe_quiescence(CompositionMssqlLedger(cursor, "dpone_control"), credentials.login_sid, (enrollment,))
    assert not cursor.steps and cursor.calls[-1][1] == (enrollment.database_id, 123)


def test_missing_or_changed_installed_logon_policy_blocks_issuance():
    with pytest.raises(CompositionAdmissionError, match="login_gate_policy"):
        require_gate_policy(CompositionMssqlLedger(Cursor([("SELECT DB_NAME()", [])]), "dpone_control"), "Control")


def closed_evidence(value, credentials):
    return {
        "schema": "dpone.composition-mssql-gate-observation.v1",
        "attempt_sha256": value.attempt_sha256,
        "service_id": SERVICE,
        "login_sid": credentials.login_sid.hex(),
        "login_name": credentials.login_name,
        "kind": "CLOSED_GATES",
        "login_disabled": True,
        "authentication_barrier": "CLOSING",
    }


def proof_insert_steps():
    return [
        ("THEN evidence_document END", []),
        ("INSERT INTO", []),
        ("operation_family, CASE", []),
        ("INSERT INTO", []),
    ]


def test_close_commits_barrier_before_disabling_and_proves_exact_sid(issuing):
    value, credentials, _ = issuing
    evidence = closed_evidence(value, credentials)
    evidence_hash = canonical_fingerprint(evidence)
    cursors = [
        Cursor(begin_steps() + gate_steps(credentials, "READY") + [("UPDATE", [("CLOSING",)])]),
        Cursor(begin_steps() + gate_steps(credentials, "CLOSING") + [("ALTER LOGIN", [])]),
        Cursor(
            begin_steps()
            + gate_steps(credentials, "CLOSING")
            + [
                ("SELECT p.name", [(credentials.login_name, credentials.login_sid, "S", True, 0, 0)]),
                ("UPDATE", [("CLOSED",)]),
            ]
            + proof_insert_steps()
        ),
        Cursor(
            begin_steps()
            + gate_steps(credentials, "CLOSED", evidence_hash)
            + [("SELECT p.name", [(credentials.login_name, credentials.login_sid, "S", True, 0, 0)])]
        ),
    ]
    connections = [Connection(cursor) for cursor in cursors]
    position = 0

    def factory():
        nonlocal position
        assert all(connection.committed for connection in connections[:position])
        result = connections[position]
        position += 1
        return result

    proof = MssqlCompositionLoginGate(factory, expected_service_id=SERVICE, control_database="Control").close(value)
    assert proof.kind == "CLOSED_GATES" and proof.evidence_sha256 == evidence_hash
    assert proof.authorities[0].principal_id == "mssql-sid:" + credentials.login_sid.hex()
    assert all(not cursor.steps for cursor in cursors) and all(connection.committed for connection in connections)
    assert not any("dm_exec_sessions" in sql for sql, _ in cursors[0].calls)


def test_unknown_close_barrier_commit_does_not_disable_or_claim_closed(issuing):
    value, credentials, _ = issuing
    cursor = Cursor(begin_steps() + gate_steps(credentials, "READY") + [("UPDATE", [("CLOSING",)])])
    connection = Connection(cursor, commit_error=RuntimeError("lost ACK"))
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        MssqlCompositionLoginGate(lambda: connection, expected_service_id=SERVICE, control_database="Control").close(
            value
        )
    assert connection.rolled_back and not any("ALTER LOGIN" in sql for sql, _ in cursor.calls)


def test_missing_principal_after_barrier_remains_blocking(issuing):
    value, credentials, _ = issuing
    first = Connection(Cursor(begin_steps() + gate_steps(credentials, "JOURNALED") + [("UPDATE", [("CLOSING",)])]))
    second = Connection(
        Cursor(
            begin_steps() + gate_steps(credentials, "CLOSING") + [("ALTER LOGIN", RuntimeError("principal missing"))]
        )
    )
    pending = iter([first, second])
    with pytest.raises(CompositionAdmissionError, match="control_operation_unknown"):
        MssqlCompositionLoginGate(lambda: next(pending), expected_service_id=SERVICE, control_database="Control").close(
            value
        )
    assert first.committed and second.rolled_back
    assert not any("'CLOSED'" in sql for sql, _ in first.value.calls + second.value.calls)


@pytest.mark.parametrize("state", ["JOURNALED", "READY", "CLOSING"])
def test_quiescence_cannot_bypass_unclosed_gate(issuing, state):
    value, credentials, _ = issuing
    cursor = Cursor(begin_steps() + gate_steps(credentials, state))
    with pytest.raises(CompositionAdmissionError, match="login_gate_not_closed"):
        MssqlCompositionLoginGate(
            lambda: Connection(cursor), expected_service_id=SERVICE, control_database="Control"
        ).prove_quiescence(value)
    assert not any("dm_exec_sessions" in sql for sql, _ in cursor.calls)


def test_quiescence_proof_is_persisted_only_after_full_server_observation(issuing):
    value, credentials, _ = issuing
    closed_hash = canonical_fingerprint(closed_evidence(value, credentials))
    cursor = Cursor(
        begin_steps()
        + gate_steps(credentials, "CLOSED", closed_hash)
        + [
            ("SELECT p.name", [(credentials.login_name, credentials.login_sid, "S", True, 0, 0)]),
            ("original_security_id", [(0,)]),
            ("dm_tran_session_transactions", [(0,)]),
            ("dm_tran_current_transaction", [(123,)]),
            ("dm_tran_database_transactions", [(0,)]),
        ]
        + proof_insert_steps()
    )
    connection = Connection(cursor)
    proof = MssqlCompositionLoginGate(
        lambda: connection, expected_service_id=SERVICE, control_database="Control"
    ).prove_quiescence(value)
    assert proof.kind == "QUIESCENCE" and connection.committed and not cursor.steps
    assert proof.authorities[0].connector == "mssql"
    last_observation = next(
        index for index, (sql, _) in enumerate(cursor.calls) if "dm_tran_database_transactions" in sql
    )
    first_evidence_write = next(index for index, (sql, _) in enumerate(cursor.calls) if "INSERT INTO" in sql)
    assert first_evidence_write > last_observation


def test_sql_gate_cannot_make_outcome_or_clickhouse_proof(issuing):
    from dataclasses import replace

    from dpone.contracts.composition_proof import CompositionProofAuthority

    value, credentials, _ = issuing
    evidence = closed_evidence(value, credentials)
    with pytest.raises(CompositionAdmissionError, match="login_proof_kind"):
        gate_proof(value, service_id=SERVICE, sid=credentials.login_sid, kind="OUTCOME", evidence=evidence)
    proof = gate_proof(value, service_id=SERVICE, sid=credentials.login_sid, kind="CLOSED_GATES", evidence=evidence)
    foreign = replace(
        proof, authorities=(CompositionProofAuthority("clickhouse", SERVICE, "clickhouse-user:" + SERVICE),)
    )
    cursor = Cursor()
    with pytest.raises(CompositionAdmissionError, match="login_proof_evidence"):
        persist_gate_proof(CompositionMssqlLedger(cursor, "dpone_control"), foreign, evidence)
    assert not cursor.calls


def test_two_invocations_interleaved_at_journal_commit_issue_only_once(issuing):
    value, credentials, _ = issuing
    phases = issue_steps(credentials)
    contender = Cursor(begin_steps() + [("operation_key FROM", [(value.attempt_sha256,)])])
    attempted = []
    other = MssqlCompositionLoginGate(
        lambda: Connection(contender), expected_service_id=SERVICE, control_database="Control"
    )

    class JournalConnection(Connection):
        def commit(self):
            super().commit()
            with pytest.raises(CompositionAdmissionError, match="login_issuance_replay"):
                other.issue_once(value)
            attempted.append(True)

    connections = [JournalConnection(Cursor(phases[0])), Connection(Cursor(phases[1])), Connection(Cursor(phases[2]))]
    pending = iter(connections)
    original = MssqlCompositionLoginGate(lambda: next(pending), expected_service_id=SERVICE, control_database="Control")
    assert original.issue_once(value) == credentials and attempted == [True]
    all_sql = [sql for connection in connections for sql, _ in connection.value.calls] + [
        sql for sql, _ in contender.calls
    ]
    assert sum("CREATE LOGIN" in sql for sql in all_sql) == 1
