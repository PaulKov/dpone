"""Actual SQL journal DDL and invariants; no ClickHouse execution certification."""

from contextlib import closing
from hashlib import sha256
from uuid import uuid4

import pytest
from tests.integration.composition import mssql_store_live_support as support

from dpone.adapters.composition_clickhouse_dispatch_schema import (
    render_clickhouse_dispatch_schema,
    require_clickhouse_dispatch_schema,
)
from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore, composition_control_transaction
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_identity import CompositionAdmissionError

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]
sql_case = support.sql_case


def test_dispatch_catalog_and_append_only_closure(sql_case):
    """Exercise actual module metadata, global lock, original hashes and immutability."""
    case = sql_case
    case.install()
    case.sql(render_clickhouse_dispatch_schema(case.schema))
    with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
        require_clickhouse_dispatch_schema(ledger.cursor, case.schema)
    gate = str(uuid4())
    original = b'{"component":"closure"}'
    digest = "sha256:" + sha256(original).hexdigest()
    statement = (
        f"INSERT INTO {case.table('ch_dispatch_closures')} "
        "(gate_id,phase,evidence_sha256,evidence_document) VALUES (?, ?, ?, ?)"
    )
    with closing(case.database.connect()) as connection:
        with pytest.raises(support.SqlFailure) as refused:
            support.execute(connection, statement, gate, "CLOSING", digest, original)
        assert refused.value.code == 51000
    with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
        ledger.cursor.execute(statement, gate, "CLOSING", digest, original)
    assert case.sql(f"SELECT phase,evidence_document FROM {case.table('ch_dispatch_closures')}") == (
        ("CLOSING", original),
    )
    for mutation in (
        f"DELETE FROM {case.table('ch_dispatch_closures')}",
        f"UPDATE {case.table('ch_dispatch_closures')} SET phase='CLOSING'",
    ):
        reject_invariant(case, mutation)
    for phase, evidence_hash in (("INVALID", digest), ("CLOSED", "sha256:" + "0" * 64)):
        reject_invariant(case, statement, gate, phase, evidence_hash, original)
    with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
        ledger.cursor.execute(statement, gate, "CLOSED", digest, original)
    assert case.sql(f"SELECT phase FROM {case.table('ch_dispatch_closures')} ORDER BY phase") == (
        ("CLOSED",),
        ("CLOSING",),
    )


def test_dispatch_catalog_drift_is_rejected(sql_case):
    case = sql_case
    case.install()
    case.sql(render_clickhouse_dispatch_schema(case.schema))
    case.sql(f"DISABLE TRIGGER [{case.schema}].[composition_ch_dispatches_invariant] ON {case.table('ch_dispatches')}")
    with pytest.raises(CompositionAdmissionError):
        with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
            require_clickhouse_dispatch_schema(ledger.cursor, case.schema)


def reject_invariant(case, statement, *parameters, codes=(51000,)):
    """An unrelated SQL error must never satisfy an invariant-rejection test."""
    with pytest.raises(CompositionAdmissionError, match="expected_sql_invariant"):
        with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
            try:
                ledger.cursor.execute(statement, *parameters)
                support.drain_results(ledger.cursor)
            except Exception as error:
                assert support.failure(error).code in codes
                raise CompositionAdmissionError("expected_sql_invariant") from None
            raise AssertionError("invariant accepted the forbidden mutation")


def test_dispatch_claim_terminal_and_closure_order(sql_case):
    case = sql_case
    case.install()
    case.sql(render_clickhouse_dispatch_schema(case.schema))
    case.store().prepare(case.request)
    occurrence = case.store().activate(case.request)
    workload = case.request.workloads[0]
    guards = {
        resource.guard_id
        for resource in case.request.resources
        if set(resource.write_subjects).intersection(workload.write_subjects)
    }
    attempt = CompositionAttemptIdentity(
        case.request.request_sha256,
        workload.workload_id,
        workload.constituent_id,
        workload.pack_sha256,
        support.digest("dispatch component plan"),
        "dispatch component run",
        "task",
        1,
        -1,
        tuple(pair for pair in occurrence.receipt.guard_epochs if pair[0] in guards),
    )
    MssqlCompositionAttemptStore(
        case.database.connect, expected_service_id=case.service_id, control_schema=case.schema
    ).admit_once(attempt)
    gate = str(uuid4())
    original = b'{"component":"dispatch"}'
    digest = "sha256:" + sha256(original).hexdigest()
    claim = (
        f"INSERT INTO {case.table('ch_dispatches')} "
        "(dispatch_sha256,claim_key,operation_key,gate_id,query_id,dispatch_document) VALUES (?,?,?,?,?,?)"
    )
    values = (digest, support.digest("claim"), attempt.attempt_sha256, gate, "component-query", original)
    with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
        ledger.cursor.execute(claim, *values)
    reject_invariant(case, claim, *values, codes=(2601, 2627))
    closure = (
        f"INSERT INTO {case.table('ch_dispatch_closures')} "
        "(gate_id,phase,evidence_sha256,evidence_document) VALUES (?,?,?,?)"
    )
    with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
        ledger.cursor.execute(closure, gate, "CLOSING", digest, original)
    reject_invariant(case, closure, gate, "CLOSED", digest, original)
    another = b'{"component":"late"}'
    late_digest = "sha256:" + sha256(another).hexdigest()
    reject_invariant(
        case, claim, late_digest, support.digest("late"), attempt.attempt_sha256, gate, "late-query", another
    )
    with composition_control_transaction(case.database.connect, case.schema, case.service_id) as ledger:
        ledger.cursor.execute(
            f"INSERT INTO {case.table('ch_dispatch_terminals')} "
            "(dispatch_sha256,terminal_sha256,terminal_document) VALUES (?,?,?)",
            digest,
            digest,
            original,
        )
        ledger.cursor.execute(closure, gate, "CLOSED", digest, original)
    assert case.sql(f"SELECT COUNT(*) FROM {case.table('ch_dispatch_terminals')}") == ((1,),)
