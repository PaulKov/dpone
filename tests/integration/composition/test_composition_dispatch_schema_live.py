"""Actual SQL journal DDL and invariants; no ClickHouse execution certification."""

from contextlib import closing, contextmanager
from hashlib import sha256
from uuid import uuid4

import pytest
from tests.integration.composition import mssql_store_live_support as support
from tests.integration.composition.mssql_gate_live_provisioning import ControlDiagnostics
from tests.integration.composition.mssql_gate_live_support import observation_document

from dpone.adapters.composition_clickhouse_dispatch_schema import (
    render_clickhouse_dispatch_schema,
    require_clickhouse_dispatch_schema,
)
from dpone.adapters.composition_mssql_attempts import MssqlCompositionAttemptStore, composition_control_transaction
from dpone.contracts.composition_attempt import CompositionAttemptIdentity
from dpone.contracts.composition_identity import CompositionAdmissionError

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql]
sql_case = support.sql_case


def test_dispatch_catalog_and_append_only_closure(sql_case, record_property):
    """Exercise actual module metadata, global lock, original hashes and immutability."""
    case = sql_case
    case.install()
    case.sql(render_clickhouse_dispatch_schema(case.schema))
    with observed_transaction(case, "catalog", record_property) as ledger:
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
    with observed_transaction(case, "closing_insert", record_property) as ledger:
        ledger.cursor.execute(statement, gate, "CLOSING", digest, original)
    assert case.sql(f"SELECT phase,evidence_document FROM {case.table('ch_dispatch_closures')}") == (
        ("CLOSING", original),
    )
    for stage, mutation in (
        ("deny_delete", f"DELETE FROM {case.table('ch_dispatch_closures')}"),
        ("deny_update", f"UPDATE {case.table('ch_dispatch_closures')} SET phase='CLOSING'"),
    ):
        reject_invariant(case, mutation, stage=stage, record_property=record_property)
    for stage, phase, evidence_hash in (
        ("deny_phase", "INVALID", digest),
        ("deny_hash", "CLOSED", "sha256:" + "0" * 64),
    ):
        reject_invariant(
            case, statement, gate, phase, evidence_hash, original, stage=stage, record_property=record_property
        )
    with observed_transaction(case, "closed_insert", record_property) as ledger:
        ledger.cursor.execute(statement, gate, "CLOSED", digest, original)
    assert case.sql(f"SELECT phase FROM {case.table('ch_dispatch_closures')} ORDER BY phase") == (
        ("CLOSED",),
        ("CLOSING",),
    )


def test_dispatch_catalog_drift_is_rejected(sql_case, record_property):
    case = sql_case
    case.install()
    case.sql(render_clickhouse_dispatch_schema(case.schema))
    case.sql(f"DISABLE TRIGGER [{case.schema}].[composition_ch_dispatches_invariant] ON {case.table('ch_dispatches')}")
    with pytest.raises(CompositionAdmissionError):
        with observed_transaction(case, "catalog_drift", record_property) as ledger:
            require_clickhouse_dispatch_schema(ledger.cursor, case.schema)


_STAGES = frozenset(
    {
        "catalog",
        "closing_insert",
        "closed_insert",
        "catalog_drift",
        "claim_insert",
        "terminal_and_closed_insert",
        "deny_delete",
        "deny_update",
        "deny_phase",
        "deny_hash",
        "deny_duplicate_claim",
        "deny_unresolved_close",
        "deny_late_claim",
    }
)


@contextmanager
def observed_transaction(case, stage, record_property):
    """Record bounded numeric driver diagnostics before transaction sanitization.

    Reuse the established SQL observer: no SQL bodies, parameters, exception
    text, login names or credentials enter artifacts. Fixed stage names locate
    failures during begin, execute, fetch or commit without changing execution.
    """
    if stage not in _STAGES:
        raise ValueError("dispatch_diagnostic_stage")
    diagnostics = ControlDiagnostics()
    factory = diagnostics.factory(case.database.connect, scope="attempt")
    try:
        with composition_control_transaction(factory, case.schema, case.service_id) as ledger:
            yield ledger
    finally:
        record_property("dpone.dispatch." + stage, observation_document(diagnostics.snapshot()))


def reject_invariant(case, statement, *parameters, stage, record_property, codes=(51000,)):
    """Assert the exact refusal outside the production transaction sanitizer."""
    observed_code = None
    with pytest.raises(CompositionAdmissionError, match="expected_sql_invariant"):
        with observed_transaction(case, stage, record_property) as ledger:
            try:
                ledger.cursor.execute(statement, *parameters)
                support.drain_results(ledger.cursor)
            except Exception as error:
                observed_code = support.failure(error).code
                record_property(
                    "dpone.dispatch.refusal_" + stage,
                    observation_document(
                        {
                            "selected_code": observed_code,
                            "expected_codes": list(codes),
                        }
                    ),
                )
                raise CompositionAdmissionError("expected_sql_invariant") from None
            raise CompositionAdmissionError("forbidden_mutation_accepted")
    assert observed_code in codes, (stage, observed_code, codes)


def test_dispatch_claim_terminal_and_closure_order(sql_case, record_property):
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
    with observed_transaction(case, "claim_insert", record_property) as ledger:
        ledger.cursor.execute(claim, *values)
    reject_invariant(
        case, claim, *values, codes=(2601, 2627), stage="deny_duplicate_claim", record_property=record_property
    )
    closure = (
        f"INSERT INTO {case.table('ch_dispatch_closures')} "
        "(gate_id,phase,evidence_sha256,evidence_document) VALUES (?,?,?,?)"
    )
    with observed_transaction(case, "closing_insert", record_property) as ledger:
        ledger.cursor.execute(closure, gate, "CLOSING", digest, original)
    reject_invariant(
        case, closure, gate, "CLOSED", digest, original, stage="deny_unresolved_close", record_property=record_property
    )
    another = b'{"component":"late"}'
    late_digest = "sha256:" + sha256(another).hexdigest()
    reject_invariant(
        case,
        claim,
        late_digest,
        support.digest("late"),
        attempt.attempt_sha256,
        gate,
        "late-query",
        another,
        stage="deny_late_claim",
        record_property=record_property,
    )
    with observed_transaction(case, "terminal_and_closed_insert", record_property) as ledger:
        ledger.cursor.execute(
            f"INSERT INTO {case.table('ch_dispatch_terminals')} "
            "(dispatch_sha256,terminal_sha256,terminal_document) VALUES (?,?,?)",
            digest,
            digest,
            original,
        )
        ledger.cursor.execute(closure, gate, "CLOSED", digest, original)
    assert case.sql(f"SELECT COUNT(*) FROM {case.table('ch_dispatch_terminals')}") == ((1,),)
