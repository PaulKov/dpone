"""Synthetic plan producer with real cross-database SQL control and worker I/O.

No source bytes, generic governance issuance, PostgreSQL route, or load receipt
is certified here. The explicitly synthetic plan verifies one fixture operation;
all composition ledgers, login grants, stored modules and target DML are real.
"""

from __future__ import annotations

import re
from contextlib import closing
from dataclasses import replace
from hashlib import sha256
from types import SimpleNamespace

import pytest
from tests.integration.composition.mssql_gate_live_provisioning import ControlDiagnostics
from tests.integration.composition.mssql_gate_live_support import GateCase, observation_document
from tests.integration.composition.mssql_store_live_support import drain_results, execute, failure

from dpone.adapters.composition_mssql_login_gate import MssqlCompositionLoginGate
from dpone.adapters.composition_mssql_transaction_binding import MssqlCompositionTransactionBindings
from dpone.adapters.composition_mssql_transaction_fence import MssqlCompositionTransactionFence
from dpone.adapters.composition_mssql_transaction_fence_schema import (
    TRANSFER_PROCEDURE,
    module_sha256,
    render_composition_mssql_transaction_fence,
    transfer_procedure_sql,
)
from dpone.adapters.composition_mssql_transfer_access import MssqlCompositionTransferAccess
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_execution import CompositionExecutionPlan
from dpone.contracts.dbt_relation_writes import DbtRelationWrite
from dpone.contracts.dbt_workspace_activation import dbt_relation_write_subject
from dpone.contracts.mssql_transaction_governance import (
    InvocationIdentity,
    MssqlAttemptRequest,
    MssqlOperationRequest,
    MssqlTransactionAttempt,
    MssqlTransactionOperation,
)


def transfer_batches(database, schema):
    """Split only administrator renderer batches; never run DDL through workers."""
    return tuple(
        batch.strip()
        for batch in re.split(
            r"(?m)^GO\s*$", render_composition_mssql_transaction_fence(control_database=database, control_schema=schema)
        )
        if batch.strip()
    )


class TransferGateCase(GateCase):
    """Retain existing live provisioning/cleanup and select its ordinary transfer."""

    def _request(self):
        parent = super()._request()
        self.write = DbtRelationWrite(
            "component",
            "ordinary",
            "component_transfer",
            "transfer",
            "mssql",
            "synthetic_target",
            self.target,
            self.environment.managed_schema,
            "rows",
        )
        self.native_write = replace(self.write, resource_id="component_native", kind="model")
        writes = (dbt_relation_write_subject(self.native_write), dbt_relation_write_subject(self.write))
        workloads = tuple(
            replace(row, write_subjects=(subject,)) for row, subject in zip(parent.workloads, writes, strict=True)
        )
        return replace(
            parent, workloads=workloads, resources=(replace(parent.resources[0], write_subjects=tuple(sorted(writes))),)
        )

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        workload = self.request.workloads[1]
        self.attempt = replace(
            self.attempt,
            workload_id=workload.workload_id,
            constituent_id=workload.constituent_id,
            pack_sha256=workload.pack_sha256,
        )
        request = MssqlAttemptRequest(
            InvocationIdentity(self.attempt.dag_run_id, "component", self.attempt.task_id),
            sha256(self.target.encode()).digest(),
            sha256(b"component-only-route").digest(),
            "component-load",
            self.target,
            self.environment.managed_schema,
            "rows",
            "full_refresh",
        )
        generic_attempt = MssqlTransactionAttempt(request, generation=1)
        operation = MssqlOperationRequest(
            sha256(b"component-scope").digest(), sha256(self.attempt.attempt_sha256.encode()).digest()
        )
        self.operation = MssqlTransactionOperation(
            generic_attempt, operation.operation_key(generic_attempt), operation.scope_hash, operation.owner_digest, 1
        )
        self.mutation_digest = sha256(b"component INSERT managed.rows(1,'component'); not route certification").digest()
        # This source-shaped value is intentionally synthetic. The actual source
        # verifier is exercised by separate producer tests, not this SQL fixture.
        sources = SimpleNamespace(
            __post_init__=lambda: None,
            workload_pins=tuple((row.workload_id, row.pack_sha256) for row in self.request.workloads),
            relation_writes=(self.native_write, self.write),
            subject_sha256=self.request.source_subject_sha256,
        )
        self.plan = CompositionExecutionPlan(sources, self.request.workloads, sources.relation_writes)
        self.binding = None
        self.record(
            "transfer_fixture",
            {"synthetic_plan": True, "route_certification": False, "attempt_sha256": self.attempt.attempt_sha256},
        )

    def login_gate(self, factory=None):
        return MssqlCompositionLoginGate(
            self.diagnostics.factory(factory or self.environment.connect, scope="gate"),
            expected_service_id=self.environment.service_id,
            control_database=self.environment.database.database,
            control_schema=self.environment.schema,
            transfer_access=MssqlCompositionTransferAccess(),
        )

    def verify_operation(self, attempt, operation, write, mutation_plan_sha256, plan):
        """Exact component fixture producer; never claims a real route plan."""
        if (attempt, operation, write, mutation_plan_sha256, plan) != (
            self.attempt,
            self.operation,
            self.write,
            self.mutation_digest,
            self.plan,
        ):
            raise CompositionAdmissionError("synthetic_transfer_plan")

    def bind_worker(self):
        self.issue()
        registrar = MssqlCompositionTransactionBindings(
            self.diagnostics.factory(self.environment.connect, scope="attempt"),
            expected_service_id=self.environment.service_id,
            control_database=self.environment.database.database,
            control_schema=self.environment.schema,
            read_plan=lambda attempt: self.plan,
            verify_operation=self.verify_operation,
        )
        self.binding = registrar.bind(self.attempt, self.operation, self.write, self.mutation_digest)
        assert registrar.bind(self.attempt, self.operation, self.write, self.mutation_digest) == self.binding
        self.fence = MssqlCompositionTransactionFence(self.binding, self.environment.schema)
        return self.binding

    @staticmethod
    def worker_sql(worker, statement, *parameters):
        return execute(worker, statement, *parameters)

    def begin_fence(self, worker):
        worker.autocommit = False
        execute(worker, "SET IMPLICIT_TRANSACTIONS OFF; BEGIN TRANSACTION;")
        return self.require_fence(worker)

    def require_fence(self, worker, **kwargs):
        return self.fence.require_current(
            SimpleNamespace(connection=worker), self.operation, mutation_plan_sha256=self.mutation_digest, **kwargs
        )

    def direct_fence(self, worker, *, digest=None, document=None):
        """Call actual installed module to test server-side rejection separately."""
        transaction = execute(worker, "SELECT CURRENT_TRANSACTION_ID();")[0][0]
        env = self.environment
        return execute(
            worker,
            f"EXEC [{env.database.database}].[{env.schema}].[composition_require_transfer] ?,?,?,?,?,?;",
            self.binding.digest if digest is None else digest,
            self.binding.document if document is None else document,
            transaction,
            self.target,
            self.write.schema,
            self.write.relation,
        )


def observe_transfer_module(environment):
    """Read at most two catalog rows; preserve padding and exact module hashes.

    Never select module text, login names, connection parameters or error text.
    These observations diagnose admission failure and never alter its decision.
    """
    expected = transfer_procedure_sql(environment.schema)
    rows = environment.sql(
        "SELECT TOP (2) p.type,CONVERT(int,m.uses_ansi_nulls),CONVERT(int,m.uses_quoted_identifier),"
        "m.execute_as_principal_id,HASHBYTES('SHA2_256',m.definition),"
        "CONVERT(int,p.is_auto_executed),CONVERT(int,p.is_execution_replicated),"
        "p.principal_id,s.principal_id,DATALENGTH(m.definition),"
        "CONVERT(varchar(4),CONVERT(binary(2),p.type),2) "
        "FROM sys.procedures p JOIN sys.sql_modules m ON m.object_id=p.object_id "
        "JOIN sys.schemas s ON s.schema_id=p.schema_id WHERE p.object_id=OBJECT_ID(?,N'P');",
        f"[{environment.schema}].[{TRANSFER_PROCEDURE}]",
    )
    if len(rows) > 2:
        raise RuntimeError("transfer_module_observation_budget")
    observed = []
    for row in rows:
        if (
            len(row) != 11
            or type(row[0]) is not str
            or len(row[0]) > 2
            or any(
                value is not None and (type(value) is not int or not -(2**31) <= value < 2**31)
                for value in (*row[1:4], *row[5:10])
            )
            or (row[4] is not None and (type(row[4]) is not bytes or len(row[4]) != 32))
            or type(row[10]) is not str
            or re.fullmatch(r"[0-9A-Fa-f]{4}", row[10]) is None
        ):
            raise RuntimeError("transfer_module_observation_shape")
        observed.append([*row[:4], row[4].hex() if row[4] is not None else None, *row[5:]])
    return {
        "columns": [
            "type",
            "ansi_nulls",
            "quoted_identifier",
            "execute_as_principal_id",
            "module_sha256",
            "auto_executed",
            "execution_replicated",
            "object_owner_id",
            "schema_owner_id",
            "definition_utf16_bytes",
            "type_binary_hex",
        ],
        "expected_auditor_row": [
            "P",
            1,
            1,
            -2,
            module_sha256(expected).hex(),
            0,
            0,
            None,
            1,
        ],
        "expected_definition_utf16_bytes": len(expected.encode("utf-16le")),
        "observed": observed,
    }


@pytest.fixture(scope="session")
def transfer_fence_environment(gate_environment):
    """Install once using the fixture administrator's pre-trigger lifeline."""
    env = gate_environment
    try:
        with closing(env.lifeline.cursor()) as cursor:
            for batch in transfer_batches(env.database.database, env.schema):
                cursor.execute(batch)
                drain_results(cursor)
    except Exception as error:
        raise failure(error) from None
    return env


@pytest.fixture
def transfer_case(transfer_fence_environment, record_property):
    diagnostics, case = ControlDiagnostics(), None
    record_property(
        "dpone.gate.transfer_module_catalog", observation_document(observe_transfer_module(transfer_fence_environment))
    )
    try:
        case = TransferGateCase(transfer_fence_environment, record_property, diagnostics=diagnostics)
        yield case
    finally:
        try:
            if case is not None:
                case.cleanup()
        finally:
            record_property("dpone.gate.control_failures", observation_document(diagnostics.snapshot()))
