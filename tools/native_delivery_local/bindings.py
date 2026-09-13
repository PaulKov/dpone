"""Target-only reconstruction from frozen catalog pins and immutable requests."""

from __future__ import annotations

from dataclasses import replace
from datetime import datetime

from dpone.contracts.mssql_transaction_governance import InvocationIdentity, MssqlAttemptRequest, MssqlOperationRequest
from dpone.contracts.runtime_connection import ResolvedBindingConnection, ResolvedConnectionDescriptor
from dpone.runtime.credentials.config import CredentialsConfig
from dpone.runtime.etl.mssql_schema_preplan import MSSQL_SCHEMA_PREPLAN_OPTION, MssqlSchemaPreplan
from dpone.runtime.etl.mssql_transaction_admission import ADMISSION_OPTION
from dpone.runtime.sinks.mssql_native_recovery import restore_mutation
from dpone.runtime.state.mssql_database_authority import MssqlDatabaseAuthorityVerifier
from dpone.runtime.state.mssql_generic_transaction import MssqlGenericTransactionState
from dpone.runtime.state.mssql_generic_transaction_storage import MssqlGenericTransactionStateStorage
from dpone.runtime.state.mssql_route_preflight import resolve_atomic_mssql_target


def requests(results):
    attempt = dict(results["attempt"])
    attempt["invocation"] = InvocationIdentity(**attempt["invocation"])
    for key in ("target_identity", "route_fingerprint"):
        attempt[key] = bytes.fromhex(attempt[key])
    operation = dict(results["operation_request"])
    for key in ("scope_hash", "owner_digest"):
        operation[key] = bytes.fromhex(operation[key])
    if operation["lease_expires_at_utc"] is not None:
        operation["lease_expires_at_utc"] = datetime.fromisoformat(operation["lease_expires_at_utc"])
    return MssqlAttemptRequest(**attempt), MssqlOperationRequest(**operation)


def storage_binding(environment, inventory, results, connector):
    """Verify frozen pins; never call a discovery/provisioning helper on attach."""

    def connection(properties):
        return ResolvedBindingConnection(
            CredentialsConfig(database=inventory.target_database), {}, ResolvedConnectionDescriptor("mssql", properties)
        )

    verifier = MssqlDatabaseAuthorityVerifier.from_connections(
        target_connection=connection(results["target_properties"]),
        state_connection=connection(results["state_properties"]),
        target_database=inventory.target_database,
        staging_database=inventory.target_database,
        state_database=inventory.state_database,
        master_connector_factory=lambda _: environment.sql("master"),
        staging_connector_factory=lambda _, database: environment.sql(database),
    )
    return MssqlGenericTransactionStateStorage(
        connector, database=inventory.state_database, schema=inventory.schema
    ).bind_database_authority(verifier)


def admission_binding(config, inventory, results, target, storage, *, faults=None):
    physical = resolve_atomic_mssql_target(
        target, storage, database=inventory.target_database, schema=inventory.schema, table=inventory.table
    )
    attempt, operation = requests(results)
    if physical.digest != attempt.target_identity:
        raise ValueError("local_fixture.target_identity_changed")
    state = MssqlGenericTransactionState.from_state_storage(storage)
    state.preflight(target)
    if faults is not None:
        faults.probes += 1
        if faults.armed == "unknown_commit" and "unknown_commit" in faults.events:
            raise RuntimeError("local_fixture.receipt_probe_unavailable")
    admission = state.replay_if_committed(attempt, operation) or state.admit(attempt, operation)
    saved = results["preplan"]
    preplan = MssqlSchemaPreplan(
        bytes.fromhex(saved["source_schema_sha256"]),
        restore_mutation(saved["mutation"]),
        tuple(tuple(column) for column in saved["target_column_types"]),
    )
    return (
        replace(config, options={**config.options, ADMISSION_OPTION: admission, MSSQL_SCHEMA_PREPLAN_OPTION: preplan}),
        admission,
        state,
    )
