"""Independently rederive transfer authority before registration and extraction.

The worker supplies only its admission and digest as comparison subjects. Config
comes from detached verified originals. Source observations use the existing RR
lease; target observations use a privately owned reader. No mutation, source
commit, credential discovery or recovery replay is performed by this service.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from copy import deepcopy
from dataclasses import asdict, dataclass, replace
from hashlib import sha256
from types import SimpleNamespace
from typing import Any

from dpone.contracts.composition_identity import CompositionAdmissionError, require_digest
from dpone.contracts.composition_mssql_binding import stable_operation_document
from dpone.contracts.composition_persistence import encode_attempt_identity
from dpone.contracts.dbt_relation_writes import transfer_relation_write
from dpone.contracts.mssql_transaction_governance import MssqlTransactionAdmission
from dpone.contracts.mssql_transaction_identity import validate_source_physical_identity
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.runtime.bootstrap_preflight import prepare_verified_transfer_config
from dpone.runtime.composition_transfer_preplan_store import (
    CompositionTransferPreplanStore,
    RetainedTransferPreplanReference,
    decode_transfer_preplan,
    projection_provenance,
)
from dpone.runtime.credentials.authority import RuntimeResolvedConnections
from dpone.runtime.credentials.resolved_endpoint_factory import ResolvedEndpointFactory
from dpone.runtime.etl.mssql_schema_preplan import MssqlSchemaPreplanner
from dpone.runtime.etl.mssql_schema_preplan_codec import encode_mssql_schema_preplan
from dpone.runtime.etl.mssql_transaction_identity import require_source_physical_identity_binding
from dpone.runtime.etl.mssql_transaction_route_identity import invocation_route_fingerprint
from dpone.runtime.etl.portable_scope_preflight import prepare_portable_scope_binding
from dpone.runtime.governance.mssql_hook_replay_policy import bind_replay_safe_mssql_hook_graph
from dpone.runtime.mssql_spool_route import bind_mssql_character_spool_preflight
from dpone.runtime.sinks.mssql_target_catalog_fingerprint import assert_target_catalog_expectations
from dpone.runtime.sources.postgres import PostgresSource
from dpone.runtime.sources.postgres_source_authority import PostgresSourceAuthorityVerifier
from dpone.runtime.sources.strategies.postgres.postgres_prepared_source_boundary import PreparedPostgresSourceBoundary
from dpone.runtime.sources.strategies.postgres.postgres_schema_metadata import PostgresFetchedSchema
from dpone.runtime.state.mssql_route_preflight import MssqlSessionIdentity
from dpone.runtime.state.mssql_target_identity import (
    assert_mssql_physical_target_identity,
    resolve_mssql_physical_target_identity,
)


def _detached_binding(connection: Any) -> Any:
    # Descriptor properties are already recursively frozen. Credentials and
    # safe metadata are mutable dataclasses/dicts and must be detached here.
    if connection is None or connection.descriptor is None:
        raise CompositionAdmissionError("transfer_preplan_connections")
    return replace(
        connection, credentials=deepcopy(connection.credentials), safe_metadata=deepcopy(connection.safe_metadata)
    )


@dataclass(frozen=True, slots=True)
class _VerifiedEnvironment:
    """Only the already-verified dimension consumed by shared preflight."""

    environment: str


class CompositionTransferPreplanService:
    """An attempt-scoped producer whose target identity observer is mandatory.

    ``require_target`` is root-bound to the existing adapter that verifies signed
    target/state/control database pins and the protected composition marker on
    the supplied DB-API connection. It must return its canonical observation.
    This dependency avoids a runtime-to-adapter import or an optional fallback.
    """

    def __init__(
        self,
        *,
        verified_manifest: Mapping[str, Any],
        source_target: Any,
        sink_target: Any,
        state_target: Any,
        parent_context: Any,
        attempt: Any,
        write: Any,
        plan_sha256: str,
        journal: CompositionTransferPreplanStore,
        require_target: Callable[[Any], bytes],
    ) -> None:
        require_digest(plan_sha256)
        attempt.__post_init__()
        write.__post_init__()
        if plan_sha256 != attempt.plan_sha256 or not callable(require_target):
            raise CompositionAdmissionError("transfer_preplan_context")
        self._manifest = canonical_json_bytes(dict(verified_manifest))
        expected_write = transfer_relation_write(
            project_path=attempt.constituent_id,
            workflow_id=write.workflow_id,
            workload_id=attempt.workload_id,
            manifest=strict_json_object(self._manifest),
        )
        if write != expected_write:
            raise CompositionAdmissionError("transfer_preplan_write")
        self._connections = RuntimeResolvedConnections(
            strict=True,
            source=_detached_binding(source_target),
            sink=_detached_binding(sink_target),
            state=_detached_binding(state_target),
        )
        environment = getattr(parent_context, "environment", None)
        if type(environment) is not str or not environment.strip():
            raise CompositionAdmissionError("transfer_preplan_environment")
        self._context = _VerifiedEnvironment(environment)
        self._attempt, self._write = attempt, write
        self._journal, self._require_target = journal, require_target

    def verify_preplan(
        self,
        admission: MssqlTransactionAdmission,
        source: PostgresSource,
        prepared_boundary: PreparedPostgresSourceBoundary,
        submitted_mutation_sha256: bytes,
    ) -> RetainedTransferPreplanReference:
        """Reobserve, compare, fsync one original, then leave source RR active."""
        if (
            type(admission) is not MssqlTransactionAdmission
            or admission.operation is None
            or not isinstance(source, PostgresSource)
            or type(prepared_boundary) is not PreparedPostgresSourceBoundary
            or type(submitted_mutation_sha256) is not bytes
            or len(submitted_mutation_sha256) != 32
        ):
            raise CompositionAdmissionError("transfer_preplan_subject")
        operation = admission.operation
        stable_operation_document(operation)
        prepared_boundary.require_active(source.connector)
        manifest = strict_json_object(self._manifest)
        config = prepare_verified_transfer_config(manifest, connections=self._connections, context=self._context)
        config = bind_mssql_character_spool_preflight(config, source=source)
        config = bind_replay_safe_mssql_hook_graph(config)
        source_connection, sink_connection = self._connections.source, self._connections.sink
        if source_connection is None or sink_connection is None:
            raise CompositionAdmissionError("transfer_preplan_connections")
        verifier = PostgresSourceAuthorityVerifier.from_connection(source_connection)
        identity = verifier.verify_snapshot(
            connector=source.connector, snapshot_lease=prepared_boundary.snapshot_lease, load_config=config
        )
        expected, expected_digest = require_source_physical_identity_binding(config)
        validate_source_physical_identity(identity, config, expected=expected, expected_digest=expected_digest)
        if identity != prepared_boundary.source_identity:
            raise CompositionAdmissionError("transfer_preplan_source_identity")
        # This separately built config contains no worker prepared-boundary
        # option, so the public method performs a real same-session catalog read.
        projection = source.fetch_schema_projection(config)
        if type(projection) is not PostgresFetchedSchema or projection != prepared_boundary.schema_projection:
            raise CompositionAdmissionError("transfer_preplan_source_projection")
        sink = ResolvedEndpointFactory.create_sink(sink_connection, None, autocommit=False)
        try:
            sink.connector.execute_query(
                "SET XACT_ABORT ON; SET LOCK_TIMEOUT 10000; SET TRANSACTION ISOLATION LEVEL SERIALIZABLE;"
            )
            observed = self._observation(sink)
            physical = resolve_mssql_physical_target_identity(
                sink.connector,
                session=MssqlSessionIdentity.read(sink.connector),
                database=config.target_database,
                schema=config.target_schema,
                table=config.target_table,
            )
            request = operation.attempt.request
            coordinates = (self._write.database, self._write.schema, self._write.relation)
            if (
                physical.digest != request.target_identity
                or (config.target_database, config.target_schema, config.target_table) != coordinates
                or (request.target_database, request.target_schema, request.target_table) != coordinates
                or request.strategy != "full_refresh"
            ):
                raise CompositionAdmissionError("transfer_preplan_target_identity")
            config = prepare_portable_scope_binding(config, source=source, sink=sink)
            route = invocation_route_fingerprint(config, target_identity=physical.digest, source_identity=identity)
            if route != request.route_fingerprint:
                raise CompositionAdmissionError("transfer_preplan_route")
            independent = MssqlSchemaPreplanner().plan(config, source=source, sink=sink, admission=admission)
            if independent.target_mutation_plan.digest != submitted_mutation_sha256:
                raise CompositionAdmissionError("transfer_preplan_mutation")
            prepared_boundary.require_active(source.connector)
            if source.fetch_schema_projection(config) != projection or self._observation(sink) != observed:
                raise CompositionAdmissionError("transfer_preplan_observation_changed")
            document = canonical_json_bytes(
                {
                    "schema": "dpone.composition-transfer-preplan.v1",
                    "attempt_original": strict_json_object(encode_attempt_identity(self._attempt)),
                    "operation_original": strict_json_object(stable_operation_document(operation)),
                    "write": asdict(self._write),
                    "plan_sha256": self._attempt.plan_sha256,
                    "manifest_sha256": "sha256:" + sha256(self._manifest).hexdigest(),
                    "preplan": strict_json_object(encode_mssql_schema_preplan(independent)),
                    "source_identity": identity.to_dict(),
                    "source_projection": asdict(projection),
                    "source_provenance_sha256": projection_provenance(projection),
                    "target_identity": {**asdict(physical), "binding_id": str(physical.binding_id)},
                    "route_fingerprint": route.hex(),
                    "connection_observation": strict_json_object(observed),
                }
            )
            return self._journal.capture(self._attempt, document)
        finally:
            try:
                sink.connector.rollback()
            finally:
                sink.connector.close()

    def _observation(self, sink: Any) -> bytes:
        if sink.connector.connection.autocommit is not False:
            raise CompositionAdmissionError("transfer_preplan_transaction")
        document = self._require_target(sink.connector.connection)
        if type(document) is not bytes or canonical_json_bytes(strict_json_object(document)) != document:
            raise CompositionAdmissionError("transfer_preplan_connection")
        return document


def verify_retained_transfer_commit(
    reference: RetainedTransferPreplanReference, *, binding: Any, receipt: Any, payload: Any, connector: Any
) -> None:
    """Verify committed originals on the caller's existing read transaction.

    Reopening the registry and after-catalog does not reconstruct a plan. The
    caller owns transaction continuity, target service/pin observations and
    quiescence, and invokes this both before and after content reconciliation.
    No connection is created, committed or closed by this function.
    """
    expected = binding.require_preplan()
    if (
        type(reference) is not RetainedTransferPreplanReference
        or expected != "sha256:" + reference.document_sha256.hex()
    ):
        raise CompositionAdmissionError("transfer_preplan_binding")
    retained = decode_transfer_preplan(reference.document, reference.document_sha256, attempt=binding.attempt)
    body = retained.body
    mutation = retained.preplan.target_mutation_plan
    if (
        canonical_json_bytes(body["attempt_original"]) != encode_attempt_identity(binding.attempt)
        or canonical_json_bytes(body["operation_original"]) != stable_operation_document(binding.operation)
        or body["write"] != asdict(binding.write)
        or mutation.digest != binding.mutation_plan_sha256
    ):
        raise CompositionAdmissionError("transfer_preplan_binding")
    binding.require_receipt(receipt)
    if (
        receipt.target_before_sha256 != mutation.expected_before_sha256
        or receipt.target_after_sha256 != mutation.expected_after_sha256
        or payload.source_provenance_sha256 != body["source_provenance_sha256"]
    ):
        raise CompositionAdmissionError("transfer_preplan_receipt")
    write = binding.write
    assert_mssql_physical_target_identity(
        connector,
        database=write.database,
        schema=write.schema,
        table=write.relation,
        expected=binding.operation.attempt.request.target_identity,
    )
    assert_target_catalog_expectations(
        SimpleNamespace(connector=connector),
        SimpleNamespace(target_database=write.database, target_schema=write.schema, target_table=write.relation),
        mutation.expectations,
        boundary="after",
    )
