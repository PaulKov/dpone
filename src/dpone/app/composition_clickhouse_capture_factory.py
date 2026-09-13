"""Compose runtime capture from the verified source and protected SQL/PVC authority."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.adapters.composition_clickhouse_http import BoundedClickHouseHttp
from dpone.adapters.composition_clickhouse_visibility import ClickHouseCatalogVisibility
from dpone.adapters.composition_mssql_connection_identity import require_mssql_connection_identity
from dpone.adapters.composition_snapshot_capture_store import MssqlSnapshotCaptureStore, ProtectedSnapshotFiles
from dpone.app.composition_clickhouse_capture import CompositionClickHouseCapture
from dpone.app.composition_clickhouse_catalog import ClickHouseHttpSnapshotCatalog
from dpone.app.composition_clickhouse_source import MssqlClickHouseSourceReader
from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_identity import CompositionAdmissionError
from dpone.contracts.composition_snapshot_capture import SnapshotCaptureSubject
from dpone.contracts.composition_snapshot_materialization import (
    SNAPSHOT_MATERIALIZATION_RESPONSE_BYTES,
    snapshot_materialization_catalog_budget,
)
from dpone.contracts.mssql_database_authority import MssqlDatabaseAuthorityContractError, MssqlDatabaseAuthoritySet
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory


@dataclass(frozen=True)
class ClickHouseCaptureComponents:
    capture: CompositionClickHouseCapture
    catalog: ClickHouseHttpSnapshotCatalog
    source: MssqlClickHouseSourceReader


def build_clickhouse_capture_components(
    *,
    parent: Any,
    manifest: Mapping[str, Any],
    plan: Any,
    attempt: Any,
    target: Any,
    limits: Any,
    root: Path,
    endpoint: str,
    credentials: Any,
    ca_file: str | None,
) -> ClickHouseCaptureComponents:
    """Bind one exact attempt; no source read or target mutation during construction.

    The source verifier is intentionally SQL-free: capture journals call it while
    holding their control transaction. Current owner/epochs are reopened by the
    journal on that same transaction, rather than through a second connection.
    """
    control = parent["control"]
    source = manifest.get("source")
    table = source.get("table") if isinstance(source, Mapping) else None
    if not isinstance(source, Mapping) or not isinstance(table, Mapping):
        raise CompositionAdmissionError("snapshot_capture_source_binding")
    source_ref = source.get("connection_ref")
    if type(source_ref) is not str or not source_ref:
        raise CompositionAdmissionError("snapshot_capture_source_binding")
    resolved = parent["resolver"].resolve(source_ref)
    descriptor = resolved.descriptor
    if (
        descriptor is None
        or descriptor.connection_type != "mssql"
        or descriptor.properties.get("composition_service_id") != control.expected_service_id
    ):
        raise CompositionAdmissionError("snapshot_capture_source_service")
    database = table.get("database") or resolved.credentials.database
    try:
        authorities = MssqlDatabaseAuthoritySet.from_connection_properties(descriptor.properties, capability="source")
        source_pin = authorities.require(database, capability="source")
        control_pin = authorities.require(control.control_database, capability="source")
    except MssqlDatabaseAuthorityContractError:
        raise CompositionAdmissionError("snapshot_capture_source_authority") from None
    schema_name, table_name = table.get("schema"), table.get("name")
    if not isinstance(database, str) or not isinstance(schema_name, str) or not isinstance(table_name, str):
        raise CompositionAdmissionError("snapshot_capture_source_binding")
    source_table = (database, schema_name, table_name)
    source_binding = canonical_fingerprint(
        {
            "schema": "dpone.composition-snapshot-source-binding.v1",
            "runtime_context_sha256": parent["context"].runtime_context_sha256,
            "connection_ref": source_ref,
            "service_id": control.expected_service_id,
            "plan_sha256": plan.sources.subject_sha256,
        }
    )
    subject = SnapshotCaptureSubject(attempt, target, source_binding, source_table, limits)

    def verify_source(candidate: Any) -> SnapshotCaptureSubject:
        if candidate != attempt or candidate.plan_sha256 != plan.sources.subject_sha256:
            raise CompositionAdmissionError("snapshot_capture_source_binding")
        subject.__post_init__()
        return subject

    def require_source(connection: Any) -> bytes:
        return require_mssql_connection_identity(
            connection,
            pins=(source_pin, control_pin),
            control_database=control.control_database,
            control_schema=control.control_schema,
            service_id=control.expected_service_id,
        )

    reader = MssqlClickHouseSourceReader(
        open_connection=lambda: ResolvedConnectorFactory.create(resolved, autocommit=False).connection,
        table={"database": database, "schema": source_table[1], "name": source_table[2]},
        limits=limits,
        require_source=require_source,
    )
    http = BoundedClickHouseHttp(
        endpoint=endpoint,
        credentials=credentials,
        timeout_seconds=30.0,
        max_response_bytes=SNAPSHOT_MATERIALIZATION_RESPONSE_BYTES,
        ca_file=ca_file,
    )
    visibility = ClickHouseCatalogVisibility(http=http, service_id=target.service_id, username=credentials.username)
    catalog = ClickHouseHttpSnapshotCatalog(
        http,
        max_rows=limits.max_rows,
        max_content_bytes=snapshot_materialization_catalog_budget(
            max_source_bytes=limits.max_source_bytes, max_rows=limits.max_rows
        ),
        require_visibility=lambda _subject: visibility(),
    )
    store = MssqlSnapshotCaptureStore(
        control.connection_factory,
        expected_service_id=control.expected_service_id,
        source_verifier=verify_source,
        control_schema=control.control_schema,
    )
    return ClickHouseCaptureComponents(
        CompositionClickHouseCapture(
            store=store, files=ProtectedSnapshotFiles(root), source_reader=reader, catalog=catalog
        ),
        catalog,
        reader,
    )
