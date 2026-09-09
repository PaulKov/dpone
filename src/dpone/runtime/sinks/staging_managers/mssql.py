"""SQL Server staging manager with bcp-backed materialization."""

from __future__ import annotations

import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.contracts.mssql_object_name import MSSQLObjectName, mssql_ensure_schema_statement
from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.file_artifacts import FileExportArtifact, PartitionedFileExportArtifact
from dpone.runtime.incremental_snapshot import (
    DELTA_HASH_COLUMN,
    KEY_HASH_COLUMN,
    KeySnapshotReconciliationPolicy,
)
from dpone.runtime.sink_logging import ETLLogger, etl_logger
from dpone.runtime.sinks.staging_managers.mssql_spool_storage import MssqlCharacterSpool
from dpone.runtime.sinks.staging_managers.mssql_staging_evidence import (
    MssqlStagingEvidenceAuthority,
    initialize_runtime_provenance,
    source_provenance_sha256,
)
from dpone.runtime.sinks.staging_managers.mssql_staging_support import (
    MssqlStagingDatabaseAuthorityMixin,
    internal_column_collations,
    internal_direct_native_staging,
    internal_not_null_columns,
    internal_physical_not_null_columns,
    internal_typed_file_ingestion,
    internal_wire_schema,
)
from dpone.runtime.sinks.staging_managers.mssql_typed_file import MssqlTypedFileIngestor
from dpone.runtime.staging import StagingManager
from dpone.runtime.storage_policy import RuntimeStoragePolicy, StoragePreflightService
from dpone.runtime.support.mssql_bulk import is_mssql_character_bulk_unsafe_type
from dpone.runtime.support.mssql_hex_binary import plan_mssql_character_staging_types

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig
    from dpone.ports.mssql_connector import MSSQLConnectorPort


class MSSQLStagingManager(MssqlStagingDatabaseAuthorityMixin, StagingManager):
    """Creates and fills SQL Server staging tables."""

    def __init__(
        self,
        connector: MSSQLConnectorPort,
        logger: ETLLogger | None = None,
        *,
        database_authority: Any | None = None,
        storage_policy: RuntimeStoragePolicy | None = None,
        storage_preflight_service: StoragePreflightService | None = None,
    ):
        self.connector = connector
        self.logger = logger or etl_logger
        self._database_authority = database_authority
        self._authority_leases: dict[int, Any] = {}
        self._evidence_authority = MssqlStagingEvidenceAuthority(connector)
        self._typed_file_ingestor = MssqlTypedFileIngestor(connector, self.logger)
        self.storage_policy = storage_policy or RuntimeStoragePolicy.from_options(None)
        self._character_spool = MssqlCharacterSpool(
            self.storage_policy,
            storage_preflight_service or StoragePreflightService(),
        )

    def preflight_storage(self) -> None:
        """Pin writable spool storage before the source can be scanned."""

        self._character_spool.preflight()

    def create(self, load_config: LoadConfig, schema: Sequence[tuple[str, str]]) -> StagingTableArtifact:
        authority_lease = self._acquire_database_authority_lease()
        staging_schema = load_config.staging_schema or load_config.target_schema
        table = load_config.staging_table or f"stg_{load_config.target_table}_{uuid.uuid4().hex[:8]}"
        staging_database = None if "." in str(staging_schema) else load_config.staging_database
        staging_name = MSSQLObjectName.from_parts(
            schema=staging_schema,
            table=table,
            database=staging_database,
        )
        reconciliation = KeySnapshotReconciliationPolicy.from_runtime(
            getattr(load_config, "options", None),
            legacy_enabled=bool(getattr(load_config, "reconciliation", False)),
        )
        try:
            self._assert_database_authority_lease(authority_lease)
            if reconciliation.key_snapshot_enabled:
                self._require_existing_schema(staging_name)
            else:
                self.connector.execute_query(*mssql_ensure_schema_statement(staging_name.schema_label))
            column_types, hex_binary_columns, target_column_types = self._column_plan(
                load_config,
                schema,
                reconciliation=reconciliation,
            )
            not_null = internal_not_null_columns(load_config)
            physical_not_null = internal_physical_not_null_columns(load_config, column_types)
            collations = internal_column_collations(load_config)
            columns_sql = ", ".join(
                f"{self.connector.quote_identifier(column)} {column_types[column]} "
                f"{f'COLLATE {collations[column]} ' if column in collations else ''}"
                f"{'NOT NULL' if column in physical_not_null else 'NULL'}"
                for column, _ in schema
            )
            self.connector.execute_query(f"CREATE TABLE {staging_name.quoted()} ({columns_sql})")
            self._assert_database_authority_lease(authority_lease)
            wire_schema = internal_wire_schema(load_config) or tuple(
                (str(column), str(dtype)) for column, dtype in schema
            )
            direct_native = internal_direct_native_staging(load_config)
            artifact = StagingTableArtifact(
                database=staging_name.database,
                schema=staging_name.schema,
                table=table,
                columns=[column for column, _ in schema],
                staging_manager=self,
                column_types=column_types,
                target_column_types=target_column_types,
                target_column_nullability={column: column not in not_null for column, _dtype in schema},
                target_column_collations=dict(collations),
                hex_binary_columns=hex_binary_columns,
                allow_unsafe_raw_bulk_file=bool(load_config.options.get("allow_unsafe_raw_mssql_bulk_files", False)),
                bulk_options=BulkOptionsResolver.resolve(
                    load_config.options,
                    default_batch_size=load_config.batch_size,
                ),
                wire_schema=wire_schema,
                source_provenance_sha256=source_provenance_sha256(load_config, wire_schema),
                typed_file_ingestion=internal_typed_file_ingestion(load_config),
                typed_file_row_hash_validation=not direct_native,
                typed_file_deferred_native_evidence=direct_native,
                direct_native_staging=direct_native,
            )
            if authority_lease is not None:
                self._authority_leases[id(artifact)] = authority_lease
            return artifact
        except BaseException:
            self._close_database_authority_lease(authority_lease)
            raise

    @staticmethod
    def _column_plan(
        load_config: LoadConfig,
        schema: Sequence[tuple[str, str]],
        *,
        reconciliation: KeySnapshotReconciliationPolicy,
    ) -> tuple[dict[str, str], frozenset[str], dict[str, str]]:
        options = getattr(load_config, "options", {}) or {}
        native = (
            options.get("__dpone_snapshot_native_staging") is True
            or options.get("__dpone_mssql_native_staging") is True
        )
        if reconciliation.key_snapshot_enabled and not native:
            raw_types = {
                column: "char(64)" if column in {KEY_HASH_COLUMN, DELTA_HASH_COLUMN} else "nvarchar(max)"
                for column, _dtype in schema
            }
            return raw_types, frozenset(), dict(raw_types)
        if native:
            configured = options.get("__dpone_mssql_native_column_types")
            if not isinstance(configured, Mapping):
                configured = options.get("__dpone_snapshot_native_column_types")
            if not isinstance(configured, Mapping):
                raise ValueError("mssql_native_staging_types_missing")
            column_types = {column: str(configured[column]) for column, _dtype in schema}
            return column_types, frozenset(), dict(column_types)
        return plan_mssql_character_staging_types(schema)

    def _require_existing_schema(self, name: MSSQLObjectName) -> None:
        prefix = f"{self.connector.quote_identifier(name.database)}." if name.database else ""
        rows = self.connector.get_records(
            f"SELECT CASE WHEN EXISTS (SELECT 1 FROM {prefix}sys.schemas WHERE name = ?) "
            "THEN 1 ELSE 0 END AS schema_exists",
            (name.schema,),
            as_dict=True,
        )
        if not rows or not int(rows[0]["schema_exists"]):
            raise RuntimeError("mssql_key_snapshot_staging_schema_missing")

    def insert_rows(self, artifact: StagingTableArtifact, rows: Iterable[Mapping[str, object]]) -> int:
        return self._character_spool.import_rows(
            artifact,
            rows,
            connector=self.connector,
            importer=self._verified_bcp_import,
        )

    def insert_streaming_rows(
        self,
        artifact: StagingTableArtifact,
        rows: Iterable[Mapping[str, object]],
    ) -> int:
        """Spool one bounded-memory logical stream and import it with one BCP process."""

        return self.insert_rows(artifact, rows)

    def insert_from_query(
        self,
        artifact: StagingTableArtifact,
        query: str,
        schema: Sequence[tuple[str, str]],
        params: Sequence[Any] | None = None,
    ) -> int:
        with self.database_authority_scope(artifact):
            columns = ", ".join(self.connector.quote_identifier(column) for column, _ in schema)
            return self.connector.execute_query(
                f"INSERT INTO {self._qualified_name(artifact.schema, artifact.table, database=artifact.database)} "
                f"({columns}) {query}",
                params,
            )

    def load_from_file(self, artifact: StagingTableArtifact, file_artifact: FileExportArtifact) -> int:
        if artifact.typed_file_ingestion:
            with self.database_authority_scope(artifact):
                return self._typed_file_ingestor.ingest(artifact, file_artifact)
        # Bind the current format/columns/compression/codec interpretation
        # before any branch can act on those mutable public attributes.
        file_artifact.require_integrity_receipt()
        if file_artifact.compressed:
            raise ValueError(
                "MSSQL bcp cannot load gzip artifacts directly; set compress_export=false for MSSQL sinks."
            )
        if file_artifact.format == "binary":
            raise ValueError("MSSQL sink cannot load PostgreSQL binary COPY artifacts; set export_format=csv.")
        if file_artifact.format == "mssql-native":
            bulk = artifact.bulk_options or BulkOptionsResolver.resolve({})
            options = bulk.bcp.to_bcp_options(
                bcp_path=getattr(self.connector, "bcp_path", "bcp"),
                file_format="native",
                trust_server_certificate=self.connector.trust_server_certificate == "yes",
            )
            options = self._with_partition_error_file(options, file_artifact)
            return self._verified_bcp_import(artifact, file_artifact, options=options)
        self._validate_character_bulk_file_safety(artifact, file_artifact)
        bulk = artifact.bulk_options or BulkOptionsResolver.resolve({})
        options = bulk.bcp.to_bcp_options(
            bcp_path=getattr(self.connector, "bcp_path", "bcp"),
            field_terminator="\t" if file_artifact.format == "mssql-delimited" else ",",
            row_terminator="\n",
            trust_server_certificate=self.connector.trust_server_certificate == "yes",
        )
        options = self._with_partition_error_file(options, file_artifact)
        if file_artifact.bulk_text_codec is not None:
            artifact.bulk_text_codec = file_artifact.bulk_text_codec
        return self._verified_bcp_import(artifact, file_artifact, options=options)

    def load_file_into_table(self, schema: str, table: str, file_artifact: FileExportArtifact) -> int:
        raise NotImplementedError("Use load_from_file so MSSQL bulk safety can inspect staging metadata.")

    def _validate_character_bulk_file_safety(
        self,
        artifact: StagingTableArtifact,
        file_artifact: FileExportArtifact,
    ) -> None:
        unsafe_types = [
            f"{column} ({dtype})"
            for column, dtype in artifact.column_types.items()
            if is_mssql_character_bulk_unsafe_type(dtype)
        ]
        if unsafe_types and not artifact.allow_unsafe_raw_bulk_file:
            raise ValueError(
                "MSSQL character bcp is unsafe for these target types: "
                + ", ".join(unsafe_types)
                + ". Use an MSSQL native bcp artifact or set allow_unsafe_raw_mssql_bulk_files=true explicitly."
            )
        text_columns = [
            column
            for column, dtype in artifact.column_types.items()
            if any(token in dtype.lower() for token in ("char", "text", "json", "xml", "string"))
        ]
        if (
            text_columns
            and file_artifact.format in {"csv", "mssql-delimited"}
            and file_artifact.bulk_text_codec is None
            and not artifact.allow_unsafe_raw_bulk_file
        ):
            raise ValueError(
                "MSSQL raw delimited files with text columns require dpone bulk_text_codec metadata. "
                "Set allow_unsafe_raw_mssql_bulk_files=true only for trusted files that cannot contain NULL/empty-string "
                "or delimiter ambiguities."
            )

    def _with_partition_error_file(self, options, file_artifact: FileExportArtifact):
        if not options.error_file:
            return options
        partition_bounds = getattr(file_artifact, "partition_bounds", None)
        transfer_partition_id = getattr(file_artifact, "transfer_partition_id", None)
        if not isinstance(partition_bounds, Mapping) and not transfer_partition_id:
            return options
        error_path = Path(options.error_file)
        partition_index = partition_bounds.get("index") if isinstance(partition_bounds, Mapping) else None
        partition_token = f"p{partition_index}" if partition_index is not None else "partition"
        identity_token = str(transfer_partition_id or "unknown")[:8]
        suffix = error_path.suffix or ".err"
        return replace(
            options,
            error_file=str(error_path.with_name(f"{error_path.stem}_{partition_token}_{identity_token}{suffix}")),
        )

    def load_partitioned_files_into_table(
        self,
        schema: str,
        table: str,
        artifact: PartitionedFileExportArtifact,
    ) -> int:
        staging = StagingTableArtifact(
            schema=schema,
            table=table,
            columns=artifact.columns,
            staging_manager=self,
            allow_unsafe_raw_bulk_file=False,
            wire_schema=tuple((str(column), "runtime_untyped") for column in artifact.columns),
        )
        initialize_runtime_provenance(staging)
        return artifact.load_with(lambda file_artifact: self.load_from_file(staging, file_artifact))

    def load_from_gcs_artifact(self, artifact: StagingTableArtifact, gcs_artifact) -> int:
        del artifact, gcs_artifact
        raise NotImplementedError("MSSQL sink does not load GCS artifacts directly; use file or streaming artifacts.")

    def drop(self, artifact: StagingTableArtifact) -> None:
        lease = self._authority_leases.get(id(artifact))
        try:
            with self.database_authority_scope(artifact):
                self.connector.execute_query(
                    f"DROP TABLE IF EXISTS "
                    f"{self._qualified_name(artifact.schema, artifact.table, database=artifact.database)}"
                )
        finally:
            self._authority_leases.pop(id(artifact), None)
            self._close_database_authority_lease(lease)

    def count_rows(self, schema: str, table: str) -> int:
        with self.database_authority_scope():
            rows = self.connector.get_records(
                f"SELECT COUNT_BIG(*) FROM {self.connector.qualified_name(schema, table)}"
            )
            return int(rows[0][0]) if rows else 0

    def _verified_bcp_import(
        self,
        artifact: StagingTableArtifact,
        file_artifact: FileExportArtifact,
        *,
        options: Any,
    ) -> int:
        """Verify immutable source, BCP receipt, rejects, and actual stage delta."""

        with self.database_authority_scope(artifact):
            return self._evidence_authority.verified_import(
                artifact,
                file_artifact,
                options=options,
            )

    def finalize_native_evidence(
        self,
        raw: StagingTableArtifact,
        native: StagingTableArtifact,
        *,
        columns: Sequence[Mapping[str, object]],
    ) -> None:
        """Attach complete cryptographic evidence to a native staging table."""

        with self.database_authority_scope(native):
            self._evidence_authority.finalize_native(raw, native, columns=columns)


__all__ = ["MSSQLStagingManager"]
