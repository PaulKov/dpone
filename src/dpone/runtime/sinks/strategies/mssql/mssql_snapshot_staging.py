"""Raw-wire validation and native staging for MSSQL key snapshots."""

from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.incremental_snapshot import DELTA_HASH_COLUMN, KEY_HASH_COLUMN
from dpone.runtime.sinks.strategies.mssql.mssql_decoded_staging import (
    MssqlDecodedStagingMaterializer,
    cleanup_staging_after_primary,
    derived_staging_table_name,
    mssql_native_phase,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_projection import (
    require_indexable_key_types,
    resolved_business_nullability,
)
from dpone.runtime.sinks.strategies.mssql.mssql_snapshot_reconciliation import (
    SnapshotReconciliationError,
)
from dpone.runtime.support.mssql_native_projection import (
    native_select_expression,
    resolve_native_column_types,
    validate_native_conversions,
)
from dpone.runtime.support.mssql_snapshot_projection import (
    MSSQL_TEXT_KEY_COLLATION,
    resolved_text_key_columns,
)


@dataclass(frozen=True, slots=True)
class NormalizedSnapshotStaging:
    """Native delta/key tables ready for typed validation and target DML."""

    delta: StagingTableArtifact
    keys: StagingTableArtifact

    def cleanup(self) -> None:
        self.keys.cleanup()
        self.delta.cleanup()


class MssqlSnapshotStagingNormalizer:
    """Validate immutable raw BCP evidence, then decode into native tables."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy
        self._connector = strategy.connector
        self._staging = strategy.staging_manager

    def normalize(
        self,
        load_config: Any,
        envelope: Any,
        raw_delta: StagingTableArtifact,
        raw_keys: StagingTableArtifact,
    ) -> NormalizedSnapshotStaging:
        self._require_receipts(envelope, raw_delta, raw_keys)
        if raw_delta.typed_file_ingestion or raw_keys.typed_file_ingestion:
            return self._require_typed_native(load_config, envelope, raw_delta, raw_keys)
        self._validate_raw_digest(raw_delta, envelope.delta_schema, DELTA_HASH_COLUMN)
        self._validate_raw_digest(raw_keys, envelope.key_schema, KEY_HASH_COLUMN)
        require_indexable_key_types(
            envelope.key_schema,
            load_config,
            envelope.key_receipt.key_columns,
        )
        nullability = resolved_business_nullability(
            load_config,
            envelope.delta_schema,
            envelope.key_receipt.key_columns,
        )
        required_delta = {column for column, nullable in nullability.items() if not nullable}
        text_keys = resolved_text_key_columns(
            envelope.key_schema,
            load_config,
            envelope.key_receipt.key_columns,
        )
        delta = self._normalize_one(
            load_config,
            raw_delta,
            envelope.delta_schema,
            table=derived_staging_table_name(raw_delta.table, "native"),
            not_null={DELTA_HASH_COLUMN, *required_delta},
            text_key_columns=text_keys,
        )
        try:
            keys = self._normalize_one(
                load_config,
                raw_keys,
                envelope.key_schema,
                table=derived_staging_table_name(raw_keys.table, "native"),
                not_null={KEY_HASH_COLUMN, *envelope.key_receipt.key_columns},
                text_key_columns=text_keys,
            )
        except BaseException:
            cleanup_staging_after_primary(self._strategy, delta, phase="native_cleanup")
            raise
        return NormalizedSnapshotStaging(delta=delta, keys=keys)

    def _require_typed_native(
        self,
        load_config: Any,
        envelope: Any,
        delta: StagingTableArtifact,
        keys: StagingTableArtifact,
    ) -> NormalizedSnapshotStaging:
        if not delta.typed_file_ingestion or not keys.typed_file_ingestion:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.typed_staging_pair_required")
        if (
            delta.typed_transport != "mssql.bcp.length_prefixed_utf8.v1"
            or keys.typed_transport != delta.typed_transport
        ):
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.typed_transport_receipt_required")
        for staging in (delta, keys):
            evidence = staging.consumed_payload_evidence
            if evidence is None:
                raise SnapshotReconciliationError("mssql_snapshot_reconciliation.typed_payload_evidence_required")
            evidence.require_complete(native=True)
        require_indexable_key_types(
            envelope.key_schema,
            load_config,
            envelope.key_receipt.key_columns,
        )
        expected_delta = resolve_native_column_types(
            load_config,
            envelope.delta_schema,
            fixed_types={DELTA_HASH_COLUMN: "char(64)"},
        )
        expected_keys = resolve_native_column_types(
            load_config,
            envelope.key_schema,
            fixed_types={KEY_HASH_COLUMN: "char(64)"},
        )
        if delta.column_types != expected_delta or keys.column_types != expected_keys:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.typed_staging_shape_mismatch")
        if delta.bulk_text_codec is not None or keys.bulk_text_codec is not None:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.typed_staging_codec_leak")
        return NormalizedSnapshotStaging(delta=delta, keys=keys)

    @staticmethod
    def _require_receipts(envelope: Any, delta: StagingTableArtifact, keys: StagingTableArtifact) -> None:
        delta_receipt = envelope.delta_receipt
        key_receipt = envelope.key_receipt
        if not delta_receipt.complete or delta.row_count != delta_receipt.row_count:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.incomplete_delta_snapshot")
        if not key_receipt.complete or keys.row_count != key_receipt.row_count:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.incomplete_key_snapshot")

    def _validate_raw_digest(
        self,
        staging: StagingTableArtifact,
        schema: Any,
        digest_column: str,
    ) -> None:
        columns = [str(column) for column, _dtype in schema if str(column) != digest_column]
        values = [
            f"COALESCE(CONVERT(nvarchar(max), r.{self._connector.quote_identifier(column)}), N'')" for column in columns
        ]
        if not values:
            encoded_row = "N''"
        elif len(values) == 1:
            encoded_row = values[0]
        else:
            encoded_row = f"CONCAT({', NCHAR(9), '.join(values)})"
        digest = self._connector.quote_identifier(digest_column)
        expected = f"LOWER(CONVERT(char(64), HASHBYTES('SHA2_256', CONVERT(varbinary(max), {encoded_row})), 2))"
        rows = self._connector.get_records(
            f"SELECT TOP (1) 1 AS invalid_digest FROM {self._strategy._staging_name(staging)} AS r "
            f"WHERE r.{digest} IS NULL OR LEN(r.{digest}) <> 64 "
            f"OR LOWER(r.{digest}) LIKE '%[^0-9a-f]%' OR LOWER(r.{digest}) <> {expected}",
            as_dict=True,
        )
        if rows:
            code = "delta_checksum_mismatch" if digest_column == DELTA_HASH_COLUMN else "key_checksum_mismatch"
            raise SnapshotReconciliationError(f"mssql_snapshot_reconciliation.{code}")

    def _normalize_one(
        self,
        load_config: Any,
        raw: StagingTableArtifact,
        schema: Any,
        *,
        table: str,
        not_null: set[str],
        text_key_columns: tuple[str, ...],
    ) -> StagingTableArtifact:
        types = resolve_native_column_types(
            load_config,
            schema,
            fixed_types={DELTA_HASH_COLUMN: "char(64)", KEY_HASH_COLUMN: "char(64)"},
        )
        native: StagingTableArtifact | None = None
        try:
            with MssqlDecodedStagingMaterializer(self._strategy).materialize(
                load_config,
                raw,
                error_prefix="mssql_snapshot_reconciliation.normalized_staging_",
            ) as decoded:
                with mssql_native_phase(self._strategy, "conversion_validate", rows=raw.row_count):
                    with self._staging.database_authority_scope(decoded):
                        self._validate_conversions(decoded, schema, types)
                with mssql_native_phase(self._strategy, "native_project", rows=raw.row_count):
                    native = self._materialize_native(
                        load_config,
                        raw,
                        decoded,
                        schema,
                        types,
                        table=table,
                        not_null=not_null,
                        text_key_columns=text_key_columns,
                    )
        except BaseException:
            if native is not None:
                cleanup_staging_after_primary(self._strategy, native, phase="native_cleanup")
            raise
        if native is None:
            raise SnapshotReconciliationError("mssql_snapshot_reconciliation.normalized_staging_unavailable")
        return native

    def _materialize_native(
        self,
        load_config: Any,
        evidence_raw: StagingTableArtifact,
        decoded: StagingTableArtifact,
        schema: Any,
        types: dict[str, str],
        *,
        table: str,
        not_null: set[str],
        text_key_columns: tuple[str, ...],
    ) -> StagingTableArtifact:
        """Project one validated decoded snapshot into native staging."""

        options = dict(getattr(load_config, "options", {}) or {})
        options.update(
            {
                "__dpone_snapshot_native_staging": True,
                "__dpone_snapshot_native_column_types": types,
                "__dpone_snapshot_native_not_null_columns": sorted(not_null),
                "__dpone_snapshot_native_collations": {column: MSSQL_TEXT_KEY_COLLATION for column in text_key_columns},
            }
        )
        config = replace(load_config, staging_table=table, options=options)
        native = self._staging.create(config, schema)
        try:
            columns = ", ".join(self._connector.quote_identifier(str(column)) for column, _dtype in schema)
            values = ", ".join(
                self._native_select_expression(decoded, str(column), types[str(column)]) for column, _dtype in schema
            )
            with self._staging.database_authority_scope(native):
                self._connector.execute_query(
                    "SET ANSI_WARNINGS ON; "
                    f"INSERT INTO {self._strategy._staging_name(native)} WITH (TABLOCK) ({columns}) "
                    f"SELECT {values} FROM {self._strategy._staging_name(decoded)} AS r"
                )
                native.row_count = self._count(self._strategy._staging_name(native))
            if native.row_count != evidence_raw.row_count:
                raise SnapshotReconciliationError("mssql_snapshot_reconciliation.normalized_staging_count_mismatch")
            native.bulk_text_codec = None
            return native
        except BaseException:
            cleanup_staging_after_primary(self._strategy, native, phase="native_cleanup")
            raise

    def _validate_conversions(self, raw: StagingTableArtifact, schema: Any, types: dict[str, str]) -> None:
        validate_native_conversions(
            self._strategy,
            raw,
            schema,
            types,
            error_prefix="mssql_snapshot_reconciliation.normalized_staging_",
            validate_internal_hashes=False,
        )

    def _native_select_expression(self, raw: StagingTableArtifact, column: str, target: str) -> str:
        return native_select_expression(self._strategy, raw, column, target)

    def _count(self, table: str) -> int:
        rows = self._connector.get_records(f"SELECT COUNT_BIG(*) AS row_count FROM {table}", as_dict=True)
        if not rows:
            return 0
        return int(rows[0]["row_count"] if isinstance(rows[0], dict) else rows[0][0])


__all__ = ["MssqlSnapshotStagingNormalizer", "NormalizedSnapshotStaging"]
