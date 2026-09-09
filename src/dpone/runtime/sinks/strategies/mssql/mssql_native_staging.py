"""Native staging boundary for generic SQL Server load strategies."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from typing import Any

from dpone.runtime.artifact_models import StagingTableArtifact
from dpone.runtime.sinks.strategies.mssql.mssql_decoded_staging import (
    MssqlDecodedStagingMaterializer,
    cleanup_staging_after_primary,
    derived_staging_table_name,
    mssql_native_phase,
)
from dpone.runtime.sinks.strategies.mssql.mssql_native_schema import (
    ResolvedMssqlNativeSchema,
    mssql_equality_keys,
    mssql_unique_keys,
    resolve_mssql_native_schema,
)
from dpone.runtime.support.mssql_native_canonical import row_hash_expression
from dpone.runtime.support.mssql_native_projection import (
    decoded_staging_expression,
    native_value_expression,
    validate_native_conversions,
)
from dpone.runtime.support.mssql_snapshot_projection import is_text_key_type


class MssqlNativeStagingNormalizer:
    """Decode an untyped character-wire table into a validated native table."""

    def __init__(self, strategy: Any) -> None:
        self._strategy = strategy
        self._connector = strategy.connector
        self._staging = strategy.staging_manager

    def resolve_schema(
        self,
        load_config: Any,
        schema: Sequence[tuple[str, str]],
        *,
        relation_schema: Sequence[tuple[str, str]] | None,
        relation_metadata: Any | None,
        relation_dialect: Any | None,
        target_projection: Any | None,
    ) -> ResolvedMssqlNativeSchema:
        """Resolve provenance, types and target shape before materialization."""

        del relation_metadata  # provenance is already frozen in target_projection
        return resolve_mssql_native_schema(
            load_config,
            schema,
            relation_schema=relation_schema,
            relation_dialect=relation_dialect,
            target_projection=target_projection,
        )

    def normalize(
        self,
        load_config: Any,
        raw: StagingTableArtifact,
        schema: Sequence[tuple[str, str]],
        resolved: ResolvedMssqlNativeSchema,
        *,
        lineage: Any,
    ) -> StagingTableArtifact:
        """Validate every value and materialize the handler-facing native table."""

        if raw.direct_native_staging:
            return self._finalize_direct_native(
                load_config,
                raw,
                schema,
                resolved,
                lineage=lineage,
            )
        native: StagingTableArtifact | None = None
        try:
            with MssqlDecodedStagingMaterializer(self._strategy).materialize(
                load_config,
                raw,
                error_prefix="mssql_native_projection",
            ) as decoded:
                with mssql_native_phase(self._strategy, "conversion_validate", rows=raw.row_count):
                    with self._staging.database_authority_scope(decoded):
                        validate_native_conversions(
                            self._strategy,
                            decoded,
                            [(name, resolved.source_types[name]) for name, _dtype in schema],
                            resolved.conversion_types,
                            error_prefix="mssql_native_projection",
                        )
                with mssql_native_phase(self._strategy, "native_project", rows=raw.row_count):
                    native = self._materialize_native(
                        load_config,
                        raw,
                        decoded,
                        resolved,
                        lineage=lineage,
                    )
        except BaseException:
            if native is not None:
                cleanup_staging_after_primary(self._strategy, native, phase="native_cleanup")
            raise
        if native is None:
            _raise("mssql_native_projection.native_staging_unavailable")
        return native

    def _materialize_native(
        self,
        load_config: Any,
        evidence_raw: StagingTableArtifact,
        decoded: StagingTableArtifact,
        resolved: ResolvedMssqlNativeSchema,
        *,
        lineage: Any,
    ) -> StagingTableArtifact:
        """Project one validated decoded stage into the handler-facing table."""

        keys = mssql_unique_keys(load_config)
        equality_keys = mssql_equality_keys(load_config, keys)
        target_to_wire = {target: wire for wire, target in resolved.wire_to_target.items()}
        with self._staging.database_authority_scope(decoded):
            self._validate_required_keys(decoded, [target_to_wire[key] for key in keys])
        options = dict(getattr(load_config, "options", {}) or {})
        options.update(
            {
                "__dpone_mssql_native_staging": True,
                "__dpone_mssql_native_column_types": resolved.types,
                "__dpone_mssql_native_not_null_columns": [
                    name for name, nullable in resolved.nullability.items() if not nullable
                ],
                "__dpone_mssql_native_collations": resolved.collations,
            }
        )
        config = replace(
            load_config,
            staging_table=derived_staging_table_name(evidence_raw.table, "native"),
            options=options,
        )
        native = self._staging.create(config, resolved.target_schema)
        try:
            target_to_wire = {target: wire for wire, target in resolved.wire_to_target.items()}
            generated = {column.name: column for column in resolved.generated_columns}
            source_values = {
                target: native_value_expression(
                    self._strategy,
                    decoded,
                    target_to_wire[target],
                    resolved.types[target],
                )
                for target in resolved.ordered_target_names
                if target not in generated
            }
            business_schema = tuple(
                (column, resolved.types[column])
                for column in resolved.ordered_target_names
                if not column.casefold().startswith("__dpone__")
            )
            lineage_values = (
                lineage.expressions(
                    business_schema=business_schema,
                    wire_to_target=resolved.wire_to_target,
                    value_expression=source_values.__getitem__,
                )
                if lineage.columns
                else {}
            )
            row_hash_sql = row_hash_expression(business_schema, source_values.__getitem__)
            columns = ", ".join(self._connector.quote_identifier(target) for target in resolved.ordered_target_names)
            values = ", ".join(
                f"{_native_projection_value(target, generated, source_values, lineage_values, row_hash_sql)} "
                f"AS {self._connector.quote_identifier(target)}"
                for target in resolved.ordered_target_names
            )
            with self._staging.database_authority_scope(native):
                self._connector.execute_query(
                    "SET ANSI_WARNINGS ON; "
                    f"INSERT INTO {self._strategy._staging_name(native)} WITH (TABLOCK) ({columns}) "
                    f"SELECT {values} FROM {self._strategy._staging_name(decoded)} AS r"
                )
            # INSERT ... SELECT has no filter and the freshly-created table has
            # no triggers.  SQL Server applies the statement atomically, so a
            # successful statement materializes exactly the immutable raw
            # receipt count; any conversion error aborts the statement.
            native.bulk_text_codec = None
            with self._staging.database_authority_scope(native):
                self._validate_key_sql_semantics(native, keys, equality_keys)
            if (
                isinstance(evidence_raw.row_count, bool)
                or not isinstance(evidence_raw.row_count, int)
                or evidence_raw.row_count < 0
            ):
                _raise("mssql_native_projection.raw_row_count_unavailable")
            native.row_count = evidence_raw.row_count
            self._staging.finalize_native_evidence(
                evidence_raw,
                native,
                columns=self._native_evidence_columns(resolved, lineage),
            )
            return native
        except BaseException:
            cleanup_staging_after_primary(self._strategy, native, phase="native_cleanup")
            raise

    def _finalize_direct_native(
        self,
        load_config: Any,
        native: StagingTableArtifact,
        schema: Sequence[tuple[str, str]],
        resolved: ResolvedMssqlNativeSchema,
        *,
        lineage: Any,
    ) -> StagingTableArtifact:
        """Finalize a one-table business-prefix BCP materialization."""

        wire_names = tuple(str(name) for name, _dtype in schema)
        if (
            not native.typed_file_ingestion
            or not native.typed_file_deferred_native_evidence
            or tuple(native.columns) != resolved.ordered_target_names
            or tuple(native.columns[: len(wire_names)]) != wire_names
            or native.column_types != resolved.types
            or native.target_column_types != resolved.types
            or native.bulk_text_codec is not None
        ):
            _raise("mssql_native_projection.direct_staging_contract_invalid")
        keys = mssql_unique_keys(load_config)
        equality_keys = mssql_equality_keys(load_config, keys)
        self._validate_required_keys(native, keys)
        self._project_authoritative_metadata(native, resolved, lineage)
        self._validate_key_sql_semantics(native, keys, equality_keys)
        actual_rows = self._count_rows(native)
        if actual_rows != native.row_count:
            _raise("mssql_native_projection.direct_row_count_mismatch")
        native.row_count = actual_rows
        self._staging.finalize_native_evidence(
            native,
            native,
            columns=self._native_evidence_columns(resolved, lineage),
        )
        return native

    def _project_authoritative_metadata(
        self,
        native: StagingTableArtifact,
        resolved: ResolvedMssqlNativeSchema,
        lineage: Any,
    ) -> None:
        """Project lineage and the canonical business hash in one table scan."""

        business_schema = tuple(
            (column, resolved.types[column])
            for column in resolved.ordered_target_names
            if not column.casefold().startswith("__dpone__")
        )
        assignments = list(
            lineage.assignments(
                quote_identifier=self._connector.quote_identifier,
                business_schema=business_schema,
                wire_to_target=resolved.wire_to_target,
                alias="n",
            )
            if lineage.columns
            else ()
        )
        row_hash = "__dpone__row_hash"
        if row_hash in native.columns:
            expression = row_hash_expression(
                business_schema,
                lambda column: f"n.{self._connector.quote_identifier(column)}",
            )
            assignments.append(f"{self._connector.quote_identifier(row_hash)} = {expression}")
        if assignments:
            self._connector.execute_query(
                f"UPDATE n SET {', '.join(assignments)} FROM {self._strategy._staging_name(native)} AS n"
            )

    @staticmethod
    def _native_evidence_columns(
        resolved: ResolvedMssqlNativeSchema,
        lineage: Any,
    ) -> tuple[dict[str, object], ...]:
        target_to_wire = {target: wire for wire, target in resolved.wire_to_target.items()}
        generated = {column.name: column for column in resolved.generated_columns}
        return tuple(
            {
                "wire_name": (f"framework:{target}" if target in generated else target_to_wire[target]),
                "target_name": target,
                "target_type": resolved.types[target],
                "nullable": resolved.nullability[target],
                "collation": resolved.collations.get(target),
                "generation_contract": (
                    generated[target].generation_contract
                    if target in generated
                    else "dpone.mssql.native-row-hash.v1"
                    if target.casefold() == "__dpone__row_hash"
                    else None
                ),
            }
            for target in resolved.ordered_target_names
        )

    def _validate_required_keys(self, raw: StagingTableArtifact, keys: Sequence[str]) -> None:
        if not keys:
            return
        predicates = [f"{decoded_staging_expression(self._strategy, raw, column, 'r')} IS NULL" for column in keys]
        rows = self._connector.get_records(
            f"SELECT TOP (1) 1 AS invalid_key FROM {self._strategy._staging_name(raw)} AS r "
            f"WHERE {' OR '.join(predicates)}",
            as_dict=True,
        )
        if rows:
            _raise("mssql_native_projection.unique_key_null")

    def _count_rows(self, artifact: StagingTableArtifact) -> int:
        """Read back direct-native rows before issuing terminal evidence."""

        rows = self._connector.get_records(f"SELECT COUNT_BIG(*) FROM {self._strategy._staging_name(artifact)}")
        if not rows or isinstance(rows[0][0], bool) or not isinstance(rows[0][0], int):
            _raise("mssql_native_projection.native_row_count_unavailable")
        return int(rows[0][0])

    def _validate_key_sql_semantics(
        self,
        native: StagingTableArtifact,
        keys: Sequence[str],
        equality_keys: Sequence[str],
    ) -> None:
        if not equality_keys:
            return
        text_keys = [key for key in equality_keys if is_text_key_type(native.target_column_types.get(key, ""))]
        if text_keys:
            padded = " OR ".join(
                f"DATALENGTH({self._connector.quote_identifier(key)}) <> "
                f"DATALENGTH(RTRIM({self._connector.quote_identifier(key)}))"
                for key in text_keys
            )
            rows = self._connector.get_records(
                f"SELECT TOP (1) 1 FROM {self._strategy._staging_name(native)} WHERE {padded}"
            )
            if rows:
                _raise("mssql_native_projection.text_key_trailing_space_unsupported")
        if not keys:
            return
        grouped = ", ".join(self._connector.quote_identifier(key) for key in keys)
        rows = self._connector.get_records(
            f"SELECT TOP (1) 1 FROM {self._strategy._staging_name(native)} GROUP BY {grouped} HAVING COUNT_BIG(*) > 1"
        )
        if rows:
            _raise("mssql_native_projection.key_sql_equivalence_collision")


def _raise(code: str) -> None:
    from dpone.runtime.incremental_snapshot import SnapshotReconciliationError

    raise SnapshotReconciliationError(code)


def _native_projection_value(
    target: str,
    generated: dict[str, Any],
    source_values: dict[str, str],
    lineage_values: dict[str, str],
    row_hash_sql: str,
) -> str:
    if target.casefold() == "__dpone__row_hash":
        return row_hash_sql
    if target in lineage_values:
        return lineage_values[target]
    if target in generated:
        return str(generated[target].placeholder_sql)
    return source_values[target]


__all__ = ["MssqlNativeStagingNormalizer", "ResolvedMssqlNativeSchema"]
