"""Nested object normalization with dpone lineage metadata."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Iterable, Mapping, Sequence

from dpone.runtime.lineage.identity import LineageIdentityService
from dpone.runtime.normalization.artifact_rows import ArtifactRowsReader
from dpone.runtime.normalization.clock import utc_now_iso
from dpone.runtime.normalization.contracts import HierarchyContract
from dpone.runtime.normalization.file_rows import FileRowReader
from dpone.runtime.normalization.models import NormalizationResult, NormalizedTable
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.row_emitter import NestedRowEmitter
from dpone.runtime.normalization.table_builder import NormalizedTableBuilder, infer_schema
from dpone.runtime.normalization.table_identity import GeneratedTableRegistry
from dpone.runtime.sinks.load_payload import LoadPayload


class NestedNormalizationService:
    """Normalize nested dictionaries/lists into root and child tables."""

    def __init__(
        self,
        *,
        identity_service: LineageIdentityService | None = None,
        file_row_reader: FileRowReader | None = None,
        table_builder: NormalizedTableBuilder | None = None,
        artifact_rows_reader: ArtifactRowsReader | None = None,
        row_emitter: NestedRowEmitter | None = None,
    ) -> None:
        self.identity_service = identity_service or LineageIdentityService()
        self.file_row_reader = file_row_reader or FileRowReader()
        self.table_builder = table_builder or NormalizedTableBuilder(self.identity_service)
        self.artifact_rows_reader = artifact_rows_reader or ArtifactRowsReader(self.file_row_reader)
        self.row_emitter = row_emitter or NestedRowEmitter(
            identity_service=self.identity_service,
            table_builder=self.table_builder,
        )

    def normalize_payload(
        self,
        payload: LoadPayload,
        root_table: str | None = None,
        options: NestedNormalizationOptions | None = None,
        *,
        run_id: str | None = None,
        load_id: str | None = None,
        source_type: str = "nested",
        source_schema: str = "",
        source_table: str | None = None,
        unique_key: str | Sequence[str] | None = None,
        lineage_enabled: bool = True,
    ) -> NormalizationResult:
        """Normalize a sink load payload, including local file artifacts."""

        if isinstance(root_table, NestedNormalizationOptions) and options is None:
            options = root_table
            root_table = None
        resolved_options = options or NestedNormalizationOptions()
        resolved_root = root_table or "root"
        rows = self.artifact_rows_reader.rows(payload.artifact)
        if not resolved_options.enabled:
            materialized = tuple(dict(row) for row in rows)
            return NormalizationResult((NormalizedTable(resolved_root, tuple(payload.schema), materialized),))
        return self.normalize_rows(
            rows,
            root_table=resolved_root,
            options=resolved_options,
            run_id=run_id,
            load_id=load_id,
            source_type=source_type,
            source_schema=source_schema,
            source_table=source_table or resolved_root,
            unique_key=unique_key,
            lineage_enabled=lineage_enabled,
        )

    def normalize_rows(
        self,
        rows: Iterable[Mapping[str, object]],
        *,
        root_table: str,
        options: NestedNormalizationOptions,
        schema: Sequence[tuple[str, str]] | None = None,
        run_id: str | None = None,
        load_id: str | None = None,
        source_type: str = "nested",
        source_schema: str = "",
        source_table: str | None = None,
        unique_key: str | Sequence[str] | None = None,
        lineage_enabled: bool = True,
        table_registry: GeneratedTableRegistry | None = None,
    ) -> NormalizationResult:
        """Normalize row mappings into a deterministic table collection."""

        return self._normalize_rows_with_offset(
            rows,
            root_table=root_table,
            options=options,
            schema=schema,
            run_id=run_id,
            load_id=load_id,
            source_type=source_type,
            source_schema=source_schema,
            source_table=source_table,
            unique_key=unique_key,
            lineage_enabled=lineage_enabled,
            table_registry=table_registry,
            row_index_offset=0,
        )

    def _normalize_rows_with_offset(
        self,
        rows: Iterable[Mapping[str, object]],
        *,
        root_table: str,
        options: NestedNormalizationOptions,
        schema: Sequence[tuple[str, str]] | None = None,
        run_id: str | None = None,
        load_id: str | None = None,
        source_type: str = "nested",
        source_schema: str = "",
        source_table: str | None = None,
        unique_key: str | Sequence[str] | None = None,
        lineage_enabled: bool = True,
        table_registry: GeneratedTableRegistry | None = None,
        row_index_offset: int,
    ) -> NormalizationResult:
        """Normalize rows with an internal positional identity offset."""

        if not options.enabled:
            materialized = tuple(dict(row) for row in rows)
            table_schema = tuple(schema) if schema is not None else tuple(infer_schema(materialized))
            return NormalizationResult((NormalizedTable(name=root_table, schema=table_schema, rows=materialized),))
        if not lineage_enabled:
            raise ValueError("nested normalization requires lineage; set sink.options.lineage.preset: hierarchical")

        builders: OrderedDict[str, list[dict[str, object]]] = OrderedDict()
        guard = self.row_emitter.guard(options, root_table=root_table)
        registry = table_registry or GeneratedTableRegistry(root_table=root_table)
        resolved_load_id = load_id or self.identity_service.new_load_id()
        loaded_at = utc_now_iso()
        del run_id

        for row_index, row in enumerate(rows, start=row_index_offset):
            source_row = dict(row)
            self.row_emitter.ensure_source_row(source_row)
            root_row_id = self.identity_service.row_id(
                source_type=source_type,
                source_schema=source_schema,
                source_table=source_table or root_table,
                row=source_row,
                unique_key=unique_key,
                row_path=f"$[{row_index}]",
            )
            self.row_emitter.append_root(
                builders,
                root_table=root_table,
                source_row=source_row,
                root_row_id=root_row_id,
                load_id=resolved_load_id,
                loaded_at=loaded_at,
                options=options,
                guard=guard,
                registry=registry,
            )
            self.row_emitter.walk_nested_values(
                builders,
                source_row,
                parent_table=root_table,
                field_prefix="",
                depth=0,
                parent_row_id=root_row_id,
                root_row_id=root_row_id,
                root_table=root_table,
                root_source_row=source_row,
                load_id=resolved_load_id,
                loaded_at=loaded_at,
                options=options,
                guard=guard,
                registry=registry,
            )

        result = NormalizationResult(
            tuple(self.table_builder.build_table(name, table_rows) for name, table_rows in builders.items()),
            child_unique_keys=tuple(registry.table_keys().items()),
            child_parent_tables=tuple(registry.table_parents().items()),
        )
        HierarchyContract.from_config(options.hierarchy_contract).validate(result)
        return result
