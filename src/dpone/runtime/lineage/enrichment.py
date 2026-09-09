"""Row and artifact enrichment with dpone lineage columns."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from datetime import UTC, datetime

from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.lineage.identity import LineageIdentityService
from dpone.runtime.lineage.options import LineageOptions
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.streaming_rows import StreamingRowsArtifact


class RowLineageEnricher:
    """Adds resolved lineage columns to in-memory and streaming row artifacts."""

    def __init__(
        self,
        *,
        identity_service: LineageIdentityService | None = None,
        catalog: TechnicalColumnCatalog | None = None,
    ) -> None:
        self.identity_service = identity_service or LineageIdentityService()
        self.catalog = catalog or TechnicalColumnCatalog()

    def enrich_payload(
        self,
        payload: LoadPayload,
        *,
        options: LineageOptions,
        run_id: str,
        load_id: str,
        source_type: str,
        source_schema: str,
        source_table: str,
        unique_key: str | Sequence[str] | None = None,
        extracted_at: datetime | None = None,
        loaded_at: datetime | None = None,
    ) -> LoadPayload:
        if not options.enabled:
            return payload

        enriched_schema = self._schema_with_lineage(payload.schema, options)
        if isinstance(payload.artifact, InMemoryRowsArtifact):
            artifact = InMemoryRowsArtifact(
                self.enrich_rows(
                    payload.artifact._rows,
                    options=options,
                    run_id=run_id,
                    load_id=load_id,
                    source_type=source_type,
                    source_schema=source_schema,
                    source_table=source_table,
                    unique_key=unique_key,
                    extracted_at=extracted_at,
                    loaded_at=loaded_at,
                )
            )
            return payload.rebind(
                artifact=artifact,
                schema=enriched_schema,
            )

        if isinstance(payload.artifact, StreamingRowsArtifact):
            artifact = payload.artifact.rebind_iterator(
                self.enrich_rows(
                    payload.artifact._iterator,
                    options=options,
                    run_id=run_id,
                    load_id=load_id,
                    source_type=source_type,
                    source_schema=source_schema,
                    source_table=source_table,
                    unique_key=unique_key,
                    extracted_at=extracted_at,
                    loaded_at=loaded_at,
                )
            )
            return payload.rebind(
                artifact=artifact,
                schema=enriched_schema,
            )

        return payload

    def enrich_rows(
        self,
        rows: Iterable[Mapping[str, object]],
        *,
        options: LineageOptions,
        run_id: str,
        load_id: str,
        source_type: str,
        source_schema: str,
        source_table: str,
        unique_key: str | Sequence[str] | None = None,
        extracted_at: datetime | None = None,
        loaded_at: datetime | None = None,
    ) -> Iterator[Mapping[str, object]]:
        if not options.enabled:
            yield from rows
            return

        extracted_iso = _utc_iso(extracted_at or datetime.now(UTC))
        loaded_iso = _utc_iso(loaded_at or datetime.now(UTC))
        for index, row in enumerate(rows):
            source_row = dict(row)
            enriched = dict(source_row)
            self._apply_identity_columns(
                enriched,
                source_row=source_row,
                options=options,
                run_id=run_id,
                load_id=load_id,
                source_type=source_type,
                source_schema=source_schema,
                source_table=source_table,
                unique_key=unique_key,
                extracted_at=extracted_iso,
                loaded_at=loaded_iso,
                row_index=index,
            )
            yield enriched

    def _apply_identity_columns(
        self,
        row: dict[str, object],
        *,
        source_row: Mapping[str, object],
        options: LineageOptions,
        run_id: str,
        load_id: str,
        source_type: str,
        source_schema: str,
        source_table: str,
        unique_key: str | Sequence[str] | None,
        extracted_at: str,
        loaded_at: str,
        row_index: int,
    ) -> None:
        if options.has_feature("run_identity"):
            row[self.catalog.name(TechnicalColumnRole.RUN_ID)] = run_id
        if options.has_feature("identity"):
            row[self.catalog.name(TechnicalColumnRole.LOAD_ID)] = load_id
            row[self.catalog.name(TechnicalColumnRole.LOADED_AT)] = loaded_at
        if options.has_feature("row_identity"):
            row[self.catalog.name(TechnicalColumnRole.ROW_ID)] = self.identity_service.row_id(
                source_type=source_type,
                source_schema=source_schema,
                source_table=source_table,
                row=source_row,
                unique_key=unique_key,
                row_path=str(row_index),
            )
            row[self.catalog.name(TechnicalColumnRole.EXTRACTED_AT)] = extracted_at
        if options.has_feature("hierarchy"):
            row.setdefault(self.catalog.name(TechnicalColumnRole.PARENT_ROW_ID), None)
            row.setdefault(
                self.catalog.name(TechnicalColumnRole.ROOT_ROW_ID),
                row.get(self.catalog.name(TechnicalColumnRole.ROW_ID)),
            )
            row.setdefault(self.catalog.name(TechnicalColumnRole.LIST_INDEX), row_index)
        if options.has_feature("operations"):
            row.setdefault(self.catalog.name(TechnicalColumnRole.OP), None)
        if options.has_feature("diagnostics"):
            row.setdefault(self.catalog.name(TechnicalColumnRole.META), None)

    def _schema_with_lineage(
        self,
        schema: Sequence[tuple[str, str]],
        options: LineageOptions,
    ) -> list[tuple[str, str]]:
        existing = {column.lower() for column, _ in schema}
        enriched = list(schema)
        for column, dtype in options.target_schema_columns():
            if column.lower() not in existing:
                enriched.append((column, dtype))
        return enriched


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()
