"""Strategy-specific dpone metadata enrichment."""

from __future__ import annotations

from datetime import UTC, datetime
from importlib import import_module
from typing import TYPE_CHECKING

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.technical_columns import TechnicalColumnCatalog
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.lineage.identity import LineageIdentityService
from dpone.runtime.lineage.postgres_xmin_initial_metadata import (
    is_postgres_xmin_initial_metadata_route,
)
from dpone.runtime.lineage.strategy_metadata_files import StrategyMetadataFileEnricher
from dpone.runtime.lineage.strategy_metadata_rows import StrategyMetadataRowEnricher
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.streaming_rows import StreamingRowsArtifact

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class StrategyMetadataEnricher:
    """Adds metadata required by production load strategies before staging.

    Applies to every source→sink route that uses ``snapshot_diff`` or ``scd2``:
    in-memory rows, streaming rows, and delimited file export families
    (``csv`` / ``mssql-delimited`` / ``clickhouse-tsv``), including partitioned,
    batched, and physically chunked file artifacts.

    Unsupported artifact types fail closed — never silently skip enrichment.
    """

    _HASH_STRATEGIES = {LoadStrategy.SNAPSHOT_DIFF, LoadStrategy.SCD2}

    def __init__(
        self,
        *,
        identity_service: LineageIdentityService | None = None,
        catalog: TechnicalColumnCatalog | None = None,
    ) -> None:
        self.identity_service = identity_service or LineageIdentityService()
        self.catalog = catalog or TechnicalColumnCatalog()
        self._rows = StrategyMetadataRowEnricher(
            identity_service=self.identity_service,
            catalog=self.catalog,
        )
        self._files = StrategyMetadataFileEnricher(self._rows)

    def enrich_payload(
        self,
        payload: LoadPayload,
        *,
        load_config: LoadConfig,
        effective_at: datetime | None = None,
    ) -> LoadPayload:
        # PostgreSQL XMin initial loads keep their COPY artifact immutable.
        # SQL Server native staging appends the snapshot columns and computes
        # the authoritative typed row hash set-wise after BCP ingestion.
        if is_postgres_xmin_initial_metadata_route(load_config):
            return payload
        if load_config.load_strategy not in self._HASH_STRATEGIES:
            return payload

        effective_iso = _utc_iso(effective_at or datetime.now(UTC))
        schema = self._rows.schema_with_strategy_columns(payload.schema, load_config.load_strategy)
        output_columns = [column for column, _ in schema]
        artifact = payload.artifact

        if isinstance(artifact, InMemoryRowsArtifact):
            return payload.rebind(
                artifact=InMemoryRowsArtifact(
                    self._rows.enrich_rows(
                        artifact._rows,
                        strategy=load_config.load_strategy,
                        effective_iso=effective_iso,
                    )
                ),
                schema=schema,
            )
        if isinstance(artifact, StreamingRowsArtifact):
            return payload.rebind(
                artifact=artifact.rebind_iterator(
                    self._rows.enrich_rows(
                        artifact._iterator,
                        strategy=load_config.load_strategy,
                        effective_iso=effective_iso,
                    )
                ),
                schema=schema,
            )

        enriched_artifact = self._files.enrich(
            artifact,
            strategy=load_config.load_strategy,
            effective_iso=effective_iso,
            output_columns=output_columns,
            output_schema=tuple((str(name), str(dtype)) for name, dtype in schema),
            schema_contract=_schema_contract(load_config),
        )
        return payload.rebind(
            artifact=enriched_artifact,
            schema=schema,
        )


def _utc_iso(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat()


def _schema_contract(load_config: LoadConfig) -> object | None:
    options = getattr(load_config, "options", {}) or {}
    raw = options.get("schema_contract")
    contract = import_module("dpone.readiness.schema_contracts").SchemaContract.from_config(
        raw if isinstance(raw, dict) else {}
    )
    return contract if contract.columns else None
