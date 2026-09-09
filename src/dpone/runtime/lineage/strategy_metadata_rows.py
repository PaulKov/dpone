"""Shared row/schema enrichment for strategy metadata (hash / SCD2)."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence

from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.technical_columns import TechnicalColumnCatalog, TechnicalColumnRole
from dpone.runtime.lineage.identity import LineageIdentityService


def strategy_metadata_schema_columns(strategy: LoadStrategy) -> tuple[tuple[str, str, bool], ...]:
    """Resolve the immutable strategy-owned target columns without row I/O."""

    catalog = TechnicalColumnCatalog()
    roles: tuple[TechnicalColumnRole, ...]
    if strategy == LoadStrategy.SNAPSHOT_DIFF:
        roles = (TechnicalColumnRole.ROW_HASH, TechnicalColumnRole.DELETED_AT)
    elif strategy == LoadStrategy.SCD2:
        roles = (
            TechnicalColumnRole.ROW_HASH,
            TechnicalColumnRole.VALID_FROM_AT,
            TechnicalColumnRole.VALID_TO_AT,
            TechnicalColumnRole.IS_CURRENT,
        )
    else:
        roles = ()
    return tuple(
        (
            definition.name,
            definition.logical_type,
            False if definition.role == TechnicalColumnRole.ROW_HASH else definition.nullable,
        )
        for definition in (catalog.definition(role) for role in roles)
    )


class StrategyMetadataRowEnricher:
    """Adds ``__dpone__row_hash`` and SCD2 validity columns to row mappings."""

    def __init__(
        self,
        *,
        identity_service: LineageIdentityService | None = None,
        catalog: TechnicalColumnCatalog | None = None,
    ) -> None:
        self.identity_service = identity_service or LineageIdentityService()
        self.catalog = catalog or TechnicalColumnCatalog()

    def enrich_rows(
        self,
        rows: Iterable[Mapping[str, object]],
        *,
        strategy: LoadStrategy,
        effective_iso: str,
    ) -> Iterator[dict[str, object]]:
        row_hash_column = self.catalog.name(TechnicalColumnRole.ROW_HASH)
        valid_from_column = self.catalog.name(TechnicalColumnRole.VALID_FROM_AT)
        valid_to_column = self.catalog.name(TechnicalColumnRole.VALID_TO_AT)
        is_current_column = self.catalog.name(TechnicalColumnRole.IS_CURRENT)
        deleted_at_column = self.catalog.name(TechnicalColumnRole.DELETED_AT)

        for row in rows:
            enriched = dict(row)
            source_row = {key: value for key, value in row.items() if not str(key).startswith("__dpone__")}
            enriched[row_hash_column] = self.identity_service.row_hash(source_row)
            if strategy == LoadStrategy.SNAPSHOT_DIFF:
                enriched.setdefault(deleted_at_column, None)
            if strategy == LoadStrategy.SCD2:
                enriched.setdefault(valid_from_column, effective_iso)
                enriched.setdefault(valid_to_column, None)
                enriched.setdefault(is_current_column, True)
            yield enriched

    def schema_with_strategy_columns(
        self,
        schema: Sequence[tuple[str, str]],
        strategy: LoadStrategy,
    ) -> list[tuple[str, str]]:
        enriched = list(schema)
        existing = {column.lower() for column, _ in enriched}
        for name, dtype, _nullable in strategy_metadata_schema_columns(strategy):
            if name.casefold() in existing:
                continue
            enriched.append((name, dtype))
            existing.add(name.casefold())
        return enriched


__all__ = ["StrategyMetadataRowEnricher", "strategy_metadata_schema_columns"]
