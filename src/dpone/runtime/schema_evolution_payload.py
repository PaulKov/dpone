"""Schema/provenance adapters used by runtime schema evolution."""

from __future__ import annotations

from collections.abc import Iterable, Iterator, Mapping, Sequence
from typing import Any

from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.sinks.load_payload import LoadPayload


def columns_from_schema(
    schema: Sequence[Any] | Mapping[str, str],
) -> list[ColumnDef]:
    if isinstance(schema, Mapping):
        return [ColumnDef(str(column), str(dtype)) for column, dtype in schema.items()]
    columns: list[ColumnDef] = []
    for item in schema:
        if isinstance(item, ColumnDef):
            columns.append(item)
        elif hasattr(item, "name") and hasattr(item, "dtype") and hasattr(item, "nullable"):
            columns.append(
                ColumnDef(
                    str(item.name),
                    str(item.dtype),
                    nullable=bool(item.nullable),
                    collation=(str(item.collation) if getattr(item, "collation", None) else None),
                )
            )
        else:
            values = tuple(item)
            if len(values) == 2:
                column, dtype = values
                columns.append(ColumnDef(str(column), str(dtype)))
            elif len(values) in {3, 4}:
                column, dtype, nullable, *collation = values
                columns.append(
                    ColumnDef(
                        str(column),
                        str(dtype),
                        nullable=bool(nullable),
                        collation=(str(collation[0]) if collation and collation[0] else None),
                    )
                )
            else:
                raise ValueError("target schema columns must expose name, type, and nullability")
    return columns


def source_columns(payload: LoadPayload) -> list[ColumnDef]:
    projection = payload.target_projection
    if projection is None:
        metadata = payload.relation_metadata
        if metadata is None:
            return columns_from_schema(payload.schema)
        return [
            ColumnDef(
                column.name,
                column.declared_type,
                nullable=bool(column.nullable),
                collation=column.collation_name,
            )
            for column in metadata
        ]
    return [
        ColumnDef(
            column.target_name,
            column.target_type,
            nullable=column.nullable,
            collation=column.collation,
        )
        for column in projection.columns
    ]


def effective_source_columns(
    load_config: Any,
    sink: Any,
    payload: LoadPayload,
) -> list[ColumnDef]:
    """Project source columns through the sink's physical column policy.

    Schema evolution compares the source with the table that the sink will
    actually create and populate.  Sinks without a physical projection port
    retain the source-native contract unchanged.
    """

    columns = source_columns(payload)
    projector = getattr(sink, "project_schema_evolution_source_columns", None)
    if not callable(projector):
        return columns
    projected = projector(
        load_config,
        tuple((column.name, column.dtype, column.nullable, column.collation) for column in columns),
    )
    return columns_from_schema(projected)


def remap_rows(
    rows: Iterable[Mapping[str, object]],
    column_mapping: Mapping[str, str],
) -> Iterator[Mapping[str, object]]:
    for row in rows:
        yield {column_mapping.get(str(column), str(column)): value for column, value in row.items()}


def unique_keys(load_config: Any) -> tuple[str, ...]:
    raw = getattr(load_config, "unique_key", None)
    if raw in (None, ""):
        return ()
    if isinstance(raw, str):
        values = (raw,)
    elif isinstance(raw, Sequence):
        values = tuple(raw)
    else:
        raise TypeError("unique_key must be a string or sequence of strings")
    return tuple(str(value) for value in values)


__all__ = [
    "columns_from_schema",
    "effective_source_columns",
    "remap_rows",
    "source_columns",
    "unique_keys",
]
