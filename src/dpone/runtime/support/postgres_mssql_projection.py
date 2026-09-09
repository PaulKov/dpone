"""Typed PostgreSQL→MSSQL projection authority shared by source and sink.

The public payload keeps two immutable, aligned schemas:

* ``relation_schema`` is the observed PostgreSQL declaration/provenance;
* ``schema`` is the default SQL Server wire/native declaration.

This module is the only place that joins those views with the self-service
logical/physical contract.  It prevents a mapped ``nvarchar(max)`` from
forgetting that it originated from an enum, domain, unconstrained numeric, or
another source family that requires explicit operator approval.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.contracts.incremental_snapshot import MSSQL_TEXT_KEY_COLLATION
from dpone.contracts.mssql_type_contract import normalize_mssql_physical_type
from dpone.contracts.postgres_mssql_type_policy import (
    PostgresMssqlContractOptions,
    declared_postgres_mssql_contract_blockers,
    is_textual_mssql_target,
)
from dpone.runtime.sink_dialect import is_mssql_dialect
from dpone.runtime.support.mssql_lossless_projection import (
    MssqlLosslessProjectionError,
    validate_lossless_target_projection,
)
from dpone.runtime.support.postgres_mssql_projection_models import (
    PostgresMssqlColumnProjection,
    PostgresMssqlProjectionError,
    PostgresMssqlSchemaProjection,
    RetainedMssqlTargetColumn,
)
from dpone.runtime.support.postgres_mssql_temporal_policy import configured_temporal_blocker
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper
from dpone.type_system.source_sink.provenance import SourceColumnProvenance, SourceRelationDialect


def project_postgres_mssql_relation(
    relation_schema: Sequence[tuple[str, str]],
    load_config: Any,
    *,
    relation_metadata: Sequence[SourceColumnProvenance] | None = None,
) -> PostgresMssqlSchemaProjection:
    """Derive and authorize the mapped schema before any source row is read."""

    temporal_blocker = configured_temporal_blocker(relation_schema, load_config)
    if temporal_blocker is not None:
        blocker, column = temporal_blocker
        raise PostgresMssqlProjectionError(blocker, column=column)

    return _resolve(
        relation_schema,
        projected_schema=None,
        load_config=load_config,
        relation_metadata=relation_metadata,
    )


def resolve_postgres_mssql_projection(
    relation_schema: Sequence[tuple[str, str]] | None,
    projected_schema: Sequence[tuple[str, str]],
    load_config: Any,
    *,
    relation_metadata: Sequence[SourceColumnProvenance] | None = None,
) -> PostgresMssqlSchemaProjection:
    """Validate immutable source/mapped alignment and return effective targets."""

    if relation_schema is None:
        raise PostgresMssqlProjectionError("postgres_mssql.type_contract.source_provenance_required")
    return _resolve(
        _business_schema(relation_schema),
        projected_schema=_business_schema(projected_schema),
        load_config=load_config,
        relation_metadata=relation_metadata,
    )


def ensure_postgres_mssql_payload_projection(load_config: Any, payload: Any) -> Any:
    """Attach the one canonical target projection after payload name transforms."""

    if payload.relation_dialect != SourceRelationDialect.POSTGRES:
        return payload
    options = getattr(load_config, "options", {}) or {}
    configured_sink = options.get("sink_type") or options.get("target_type") if isinstance(options, Mapping) else None
    if not is_mssql_dialect(configured_sink):
        return payload
    projection = resolve_postgres_mssql_projection(
        payload.relation_schema,
        payload.schema,
        load_config,
        relation_metadata=payload.relation_metadata,
    )
    return payload.rebind(target_projection=projection)


def declared_contract_blockers(
    relation_schema: Sequence[tuple[str, str]],
    options: Mapping[str, Any] | None,
) -> tuple[str, ...]:
    """Return static blockers for a manifest-declared PostgreSQL schema."""

    return declared_postgres_mssql_contract_blockers(relation_schema, options)


def _resolve(
    relation_schema: Sequence[tuple[str, str]],
    *,
    projected_schema: Sequence[tuple[str, str]] | None,
    load_config: Any,
    relation_metadata: Sequence[SourceColumnProvenance] | None,
) -> PostgresMssqlSchemaProjection:
    relation = _validated_schema(relation_schema, blocker="source_provenance_invalid")
    projected = (
        _validated_schema(projected_schema, blocker="projected_schema_invalid")
        if projected_schema is not None
        else None
    )
    if projected is not None and tuple(name for name, _ in relation) != tuple(name for name, _ in projected):
        raise PostgresMssqlProjectionError("postgres_mssql.type_contract.schema_alignment")

    options = getattr(load_config, "options", {}) or {}
    config = PostgresMssqlContractOptions(options if isinstance(options, Mapping) else {})
    if enforcement_blocker := config.file_enforcement_blocker():
        raise PostgresMssqlProjectionError(enforcement_blocker)
    metadata = _metadata_by_name(relation, relation_metadata)
    mapper = PostgresMssqlTypeMapper()
    unique_keys = _unique_keys(load_config)
    relation_names = {name for name, _dtype in relation}
    if any(key not in relation_names for key in unique_keys):
        raise PostgresMssqlProjectionError("postgres_mssql.type_contract.unique_key_missing")
    columns: list[PostgresMssqlColumnProjection] = []
    for index, (name, source_type) in enumerate(relation):
        source_observed = name in metadata
        if name.lower().startswith("__dpone__") and source_observed:
            raise PostgresMssqlProjectionError(
                "postgres_mssql.type_contract.source_reserved_column",
                column=name,
            )
        if name.lower().startswith("__dpone__"):
            mapped = projected[index][1] if projected is not None else source_type
            source_native = _normalize_type(mapped, name, "projected_type_invalid")
            decision_requires_contract = False
            representation = "dpone internal native value"
        else:
            decision = mapper.resolve(source_type)
            source_native = _normalize_type(decision.target_type, name, "projected_type_invalid")
            mapped = source_native
            decision_requires_contract = decision.requires_explicit_contract
            representation = decision.transfer_representation

        if projected is not None:
            actual = _normalize_type(projected[index][1], name, "projected_type_invalid")
            if actual != source_native:
                raise PostgresMssqlProjectionError(
                    "postgres_mssql.type_contract.projected_type_mismatch",
                    column=name,
                )
            mapped = actual

        explicit_source = config.explicit_source(name)
        if decision_requires_contract and explicit_source is None:
            raise PostgresMssqlProjectionError(
                "postgres_mssql.type_contract.explicit_contract_required",
                column=name,
            )
        override = config.physical_override(name)
        if decision_requires_contract and override is not None and not is_textual_mssql_target(override):
            raise PostgresMssqlProjectionError(
                "postgres_mssql.type_contract.textual_target_required",
                column=name,
            )
        target = _normalize_type(override, name, "target_type_invalid") if override else mapped
        if target != source_native:
            try:
                validate_lossless_target_projection(source_native, target, column=name)
            except MssqlLosslessProjectionError as exc:
                raise PostgresMssqlProjectionError(
                    "postgres_mssql.type_contract.lossy_target_type",
                    column=name,
                ) from exc
        nullable = config.nullable(name)
        if nullable is None:
            observed = metadata.get(name)
            nullable = observed.nullable if observed is not None else None
        if nullable is None and not name.lower().startswith("__dpone__"):
            raise PostgresMssqlProjectionError(
                "postgres_mssql.type_contract.nullability_required",
                column=name,
            )
        collation = config.collation(name)
        if name in unique_keys and _is_textual_target_type(target):
            if collation is not None and collation.casefold() != MSSQL_TEXT_KEY_COLLATION.casefold():
                raise PostgresMssqlProjectionError(
                    "postgres_mssql.type_contract.unique_key_collation_conflict",
                    column=name,
                )
            collation = MSSQL_TEXT_KEY_COLLATION
        columns.append(
            PostgresMssqlColumnProjection(
                name=name,
                source_name=name,
                wire_position=index,
                source_type=source_type,
                source_native_mssql_type=source_native,
                projected_type=mapped,
                target_type=target,
                transfer_representation=representation,
                requires_explicit_contract=decision_requires_contract,
                explicit_contract_source=explicit_source,
                nullable=bool(nullable) if nullable is not None else False,
                source_collation=metadata[name].collation if name in metadata else None,
                collation=collation,
            )
        )
    return PostgresMssqlSchemaProjection(tuple(columns))


def _metadata_by_name(
    relation: Sequence[tuple[str, str]],
    metadata: Sequence[SourceColumnProvenance] | None,
) -> dict[str, SourceColumnProvenance]:
    if metadata is None:
        return {}
    by_name = {column.name: column for column in metadata}
    if len(by_name) != len(metadata):
        raise PostgresMssqlProjectionError("postgres_mssql.type_contract.source_provenance_invalid")
    relation_by_name = dict(relation)
    for name, column in by_name.items():
        if name not in relation_by_name or relation_by_name[name] != column.declared_type:
            raise PostgresMssqlProjectionError("postgres_mssql.type_contract.source_provenance_invalid")
    missing_business = [
        name for name, _dtype in relation if not name.lower().startswith("__dpone__") and name not in by_name
    ]
    if missing_business:
        raise PostgresMssqlProjectionError("postgres_mssql.type_contract.source_provenance_required")
    return by_name


def _validated_schema(
    schema: Sequence[tuple[str, str]],
    *,
    blocker: str,
) -> tuple[tuple[str, str], ...]:
    output = tuple((str(name), str(dtype)) for name, dtype in schema)
    names = tuple(name for name, _ in output)
    identities = tuple(name.casefold() for name in names)
    if not output or any(not name or not dtype for name, dtype in output) or len(set(identities)) != len(names):
        raise PostgresMssqlProjectionError(f"postgres_mssql.type_contract.{blocker}")
    return output


def _normalize_type(value: str, column: str, blocker: str) -> str:
    try:
        return normalize_mssql_physical_type(value)
    except ValueError as exc:
        raise PostgresMssqlProjectionError(
            f"postgres_mssql.type_contract.{blocker}",
            column=column,
        ) from exc


def _business_schema(schema: Sequence[tuple[str, str]]) -> tuple[tuple[str, str], ...]:
    """Exclude dpone service columns from immutable source provenance alignment."""

    return tuple((name, dtype) for name, dtype in schema if not str(name).casefold().startswith("__dpone__"))


def _unique_keys(load_config: Any) -> tuple[str, ...]:
    raw = getattr(load_config, "unique_key", None)
    if raw in (None, ""):
        return ()
    if isinstance(raw, str):
        values = (raw,)
    elif isinstance(raw, Sequence):
        values = tuple(raw)
    else:
        raise PostgresMssqlProjectionError("postgres_mssql.type_contract.unique_key_invalid")
    return tuple(str(value) for value in values)


def _is_textual_target_type(dtype: str) -> bool:
    return dtype.split("(", 1)[0] in {"char", "nchar", "nvarchar", "varchar"}


__all__ = [
    "declared_contract_blockers",
    "ensure_postgres_mssql_payload_projection",
    "PostgresMssqlColumnProjection",
    "PostgresMssqlProjectionError",
    "PostgresMssqlSchemaProjection",
    "RetainedMssqlTargetColumn",
    "project_postgres_mssql_relation",
    "resolve_postgres_mssql_projection",
]
