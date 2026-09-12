"""Pure compatibility projection of canonical source-schema authority."""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import dpone.runtime.sources.strategies.postgres as postgres_strategies
from dpone.adapters.postgres_mssql_source_schema_rendering import (
    render_mssql_target_type as render_mssql_target_type,
)
from dpone.adapters.postgres_mssql_source_schema_rendering import (
    render_postgres_declared_type as render_postgres_declared_type,
)
from dpone.adapters.postgres_mssql_source_schema_rendering import (
    render_transfer_representation as render_transfer_representation,
)
from dpone.runtime.sources.postgres_mssql_source_schema_types import (
    PostgresMssqlSelectedRelationSchemaAuthorityV1,
    PostgresMssqlSourceSchemaAuthorityErrorV1,
)
from dpone.runtime.support.postgres_mssql_projection_models import (
    PostgresMssqlColumnProjection,
    PostgresMssqlSchemaProjection,
)
from dpone.type_system.source_sink.provenance import SourceColumnProvenance


@dataclass(frozen=True, slots=True)
class PostgresMssqlSourceSchemaProjectionAdapterV1:
    """Build legacy DTOs only through one injected constructor."""

    _fetched_schema_factory: Callable[..., object]

    def __init__(self, *, fetched_schema_factory: Callable[..., object]) -> None:
        if not callable(fetched_schema_factory):
            raise TypeError("fetched_schema_factory must be callable")
        object.__setattr__(self, "_fetched_schema_factory", fetched_schema_factory)

    @property
    def fetched_schema_factory(self) -> Callable[..., object]:
        return self._fetched_schema_factory

    def project(self, authority: PostgresMssqlSelectedRelationSchemaAuthorityV1) -> object:
        if type(authority) is not PostgresMssqlSelectedRelationSchemaAuthorityV1:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("exact_type_violation")
        try:
            provenance = tuple(_provenance(column) for column in authority.ordered_columns)
            projections = tuple(_projection(authority, column) for column in authority.ordered_columns)
            target = PostgresMssqlSchemaProjection(projections, ())
            relation = tuple((item.name, item.declared_type) for item in provenance)
            projected = tuple((item.name, item.projected_type) for item in projections)
            canonical = postgres_strategies.postgres_prepared_source_boundary.build_r1_postgres_fetched_schema
            if self._fetched_schema_factory is canonical:
                return postgres_strategies.postgres_prepared_source_boundary.build_r1_postgres_fetched_schema(
                    relation_schema=relation,
                    projected_schema=projected,
                    relation_metadata=provenance,
                    target_projection=target,
                )
            return self._fetched_schema_factory(
                relation_schema=relation,
                projected_schema=projected,
                relation_metadata=provenance,
                target_projection=target,
            )
        except asyncio.CancelledError:
            raise
        except PostgresMssqlSourceSchemaAuthorityErrorV1:
            raise
        except Exception:
            raise PostgresMssqlSourceSchemaAuthorityErrorV1("internal_invariant_violation") from None


def _provenance(column: Any) -> SourceColumnProvenance:
    shape = column.source_shape
    temporal = shape.family.value in {"time", "timestamp", "timestamptz"}
    generation = None if column.identity_kind == "" else "ALWAYS" if column.identity_kind == "a" else "BY DEFAULT"
    return SourceColumnProvenance(
        name=column.name,
        declared_type=render_postgres_declared_type(shape),
        nullable=column.nullable,
        type_schema=column.type_namespace_name,
        type_name=column.type_name,
        type_kind=column.type_kind,
        datetime_precision=shape.precision if temporal else None,
        is_identity=column.identity_kind != "",
        identity_generation=generation,
        generation_kind="NEVER",
    )


def _projection(
    authority: PostgresMssqlSelectedRelationSchemaAuthorityV1,
    column: Any,
) -> PostgresMssqlColumnProjection:
    decision = authority.decision_for(column.source_shape)
    source = render_postgres_declared_type(column.source_shape)
    stage = render_mssql_target_type(decision.stage_shape)
    target = render_mssql_target_type(decision.target_shape)
    return PostgresMssqlColumnProjection(
        column.name,
        column.name,
        column.projection_ordinal - 1,
        source,
        target,
        stage,
        target,
        render_transfer_representation(decision.codec),
        False,
        None,
        column.nullable,
        None,
        decision.target_shape.collation,
    )


__all__ = [
    "PostgresMssqlSourceSchemaProjectionAdapterV1",
    "render_mssql_target_type",
    "render_postgres_declared_type",
    "render_transfer_representation",
]
