"""Runtime alias projection for schema identity contracts."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from dpone.readiness.schema_identity import AliasProjectionPlanner, SchemaIdentityOptions
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.streaming_rows import StreamingRowsArtifact
from dpone.type_system.source_sink.provenance import SourceColumnProvenance


class SchemaIdentityProjectionService:
    """Project source aliases into canonical columns before schema gates."""

    def prepare_payload(self, load_config: Any, payload: LoadPayload, logger: Any | None = None) -> LoadPayload:
        options = SchemaIdentityOptions.from_config(
            (getattr(load_config, "options", {}) or {}).get("schema_identity", {})
        )
        if not options.enabled:
            return payload
        rows = _rows(payload.artifact)
        if rows is None:
            raise RuntimeError("schema identity projection requires a row-addressable artifact")
        result = AliasProjectionPlanner(options).project_rows(rows=rows, schema=payload.schema)
        if result.blockers:
            raise RuntimeError(f"schema identity projection blocked: {', '.join(result.blockers)}")
        relation_schema, relation_metadata = project_alias_source_provenance(payload, options)
        _log_projection(logger, result.schema)
        return payload.rebind(
            artifact=_artifact_like(payload.artifact, result.rows),
            schema=list(result.schema),
            relation_schema=relation_schema,
            relation_metadata=relation_metadata,
        )


def project_alias_source_provenance(
    payload: LoadPayload,
    options: SchemaIdentityOptions,
) -> tuple[tuple[tuple[str, str], ...] | None, tuple[SourceColumnProvenance, ...] | None]:
    """Rename/drop observed source provenance in exact alias-projection lockstep."""

    relation_schema = payload.relation_schema
    metadata = payload.relation_metadata
    if relation_schema is None:
        if metadata is not None:
            raise RuntimeError("schema_identity.source_provenance_relation_schema_required")
        return None, None
    projected_relation = AliasProjectionPlanner(options).project_rows(rows=(), schema=relation_schema).schema
    if metadata is None:
        return projected_relation, None

    by_name = _unique_metadata(metadata)
    aliases = {
        alias.name.casefold(): (identity.name, alias.compatibility)
        for identity in options.columns
        for alias in identity.aliases
    }
    canonical_aliases = {
        identity.name.casefold(): tuple(alias.name.casefold() for alias in identity.aliases)
        for identity in options.columns
    }
    projected_metadata: list[SourceColumnProvenance] = []
    for name, declared_type in projected_relation:
        if name.casefold().startswith("__dpone__"):
            continue
        candidate = by_name.get(name.casefold())
        if candidate is None:
            candidate = next(
                (by_name[alias] for alias in canonical_aliases.get(name.casefold(), ()) if alias in by_name),
                None,
            )
        if candidate is None:
            raise RuntimeError(f"schema_identity.source_provenance_missing:{name}")
        source_alias = aliases.get(candidate.name.casefold())
        allowed_names = {source_alias[0].casefold()} if source_alias is not None else {candidate.name.casefold()}
        if source_alias is not None and source_alias[1] == "dual_write":
            allowed_names.add(candidate.name.casefold())
        if name.casefold() not in allowed_names or candidate.declared_type != str(declared_type):
            raise RuntimeError(f"schema_identity.source_provenance_alignment:{name}")
        projected_metadata.append(replace(candidate, name=name))

    projected_names = {
        name.casefold() for name, _dtype in projected_relation if not name.casefold().startswith("__dpone__")
    }
    if {column.name.casefold() for column in projected_metadata} != projected_names:
        raise RuntimeError("schema_identity.source_provenance_alignment")
    return projected_relation, tuple(projected_metadata)


def _unique_metadata(metadata: Any) -> dict[str, SourceColumnProvenance]:
    output: dict[str, SourceColumnProvenance] = {}
    for column in metadata:
        if not isinstance(column, SourceColumnProvenance):
            raise RuntimeError("schema_identity.source_provenance_invalid")
        key = column.name.casefold()
        if key in output:
            raise RuntimeError("schema_identity.source_provenance_duplicate")
        output[key] = column
    return output


def _rows(artifact: Any) -> Any:
    if isinstance(artifact, InMemoryRowsArtifact):
        return artifact._rows
    if isinstance(artifact, StreamingRowsArtifact):
        return artifact._iterator
    return None


def _artifact_like(artifact: Any, rows: tuple[dict[str, Any], ...]) -> Any:
    if isinstance(artifact, StreamingRowsArtifact):
        return artifact.rebind_iterator(iter(rows))
    return InMemoryRowsArtifact(list(rows))


def _log_projection(logger: Any | None, schema: tuple[tuple[str, str], ...]) -> None:
    if logger is None or not hasattr(logger, "log_etl_progress"):
        return
    logger.log_etl_progress("SCHEMA_IDENTITY_PROJECTION", {"Columns": [name for name, _ in schema]})


__all__ = ["project_alias_source_provenance", "SchemaIdentityProjectionService"]
