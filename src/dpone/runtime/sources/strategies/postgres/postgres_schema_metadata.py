"""PostgreSQL catalog metadata normalization for extraction strategies."""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from typing import Any

from dpone.runtime.support.postgres_mssql_projection_models import PostgresMssqlSchemaProjection
from dpone.type_system.source_sink.provenance import SourceColumnProvenance


@dataclass(frozen=True, slots=True)
class PostgresFetchedSchema:
    """Aligned observed PostgreSQL and sink-projected schema views."""

    relation_schema: tuple[tuple[str, str], ...]
    projected_schema: tuple[tuple[str, str], ...]
    relation_metadata: tuple[SourceColumnProvenance, ...]
    target_projection: PostgresMssqlSchemaProjection | None = None


_UDT_KIND = {
    "c": "composite",
    "d": "domain",
    "e": "enum",
    "m": "multirange",
    "r": "range",
}


def custom_type_identity(row: Any, data_type: str) -> str | None:
    domain_name = row.get("domain_name")
    if domain_name:
        return _qualified_type_token("domain", row.get("domain_schema"), domain_name)
    kind = str(row.get("udt_kind") or "").strip().lower()
    category = str(row.get("udt_category") or "").strip().upper()
    type_name = str(row.get("udt_name") or "").strip().lower()
    if type_name in {"geometry", "geography"}:
        return _qualified_type_token("spatial", row.get("udt_schema"), row.get("udt_name"))
    if data_type == "array" or category == "A":
        return _qualified_type_token("array", row.get("udt_schema"), row.get("udt_name"))
    if data_type == "user-defined" or kind in _UDT_KIND:
        return _qualified_type_token(_UDT_KIND.get(kind, "custom"), row.get("udt_schema"), row.get("udt_name"))
    return None


def column_provenance(row: Any, declared_type: str) -> SourceColumnProvenance:
    return SourceColumnProvenance(
        name=str(row["column_name"]),
        declared_type=declared_type,
        nullable=_yes_no(row.get("is_nullable")),
        type_schema=_optional_text(row.get("udt_schema")),
        type_name=_optional_text(row.get("udt_name")),
        type_kind=_optional_text(row.get("udt_kind")),
        type_category=_optional_text(row.get("udt_category")),
        domain_schema=_optional_text(row.get("domain_schema")),
        domain_name=_optional_text(row.get("domain_name")),
        datetime_precision=_optional_int(row.get("datetime_precision")),
        interval_type=_optional_text(row.get("interval_type")),
        interval_precision=_optional_int(row.get("interval_precision")),
        character_set=_optional_text(row.get("character_set_name")),
        collation_schema=_optional_text(row.get("collation_schema")),
        collation_name=_optional_text(row.get("collation_name")),
        has_default=_optional_bool(row.get("column_default") is not None),
        default_expression_sha256=_expression_digest(row.get("column_default")),
        is_identity=_yes_no(row.get("is_identity")),
        identity_generation=_optional_text(row.get("identity_generation")),
        generation_kind=_optional_text(row.get("is_generated")),
        generation_expression_sha256=_expression_digest(row.get("generation_expression")),
    )


def _qualified_type_token(kind: str, schema: Any, name: Any) -> str:
    identity = json.dumps(
        {"name": str(name or ""), "schema": str(schema or "")},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return f"{kind}:{identity}"


def _yes_no(value: Any) -> bool | None:
    normalized = str(value or "").strip().upper()
    return True if normalized == "YES" else False if normalized == "NO" else None


def _optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    return None if value is None else int(value)


def _optional_bool(value: Any) -> bool | None:
    return value if isinstance(value, bool) else None


def _expression_digest(value: Any) -> str | None:
    if value is None:
        return None
    return sha256(str(value).encode("utf-8")).hexdigest()


__all__ = ["PostgresFetchedSchema", "column_provenance", "custom_type_identity"]
