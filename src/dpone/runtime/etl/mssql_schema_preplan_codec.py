"""Canonical retained originals for the catalog-first MSSQL schema preplan.

Preserves only fields the existing producer supplies: the source schema digest,
resolved target types, mappings, retained catalog, diagnostic physical report and
full mutation plan. It does not contain a source physical identity or complete
source projection. A trusted journal must separately bind those originals and
the composition attempt; this codec is not independent outcome verification.
"""

from __future__ import annotations

from dataclasses import asdict
from hashlib import sha256
from typing import Any

from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object
from dpone.readiness.schema_evolution import ColumnDef
from dpone.runtime.etl.mssql_schema_preplan import MssqlSchemaPreplan
from dpone.runtime.sinks.mssql_target_mutation_codec import (
    MAX_RETAINED_PLAN_BYTES,
    MssqlRetainedPlanError,
    binary_digest,
    decode_mssql_target_mutation_plan,
    encode_mssql_target_mutation_plan,
    retained_document,
)

_SCHEMA = "dpone.mssql-schema-preplan.v1"
_FIELDS = {
    "schema",
    "source_schema_sha256",
    "target_mutation_plan",
    "target_column_types",
    "column_mapping",
    "retained_catalog",
    "physical_report",
}


def _text(value: Any) -> None:
    if type(value) is not str or not value or "\x00" in value:
        raise ValueError


def _pairs(value: Any) -> tuple[tuple[str, str], ...]:
    if type(value) is not list:
        raise ValueError
    result = []
    for pair in value:
        if type(pair) is not list or len(pair) != 2:
            raise ValueError
        for item in pair:
            _text(item)
        result.append((pair[0], pair[1]))
    return tuple(result)


def _columns(value: Any) -> tuple[ColumnDef, ...]:
    if type(value) is not list:
        raise ValueError
    result = []
    for column in value:
        if type(column) is not dict or set(column) != {"name", "dtype", "nullable", "collation"}:
            raise ValueError
        _text(column["name"])
        _text(column["dtype"])
        if type(column["nullable"]) is not bool:
            raise ValueError
        if column["collation"] is not None:
            _text(column["collation"])
        result.append(ColumnDef(**column))
    return tuple(result)


def _decode(value: dict[str, Any]) -> MssqlSchemaPreplan:
    if set(value) != _FIELDS or value["schema"] != _SCHEMA:
        raise ValueError
    # The existing physical report is diagnostic JSON with producer-specific
    # keys, not an authorization object. Preserve it without interpreting flags.
    if value["physical_report"] is not None and type(value["physical_report"]) is not dict:
        raise ValueError
    nested = canonical_json_bytes(value["target_mutation_plan"])
    mutation = decode_mssql_target_mutation_plan(nested, sha256(nested).digest())
    return MssqlSchemaPreplan(
        binary_digest(value["source_schema_sha256"]),
        mutation,
        _pairs(value["target_column_types"]),
        _pairs(value["column_mapping"]),
        _columns(value["retained_catalog"]),
        value["physical_report"],
    )


def encode_mssql_schema_preplan(plan: MssqlSchemaPreplan) -> bytes:
    """Serialize all existing producer fields without changing their meaning."""
    try:
        if type(plan) is not MssqlSchemaPreplan:
            raise ValueError
        document = canonical_json_bytes(
            {
                "schema": _SCHEMA,
                "source_schema_sha256": plan.source_schema_sha256.hex(),
                "target_mutation_plan": strict_json_object(
                    encode_mssql_target_mutation_plan(plan.target_mutation_plan)
                ),
                "target_column_types": plan.target_column_types,
                "column_mapping": plan.column_mapping,
                "retained_catalog": [asdict(column) for column in plan.retained_catalog],
                "physical_report": plan.physical_report,
            }
        )
        if len(document) > MAX_RETAINED_PLAN_BYTES or _decode(strict_json_object(document)) != plan:
            raise ValueError
        return document
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise MssqlRetainedPlanError("mssql_retained_schema_preplan") from None


def decode_mssql_schema_preplan(document: bytes, expected_sha256: bytes) -> MssqlSchemaPreplan:
    """Require the externally pinned hash of the whole canonical preplan."""
    try:
        plan = _decode(retained_document(document, expected_sha256))
        if encode_mssql_schema_preplan(plan) != document:
            raise ValueError
        return plan
    except (ValueError, TypeError, KeyError, AttributeError, RecursionError):
        raise MssqlRetainedPlanError("mssql_retained_schema_preplan") from None
