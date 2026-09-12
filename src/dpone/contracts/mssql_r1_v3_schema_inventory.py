"""Lazy compatibility facade for the responsibility-split schema-2 inventory."""

from __future__ import annotations

from importlib import import_module
from typing import Any

_PRIMITIVES = "dpone.contracts.mssql_r1_v3_schema_primitives"
_RELATIONS = "dpone.contracts.mssql_r1_v3_schema_relations"
_MODULES = "dpone.contracts.mssql_r1_v3_schema_modules"

_SYMBOL_MODULE = {
    "MssqlR1ConstraintKindV3": _PRIMITIVES,
    "MssqlR1ExtendedPropertyV3": _PRIMITIVES,
    "MssqlR1IndexDirectionV3": _PRIMITIVES,
    "MssqlR1ParameterDirectionV3": _PRIMITIVES,
    "MssqlR1ResultCardinalityV3": _PRIMITIVES,
    "MssqlR1ResultColumnV3": _PRIMITIVES,
    "MssqlR1SchemaColumnV3": _PRIMITIVES,
    "MssqlR1SchemaObjectKindV3": _PRIMITIVES,
    "MssqlR1SchemaProcedureParameterV3": _PRIMITIVES,
    "MssqlR1SignerProfileKindV3": _PRIMITIVES,
    "MssqlR1SupportedCodecEntryV3": _PRIMITIVES,
    "decode_members": _PRIMITIVES,
    "require_canonical_set": _PRIMITIVES,
    "require_schema_identifier": _PRIMITIVES,
    "MssqlR1ReferencedObjectV3": _RELATIONS,
    "MssqlR1SchemaConstraintV3": _RELATIONS,
    "MssqlR1SchemaIndexKeyV3": _RELATIONS,
    "MssqlR1SchemaIndexV3": _RELATIONS,
    "MssqlR1FixedResultV3": _MODULES,
    "MssqlR1ModuleOptionsV3": _MODULES,
    "MssqlR1NoResultV3": _MODULES,
    "MssqlR1PortableSchemaObjectV3": _MODULES,
    "MssqlR1PortableTriggerV3": _MODULES,
    "MssqlR1ProcedureResultContractV3": _MODULES,
    "MssqlR1StageScanTemplateV3": _MODULES,
    "module_definition_digest": _MODULES,
}

__all__ = list(_SYMBOL_MODULE)


def __getattr__(name: str) -> Any:
    """Resolve a legacy inventory symbol to its canonical responsibility module."""
    module_name = _SYMBOL_MODULE.get(name)
    if module_name is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    value = getattr(import_module(module_name), name)
    globals()[name] = value
    return value
