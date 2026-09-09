"""Semantic schema identity public facade.

The implementation is split into small model, resolver, and projection modules
so callers keep one stable import path without creating a god module.
"""

from __future__ import annotations

from dpone.readiness.schema_identity_models import (
    SCHEMA_IDENTITY_PLAN_SCHEMA,
    AliasProjectionResult,
    SchemaAlias,
    SchemaIdentityDecision,
    SchemaIdentityOptions,
    SchemaIdentityResult,
    SchemaObjectIdentity,
)
from dpone.readiness.schema_identity_projection import AliasProjectionPlanner
from dpone.readiness.schema_identity_resolver import SchemaIdentityResolver

__all__ = [
    "AliasProjectionPlanner",
    "AliasProjectionResult",
    "SCHEMA_IDENTITY_PLAN_SCHEMA",
    "SchemaAlias",
    "SchemaIdentityDecision",
    "SchemaIdentityOptions",
    "SchemaIdentityResolver",
    "SchemaIdentityResult",
    "SchemaObjectIdentity",
]
