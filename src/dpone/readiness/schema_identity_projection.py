"""Runtime alias projection planner for schema identity contracts."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from dpone.readiness.schema_identity_models import (
    AliasProjectionResult,
    SchemaIdentityOptions,
    actual_key,
    alias_lookup,
)


class AliasProjectionPlanner:
    """Project runtime rows from aliases to canonical column names."""

    def __init__(self, options: SchemaIdentityOptions) -> None:
        self._options = options

    def project_rows(
        self,
        *,
        rows: Iterable[Mapping[str, Any]],
        schema: Sequence[tuple[str, str]],
    ) -> AliasProjectionResult:
        if not self._options.enabled:
            return AliasProjectionResult(rows=tuple(dict(row) for row in rows), schema=tuple(schema))
        projected_rows: list[dict[str, Any]] = []
        blockers: list[str] = []
        for row in rows:
            projected, row_blockers = self._project_row(row)
            projected_rows.append(projected)
            blockers.extend(row_blockers)
        return AliasProjectionResult(
            rows=tuple(projected_rows),
            schema=_project_schema(schema, self._options),
            blockers=tuple(dict.fromkeys(blockers)),
        )

    def _project_row(self, row: Mapping[str, Any]) -> tuple[dict[str, Any], list[str]]:
        projected = dict(row)
        blockers: list[str] = []
        for identity in self._options.columns:
            canonical_key = actual_key(projected, identity.name)
            for alias in identity.aliases:
                alias_key = actual_key(projected, alias.name)
                if alias_key is None:
                    if alias.compatibility == "dual_write" and canonical_key is not None:
                        projected[alias.name] = projected[canonical_key]
                    continue
                alias_value = projected[alias_key]
                canonical_key = actual_key(projected, identity.name)
                if canonical_key is not None and projected[canonical_key] != alias_value:
                    blockers.append(f"schema_identity.alias_value_conflict:{identity.name}:{alias.name}")
                    continue
                projected[identity.name] = alias_value
                if alias.compatibility != "dual_write":
                    projected.pop(alias_key, None)
        return projected, blockers


def _project_schema(schema: Sequence[tuple[str, str]], options: SchemaIdentityOptions) -> tuple[tuple[str, str], ...]:
    aliases = alias_lookup(options)
    result: dict[str, tuple[str, str]] = {}
    for name, dtype in schema:
        match = aliases.get(str(name).lower())
        if match is None:
            result.setdefault(str(name).lower(), (str(name), str(dtype)))
            continue
        identity, alias = match
        result[identity.name.lower()] = (identity.name, str(dtype))
        if alias.compatibility == "dual_write":
            result[alias.name.lower()] = (alias.name, str(dtype))
    return tuple(result.values())


__all__ = ["AliasProjectionPlanner"]
