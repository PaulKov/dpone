"""Pure schema identity resolution service."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import date

from dpone.readiness.schema_evolution import ColumnDef
from dpone.readiness.schema_identity_models import (
    SchemaIdentityDecision,
    SchemaIdentityOptions,
    SchemaIdentityResult,
    alias_lookup,
    expiration_issue,
)
from dpone.readiness.schema_type_compatibility import types_equal


class SchemaIdentityResolver:
    """Resolve observed schema names to configured semantic identities."""

    def __init__(self, options: SchemaIdentityOptions) -> None:
        self._options = options

    def resolve(
        self,
        *,
        source: Sequence[ColumnDef],
        target: Sequence[ColumnDef] = (),
        today: date | None = None,
    ) -> SchemaIdentityResult:
        if not self._options.enabled:
            return SchemaIdentityResult(canonical_source=tuple(source))
        current_date = today or date.today()
        decisions: list[SchemaIdentityDecision] = []
        blockers: list[str] = []
        warnings: list[str] = []
        canonical_source = _canonicalize_source(
            source=source,
            options=self._options,
            today=current_date,
            decisions=decisions,
            blockers=blockers,
            warnings=warnings,
        )
        decisions.extend(_target_alias_decisions(self._options, target))
        if self._options.rename_detection == "suggest":
            decisions.extend(_suggestions(canonical_source, target))
        return SchemaIdentityResult(
            canonical_source=tuple(canonical_source),
            decisions=tuple(decisions),
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=tuple(dict.fromkeys(warnings)),
        )


def _canonicalize_source(
    *,
    source: Sequence[ColumnDef],
    options: SchemaIdentityOptions,
    today: date,
    decisions: list[SchemaIdentityDecision],
    blockers: list[str],
    warnings: list[str],
) -> list[ColumnDef]:
    aliases = alias_lookup(options)
    emitted: dict[str, ColumnDef] = {}
    for column in source:
        match = aliases.get(column.name.lower())
        if match is None:
            emitted.setdefault(column.name.lower(), column)
            continue
        identity, alias = match
        issue = expiration_issue(identity.name, alias, today)
        if issue:
            if options.expired_alias == "block":
                blockers.append(issue)
            else:
                warnings.append(issue)
        emitted[identity.name.lower()] = ColumnDef(identity.name, column.dtype, column.nullable)
        decisions.append(
            SchemaIdentityDecision(
                canonical_name=identity.name,
                observed_name=alias.name,
                action="rename_alias",
                compatibility=alias.compatibility,
                warning=issue if options.expired_alias == "warn" else None,
                blocker=issue if options.expired_alias == "block" else None,
                source_type=column.dtype,
            )
        )
    return list(emitted.values())


def _target_alias_decisions(
    options: SchemaIdentityOptions,
    target: Sequence[ColumnDef],
) -> tuple[SchemaIdentityDecision, ...]:
    names = {item.name.lower(): item for item in target}
    decisions: list[SchemaIdentityDecision] = []
    for identity in options.columns:
        if identity.name.lower() in names:
            continue
        for alias in identity.aliases:
            target_col = names.get(alias.name.lower())
            if target_col is not None:
                decisions.append(
                    SchemaIdentityDecision(
                        canonical_name=identity.name,
                        observed_name=alias.name,
                        action="target_alias_only",
                        risk="migration_required",
                        compatibility=alias.compatibility,
                        target_type=target_col.dtype,
                    )
                )
    return tuple(decisions)


def _suggestions(source: Sequence[ColumnDef], target: Sequence[ColumnDef]) -> tuple[SchemaIdentityDecision, ...]:
    if len(source) != 1 or len(target) != 1:
        return ()
    source_col = source[0]
    target_col = target[0]
    if source_col.name.lower() == target_col.name.lower() or not types_equal(source_col.dtype, target_col.dtype):
        return ()
    return (
        SchemaIdentityDecision(
            canonical_name=source_col.name,
            observed_name=target_col.name,
            action="suggested_rename",
            risk="advisory",
            source_type=source_col.dtype,
            target_type=target_col.dtype,
        ),
    )


__all__ = ["SchemaIdentityResolver"]
