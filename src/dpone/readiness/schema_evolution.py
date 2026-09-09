"""Schema evolution planner with explicit safety policies.

The planner is intentionally side-effect free. Runtime code can use the plan to
apply safe DDL, render CLI output, or remap incoming source columns to
framework-generated compatibility columns.
"""

from __future__ import annotations

from dpone.contracts.technical_columns import (
    TechnicalColumnCatalog,
    is_offset_minutes_column,
)
from dpone.readiness.schema_evolution_models import (
    ColumnDef,
    SchemaChange,
    SchemaComparisonError,
    SchemaEvolutionPolicy,
    SchemaPlan,
)
from dpone.readiness.schema_type_compatibility import (
    GenericSchemaTypeCompatibility,
    SchemaTypeCompatibility,
)


class SchemaComparator:
    def __init__(
        self,
        policy: SchemaEvolutionPolicy | None = None,
        *,
        type_compatibility: SchemaTypeCompatibility | None = None,
    ) -> None:
        self.policy = policy or SchemaEvolutionPolicy()
        self.type_compatibility = type_compatibility or GenericSchemaTypeCompatibility()

    def compare(self, source: list[ColumnDef], target: list[ColumnDef]) -> SchemaPlan:
        _validate_unambiguous_names(source, side="source")
        _validate_unambiguous_names(target, side="target")
        changes: list[SchemaChange] = []
        source_by_name = {item.name.casefold(): item for item in source}
        target_by_name = {item.name.casefold(): item for item in target}
        ordered_names = list(
            dict.fromkeys([item.name.casefold() for item in source] + [item.name.casefold() for item in target])
        )

        for src in source:
            if self._is_reserved_source_column(src.name):
                changes.append(
                    SchemaChange(
                        "reserved_column",
                        src.name,
                        source=src,
                        breaking=True,
                        reason="__dpone__* is reserved for framework-generated columns",
                    )
                )

        for name in ordered_names:
            source_col = source_by_name.get(name)
            target_col = target_by_name.get(name)
            if source_col and self._is_reserved_source_column(source_col.name):
                continue
            if source_col and not target_col:
                breaking = self.policy.mode == "strict" or not source_col.nullable
                changes.append(
                    SchemaChange(
                        "add_column",
                        source_col.name,
                        source=source_col,
                        breaking=breaking,
                        reason="source column missing in target",
                    )
                )
            elif target_col and not source_col:
                if self._is_ignored_target_column(target_col.name):
                    continue
                changes.append(
                    SchemaChange(
                        "drop_column",
                        target_col.name,
                        target=target_col,
                        breaking=not self.policy.allow_drop,
                        reason="target column missing in source",
                    )
                )
            elif source_col and target_col and source_col.name != target_col.name:
                changes.append(
                    SchemaChange(
                        "column_case_change",
                        source_col.name,
                        source=source_col,
                        target=target_col,
                        breaking=True,
                        reason="case-only identifier changes require an explicit schema-identity contract",
                    )
                )
            elif source_col and target_col:
                changes.extend(self._compare_existing_column(source_col, target_col, target_by_name))
        return SchemaPlan(changes, self.policy)

    def _compare_existing_column(
        self,
        source_col: ColumnDef,
        target_col: ColumnDef,
        target_by_name: dict[str, ColumnDef],
    ) -> list[SchemaChange]:
        changes: list[SchemaChange] = []
        types_equal = self.type_compatibility.types_equal(source_col.dtype, target_col.dtype)
        accepts_existing_nullable_target = (
            self.policy.accept_existing_nullable_target and not source_col.nullable and target_col.nullable
        )
        tightening = not accepts_existing_nullable_target and not source_col.nullable and target_col.nullable
        if not types_equal:
            widening = self.type_compatibility.is_widening(source_col.dtype, target_col.dtype)
            if widening:
                allowed = self.policy.mode == "widening" and widening
                changes.append(
                    SchemaChange(
                        "type_widen",
                        source_col.name,
                        source=source_col,
                        target=target_col,
                        breaking=not allowed or tightening,
                        reason=(
                            "type can widen, but requested nullability tightening requires approval"
                            if tightening
                            else "type differs but can be widened"
                        ),
                    )
                )
            elif self.policy.on_type_change == "new_column":
                changes.append(self._generated_column_change(source_col, target_by_name))
                return changes
            else:
                changes.append(
                    SchemaChange(
                        "type_change",
                        source_col.name,
                        source=source_col,
                        target=target_col,
                        breaking=True,
                        reason="type differs and is not a safe widening",
                    )
                )

        if source_col.nullable != target_col.nullable and not accepts_existing_nullable_target:
            change_type = "nullability_relax" if source_col.nullable else "nullability_tighten"
            changes.append(
                SchemaChange(
                    change_type,
                    source_col.name,
                    source=source_col,
                    target=target_col,
                    breaking=not source_col.nullable,
                    reason=(
                        "target NOT NULL must be relaxed for nullable source values"
                        if source_col.nullable
                        else "requested NOT NULL tightening requires validated data and explicit approval"
                    ),
                )
            )
        if (
            source_col.collation is not None
            and source_col.collation.casefold() != (target_col.collation or "").casefold()
        ):
            changes.append(
                SchemaChange(
                    "collation_change",
                    source_col.name,
                    source=source_col,
                    target=target_col,
                    breaking=True,
                    reason="requested collation differs from the observed target collation",
                )
            )
        return changes

    def _generated_column_change(self, source: ColumnDef, target_by_name: dict[str, ColumnDef]) -> SchemaChange:
        if source.name.casefold() in {column.casefold() for column in self.policy.protected_columns}:
            return SchemaChange(
                "protected_column_type_change",
                source.name,
                source=source,
                target=target_by_name.get(source.name.casefold()),
                breaking=True,
                reason=(
                    "key/authority columns cannot route to a generated compatibility "
                    "column without an explicit authority-transfer contract"
                ),
            )
        generated_name = f"{self.policy.new_column_prefix}{source.name}"
        existing = target_by_name.get(generated_name.casefold())
        if existing and not self.type_compatibility.types_equal(source.dtype, existing.dtype):
            return SchemaChange(
                "generated_column_conflict",
                source.name,
                source=source,
                target=existing,
                breaking=True,
                reason=f"generated column {generated_name} already exists with incompatible type",
                generated_column=generated_name,
            )
        if existing:
            return SchemaChange(
                "map_to_generated_column",
                source.name,
                source=source,
                target=existing,
                breaking=False,
                reason="existing generated column will receive source values",
                generated_column=generated_name,
            )
        return SchemaChange(
            "add_generated_column",
            source.name,
            source=source,
            breaking=False,
            reason="incompatible type change is routed to generated compatibility column",
            generated_column=generated_name,
        )

    def _is_reserved_source_column(self, name: str) -> bool:
        if self.policy.allow_reserved_dpone_columns:
            return False
        normalized = str(name)
        if not normalized.startswith("__dpone__"):
            return False
        return not (TechnicalColumnCatalog().is_canonical(normalized) or is_offset_minutes_column(normalized))

    def _is_ignored_target_column(self, name: str) -> bool:
        normalized = str(name).casefold()
        if normalized.startswith("__dpone__"):
            return True
        canonical_legacy_names = {
            "__dpone__loaded_at",
            "__dpone__updated_at",
            "__dpone__deleted_at",
            "__dpone__xmin",
        }
        if normalized in canonical_legacy_names:
            return True
        return normalized in {column.casefold() for column in self.policy.ignored_target_columns}


def _validate_unambiguous_names(columns: list[ColumnDef], *, side: str) -> None:
    by_identity: dict[str, list[str]] = {}
    for column in columns:
        by_identity.setdefault(column.name.casefold(), []).append(column.name)
    collisions = tuple(tuple(names) for names in by_identity.values() if len(names) > 1)
    if collisions:
        rendered = ",".join("/".join(names) for names in collisions)
        raise SchemaComparisonError(f"schema_evolution.identifier_case_ambiguous:{side}:{rendered}")


__all__ = [
    "ColumnDef",
    "SchemaChange",
    "SchemaComparator",
    "SchemaComparisonError",
    "SchemaEvolutionPolicy",
    "SchemaPlan",
]
