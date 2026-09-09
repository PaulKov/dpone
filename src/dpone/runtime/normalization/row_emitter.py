"""Row emission collaborator for nested normalization."""

from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping

from dpone.runtime.lineage.identity import LineageIdentityService
from dpone.runtime.normalization.explosion import ExplosionGuard
from dpone.runtime.normalization.normalizer_utils import (
    ensure_no_framework_columns,
    identity_row,
    is_nested,
    join_path,
    join_table,
    with_identity_columns,
)
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.scalar_child_rows import append_scalar_child_row
from dpone.runtime.normalization.table_builder import NormalizedTableBuilder
from dpone.runtime.normalization.table_identity import GeneratedTableRegistry


class NestedRowEmitter:
    """Emit root, mapping-child, and scalar-child rows for nested payloads."""

    def __init__(
        self,
        *,
        identity_service: LineageIdentityService,
        table_builder: NormalizedTableBuilder,
    ) -> None:
        self.identity_service = identity_service
        self.table_builder = table_builder

    def guard(self, options: NestedNormalizationOptions, *, root_table: str) -> ExplosionGuard:
        return ExplosionGuard(options.guardrails, root_table=root_table)

    def ensure_source_row(self, row: Mapping[str, object]) -> None:
        ensure_no_framework_columns(row)

    def append_root(
        self,
        builders: OrderedDict[str, list[dict[str, object]]],
        *,
        root_table: str,
        source_row: Mapping[str, object],
        root_row_id: str,
        load_id: str,
        loaded_at: str,
        options: NestedNormalizationOptions,
        guard: ExplosionGuard,
        registry: GeneratedTableRegistry,
    ) -> None:
        if options.raw_landing.enabled:
            registry.register_auxiliary(
                physical_table=f"{root_table}{options.raw_landing.table_suffix}",
                source_path="$.__dpone_raw",
            )
            self.table_builder.append_raw_row(
                builders,
                root_table=root_table,
                source_row=source_row,
                root_row_id=root_row_id,
                load_id=load_id,
                loaded_at=loaded_at,
                guard=guard,
                options=options,
            )
        self.table_builder.append_row(
            builders,
            table=root_table,
            row=self._project_row(source_row, field_prefix="", depth=0, options=options),
            row_id=root_row_id,
            parent_row_id=None,
            root_row_id=root_row_id,
            list_index=0,
            load_id=load_id,
            loaded_at=loaded_at,
            guard=guard,
        )

    def walk_nested_values(
        self,
        builders: OrderedDict[str, list[dict[str, object]]],
        row: Mapping[str, object],
        *,
        parent_table: str,
        field_prefix: str,
        depth: int,
        parent_row_id: str,
        root_row_id: str,
        root_table: str,
        root_source_row: Mapping[str, object],
        load_id: str,
        loaded_at: str,
        options: NestedNormalizationOptions,
        guard: ExplosionGuard,
        registry: GeneratedTableRegistry,
    ) -> None:
        if depth >= options.nested_level:
            if any(is_nested(value) and not _is_empty_container(value) for value in row.values()):
                raise ValueError(f"nested_level={options.nested_level} prevents deeper child tables")
            return
        for field, value in row.items():
            self._walk_field(
                builders,
                field=str(field),
                value=value,
                parent_table=parent_table,
                field_prefix=field_prefix,
                depth=depth,
                parent_row_id=parent_row_id,
                root_row_id=root_row_id,
                root_table=root_table,
                root_source_row=root_source_row,
                load_id=load_id,
                loaded_at=loaded_at,
                options=options,
                guard=guard,
                registry=registry,
            )

    def _walk_field(
        self,
        builders: OrderedDict[str, list[dict[str, object]]],
        *,
        field: str,
        value: object,
        parent_table: str,
        field_prefix: str,
        depth: int,
        parent_row_id: str,
        root_row_id: str,
        root_table: str,
        root_source_row: Mapping[str, object],
        load_id: str,
        loaded_at: str,
        options: NestedNormalizationOptions,
        guard: ExplosionGuard,
        registry: GeneratedTableRegistry,
    ) -> None:
        if not is_nested(value):
            return
        field_path = join_path(field_prefix, field)
        policy = options.path_policies.policy_for(field_path)
        if policy in {"ignore", "preserve_json"}:
            return
        if policy == "quarantine":
            registry.register_auxiliary(
                physical_table=f"{root_table}__quarantine",
                source_path="$.__dpone_quarantine",
            )
            self.table_builder.append_quarantine(
                builders,
                root_table=root_table,
                path=field_path,
                payload=value,
                parent_row_id=parent_row_id,
                root_row_id=root_row_id,
                load_id=load_id,
                loaded_at=loaded_at,
                guard=guard,
            )
            return
        table = options.path_policies.table_for(field_path) or join_table(parent_table, field, options.table_separator)
        registry.register_generated(
            physical_table=table,
            source_path=field_path,
            parent_table=parent_table,
            unique_key=options.path_policies.unique_key_for(field_path),
        )
        self._walk_value(
            builders,
            value,
            table=table,
            field_path=field_path,
            depth=depth + 1,
            parent_row_id=parent_row_id,
            root_row_id=root_row_id,
            root_table=root_table,
            root_source_row=root_source_row,
            load_id=load_id,
            loaded_at=loaded_at,
            options=options,
            guard=guard,
            registry=registry,
        )

    def _walk_value(
        self,
        builders: OrderedDict[str, list[dict[str, object]]],
        value: object,
        *,
        table: str,
        field_path: str,
        depth: int,
        parent_row_id: str,
        root_row_id: str,
        root_table: str,
        root_source_row: Mapping[str, object],
        load_id: str,
        loaded_at: str,
        options: NestedNormalizationOptions,
        guard: ExplosionGuard,
        registry: GeneratedTableRegistry,
    ) -> None:
        if _is_empty_container(value):
            return
        if isinstance(value, Mapping):
            self._append_mapping_child(
                builders,
                dict(value),
                table=table,
                field_path=field_path,
                row_path=f"{parent_row_id}:{field_path}",
                depth=depth,
                parent_row_id=parent_row_id,
                root_row_id=root_row_id,
                root_table=root_table,
                root_source_row=root_source_row,
                load_id=load_id,
                loaded_at=loaded_at,
                options=options,
                guard=guard,
                registry=registry,
            )
            return
        if isinstance(value, list):
            self._walk_list(
                builders,
                value,
                table=table,
                field_path=field_path,
                depth=depth,
                parent_row_id=parent_row_id,
                root_row_id=root_row_id,
                root_table=root_table,
                root_source_row=root_source_row,
                load_id=load_id,
                loaded_at=loaded_at,
                options=options,
                guard=guard,
                registry=registry,
            )

    def _walk_list(
        self,
        builders: OrderedDict[str, list[dict[str, object]]],
        items: list[object],
        *,
        table: str,
        field_path: str,
        depth: int,
        parent_row_id: str,
        root_row_id: str,
        root_table: str,
        root_source_row: Mapping[str, object],
        load_id: str,
        loaded_at: str,
        options: NestedNormalizationOptions,
        guard: ExplosionGuard,
        registry: GeneratedTableRegistry,
    ) -> None:
        guard.check_array_length(field_path, len(items))
        for index, item in enumerate(items):
            row_path = f"{parent_row_id}:{field_path}[{index}]"
            if isinstance(item, Mapping):
                self._append_mapping_child(
                    builders,
                    dict(item),
                    table=table,
                    field_path=field_path,
                    row_path=row_path,
                    depth=depth,
                    parent_row_id=parent_row_id,
                    root_row_id=root_row_id,
                    root_table=root_table,
                    root_source_row=root_source_row,
                    load_id=load_id,
                    loaded_at=loaded_at,
                    options=options,
                    guard=guard,
                    registry=registry,
                    list_index=index,
                )
            else:
                append_scalar_child_row(
                    builders=builders,
                    item=item,
                    table=table,
                    field_path=field_path,
                    row_path=row_path,
                    parent_row_id=parent_row_id,
                    root_row_id=root_row_id,
                    root_table=root_table,
                    root_source_row=root_source_row,
                    load_id=load_id,
                    loaded_at=loaded_at,
                    options=options,
                    guard=guard,
                    list_index=index,
                    identity_service=self.identity_service,
                    table_builder=self.table_builder,
                )

    def _append_mapping_child(
        self,
        builders: OrderedDict[str, list[dict[str, object]]],
        item_row: dict[str, object],
        *,
        table: str,
        field_path: str,
        row_path: str,
        depth: int,
        parent_row_id: str,
        root_row_id: str,
        root_table: str,
        root_source_row: Mapping[str, object],
        load_id: str,
        loaded_at: str,
        options: NestedNormalizationOptions,
        guard: ExplosionGuard,
        registry: GeneratedTableRegistry,
        list_index: int | None = None,
    ) -> None:
        ensure_no_framework_columns(item_row)
        child_unique_key = options.path_policies.unique_key_for(field_path)
        projected = self._project_row(item_row, field_prefix=field_path, depth=depth, options=options)
        projected = with_identity_columns(projected, root_source_row, child_unique_key)
        row_id = self.identity_service.row_id(
            source_type="nested",
            source_schema=root_table,
            source_table=table,
            row=identity_row(projected, root_source_row, child_unique_key),
            unique_key=child_unique_key or None,
            row_path=row_path,
        )
        self.table_builder.append_row(
            builders,
            table=table,
            row=projected,
            row_id=row_id,
            parent_row_id=parent_row_id,
            root_row_id=root_row_id,
            list_index=list_index,
            load_id=load_id,
            loaded_at=loaded_at,
            guard=guard,
        )
        self.walk_nested_values(
            builders,
            item_row,
            parent_table=table,
            field_prefix=field_path,
            depth=depth,
            parent_row_id=row_id,
            root_row_id=root_row_id,
            root_table=root_table,
            root_source_row=root_source_row,
            load_id=load_id,
            loaded_at=loaded_at,
            options=options,
            guard=guard,
            registry=registry,
        )

    def _project_row(
        self,
        row: Mapping[str, object],
        *,
        field_prefix: str,
        depth: int,
        options: NestedNormalizationOptions,
    ) -> dict[str, object]:
        projected: dict[str, object] = {}
        for field, value in row.items():
            field_path = join_path(field_prefix, str(field))
            policy = options.path_policies.policy_for(field_path)
            if policy in {"ignore", "quarantine"}:
                continue
            if is_nested(value):
                if (
                    _is_empty_container(value)
                    or options.preserve_nested_json
                    or policy == "preserve_json"
                    or depth >= options.nested_level
                ):
                    projected[str(field)] = value
                continue
            projected[str(field)] = value
        return projected


def _is_empty_container(value: object) -> bool:
    return (isinstance(value, Mapping) and not value) or (isinstance(value, list) and not value)
