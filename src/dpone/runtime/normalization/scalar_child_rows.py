from __future__ import annotations

from collections import OrderedDict
from collections.abc import Mapping

from dpone.runtime.lineage.identity import LineageIdentityService
from dpone.runtime.normalization.explosion import ExplosionGuard
from dpone.runtime.normalization.normalizer_utils import identity_row, with_identity_columns
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.table_builder import NormalizedTableBuilder


def append_scalar_child_row(
    *,
    builders: OrderedDict[str, list[dict[str, object]]],
    item: object,
    table: str,
    field_path: str,
    row_path: str,
    parent_row_id: str,
    root_row_id: str,
    root_table: str,
    root_source_row: Mapping[str, object],
    load_id: str,
    loaded_at: str,
    options: NestedNormalizationOptions,
    guard: ExplosionGuard,
    list_index: int,
    identity_service: LineageIdentityService,
    table_builder: NormalizedTableBuilder,
) -> None:
    child_unique_key = options.path_policies.unique_key_for(field_path)
    row = with_identity_columns({options.scalar_list_value_column: item}, root_source_row, child_unique_key)
    table_builder.append_row(
        builders,
        table=table,
        row=row,
        row_id=identity_service.row_id(
            source_type="nested",
            source_schema=root_table,
            source_table=table,
            row=identity_row(row, root_source_row, child_unique_key),
            unique_key=child_unique_key or None,
            row_path=row_path,
        ),
        parent_row_id=parent_row_id,
        root_row_id=root_row_id,
        list_index=list_index,
        load_id=load_id,
        loaded_at=loaded_at,
        guard=guard,
    )
