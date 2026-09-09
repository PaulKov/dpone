"""LoadConfig derivation for normalized nested tables."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace
from typing import TYPE_CHECKING

from dpone.runtime.normalization import NestedNormalizationOptions

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


def table_load_config(
    load_config: LoadConfig,
    table_name: str,
    nested_options: NestedNormalizationOptions,
    *,
    resolved_child_unique_key: Sequence[str] | None = None,
) -> LoadConfig:
    """Return a root or child table load configuration."""

    if table_name == load_config.target_table:
        return load_config
    child_unique_key = (
        tuple(resolved_child_unique_key or ())
        or nested_options.path_policies.unique_key_for_table(table_name)
        or ("__dpone__row_id",)
    )
    return replace(
        load_config,
        target_table=table_name,
        unique_key=list(child_unique_key),
        options={
            **(load_config.options or {}),
            "_dpone_normalized_child_table": True,
            "_dpone_nested_child_unique_key": list(child_unique_key),
            "_dpone_nested_delete_policy": nested_options.path_policies.delete_policy_for_table(table_name),
        },
    )
