# ruff: noqa: F822
"""Compatibility facade for legacy-to-batch manifest migration utilities."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "BatchPlan",
    "GroupKey",
    "LegacyProcess",
    "MigrationConfig",
    "MigrationPlan",
    "ProcessRef",
    "build_batch_manifest",
    "deep_common_dict",
    "deep_diff",
    "normalize_depends_on",
    "plan_migration",
    "relative_posix",
    "rewrite_depends_on",
    "run_migration",
    "_deepcopy_yaml",
    "_drop_path",
    "_ensure_required_defaults",
    "_get_path",
    "_group_processes",
    "_infer_dataset_vars_and_naming",
    "_load_legacy_file",
    "_ratio",
    "_scan_legacy_manifests",
    "_strip_empty_dicts",
    "_to_identifier",
    "_write_batch_yaml",
]

_EXPORTS: dict[str, str] = {
    "BatchPlan": "dpone.manifest.migration_models:BatchPlan",
    "GroupKey": "dpone.manifest.migration_models:GroupKey",
    "LegacyProcess": "dpone.manifest.migration_models:LegacyProcess",
    "MigrationConfig": "dpone.manifest.migration_models:MigrationConfig",
    "MigrationPlan": "dpone.manifest.migration_models:MigrationPlan",
    "ProcessRef": "dpone.manifest.migration_models:ProcessRef",
    "build_batch_manifest": "dpone.manifest.migration_builder:build_batch_manifest",
    "deep_common_dict": "dpone.manifest.migration_diff:deep_common_dict",
    "deep_diff": "dpone.manifest.migration_diff:deep_diff",
    "normalize_depends_on": "dpone.manifest.migration_dependencies:normalize_depends_on",
    "plan_migration": "dpone.manifest.migration_runner:plan_migration",
    "relative_posix": "dpone.manifest.migration_dependencies:relative_posix",
    "rewrite_depends_on": "dpone.manifest.migration_dependencies:rewrite_depends_on",
    "run_migration": "dpone.manifest.migration_runner:run_migration",
    "_deepcopy_yaml": "dpone.manifest.migration_diff:_deepcopy_yaml",
    "_drop_path": "dpone.manifest.migration_diff:_drop_path",
    "_ensure_required_defaults": "dpone.manifest.migration_diff:_ensure_required_defaults",
    "_get_path": "dpone.manifest.migration_diff:_get_path",
    "_group_processes": "dpone.manifest.migration_scanner:_group_processes",
    "_infer_dataset_vars_and_naming": "dpone.manifest.migration_naming:_infer_dataset_vars_and_naming",
    "_load_legacy_file": "dpone.manifest.migration_scanner:_load_legacy_file",
    "_ratio": "dpone.manifest.migration_naming:_ratio",
    "_scan_legacy_manifests": "dpone.manifest.migration_scanner:_scan_legacy_manifests",
    "_strip_empty_dicts": "dpone.manifest.migration_diff:_strip_empty_dicts",
    "_to_identifier": "dpone.manifest.migration_naming:_to_identifier",
    "_write_batch_yaml": "dpone.manifest.migration_writer:_write_batch_yaml",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":")
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value
