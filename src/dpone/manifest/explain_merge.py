from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any

from .explain_models import ExplainChange
from .explain_utils import is_mapping, safe_equal


def deep_patch(
    before: Any,
    after: Any,
    *,
    _path: tuple[str, ...] = (),
    _append_list_keys: frozenset[str] = frozenset({"depends_on", "transforms"}),
) -> tuple[Any, list[str]]:
    removed: list[str] = []

    if is_mapping(before) and is_mapping(after):
        patch: dict[str, Any] = {}
        keys = sorted(set(before.keys()) | set(after.keys()), key=lambda x: str(x))
        for k in keys:
            key = str(k)
            if key not in after:
                removed.append(".".join((*_path, key)))
                continue
            if key not in before:
                patch[key] = copy.deepcopy(after[key])
                continue

            b = before.get(key)
            a = after.get(key)
            if safe_equal(b, a):
                continue

            if is_mapping(b) and is_mapping(a):
                child_patch, child_removed = deep_patch(
                    b,
                    a,
                    _path=(*_path, key),
                    _append_list_keys=_append_list_keys,
                )
                removed.extend(child_removed)
                if child_patch:
                    patch[key] = child_patch
                continue

            if isinstance(b, list) and isinstance(a, list):
                if len(_path) == 0 and key in _append_list_keys and len(b) <= len(a) and b == a[: len(b)]:
                    patch[key] = copy.deepcopy(a[len(b) :])
                else:
                    patch[key] = copy.deepcopy(a)
                continue

            patch[key] = copy.deepcopy(a)
        return patch, removed

    return copy.deepcopy(after), removed


def merge_with_origins(
    base: dict[str, Any],
    patch: dict[str, Any],
    *,
    origin: str,
    origins: dict[str, str],
    prefix: str,
) -> dict[str, Any]:
    return deep_merge_with_origin(base, patch, origin_label=origin, origin_map=origins, _prefix=prefix)


def deep_merge_with_origin(
    base: Any,
    patch: Any,
    *,
    origin_label: str,
    origin_map: dict[str, str],
    _prefix: str = "",
    _append_list_keys: frozenset[str] = frozenset({"depends_on", "transforms"}),
) -> Any:
    def clear_descendants(prefix: str) -> None:
        if not prefix:
            return
        for k in list(origin_map.keys()):
            if k.startswith(prefix + ".") or k.startswith(prefix + "["):
                origin_map.pop(k, None)

    def clear_list_items(prefix: str) -> None:
        if not prefix:
            return
        for k in list(origin_map.keys()):
            if k.startswith(prefix + "["):
                origin_map.pop(k, None)

    if is_mapping(base) and is_mapping(patch):
        result: dict[str, Any] = dict(base)
        for k, v in patch.items():
            key = str(k)
            p = f"{_prefix}.{key}" if _prefix else key
            if key in result:
                result[key] = deep_merge_with_origin(
                    result[key],
                    v,
                    origin_label=origin_label,
                    origin_map=origin_map,
                    _prefix=p,
                    _append_list_keys=_append_list_keys,
                )
            else:
                result[key] = copy.deepcopy(v)
                record_subtree_origins(v, origin_label=origin_label, origin_map=origin_map, prefix=p)
        return result

    if isinstance(base, list) and isinstance(patch, list):
        if _prefix and "." not in _prefix and _prefix in _append_list_keys:
            origin_map[_prefix] = origin_label
            start = len(base)
            out = [*base, *patch]
            for i, item in enumerate(patch, start=start):
                record_subtree_origins(
                    item,
                    origin_label=origin_label,
                    origin_map=origin_map,
                    prefix=f"{_prefix}[{i}]",
                )
            return out

        clear_list_items(_prefix)
        origin_map[_prefix] = origin_label
        out2 = copy.deepcopy(patch)
        for i, item in enumerate(out2):
            record_subtree_origins(
                item,
                origin_label=origin_label,
                origin_map=origin_map,
                prefix=f"{_prefix}[{i}]",
            )
        return out2

    clear_descendants(_prefix)
    origin_map[_prefix] = origin_label
    return copy.deepcopy(patch)


def record_subtree_origins(value: Any, *, origin_label: str, origin_map: dict[str, str], prefix: str) -> None:
    if prefix:
        origin_map[prefix] = origin_label
    if isinstance(value, list):
        for i, item in enumerate(value):
            ip = f"{prefix}[{i}]" if prefix else f"[{i}]"
            origin_map[ip] = origin_label
            if is_mapping(item):
                for k, v in item.items():
                    p = f"{ip}.{k}"
                    record_subtree_origins(v, origin_label=origin_label, origin_map=origin_map, prefix=p)
            elif isinstance(item, list):
                record_subtree_origins(item, origin_label=origin_label, origin_map=origin_map, prefix=ip)
        return
    if is_mapping(value):
        for k, v in value.items():
            p = f"{prefix}.{k}" if prefix else str(k)
            record_subtree_origins(v, origin_label=origin_label, origin_map=origin_map, prefix=p)


def normalize_list(value: Any) -> list[Any]:
    if value is None:
        return []
    if isinstance(value, list):
        return list(value)
    if isinstance(value, str | Mapping):
        return [value]
    return [value]


def diff(before: Any, after: Any, *, _prefix: str = "") -> list[ExplainChange]:
    changes: list[ExplainChange] = []

    if is_mapping(before) and is_mapping(after):
        keys = sorted(set(before.keys()) | set(after.keys()), key=lambda x: str(x))
        for k in keys:
            key = str(k)
            p = f"{_prefix}.{key}" if _prefix else key
            if key not in before:
                changes.append(ExplainChange(path=p, kind="add", before=None, after=copy.deepcopy(after[key])))
            elif key not in after:
                changes.append(ExplainChange(path=p, kind="remove", before=copy.deepcopy(before[key]), after=None))
            else:
                changes.extend(diff(before[key], after[key], _prefix=p))
        return changes

    if isinstance(before, list) and isinstance(after, list):
        if before == after:
            return []
        if len(before) <= len(after) and before == after[: len(before)]:
            for i in range(len(before), len(after)):
                changes.append(
                    ExplainChange(
                        path=f"{_prefix}[+{i}]",
                        kind="append",
                        before=None,
                        after=copy.deepcopy(after[i]),
                    )
                )
            return changes
        changes.append(
            ExplainChange(path=_prefix, kind="change", before=copy.deepcopy(before), after=copy.deepcopy(after))
        )
        return changes

    if before != after:
        changes.append(
            ExplainChange(path=_prefix, kind="change", before=copy.deepcopy(before), after=copy.deepcopy(after))
        )
    return changes


def mark_naming_origins(
    *,
    before: Mapping[str, Any],
    after: Mapping[str, Any],
    origin_map: dict[str, str],
    derived_fields: dict[str, str],
    selector: str,
    naming_origin: Mapping[str, str],
) -> None:
    def get_path(obj: Mapping[str, Any], path: str) -> Any:
        cur: Any = obj
        for part in path.split("."):
            if not isinstance(cur, Mapping) or part not in cur:
                return None
            cur = cur[part]
        return cur

    targets = {
        "sink.table.schema": "sink_dataset",
        "sink.table.name": "sink_table",
        "name": "process_name",
        "task_group": "task_group",
        "description": "description",
        "sink.options.table_description": "sink_table_description",
        "sink.options.table_labels": "labels",
    }

    for path, naming_key in targets.items():
        b = get_path(before, path)
        a = get_path(after, path)
        if b != a and a is not None:
            if naming_key == "sink_table_description" and naming_key not in naming_origin:
                tmpl_origin = naming_origin.get("table_description", "unknown")
                naming_key_eff = "table_description" if "table_description" in naming_origin else naming_key
            else:
                tmpl_origin = naming_origin.get(naming_key, "unknown")
                naming_key_eff = naming_key
            origin_map[path] = f"naming.{naming_key_eff}@{tmpl_origin}"
            derived_fields[path] = naming_key_eff


__all__ = [
    "deep_patch",
    "merge_with_origins",
    "deep_merge_with_origin",
    "record_subtree_origins",
    "normalize_list",
    "diff",
    "mark_naming_origins",
]
