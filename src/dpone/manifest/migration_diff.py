from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from dpone.manifest.errors import ManifestConfigurationError


def deep_common_dict(dicts: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Returns the deep intersection (common subtree) of a list of dicts."""
    if not dicts:
        return {}

    first = dicts[0]
    if not isinstance(first, Mapping):
        return {}

    result: dict[str, Any] = {}
    keys = set(first.keys())
    for d in dicts[1:]:
        if isinstance(d, Mapping):
            keys &= set(d.keys())
        else:
            keys = set()
            break

    for k in sorted(keys):
        vals = [d.get(k) for d in dicts]
        if all(isinstance(v, Mapping) for v in vals):
            sub = deep_common_dict([v for v in vals if isinstance(v, Mapping)])
            sub = _strip_empty_dicts(sub)
            if sub:
                result[k] = sub
            continue
        if all(isinstance(v, list) for v in vals):
            if all(v == vals[0] for v in vals[1:]):
                result[k] = _deepcopy_yaml(vals[0])
            continue
        if all(v == vals[0] for v in vals[1:]):
            result[k] = _deepcopy_yaml(vals[0])

    return result


def deep_diff(value: Any, base: Any) -> Any:
    """Returns a deep diff object so that deep_merge(base, diff) ~ value."""
    if isinstance(value, Mapping) and isinstance(base, Mapping):
        out: dict[str, Any] = {}
        for k, v in value.items():
            if k not in base:
                out[k] = _deepcopy_yaml(v)
                continue
            sub = deep_diff(v, base.get(k))
            if sub is None:
                continue
            # drop empty dict diffs
            if isinstance(sub, Mapping) and not sub:
                continue
            out[k] = sub
        return out

    if isinstance(value, list) and isinstance(base, list):
        return None if value == base else _deepcopy_yaml(value)

    return None if value == base else _deepcopy_yaml(value)


def _strip_empty_dicts(obj: Any) -> Any:
    if isinstance(obj, Mapping):
        new: dict[str, Any] = {}
        for k, v in obj.items():
            v2 = _strip_empty_dicts(v)
            if isinstance(v2, Mapping) and not v2:
                continue
            new[k] = v2
        return new
    if isinstance(obj, list):
        return [_strip_empty_dicts(v) for v in obj]
    return obj


def _deepcopy_yaml(obj: Any) -> Any:
    # safe, simple deep copy for YAML-native types
    if isinstance(obj, Mapping):
        return {k: _deepcopy_yaml(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_deepcopy_yaml(v) for v in obj]
    return obj


def _ensure_required_defaults(
    defaults: dict[str, Any], sample: Mapping[str, Any], *, manifest_name: str
) -> dict[str, Any]:
    """Ensures the batch manifest has required root blocks.

    Batch compiler requires `defaults.sink` to exist (at least as a mapping).
    """
    out = dict(defaults)
    # sink must exist
    if "sink" not in out or not isinstance(out.get("sink"), Mapping):
        sink = sample.get("sink")
        if not isinstance(sink, Mapping):
            raise ManifestConfigurationError(f"Невозможно мигрировать: отсутствует sink в sample ({manifest_name})")
        out["sink"] = _deepcopy_yaml(sink)
    # source is not strictly required by compiler, but required by ETLProcessConfig
    if "source" not in out or not isinstance(out.get("source"), Mapping):
        source = sample.get("source")
        if not isinstance(source, Mapping):
            raise ManifestConfigurationError(f"Невозможно мигрировать: отсутствует source в sample ({manifest_name})")
        out["source"] = _deepcopy_yaml(source)
    return out


def _get_path(obj: Any, path: Sequence[str]) -> Any:
    cur = obj
    for p in path:
        if not isinstance(cur, Mapping):
            return None
        cur = cur.get(p)
    return cur


def _drop_path(obj: Any, path: Sequence[str]) -> None:
    if not path:
        return
    cur = obj
    for p in path[:-1]:
        if not isinstance(cur, dict):
            return
        nxt = cur.get(p)
        if not isinstance(nxt, dict):
            return
        cur = nxt
    if isinstance(cur, dict):
        cur.pop(path[-1], None)
