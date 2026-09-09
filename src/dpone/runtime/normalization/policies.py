"""Per-path nested normalization policies."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Literal

from dpone.contracts.nested_paths import normalize_path

NormalizationPolicy = Literal["child_table", "preserve_json", "ignore", "quarantine"]
DeletePolicy = Literal["none", "replace_parent_children", "snapshot_reconcile"]

_VALID_POLICIES: set[str] = {"child_table", "preserve_json", "ignore", "quarantine"}
_VALID_DELETE_POLICIES: set[str] = {"none", "replace_parent_children", "snapshot_reconcile"}


@dataclass(frozen=True, slots=True)
class PathPolicy:
    """Policy applied to one logical nested field path."""

    path: str
    policy: NormalizationPolicy = "child_table"
    table: str | None = None
    unique_key: tuple[str, ...] = ()
    delete_policy: DeletePolicy = "none"


class PathPolicyResolver:
    """Resolve normalization policies for logical field paths."""

    def __init__(self, policies: list[PathPolicy] | None = None) -> None:
        self._policies = {normalize_path(item.path): item for item in policies or []}
        self._tables = {item.table: item for item in policies or [] if item.table}

    @classmethod
    def from_config(cls, config: Any) -> PathPolicyResolver:
        if not isinstance(config, dict):
            return cls()
        policies: list[PathPolicy] = []
        paths = config.get("paths")
        if isinstance(paths, dict):
            for path, value in paths.items():
                if isinstance(value, dict):
                    policies.append(_policy_from_mapping(path, value))
                else:
                    policies.append(PathPolicy(path=normalize_path(str(path)), policy=_parse_policy(value, path=path)))
        split_paths = config.get("split_paths")
        if isinstance(split_paths, list):
            for item in split_paths:
                if isinstance(item, dict) and item.get("path"):
                    policies.append(_policy_from_mapping(item["path"], item))
        return cls(policies)

    def policy_for(self, path: str) -> NormalizationPolicy:
        matched = self._match(path)
        return matched.policy if matched is not None else "child_table"

    def table_for(self, path: str) -> str | None:
        matched = self._match(path)
        return matched.table if matched is not None else None

    def unique_key_for(self, path: str) -> tuple[str, ...]:
        matched = self._match(path)
        return matched.unique_key if matched is not None else ()

    def unique_key_for_table(self, table: str) -> tuple[str, ...]:
        matched = self._tables.get(table)
        return matched.unique_key if matched is not None else ()

    def delete_policy_for_table(self, table: str) -> DeletePolicy:
        matched = self._tables.get(table)
        return matched.delete_policy if matched is not None else "none"

    def table_keys(self) -> dict[str, tuple[str, ...]]:
        return {table: policy.unique_key for table, policy in self._tables.items() if policy.unique_key}

    def _match(self, path: str) -> PathPolicy | None:
        normalized = normalize_path(path)
        if normalized in self._policies:
            return self._policies[normalized]
        parts = normalized.split(".") if normalized else []
        for index in range(len(parts) - 1, 0, -1):
            candidate = ".".join(parts[:index])
            if candidate in self._policies:
                return self._policies[candidate]
        return None


def _policy_from_mapping(path: object, value: dict[str, Any]) -> PathPolicy:
    table = value.get("table")
    return PathPolicy(
        path=normalize_path(str(path)),
        policy=_parse_policy(value.get("policy", "child_table"), path=path),
        table=str(table) if table else None,
        unique_key=_parse_unique_key(value.get("unique_key")),
        delete_policy=_parse_delete_policy(value.get("delete_policy", "none"), path=path),
    )


def _parse_policy(value: object, *, path: object) -> NormalizationPolicy:
    policy = str(value or "child_table").strip().lower()
    if policy not in _VALID_POLICIES:
        raise ValueError(f"Unsupported nested normalization policy `{policy}` for path `{path}`")
    return policy  # type: ignore[return-value]


def _parse_delete_policy(value: object, *, path: object) -> DeletePolicy:
    policy = str(value or "none").strip().lower()
    if policy not in _VALID_DELETE_POLICIES:
        raise ValueError(f"Unsupported nested delete policy `{policy}` for path `{path}`")
    return policy  # type: ignore[return-value]


def _parse_unique_key(value: object) -> tuple[str, ...]:
    if value is None or value == "":
        return ()
    if isinstance(value, str):
        return (value,)
    if isinstance(value, (list, tuple)):
        return tuple(str(item) for item in value)
    raise TypeError("nested path unique_key must be a string or list of strings")
