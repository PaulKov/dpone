"""Workload-scoped dependency closure for Airflow connection projections."""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from dpone.contracts.credential_resolution import is_valid_connection_ref

_CLOSED_PROJECTION_MODE = "kubernetes_secret_volume"
_OBJECT_STORAGE_AUTHORITIES = ("runtime_access", "clickhouse_write_access")
_DISABLED_STATE_TYPES = frozenset({"disabled", "noop", "none", "off"})
_SCALAR_OVERRIDE_FIELDS = ("scheme_overrides", "database_overrides")
_NESTED_OVERRIDE_FIELDS = ("query_overrides",)


class AirflowConnectionProjectionClosureError(ValueError):
    """A workload projection cannot satisfy its compiled runtime dependencies."""

    def __init__(self, code: str, message: str) -> None:
        self.code = code
        super().__init__(message)


def required_runtime_connection_refs(processes: Iterable[object]) -> tuple[str, ...]:
    """Return the exact logical refs selected by runtime connection authority.

    The paths intentionally mirror the runtime resolvers: direct connection
    authorities plus the active columnar fast-path ``runtime_access`` alias.
    ``state.reuse: sink`` contributes no new ref.
    """

    refs: set[str] = set()
    for process in processes:
        config = _mapping(getattr(process, "raw_config", None))
        for field in ("source", "sink", "bigquery_proxy"):
            _add_connection_ref(refs, _mapping(config.get(field)))
        _add_source_materialization_work_ref(refs, _mapping(config.get("source")))
        state = _mapping(config.get("state"))
        state_type = _text(state.get("type")).lower()
        if state_type not in _DISABLED_STATE_TYPES and not _text(state.get("reuse")):
            _add_connection_ref(refs, state)
        object_storage = _mapping(config.get("object_storage"))
        for field in _OBJECT_STORAGE_AUTHORITIES:
            _add_connection_ref(refs, _mapping(object_storage.get(field)))
        _add_object_storage_runtime_ref(refs, _columnar_object_storage(config))
    return tuple(sorted(refs))


def _add_source_materialization_work_ref(refs: set[str], source: Mapping[str, Any]) -> None:
    options = _mapping(source.get("options"))
    native_transfer = _mapping(options.get("native_transfer"))
    snapshot = _mapping(native_transfer.get("snapshot"))
    materialization = _mapping(snapshot.get("materialization"))
    value = _text(materialization.get("work_connection_ref"))
    if value:
        refs.add(value)


def close_connection_projection(
    projection: Mapping[str, Any],
    *,
    required_refs: Sequence[str],
) -> dict[str, Any]:
    """Restrict a closed operator bridge to one workload's logical refs.

    Required aliases resolve by exact logical, registry, then physical match.
    A shared physical ID is accepted only when a more specific alias identifies
    one entry. URI overrides use the same aliases and normalize to the physical
    key consumed by the Airflow operator.
    """

    closed = deepcopy(dict(projection))
    if str(closed.get("mode") or "") != _CLOSED_PROJECTION_MODE:
        return closed
    required = tuple(sorted({_text(item) for item in required_refs if _text(item)}))
    if not required:
        return {}

    entries = _connection_entries(closed.get("connections"))
    selected_indexes, missing = _resolve_required_entries(entries, required)
    if missing:
        raise AirflowConnectionProjectionClosureError(
            "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_MISSING",
            "Airflow connection projection is missing required connection aliases: "
            + ", ".join(_safe_alias(ref) for ref in missing)
            + ". Add a matching connection_projection.connections entry or correct the manifest authority alias.",
        )
    selected = tuple(entry for index, entry in enumerate(entries) if index in selected_indexes)
    _require_physical_ids(selected)

    closed["connections"] = [deepcopy(dict(entry)) for entry in selected]
    closed["connection_ids"] = list(_selected_connection_ids(closed.get("connection_ids"), selected))
    for field in _SCALAR_OVERRIDE_FIELDS:
        _replace_override_field(
            closed,
            field=field,
            values=_closed_scalar_overrides(field, closed.get(field), selected),
        )
    for field in _NESTED_OVERRIDE_FIELDS:
        _replace_override_field(
            closed,
            field=field,
            values=_closed_nested_overrides(field, closed.get(field), selected),
        )
    closed.pop("workspace_query_overrides", None)
    return closed


def _connection_entries(value: object) -> tuple[Mapping[str, Any], ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item for item in value if isinstance(item, Mapping))


def _resolve_required_entries(
    entries: tuple[Mapping[str, Any], ...],
    required_refs: tuple[str, ...],
) -> tuple[frozenset[int], tuple[str, ...]]:
    selected: set[int] = set()
    missing: list[str] = []
    for required_ref in required_refs:
        index = _resolve_required_entry(entries, required_ref)
        if index is None:
            missing.append(required_ref)
        else:
            selected.add(index)
    return frozenset(selected), tuple(missing)


def _resolve_required_entry(entries: tuple[Mapping[str, Any], ...], required_ref: str) -> int | None:
    alias_tiers = (
        ("logical", tuple(_logical_ref(entry) for entry in entries)),
        ("registry", tuple(_registry_ref(entry) for entry in entries)),
        ("physical", tuple(_physical_id(entry) for entry in entries)),
    )
    for tier, aliases in alias_tiers:
        matches = tuple(index for index, alias in enumerate(aliases) if alias == required_ref)
        if len(matches) > 1:
            raise AirflowConnectionProjectionClosureError(
                "DPONE_AIRFLOW_CONNECTION_PROJECTION_REF_AMBIGUOUS",
                "Airflow connection projection has multiple entries for required "
                f"{tier} alias {_safe_alias(required_ref)}. Use a unique logical or registry alias in the manifest.",
            )
        if matches:
            return matches[0]
    return None


def _require_physical_ids(entries: tuple[Mapping[str, Any], ...]) -> None:
    invalid = tuple(
        _safe_alias(_logical_ref(entry) or _registry_ref(entry)) for entry in entries if not _physical_id(entry)
    )
    if invalid:
        raise AirflowConnectionProjectionClosureError(
            "DPONE_AIRFLOW_CONNECTION_PROJECTION_ENTRY_INVALID",
            "Airflow connection projection entries require a physical connection_id: " + ", ".join(invalid),
        )


def _selected_connection_ids(
    configured: object,
    entries: tuple[Mapping[str, Any], ...],
) -> tuple[str, ...]:
    selected = tuple(dict.fromkeys(_physical_id(entry) for entry in entries))
    selected_set = set(selected)
    configured_ids = (
        tuple(_text(item) for item in configured if _text(item)) if isinstance(configured, list | tuple) else ()
    )
    ordered = tuple(item for item in configured_ids if item in selected_set)
    return tuple(dict.fromkeys((*ordered, *selected)))


def _closed_scalar_overrides(
    field: str,
    raw: object,
    entries: tuple[Mapping[str, Any], ...],
) -> dict[str, str]:
    overrides = _mapping(raw)
    closed: dict[str, str] = {}
    for physical_id, shared_entries in _entries_by_physical_id(entries).items():
        resolved = tuple(_scalar_override(overrides, entry) for entry in shared_entries)
        values = {value for value in resolved if value is not None}
        if len(values) > 1 or (values and any(value is None for value in resolved)):
            _raise_override_conflict(field, physical_id)
        if values:
            closed[physical_id] = values.pop()
    return closed


def _closed_nested_overrides(
    field: str,
    raw: object,
    entries: tuple[Mapping[str, Any], ...],
) -> dict[str, dict[str, str]]:
    overrides = _mapping(raw)
    closed: dict[str, dict[str, str]] = {}
    for physical_id, shared_entries in _entries_by_physical_id(entries).items():
        resolved = tuple(_nested_override(overrides, entry) for entry in shared_entries)
        canonical = {tuple(sorted(value.items())) for value in resolved}
        if len(canonical) > 1:
            _raise_override_conflict(field, physical_id)
        if resolved and resolved[0]:
            closed[physical_id] = resolved[0]
    return closed


def _scalar_override(overrides: Mapping[str, Any], entry: Mapping[str, Any]) -> str | None:
    selected: str | None = None
    for alias in _override_aliases(entry):
        value = overrides.get(alias)
        if value not in (None, ""):
            selected = str(value)
    return selected


def _nested_override(overrides: Mapping[str, Any], entry: Mapping[str, Any]) -> dict[str, str]:
    selected: dict[str, str] = {}
    for alias in _override_aliases(entry):
        values = overrides.get(alias)
        if not isinstance(values, Mapping):
            continue
        selected.update(
            {str(key): str(value) for key, value in values.items() if str(key).strip() and value not in (None, "")}
        )
    return selected


def _override_aliases(entry: Mapping[str, Any]) -> tuple[str, ...]:
    # Least-specific physical defaults are overlaid by registry and then
    # workload-logical aliases. Dedupe is required when aliases are identical.
    return tuple(
        dict.fromkeys(
            (
                _physical_id(entry),
                _registry_ref(entry),
                _logical_ref(entry),
            )
        )
    )


def _entries_by_physical_id(
    entries: tuple[Mapping[str, Any], ...],
) -> dict[str, tuple[Mapping[str, Any], ...]]:
    grouped: dict[str, list[Mapping[str, Any]]] = {}
    for entry in entries:
        grouped.setdefault(_physical_id(entry), []).append(entry)
    return {physical_id: tuple(items) for physical_id, items in grouped.items()}


def _replace_override_field(closed: dict[str, Any], *, field: str, values: Mapping[str, Any]) -> None:
    if values:
        closed[field] = dict(values)
    else:
        closed.pop(field, None)


def _raise_override_conflict(field: str, physical_id: str) -> None:
    raise AirflowConnectionProjectionClosureError(
        "DPONE_AIRFLOW_CONNECTION_PROJECTION_OVERRIDE_CONFLICT",
        f"Airflow {field} conflict for logical refs sharing connection_id: {_safe_alias(physical_id)}",
    )


def _add_connection_ref(refs: set[str], endpoint: Mapping[str, Any]) -> None:
    value = _text(endpoint.get("connection_ref"))
    if value:
        refs.add(value)


def _add_object_storage_runtime_ref(refs: set[str], object_storage: Mapping[str, Any]) -> None:
    runtime_access = _mapping(object_storage.get("runtime_access"))
    value = _text(runtime_access.get("connection_id")) or _text(runtime_access.get("connection_ref"))
    if value:
        refs.add(value)


def _columnar_object_storage(config: Mapping[str, Any]) -> Mapping[str, Any]:
    source_options = _mapping(_mapping(config.get("source")).get("options"))
    native_transfer = _mapping(source_options.get("native_transfer"))
    snapshot = _mapping(native_transfer.get("snapshot"))
    columnar = _mapping(snapshot.get("columnar_fast_path") or source_options.get("columnar_fast_path"))
    if _text(columnar.get("mode")).lower() in {"off", "benchmark_only"}:
        return {}
    return _mapping(columnar.get("object_storage"))


def _logical_ref(entry: Mapping[str, Any]) -> str:
    return _text(entry.get("connection_ref"))


def _registry_ref(entry: Mapping[str, Any]) -> str:
    return _text(entry.get("registry_connection_ref")) or _logical_ref(entry)


def _physical_id(entry: Mapping[str, Any]) -> str:
    return _text(entry.get("connection_id"))


def _safe_alias(value: str) -> str:
    return value if is_valid_connection_ref(value) else "<redacted-invalid-alias>"


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


__all__ = [
    "AirflowConnectionProjectionClosureError",
    "close_connection_projection",
    "required_runtime_connection_refs",
]
