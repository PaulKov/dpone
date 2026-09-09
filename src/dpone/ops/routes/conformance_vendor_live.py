"""Vendor-live Route Conformance adapters built on route bindings."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Protocol

from dpone.ops.routes.conformance_live_models import RouteConformanceLiveConfig, RouteConformanceLiveStep
from dpone.ops.routes.conformance_models import (
    RouteConformanceColumn,
    RouteConformanceDataset,
    RouteConformanceSnapshot,
)
from dpone.ops.routes.models import RouteKey

TRUTHY = {"1", "true", "yes", "on"}


class RouteConformanceLiveStore(Protocol):
    """Minimal store contract used by vendor-live conformance adapters."""

    @property
    def backend(self) -> str: ...

    def reset(self, *, dataset_id: str, columns: Sequence[RouteConformanceColumn]) -> None: ...

    def seed(
        self,
        *,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int: ...

    def add_column(self, *, dataset_id: str, column: RouteConformanceColumn, default_value: object) -> None: ...

    def read_rows(
        self,
        *,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> tuple[Mapping[str, object], ...]: ...

    def replace_rows(
        self,
        *,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int: ...

    def read_snapshot(
        self,
        *,
        dataset_id: str,
        kind: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> RouteConformanceSnapshot: ...


class MemoryRouteConformanceLiveStore:
    """Deterministic store for unit tests and local adapter development."""

    def __init__(self, backend: str) -> None:
        self._backend = backend
        self._tables: dict[str, tuple[tuple[RouteConformanceColumn, ...], tuple[Mapping[str, object], ...]]] = {}

    @property
    def backend(self) -> str:
        return self._backend

    def reset(self, *, dataset_id: str, columns: Sequence[RouteConformanceColumn]) -> None:
        self._tables[dataset_id] = (tuple(columns), tuple())

    def seed(
        self,
        *,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int:
        self._tables[dataset_id] = (tuple(columns), _normalize_rows(rows, columns=columns))
        return len(rows)

    def add_column(self, *, dataset_id: str, column: RouteConformanceColumn, default_value: object) -> None:
        columns, rows = self._tables[dataset_id]
        if any(item.name == column.name for item in columns):
            return
        evolved_rows = tuple({**dict(row), column.name: default_value} for row in rows)
        self._tables[dataset_id] = ((*columns, column), evolved_rows)

    def read_rows(
        self,
        *,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> tuple[Mapping[str, object], ...]:
        _, rows = self._tables[dataset_id]
        return _normalize_rows(rows, columns=columns)

    def replace_rows(
        self,
        *,
        dataset_id: str,
        columns: Sequence[RouteConformanceColumn],
        rows: Sequence[Mapping[str, object]],
    ) -> int:
        normalized = _normalize_rows(rows, columns=columns)
        self._tables[dataset_id] = (tuple(columns), normalized)
        return len(normalized)

    def read_snapshot(
        self,
        *,
        dataset_id: str,
        kind: str,
        columns: Sequence[RouteConformanceColumn],
    ) -> RouteConformanceSnapshot:
        rows = self.read_rows(dataset_id=dataset_id, columns=columns)
        return RouteConformanceSnapshot(
            kind=kind,
            columns=tuple(columns),
            rows=rows,
            fingerprint=_fingerprint(columns=columns, rows=rows),
        )


@dataclass(frozen=True, slots=True)
class VendorLiveRouteBinding:
    """Route-specific store binding kept outside the live service and CLI."""

    source: str
    sink: str
    strategy: str
    source_store: RouteConformanceLiveStore
    sink_store: RouteConformanceLiveStore
    native_path: str = "vendor_live_store_copy"

    def matches(self, route: RouteKey) -> bool:
        return self.source == route.source and self.sink == route.sink and self.strategy == route.strategy


@dataclass(frozen=True, slots=True)
class _RunState:
    binding: VendorLiveRouteBinding
    dataset_id: str
    columns: tuple[RouteConformanceColumn, ...]


class VendorRouteConformanceLiveAdapter:
    """Run route conformance through live stores selected by route bindings."""

    def __init__(
        self,
        *,
        bindings: Sequence[VendorLiveRouteBinding],
        require_opt_in: bool = True,
        opt_in_var: str = "DPONE_VENDOR_LIVE",
        environ: Mapping[str, str] | None = None,
    ) -> None:
        self._bindings = tuple(bindings)
        self._require_opt_in = require_opt_in
        self._opt_in_var = opt_in_var
        self._environ = environ
        self._states: dict[str, _RunState] = {}

    def seed_source(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep:
        opt_in_blocker = self._opt_in_blocker()
        if opt_in_blocker:
            return _blocked_step("seed_source", "vendor-live opt-in is missing", opt_in_blocker)
        binding = self._binding_for(route)
        if binding is None:
            return _blocked_step(
                "seed_source",
                f"vendor-live route binding is missing for {route.colon_id}",
                f"vendor_live.binding_missing:{route.colon_id}",
            )
        dataset_id = _dataset_id(route, config)
        try:
            binding.source_store.seed(dataset_id=dataset_id, columns=dataset.columns, rows=dataset.rows)
            binding.sink_store.reset(dataset_id=dataset_id, columns=dataset.columns)
        except Exception as exc:  # pragma: no cover - external vendor endpoint failure
            return _blocked_step(
                "seed_source",
                f"vendor-live source seed failed for {route.case_id}",
                f"vendor_live.seed_source.failed:{type(exc).__name__}",
            )
        self._states[route.case_id] = _RunState(
            binding=binding,
            dataset_id=dataset_id,
            columns=tuple(dataset.columns),
        )
        return RouteConformanceLiveStep(
            name="seed_source",
            status="verified",
            summary=f"seeded {binding.source_store.backend} source and reset {binding.sink_store.backend} sink",
            rows=len(dataset.rows),
        )

    def apply_schema_evolution(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep:
        state = self._state_for(route)
        if state is None:
            return _blocked_step(
                "apply_schema_evolution",
                "vendor-live state is missing before schema evolution",
                f"vendor_live.state_missing:{route.case_id}",
            )
        if not config.require_schema_evolution and not dataset.profile.include_schema_evolution:
            return RouteConformanceLiveStep(
                name="apply_schema_evolution",
                status="verified",
                summary="schema evolution not requested",
                rows=0,
            )
        column = _evolved_column(state.columns)
        try:
            state.binding.source_store.add_column(
                dataset_id=state.dataset_id,
                column=column,
                default_value="0.0001000000",
            )
            state.binding.sink_store.add_column(dataset_id=state.dataset_id, column=column, default_value=None)
        except Exception as exc:  # pragma: no cover - external vendor endpoint failure
            return _blocked_step(
                "apply_schema_evolution",
                f"vendor-live schema evolution failed for {route.case_id}",
                f"vendor_live.schema_evolution.failed:{type(exc).__name__}",
            )
        self._states[route.case_id] = _RunState(
            binding=state.binding,
            dataset_id=state.dataset_id,
            columns=(*state.columns, column),
        )
        return RouteConformanceLiveStep(
            name="apply_schema_evolution",
            status="verified",
            summary="applied nullable decimal evolution to source and sink",
            rows=0,
        )

    def execute_route(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceLiveStep:
        del dataset, config
        state = self._state_for(route)
        if state is None:
            return _blocked_step(
                "execute_route",
                "vendor-live state is missing before route execution",
                f"vendor_live.state_missing:{route.case_id}",
            )
        try:
            rows = state.binding.source_store.read_rows(dataset_id=state.dataset_id, columns=state.columns)
            written = state.binding.sink_store.replace_rows(
                dataset_id=state.dataset_id,
                columns=state.columns,
                rows=rows,
            )
        except Exception as exc:  # pragma: no cover - external vendor endpoint failure
            return _blocked_step(
                "execute_route",
                f"vendor-live route execution failed for {route.case_id}",
                f"vendor_live.execute_route.failed:{type(exc).__name__}",
            )
        return RouteConformanceLiveStep(
            name="execute_route",
            status="verified",
            summary=f"copied rows through {state.binding.native_path}",
            rows=written,
        )

    def read_source_snapshot(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceSnapshot:
        del dataset, config
        state = self._required_state(route)
        return state.binding.source_store.read_snapshot(
            dataset_id=state.dataset_id,
            kind="live_source",
            columns=state.columns,
        )

    def read_sink_snapshot(
        self,
        *,
        route: RouteKey,
        dataset: RouteConformanceDataset,
        config: RouteConformanceLiveConfig,
    ) -> RouteConformanceSnapshot:
        del dataset, config
        state = self._required_state(route)
        return state.binding.sink_store.read_snapshot(
            dataset_id=state.dataset_id,
            kind="live_sink",
            columns=state.columns,
        )

    def _binding_for(self, route: RouteKey) -> VendorLiveRouteBinding | None:
        return next((binding for binding in self._bindings if binding.matches(route)), None)

    def _state_for(self, route: RouteKey) -> _RunState | None:
        return self._states.get(route.case_id)

    def _required_state(self, route: RouteKey) -> _RunState:
        state = self._state_for(route)
        if state is None:
            raise RuntimeError(f"Vendor-live state is missing for {route.case_id}")
        return state

    def _opt_in_blocker(self) -> str | None:
        if not self._require_opt_in:
            return None
        source = self._environ
        if source is None:
            import os

            source = os.environ
        if str(source.get(self._opt_in_var, "0")).strip().lower() in TRUTHY:
            return None
        return f"vendor_live.opt_in_missing:{self._opt_in_var}"


def _dataset_id(route: RouteKey, config: RouteConformanceLiveConfig) -> str:
    identity = _safe_identifier(
        f"{route.source}_{route.sink}_{route.strategy}_{config.dataset.seed}_"
        f"{config.dataset.row_count}_{config.dataset.column_count}"
    )
    digest = hashlib.sha256(f"{route.colon_id}:{config.dataset.to_dict()}".encode()).hexdigest()[:10]
    return f"dpone_rc.{identity[:64]}_{digest}"


def _evolved_column(columns: Sequence[RouteConformanceColumn]) -> RouteConformanceColumn:
    return RouteConformanceColumn(
        name="evolved_score",
        logical_type="decimal",
        physical_contract="decimal(38,10)",
        nullable=True,
        ordinal=len(columns),
    )


def _normalize_rows(
    rows: Sequence[Mapping[str, object]],
    *,
    columns: Sequence[RouteConformanceColumn],
) -> tuple[Mapping[str, object], ...]:
    return tuple({column.name: _normalize_value(row.get(column.name)) for column in columns} for row in rows)


def _normalize_value(value: object) -> object:
    if isinstance(value, dict | list):
        return json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return value


def _fingerprint(*, columns: Sequence[RouteConformanceColumn], rows: Sequence[Mapping[str, object]]) -> str:
    payload = {
        "columns": [column.to_dict() for column in columns],
        "rows": [dict(row) for row in rows],
    }
    data = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(data.encode("utf-8")).hexdigest()


def _blocked_step(name: str, summary: str, blocker: str) -> RouteConformanceLiveStep:
    return RouteConformanceLiveStep(name=name, status="blocked", summary=summary, blockers=(blocker,))


def _safe_identifier(value: str) -> str:
    return re.sub(r"[^a-zA-Z0-9_]+", "_", value).strip("_").lower()


__all__ = [
    "MemoryRouteConformanceLiveStore",
    "RouteConformanceLiveStore",
    "VendorLiveRouteBinding",
    "VendorRouteConformanceLiveAdapter",
]
