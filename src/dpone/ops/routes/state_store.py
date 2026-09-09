"""Route source-state stores for promotion decisions."""

from __future__ import annotations

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Protocol

from dpone.ops.routes.state_promotion_models import RouteStateRecord

_STATE_FILE = "route_state_store.json"


class RouteStateStore(Protocol):
    """Persistence port for promoted route source state."""

    @property
    def backend_name(self) -> str: ...

    def read_state(
        self,
        *,
        output_dir: str | Path,
        route: _RouteIdentity,
        dataset: str,
    ) -> RouteStateRecord | None: ...

    def write_state_if_version(
        self,
        *,
        output_dir: str | Path,
        route: _RouteIdentity,
        dataset: str,
        expected_version: int,
        record: RouteStateRecord,
    ) -> tuple[Path, bool]: ...

    def state_path(self, *, output_dir: str | Path, route: _RouteIdentity, dataset: str) -> Path: ...


class LocalRouteStateStore:
    """Persist promoted route source state in deterministic local JSON."""

    @property
    def backend_name(self) -> str:
        return "local_json"

    def read_state(
        self,
        *,
        output_dir: str | Path,
        route: _RouteIdentity,
        dataset: str,
    ) -> RouteStateRecord | None:
        states = self._read_states(self.state_path(output_dir=output_dir, route=route, dataset=dataset))
        value = states.get(_state_key(route=route, dataset=dataset))
        return RouteStateRecord.from_dict(value) if isinstance(value, Mapping) else None

    def write_state_if_version(
        self,
        *,
        output_dir: str | Path,
        route: _RouteIdentity,
        dataset: str,
        expected_version: int,
        record: RouteStateRecord,
    ) -> tuple[Path, bool]:
        path = self.state_path(output_dir=output_dir, route=route, dataset=dataset)
        states = self._read_states(path)
        key = _state_key(route=route, dataset=dataset)
        current = states.get(key)
        current_version = int(current.get("version", 0)) if isinstance(current, Mapping) else 0
        if current_version != expected_version:
            return path, False
        path.parent.mkdir(parents=True, exist_ok=True)
        states[key] = record.to_dict()
        path.write_text(
            json.dumps({"state_count": len(states), "states": states}, ensure_ascii=False, indent=2, sort_keys=True)
            + "\n",
            encoding="utf-8",
        )
        return path, True

    def state_path(self, *, output_dir: str | Path, route: _RouteIdentity, dataset: str) -> Path:
        del route, dataset
        return Path(output_dir) / _STATE_FILE

    @staticmethod
    def _read_states(path: Path) -> dict[str, object]:
        if not path.exists():
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        states = payload.get("states", {}) if isinstance(payload, Mapping) else {}
        return dict(states) if isinstance(states, Mapping) else {}


def _state_key(*, route: _RouteIdentity, dataset: str) -> str:
    return f"{route.case_id}:{dataset}"


class _RouteIdentity(Protocol):
    @property
    def case_id(self) -> str: ...
