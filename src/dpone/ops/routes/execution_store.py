"""Local JSON store for route execution ledgers and commit-fencing leases."""

from __future__ import annotations

import json
import re
from collections.abc import Mapping
from datetime import datetime, timezone
from pathlib import Path
from typing import Protocol

from dpone.ops.routes.execution_models import RouteExecutionLease, RouteExecutionStep
from dpone.ops.routes.models import RouteKey

_UTC = timezone.utc  # noqa: UP017
_RUNS_DIR = "runs"
_LEASES_FILE = "route_execution_leases.json"
_INDEX_FILE = "route_execution_index.json"


class RouteExecutionLedgerStore(Protocol):
    """Persistence port for route execution ledger stores."""

    @property
    def backend_name(self) -> str: ...

    def read_steps(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
    ) -> tuple[RouteExecutionStep, ...]: ...

    def write_steps(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
        steps: tuple[RouteExecutionStep, ...],
    ) -> Path: ...

    def append_steps_if_version(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
        expected_step_count: int,
        steps: tuple[RouteExecutionStep, ...],
    ) -> tuple[Path, bool]: ...

    def acquire_lease(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        owner: str,
        now: datetime,
        ttl_seconds: int,
    ) -> tuple[RouteExecutionLease, tuple[str, ...]]: ...

    def ledger_path(self, *, output_dir: str | Path, route: RouteKey, dataset: str, run_id: str) -> Path: ...


class LocalRouteExecutionLedgerStore:
    """Persist route execution ledger state in deterministic local JSON files."""

    @property
    def backend_name(self) -> str:
        return "local_json"

    def read_steps(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
    ) -> tuple[RouteExecutionStep, ...]:
        path = self.ledger_path(output_dir=output_dir, route=route, dataset=dataset, run_id=run_id)
        if not path.exists():
            return tuple()
        payload = self._read_json(path)
        steps = payload.get("steps", [])
        if not isinstance(steps, list):
            return tuple()
        return tuple(RouteExecutionStep.from_dict(item) for item in steps if isinstance(item, Mapping))

    def write_steps(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
        steps: tuple[RouteExecutionStep, ...],
    ) -> Path:
        path = self.ledger_path(output_dir=output_dir, route=route, dataset=dataset, run_id=run_id)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(
            json.dumps(
                {
                    "route": route.to_dict(),
                    "dataset": dataset,
                    "run_id": run_id,
                    "step_count": len(steps),
                    "steps": [step.to_dict() for step in steps],
                },
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )
        self._write_index(output_dir=output_dir, route=route, dataset=dataset, run_id=run_id, ledger_path=path)
        return path

    def append_steps_if_version(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
        expected_step_count: int,
        steps: tuple[RouteExecutionStep, ...],
    ) -> tuple[Path, bool]:
        path = self.ledger_path(output_dir=output_dir, route=route, dataset=dataset, run_id=run_id)
        current_steps = self.read_steps(output_dir=output_dir, route=route, dataset=dataset, run_id=run_id)
        if len(current_steps) != expected_step_count:
            return path, False
        return self.write_steps(output_dir=output_dir, route=route, dataset=dataset, run_id=run_id, steps=steps), True

    def acquire_lease(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        owner: str,
        now: datetime,
        ttl_seconds: int,
    ) -> tuple[RouteExecutionLease, tuple[str, ...]]:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        lease_key = self.lease_key(route=route, dataset=dataset)
        leases = self._read_leases(directory)
        existing = leases.get(lease_key)
        if existing and existing.owner != owner and _parse_datetime(existing.expires_at) > now:
            return existing, ("route_execution.lease_held_by_another_runner",)
        expires_at = now.timestamp() + max(ttl_seconds, 1)
        lease = RouteExecutionLease(
            lease_key=lease_key,
            owner=owner,
            acquired_at=now.isoformat(),
            expires_at=datetime.fromtimestamp(expires_at, _UTC).isoformat(),
            fencing_token=f"{lease_key}:{owner}:{int(now.timestamp())}",
        )
        leases[lease_key] = lease
        self._write_leases(directory, leases)
        return lease, tuple()

    def ledger_path(self, *, output_dir: str | Path, route: RouteKey, dataset: str, run_id: str) -> Path:
        return Path(output_dir) / _RUNS_DIR / f"{route.case_id}__{_safe(dataset)}__{_safe(run_id)}.json"

    @staticmethod
    def lease_key(*, route: RouteKey, dataset: str) -> str:
        return f"{route.case_id}:{dataset}"

    def _write_index(
        self,
        *,
        output_dir: str | Path,
        route: RouteKey,
        dataset: str,
        run_id: str,
        ledger_path: Path,
    ) -> None:
        path = Path(output_dir) / _INDEX_FILE
        payload = self._read_json(path)
        entries = payload.get("entries", [])
        if not isinstance(entries, list):
            entries = []
        normalized = [item for item in entries if isinstance(item, Mapping)]
        entry = {
            "route": route.to_dict(),
            "dataset": dataset,
            "run_id": run_id,
            "ledger_path": str(ledger_path),
        }
        filtered = [
            item
            for item in normalized
            if not (
                item.get("dataset") == dataset
                and item.get("run_id") == run_id
                and isinstance(item.get("route"), Mapping)
                and item["route"].get("case_id") == route.case_id
            )
        ]
        filtered.append(entry)
        path.write_text(
            json.dumps(
                {"entry_count": len(filtered), "entries": filtered}, ensure_ascii=False, indent=2, sort_keys=True
            )
            + "\n",
            encoding="utf-8",
        )

    def _read_leases(self, directory: Path) -> dict[str, RouteExecutionLease]:
        payload = self._read_json(directory / _LEASES_FILE)
        leases = payload.get("leases", {})
        if not isinstance(leases, Mapping):
            return {}
        return {
            str(key): RouteExecutionLease.from_dict(value)
            for key, value in leases.items()
            if isinstance(value, Mapping)
        }

    @staticmethod
    def _write_leases(directory: Path, leases: Mapping[str, RouteExecutionLease]) -> None:
        (directory / _LEASES_FILE).write_text(
            json.dumps(
                {"lease_count": len(leases), "leases": {key: lease.to_dict() for key, lease in leases.items()}},
                ensure_ascii=False,
                indent=2,
                sort_keys=True,
            )
            + "\n",
            encoding="utf-8",
        )

    @staticmethod
    def _read_json(path: Path) -> Mapping[str, object]:
        if not path.exists() or path.suffix.lower() != ".json":
            return {}
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return {}
        return payload if isinstance(payload, Mapping) else {}


def _safe(value: str) -> str:
    normalized = re.sub(r"[^a-zA-Z0-9_.-]+", "_", value.strip())
    return normalized.strip("_") or "unknown"


def _parse_datetime(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        return datetime.fromtimestamp(0, _UTC)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=_UTC)
