"""Durable local job state for orchestrated dpone runs."""

from __future__ import annotations

import json
import os
import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol


@dataclass(frozen=True, slots=True)
class OrchestrationJobState:
    run_id: str
    process: str
    manifest_path: str
    selector: str | None
    lock_key: str
    status: str
    started_at: str
    updated_at: str
    attempt: int
    transitions: tuple[str, ...]
    blockers: tuple[str, ...]
    run_result: Mapping[str, object]
    output_dir: str
    state_path: str
    resumable: bool

    def to_dict(self) -> dict[str, object]:
        payload = asdict(self)
        payload["transitions"] = list(self.transitions)
        payload["blockers"] = list(self.blockers)
        payload["run_result"] = dict(self.run_result)
        return payload

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"


class JobStateStore(Protocol):
    def get(self, run_id: str) -> OrchestrationJobState | None: ...

    def mark_started(
        self,
        *,
        run_id: str,
        process: str,
        manifest_path: str | Path,
        selector: str | None,
        lock_key: str,
        output_dir: str | Path,
    ) -> OrchestrationJobState: ...

    def mark_running(self, *, run_id: str, attempt: int) -> OrchestrationJobState: ...

    def mark_committed(self, *, run_id: str, run_result: Mapping[str, object]) -> OrchestrationJobState: ...

    def mark_failed(
        self,
        *,
        run_id: str,
        blockers: tuple[str, ...],
        run_result: Mapping[str, object],
    ) -> OrchestrationJobState: ...

    def mark_blocked(self, *, run_id: str, blockers: tuple[str, ...]) -> OrchestrationJobState: ...


class NoopJobStateStore:
    """No-op adapter for callers that do not need durable orchestration state."""

    def get(self, run_id: str) -> OrchestrationJobState | None:
        del run_id
        return None

    def mark_started(
        self,
        *,
        run_id: str,
        process: str,
        manifest_path: str | Path,
        selector: str | None,
        lock_key: str,
        output_dir: str | Path,
    ) -> OrchestrationJobState:
        now = _utc_now()
        return OrchestrationJobState(
            run_id=run_id,
            process=process,
            manifest_path=str(manifest_path),
            selector=selector,
            lock_key=lock_key,
            status="started",
            started_at=now,
            updated_at=now,
            attempt=0,
            transitions=("started",),
            blockers=tuple(),
            run_result={},
            output_dir=str(output_dir),
            state_path="",
            resumable=False,
        )

    def mark_running(self, *, run_id: str, attempt: int) -> OrchestrationJobState:
        return self._state(run_id=run_id, status="running", attempt=attempt)

    def mark_committed(self, *, run_id: str, run_result: Mapping[str, object]) -> OrchestrationJobState:
        return self._state(run_id=run_id, status="committed", run_result=run_result)

    def mark_failed(
        self,
        *,
        run_id: str,
        blockers: tuple[str, ...],
        run_result: Mapping[str, object],
    ) -> OrchestrationJobState:
        return self._state(run_id=run_id, status="failed", blockers=blockers, run_result=run_result, resumable=True)

    def mark_blocked(self, *, run_id: str, blockers: tuple[str, ...]) -> OrchestrationJobState:
        return self._state(run_id=run_id, status="blocked", blockers=blockers, resumable=True)

    @staticmethod
    def _state(
        *,
        run_id: str,
        status: str,
        attempt: int = 0,
        blockers: tuple[str, ...] = tuple(),
        run_result: Mapping[str, object] | None = None,
        resumable: bool = False,
    ) -> OrchestrationJobState:
        now = _utc_now()
        return OrchestrationJobState(
            run_id=run_id,
            process="",
            manifest_path="",
            selector=None,
            lock_key="",
            status=status,
            started_at=now,
            updated_at=now,
            attempt=attempt,
            transitions=(status,),
            blockers=blockers,
            run_result=run_result or {},
            output_dir="",
            state_path="",
            resumable=resumable,
        )


class LocalJobStateStore:
    """JSON-backed state store for one local scheduler/worker workspace."""

    def __init__(self, state_dir: str | Path) -> None:
        self._state_dir = Path(state_dir)

    @property
    def state_dir(self) -> str:
        return str(self._state_dir)

    def get(self, run_id: str) -> OrchestrationJobState | None:
        path = self._path(run_id)
        if not path.exists():
            return None
        return self._from_payload(self._payload(path), state_path=path)

    def mark_started(
        self,
        *,
        run_id: str,
        process: str,
        manifest_path: str | Path,
        selector: str | None,
        lock_key: str,
        output_dir: str | Path,
    ) -> OrchestrationJobState:
        now = _utc_now()
        previous = self.get(run_id)
        state = OrchestrationJobState(
            run_id=run_id,
            process=process,
            manifest_path=str(manifest_path),
            selector=selector,
            lock_key=lock_key,
            status="started",
            started_at=previous.started_at if previous else now,
            updated_at=now,
            attempt=0,
            transitions=self._append_transition(previous, "started"),
            blockers=tuple(),
            run_result={},
            output_dir=str(output_dir),
            state_path=str(self._path(run_id)),
            resumable=False,
        )
        return self._write(state)

    def mark_running(self, *, run_id: str, attempt: int) -> OrchestrationJobState:
        return self._transition(run_id=run_id, status="running", attempt=attempt, resumable=False)

    def mark_committed(self, *, run_id: str, run_result: Mapping[str, object]) -> OrchestrationJobState:
        return self._transition(run_id=run_id, status="committed", run_result=run_result, resumable=False)

    def mark_failed(
        self,
        *,
        run_id: str,
        blockers: tuple[str, ...],
        run_result: Mapping[str, object],
    ) -> OrchestrationJobState:
        return self._transition(
            run_id=run_id,
            status="failed",
            blockers=blockers,
            run_result=run_result,
            resumable=True,
        )

    def mark_blocked(self, *, run_id: str, blockers: tuple[str, ...]) -> OrchestrationJobState:
        return self._transition(run_id=run_id, status="blocked", blockers=blockers, resumable=True)

    def _transition(
        self,
        *,
        run_id: str,
        status: str,
        attempt: int | None = None,
        blockers: tuple[str, ...] = tuple(),
        run_result: Mapping[str, object] | None = None,
        resumable: bool,
    ) -> OrchestrationJobState:
        previous = self.get(run_id)
        if previous is None:
            now = _utc_now()
            previous = OrchestrationJobState(
                run_id=run_id,
                process="",
                manifest_path="",
                selector=None,
                lock_key="",
                status="created",
                started_at=now,
                updated_at=now,
                attempt=0,
                transitions=tuple(),
                blockers=tuple(),
                run_result={},
                output_dir="",
                state_path=str(self._path(run_id)),
                resumable=False,
            )
        state = OrchestrationJobState(
            run_id=previous.run_id,
            process=previous.process,
            manifest_path=previous.manifest_path,
            selector=previous.selector,
            lock_key=previous.lock_key,
            status=status,
            started_at=previous.started_at,
            updated_at=_utc_now(),
            attempt=attempt if attempt is not None else previous.attempt,
            transitions=self._append_transition(previous, status),
            blockers=blockers,
            run_result=run_result or previous.run_result,
            output_dir=previous.output_dir,
            state_path=str(self._path(run_id)),
            resumable=resumable,
        )
        return self._write(state)

    def _write(self, state: OrchestrationJobState) -> OrchestrationJobState:
        self._state_dir.mkdir(parents=True, exist_ok=True)
        path = self._path(state.run_id)
        tmp_path = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
        tmp_path.write_text(state.to_json(), encoding="utf-8")
        tmp_path.replace(path)
        return state

    def _path(self, run_id: str) -> Path:
        safe_run_id = re.sub(r"[^A-Za-z0-9_.-]+", "_", run_id).strip("._") or "default"
        return self._state_dir / f"{safe_run_id}.job_state.json"

    @staticmethod
    def _append_transition(previous: OrchestrationJobState | None, status: str) -> tuple[str, ...]:
        if previous is None:
            return (status,)
        if previous.transitions and previous.transitions[-1] == status:
            return previous.transitions
        return (*previous.transitions, status)

    @staticmethod
    def _payload(path: Path) -> dict[str, Any]:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (json.JSONDecodeError, OSError):
            return {}
        return payload if isinstance(payload, dict) else {}

    @staticmethod
    def _from_payload(payload: Mapping[str, Any], *, state_path: Path) -> OrchestrationJobState:
        return OrchestrationJobState(
            run_id=str(payload.get("run_id", "")),
            process=str(payload.get("process", "")),
            manifest_path=str(payload.get("manifest_path", "")),
            selector=payload.get("selector") if isinstance(payload.get("selector"), str) else None,
            lock_key=str(payload.get("lock_key", "")),
            status=str(payload.get("status", "unknown")),
            started_at=str(payload.get("started_at", "")),
            updated_at=str(payload.get("updated_at", "")),
            attempt=int(payload.get("attempt", 0)),
            transitions=tuple(str(item) for item in payload.get("transitions", []) if isinstance(item, str)),
            blockers=tuple(str(item) for item in payload.get("blockers", []) if isinstance(item, str)),
            run_result=payload.get("run_result", {}) if isinstance(payload.get("run_result"), Mapping) else {},
            output_dir=str(payload.get("output_dir", "")),
            state_path=str(state_path),
            resumable=bool(payload.get("resumable", False)),
        )


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()  # noqa: UP017
