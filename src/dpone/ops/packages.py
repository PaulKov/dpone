"""Load package lifecycle and idempotency metadata."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from dpone._compat import StrEnum
from dpone.ops.ids import new_ulid, utc_now_iso


class LoadPackageStatus(StrEnum):
    STARTED = "started"
    STAGED = "staged"
    COMMITTED = "committed"
    FAILED = "failed"
    CANCELLED = "cancelled"


@dataclass(frozen=True, slots=True)
class LoadPackage:
    run_id: str
    load_id: str
    target: str
    chunk_id: str | None
    status: LoadPackageStatus
    started_at: str
    updated_at: str
    rows_staged: int = 0
    rows_loaded: int = 0
    artifact_uri: str | None = None
    state_after: dict[str, Any] = field(default_factory=dict)
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["status"] = self.status.value
        return payload

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> LoadPackage:
        raw = dict(payload)
        raw["status"] = LoadPackageStatus(raw["status"])
        return cls(**raw)


class LoadPackageService:
    """File-backed load package store for local OSS and CI workflows."""

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)

    def start(self, *, run_id: str, target: str, chunk_id: str | None = None) -> LoadPackage:
        now = utc_now_iso()
        package = LoadPackage(
            run_id=run_id,
            load_id=new_ulid(),
            target=target,
            chunk_id=chunk_id,
            status=LoadPackageStatus.STARTED,
            started_at=now,
            updated_at=now,
        )
        self._save(package)
        return package

    def mark_staged(self, load_id: str, *, rows_staged: int, artifact_uri: str) -> LoadPackage:
        package = self._load(load_id)
        return self._replace(
            package,
            status=LoadPackageStatus.STAGED,
            rows_staged=int(rows_staged),
            artifact_uri=artifact_uri,
        )

    def mark_committed(self, load_id: str, *, rows_loaded: int, state_after: dict[str, Any]) -> LoadPackage:
        package = self._load(load_id)
        return self._replace(
            package,
            status=LoadPackageStatus.COMMITTED,
            rows_loaded=int(rows_loaded),
            state_after=dict(state_after),
        )

    def mark_failed(self, load_id: str, *, error: str) -> LoadPackage:
        return self._replace(self._load(load_id), status=LoadPackageStatus.FAILED, error=error)

    def state_commit_payload(self, load_id: str) -> dict[str, Any]:
        package = self._load(load_id)
        if package.status is not LoadPackageStatus.COMMITTED:
            raise ValueError("State can be advanced only after load package status is committed.")
        return dict(package.state_after)

    def _replace(self, package: LoadPackage, **changes: Any) -> LoadPackage:
        payload = package.to_dict()
        payload.update(changes)
        payload["updated_at"] = utc_now_iso()
        updated = LoadPackage.from_dict(payload)
        self._save(updated)
        return updated

    def _save(self, package: LoadPackage) -> None:
        self._path(package.load_id).write_text(
            json.dumps(package.to_dict(), ensure_ascii=False, sort_keys=True, indent=2) + "\n",
            encoding="utf-8",
        )

    def _load(self, load_id: str) -> LoadPackage:
        return LoadPackage.from_dict(json.loads(self._path(load_id).read_text(encoding="utf-8")))

    def _path(self, load_id: str) -> Path:
        return self.directory / f"{load_id}.json"
