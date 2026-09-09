"""Immutable models for explicit Airflow authoring-mode migration."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any, Literal

AuthoringMode = Literal["classic", "flow", "folder"]
MigrationStatus = Literal["ready", "no_op", "blocked", "applied"]


@dataclass(frozen=True, slots=True)
class AuthoringMigrationIdentity:
    mode: AuthoringMode
    semantic_fingerprint: str
    path: str = ""
    sha256: str = ""

    def to_jsonable(self) -> dict[str, str]:
        payload = {"mode": self.mode, "semantic_fingerprint": self.semantic_fingerprint}
        if self.path:
            payload["path"] = self.path
        if self.sha256:
            payload["sha256"] = self.sha256
        return payload


@dataclass(frozen=True, slots=True)
class AuthoringMigrationChange:
    action: Literal["create", "modify", "no_op"]
    path: str
    before_sha256: str | None
    after_sha256: str
    unified_diff: str

    def to_jsonable(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AuthoringMigrationResult:
    mode: Literal["plan", "apply"]
    status: MigrationStatus
    plan_id: str
    source: AuthoringMigrationIdentity
    target: AuthoringMigrationIdentity
    changes: tuple[AuthoringMigrationChange, ...] = ()
    retained_files: tuple[str, ...] = ()
    warnings: tuple[str, ...] = ()
    errors: tuple[dict[str, Any], ...] = ()
    exit_code: int = 0

    @property
    def passed(self) -> bool:
        return not self.errors and self.status != "blocked"

    def to_jsonable(self) -> dict[str, Any]:
        return {
            "kind": "dpone.authoring-migration.v1",
            "schema": "dpone.authoring-migration.v1",
            "passed": self.passed,
            "mode": self.mode,
            "status": self.status,
            "plan_id": self.plan_id,
            "source": self.source.to_jsonable(),
            "target": self.target.to_jsonable(),
            "changes": [change.to_jsonable() for change in self.changes],
            "retained_files": list(self.retained_files),
            "warnings": list(self.warnings),
            "errors": list(self.errors),
        }


__all__ = [
    "AuthoringMigrationChange",
    "AuthoringMigrationIdentity",
    "AuthoringMigrationResult",
    "AuthoringMode",
    "MigrationStatus",
]
