"""Route state promotion models and report contracts."""

from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from dpone.ops.routes.models import RouteKey

SCHEMA_VERSION = "dpone.route_state_promotion.v1"


@dataclass(frozen=True, slots=True)
class RouteCommitReceipt:
    """Durable sink commit receipt used to promote source state."""

    route: RouteKey
    dataset: str
    run_id: str
    proposed_state: str
    source_boundary: str
    sink_boundary: str
    idempotency_key: str
    commit_token: str
    target: str
    fencing_token: str
    rows_applied: int
    events_applied: int
    created_at: str

    def to_dict(self) -> dict[str, object]:
        return {
            "route": self.route.to_dict(),
            "dataset": self.dataset,
            "run_id": self.run_id,
            "proposed_state": self.proposed_state,
            "source_boundary": self.source_boundary,
            "sink_boundary": self.sink_boundary,
            "idempotency_key": self.idempotency_key,
            "commit_token": self.commit_token,
            "target": self.target,
            "fencing_token": self.fencing_token,
            "rows_applied": self.rows_applied,
            "events_applied": self.events_applied,
            "created_at": self.created_at,
        }


@dataclass(frozen=True, slots=True)
class RouteStateRecord:
    """Promoted source state for one route and dataset."""

    route: RouteKey
    dataset: str
    source_state: str
    source_boundary: str
    sink_boundary: str
    run_id: str
    idempotency_key: str
    fencing_token: str
    commit_token: str
    promoted_at: str
    version: int

    def to_dict(self) -> dict[str, object]:
        return {
            "route": self.route.to_dict(),
            "dataset": self.dataset,
            "source_state": self.source_state,
            "source_boundary": self.source_boundary,
            "sink_boundary": self.sink_boundary,
            "run_id": self.run_id,
            "idempotency_key": self.idempotency_key,
            "fencing_token": self.fencing_token,
            "commit_token": self.commit_token,
            "promoted_at": self.promoted_at,
            "version": self.version,
        }

    @classmethod
    def from_dict(cls, payload: Mapping[str, object]) -> RouteStateRecord:
        route_payload = payload.get("route", {})
        route = RouteKey.of(
            str(route_payload.get("source", "unknown")) if isinstance(route_payload, Mapping) else "unknown",
            str(route_payload.get("sink", "unknown")) if isinstance(route_payload, Mapping) else "unknown",
            str(route_payload.get("strategy", "unknown")) if isinstance(route_payload, Mapping) else "unknown",
        )
        return cls(
            route=route,
            dataset=str(payload.get("dataset", "")),
            source_state=str(payload.get("source_state", "")),
            source_boundary=str(payload.get("source_boundary", "")),
            sink_boundary=str(payload.get("sink_boundary", "")),
            run_id=str(payload.get("run_id", "")),
            idempotency_key=str(payload.get("idempotency_key", "")),
            fencing_token=str(payload.get("fencing_token", "")),
            commit_token=str(payload.get("commit_token", "")),
            promoted_at=str(payload.get("promoted_at", "")),
            version=_int_value(payload.get("version", 0)),
        )


@dataclass(frozen=True, slots=True)
class RouteStatePromotionReport:
    """Stable JSON/Markdown route state promotion report."""

    route: RouteKey
    dataset: str
    run_id: str
    passed: bool
    level: str
    blockers: tuple[str, ...]
    warnings: tuple[str, ...]
    next_actions: tuple[str, ...]
    receipt: RouteCommitReceipt
    previous_state: RouteStateRecord | None
    promoted_state: RouteStateRecord | None
    ledger_path: str
    state_path: str
    output_dir: str
    json_path: str
    markdown_path: str
    state_backend: str

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "route": self.route.to_dict(),
            "dataset": self.dataset,
            "run_id": self.run_id,
            "passed": self.passed,
            "level": self.level,
            "blockers": list(self.blockers),
            "warnings": list(self.warnings),
            "next_actions": list(self.next_actions),
            "receipt": self.receipt.to_dict(),
            "previous_state": self.previous_state.to_dict() if self.previous_state else None,
            "promoted_state": self.promoted_state.to_dict() if self.promoted_state else None,
            "ledger_path": self.ledger_path,
            "state_path": self.state_path,
            "output_dir": self.output_dir,
            "json_path": self.json_path,
            "markdown_path": self.markdown_path,
            "state_backend": self.state_backend,
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone route state promotion",
            "",
            f"- Route: `{self.route.case_id}`",
            f"- Dataset: `{self.dataset}`",
            f"- Run ID: `{self.run_id}`",
            f"- Passed: `{self.passed}`",
            f"- Level: `{self.level}`",
            f"- State backend: `{self.state_backend}`",
            f"- Proposed state: `{self.receipt.proposed_state}`",
            "",
            "## Receipt",
            "",
            f"- Source boundary: `{self.receipt.source_boundary}`",
            f"- Sink boundary: `{self.receipt.sink_boundary}`",
            f"- Commit token: `{self.receipt.commit_token}`",
            f"- Target: `{self.receipt.target}`",
            f"- Fencing token: `{self.receipt.fencing_token}`",
            "",
            "## Blockers",
            "",
        ]
        lines.extend(f"- `{item}`" for item in self.blockers)
        if not self.blockers:
            lines.append("- none")
        lines.extend(["", "## Warnings", ""])
        lines.extend(f"- `{item}`" for item in self.warnings)
        if not self.warnings:
            lines.append("- none")
        lines.extend(["", "## Operator runbook", ""])
        if self.next_actions:
            lines.extend(f"- {item}" for item in self.next_actions)
        else:
            lines.extend(
                [
                    "- Attach this promotion report to route readiness and release review.",
                    "- Record the matching `state_committed` route execution step after promotion.",
                ]
            )
        lines.append("")
        return "\n".join(lines)

    def write(self) -> None:
        Path(self.output_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_path).write_text(self.to_json(), encoding="utf-8")
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


def _int_value(value: object) -> int:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return 0
    return 0
