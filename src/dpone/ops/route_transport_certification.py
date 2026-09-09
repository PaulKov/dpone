from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.readiness.managed_planning import ExecutionPlanService
from dpone.runtime.native_transfer_route_models import ROUTE_CERTIFICATION_SCHEMA_VERSION
from dpone.runtime.native_transfer_route_registry import route_capability_hash


@dataclass(frozen=True, slots=True)
class RouteTransportCertificationReport:
    payload: dict[str, Any]
    json_path: str
    markdown_path: str

    @property
    def passed(self) -> bool:
        return bool(self.payload.get("passed"))

    def to_dict(self) -> dict[str, Any]:
        return dict(self.payload)

    def to_markdown(self) -> str:
        route = self.payload["route"]
        transports = ", ".join(self.payload.get("certified_transports") or [])
        blockers = self.payload.get("blockers") or []
        lines = [
            "# Native transfer route certification",
            "",
            f"- route: `{route['source']} -> {route['sink']} ({route['strategy']})`",
            f"- profile: `{self.payload['profile']}`",
            f"- status: `{self.payload['status']}`",
            f"- certified_transports: `{transports}`",
            f"- release_gate: `{self.payload['decision'].get('release_gate')}`",
        ]
        if blockers:
            lines.append("- blockers:")
            lines.extend(f"  - `{item}`" for item in blockers)
        return "\n".join(lines) + "\n"

    def write(self) -> None:
        Path(self.json_path).write_text(
            json.dumps(self.payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        Path(self.markdown_path).write_text(self.to_markdown(), encoding="utf-8")


class RouteTransportCertificationService:
    """Writes route-level native-transfer certification evidence."""

    def __init__(self, planner: ExecutionPlanService | None = None) -> None:
        self._planner = planner or ExecutionPlanService()

    def certify(
        self,
        *,
        manifest: str | Path,
        artifact_dir: str | Path,
        profile: str = "static",
    ) -> RouteTransportCertificationReport:
        directory = Path(artifact_dir)
        directory.mkdir(parents=True, exist_ok=True)
        plan = self._planner.plan_manifest(manifest, route_certification_mode_override="advisory")
        decision = plan.get("native_transfer_route_decision") or {}
        route = decision.get("route") or {
            "source": plan["source"]["type"],
            "sink": plan["sink"]["type"],
            "strategy": plan["strategy"]["mode"],
        }
        certified = _certified_transports(decision)
        blockers = tuple(decision.get("blockers") or ()) if not certified else tuple()
        behavior_passed = bool(certified) and not blockers
        status = "unverified" if behavior_passed else "blocked"
        report_blockers = (
            ("native_transfer_route_certification.unverified_static_profile",) if behavior_passed else blockers
        )
        payload = {
            "schema_version": ROUTE_CERTIFICATION_SCHEMA_VERSION,
            "passed": False,
            "behavior_passed": behavior_passed,
            "evidence_status": "UNVERIFIED",
            "status": status,
            "profile": profile,
            "route": route,
            "certified_transports": certified,
            "codec": "tabseparated",
            "capability_hash": route_capability_hash(
                source=str(route["source"]),
                sink=str(route["sink"]),
                strategy=str(route["strategy"]),
                transports=certified,
            )
            if certified
            else "",
            "blockers": list(report_blockers),
            "decision": decision,
            "summary": f"native transfer route {status}",
        }
        report = RouteTransportCertificationReport(
            payload=payload,
            json_path=str(directory / "native_transfer_route_certification.json"),
            markdown_path=str(directory / "native_transfer_route_certification.md"),
        )
        report.write()
        return report


def _certified_transports(decision: dict[str, Any]) -> list[str]:
    selected = decision.get("selected_transport")
    if not selected:
        return []
    candidates = (decision.get("matrix") or {}).get("candidates") or []
    for candidate in candidates:
        if candidate.get("transport") == selected and candidate.get("technically_eligible"):
            return [str(selected)]
    return []


__all__ = ["RouteTransportCertificationReport", "RouteTransportCertificationService"]
