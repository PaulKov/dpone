"""DTOs and ports for route capability certification."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Protocol

SCHEMA_VERSION = "dpone.route_capability_certification.v1"
DEFAULT_BENCHMARK_ROUTES = (
    "typed_raw_streaming",
    "typed_binary_streaming",
    "direct_push_columnar",
    "object_storage_pull_s3",
    "object_storage_pull_s3cluster",
)
PREFLIGHT_ROUTES = ("object_storage_pull_s3cluster", "object_storage_pull_s3", "direct_push_columnar")
ROUTE_TARGET_PREFIX = "__dpone_cert"
SECRET_MARKERS = ("password", "secret", "token", "credential", "access_key", "presigned")


@dataclass(frozen=True, slots=True)
class RouteCapabilityCertificationRequest:
    manifest_path: Path
    scenario: str
    output_dir: Path
    run_id: str = "route-capability-certification"
    routes: tuple[str, ...] | None = None
    keep_artifacts: bool = False
    object_prefix: str | None = None
    target_schema: str = "DWH_Raw"
    selector: str | None = None


@dataclass(frozen=True, slots=True)
class CertificationRouteRequest:
    manifest_path: Path
    scenario: str
    route_id: str
    run_id: str
    target_schema: str
    target_table: str
    object_prefix: str
    keep_artifacts: bool
    selector: str | None = None


@dataclass(frozen=True, slots=True)
class CertificationRouteRun:
    route_id: str
    passed: bool
    run_summary: dict[str, object]
    route_decisions: list[dict[str, object]]
    load_steps: list[dict[str, object]]
    source_quality: dict[str, object]
    target_quality: dict[str, object]
    cleanup: dict[str, object]
    duration_seconds: float
    warnings: list[str] = field(default_factory=list)
    blockers: list[str] = field(default_factory=list)


@dataclass(frozen=True, slots=True)
class RouteCapabilityCertificationReport:
    scenario: str
    manifest_path: str
    output_dir: str
    passed: bool
    preferred_route_id: str | None
    blockers: list[str]
    warnings: list[str]
    safe_overrides: dict[str, object]
    routes: list[dict[str, object]]
    quality_report: dict[str, object]
    artifact_index: dict[str, str]

    def to_dict(self) -> dict[str, object]:
        return {
            "schema_version": SCHEMA_VERSION,
            "scenario": self.scenario,
            "manifest_path": self.manifest_path,
            "output_dir": self.output_dir,
            "passed": self.passed,
            "evidence_status": "UNVERIFIED",
            "preferred_route_id": self.preferred_route_id,
            "blockers": self.blockers,
            "warnings": self.warnings,
            "safe_overrides": self.safe_overrides,
            "routes": self.routes,
            "quality_report": self.quality_report,
            "artifact_index": self.artifact_index,
        }

    def to_markdown(self) -> str:
        lines = [
            "# Route capability certification",
            "",
            f"- Scenario: `{self.scenario}`",
            f"- Manifest: `{self.manifest_path}`",
            f"- Passed: `{self.passed}`",
            "- Evidence status: `UNVERIFIED`",
            f"- Preferred route: `{self.preferred_route_id or ''}`",
            "",
            "| route | passed | duration_seconds | blockers |",
            "|---|---:|---:|---|",
        ]
        for route in self.routes:
            blockers = ", ".join(str(item) for item in route.get("blockers", []))
            lines.append(
                f"| `{route['route_id']}` | `{route['passed']}` | "
                f"`{route.get('duration_seconds', '')}` | `{blockers}` |"
            )
        return "\n".join(lines) + "\n"


class RouteCertificationRunner(Protocol):
    def preflight(self, request: CertificationRouteRequest) -> Mapping[str, object]:
        """Run route capability probes without source IO."""

    def run_route(self, request: CertificationRouteRequest) -> CertificationRouteRun:
        """Execute one route and return runtime evidence."""


__all__ = [
    "DEFAULT_BENCHMARK_ROUTES",
    "PREFLIGHT_ROUTES",
    "ROUTE_TARGET_PREFIX",
    "SCHEMA_VERSION",
    "SECRET_MARKERS",
    "CertificationRouteRequest",
    "CertificationRouteRun",
    "RouteCapabilityCertificationReport",
    "RouteCapabilityCertificationRequest",
    "RouteCertificationRunner",
]
