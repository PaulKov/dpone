"""Public value objects for the six-dimensional route certification matrix."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

SCHEMA_VERSION = "dpone.route-certification-matrix.v1"
ROUTE_STATUSES = (
    "experimental",
    "route-certified",
    "production-certified",
    "enterprise-certified",
)


class RouteCertificationMatrixError(ValueError):
    """Safe public failure raised while publishing a route matrix."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class RouteCertificationDimensions:
    """Stable identity of one concrete route variant."""

    source: str
    sink: str
    strategy: str
    transport: str
    schema_evolution: str
    airflow_runtime_mode: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteCertificationProof:
    """Safe normalized view of one explicit evidence directory."""

    evidence_set: str
    evidence_status: str
    certification_level: str
    release_id: str | None
    deployment_id: str | None
    environment: str | None
    signer_identity: str | None
    expires_at: str | None
    release_set_sha256: str | None
    certification_bundle_sha256: str | None
    attestation_sha256: str | None
    verification_sha256: str | None
    production_attempted: bool
    blockers: tuple[str, ...]

    @property
    def route_certified(self) -> bool:
        return self.evidence_status == "PASS" and self.certification_level in {
            "route-certified",
            "production-certified",
        }

    @property
    def production_certified(self) -> bool:
        return self.evidence_status == "PASS" and self.certification_level == "production-certified"

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["blockers"] = list(self.blockers)
        return payload


@dataclass(frozen=True, slots=True)
class RouteCertificationMatrixRow:
    """One evidence-backed route matrix row."""

    route_id: str
    dimensions: RouteCertificationDimensions
    sampling_mode: str
    status: str
    catalog_status: str
    capability_status: str
    contract_status: str
    live_status: str
    production_status: str
    docs_link: str
    blockers: tuple[str, ...]
    proofs: tuple[RouteCertificationProof, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "route_id": self.route_id,
            "dimensions": self.dimensions.to_dict(),
            "sampling_mode": self.sampling_mode,
            "status": self.status,
            "catalog_status": self.catalog_status,
            "capability_status": self.capability_status,
            "contract_status": self.contract_status,
            "live_status": self.live_status,
            "production_status": self.production_status,
            "docs_link": self.docs_link,
            "blockers": list(self.blockers),
            "proofs": [proof.to_dict() for proof in self.proofs],
        }


@dataclass(frozen=True, slots=True)
class RouteCertificationMatrixIssue:
    """Evidence input failure that could not be assigned to one route."""

    code: str
    evidence_set: str
    message: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RouteCertificationMatrixReport:
    """Stable JSON and Markdown publication contract."""

    expected_commit: str
    evaluated_at: str
    has_input_failures: bool
    counts: dict[str, int]
    rows: tuple[RouteCertificationMatrixRow, ...]
    errors: tuple[RouteCertificationMatrixIssue, ...]
    output_dir: str

    @property
    def json_path(self) -> Path:
        return Path(self.output_dir) / "route-certification-matrix.json"

    @property
    def markdown_path(self) -> Path:
        return Path(self.output_dir) / "route-certification-matrix.md"

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": SCHEMA_VERSION,
            "expected_commit": self.expected_commit,
            "evaluated_at": self.evaluated_at,
            "has_input_failures": self.has_input_failures,
            "counts": dict(self.counts),
            "rows": [row.to_dict() for row in self.rows],
            "errors": [error.to_dict() for error in self.errors],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n"

    def to_markdown(self) -> str:
        lines = [
            "# dpone route certification matrix",
            "",
            f"- Expected commit: `{self.expected_commit}`",
            f"- Evaluated at: `{self.evaluated_at}`",
            f"- Supplied evidence failures: `{self.has_input_failures}`",
            "",
            "| source | sink | strategy | transport | schema evolution | runtime | status | evidence |",
            "|---|---|---|---|---|---|---|---|",
        ]
        for row in self.rows:
            dimensions = row.dimensions
            evidence = f"contract={row.contract_status}; live={row.live_status}; prod={row.production_status}"
            lines.append(
                f"| `{dimensions.source}` | `{dimensions.sink}` | `{dimensions.strategy}` | "
                f"`{dimensions.transport}` | `{dimensions.schema_evolution}` | "
                f"`{dimensions.airflow_runtime_mode}` | **{row.status}** | {evidence} |"
            )
        lines.extend(["", "## Counts", ""])
        lines.extend(f"- `{status}`: `{self.counts[status]}`" for status in ROUTE_STATUSES)
        lines.extend(["", "## Blockers", ""])
        blockers = tuple(
            dict.fromkeys(
                (*[blocker for row in self.rows for blocker in row.blockers], *[error.code for error in self.errors])
            )
        )
        lines.extend(f"- `{blocker}`" for blocker in blockers)
        if not blockers:
            lines.append("- none")
        lines.extend(
            [
                "",
                "## Interpretation",
                "",
                "- Catalog membership is not production authorization.",
                "- Missing or unavailable live evidence is `UNVERIFIED`, never `PASS`.",
                "- Repair upstream evidence and republish; do not edit this generated matrix.",
                "",
            ]
        )
        return "\n".join(lines)


def route_status_counts(rows: tuple[RouteCertificationMatrixRow, ...]) -> dict[str, int]:
    return {status: sum(1 for row in rows if row.status == status) for status in ROUTE_STATUSES}


__all__ = [
    "ROUTE_STATUSES",
    "RouteCertificationDimensions",
    "RouteCertificationMatrixError",
    "RouteCertificationMatrixIssue",
    "RouteCertificationMatrixReport",
    "RouteCertificationMatrixRow",
    "RouteCertificationProof",
    "route_status_counts",
]
