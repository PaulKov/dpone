"""Connector certification matrix for production readiness."""

from __future__ import annotations

import json
from dataclasses import dataclass

from dpone._compat import StrEnum


class CertificationStatus(StrEnum):
    PASS = "pass"
    FAIL = "fail"
    SKIP = "skip"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class CapabilityRequirement:
    connector: str
    capability: str
    required: bool = True


@dataclass(frozen=True)
class CertificationResult:
    connector: str
    capability: str
    status: CertificationStatus
    evidence: str = ""


@dataclass(frozen=True)
class CertificationReport:
    by_connector: dict[str, dict[str, CertificationResult]]
    missing_required: list[str]

    @property
    def passed(self) -> bool:
        return not self.missing_required and all(
            result.status == CertificationStatus.PASS
            for connector in self.by_connector.values()
            for result in connector.values()
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "passed": self.passed,
            "missing_required": self.missing_required,
            "connectors": {
                connector: {capability: result.__dict__ for capability, result in capabilities.items()}
                for connector, capabilities in self.by_connector.items()
            },
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2, ensure_ascii=False) + "\n"

    def to_markdown(self) -> str:
        lines = ["| connector | capability | status | evidence |", "|---|---|---|---|"]
        for connector, capabilities in sorted(self.by_connector.items()):
            for capability, result in sorted(capabilities.items()):
                lines.append(f"| {connector} | {capability} | {result.status.value} | {result.evidence} |")
        if self.missing_required:
            lines.append("")
            lines.append("Missing required capabilities:")
            lines.extend(f"- {item}" for item in self.missing_required)
        return "\n".join(lines) + "\n"


class CertificationMatrix:
    def __init__(self, requirements: list[CapabilityRequirement]) -> None:
        self.requirements = requirements

    @classmethod
    def default(cls) -> CertificationMatrix:
        connectors = {
            "postgres": [
                "full_refresh",
                "incremental_append",
                "incremental_merge",
                "replace",
                "xmin_state",
                "schema_evolution",
                "15m_stress",
                "soak_24h",
            ],
            "mssql": [
                "full_refresh",
                "incremental_append",
                "incremental_merge",
                "replace",
                "state",
                "bcp_import",
                "bcp_queryout",
                "schema_evolution",
                "15m_stress",
            ],
            "clickhouse": ["full_refresh", "incremental_append", "replace", "http_bulk_ingest", "15m_stress"],
            "rest": ["full_refresh", "incremental_cursor", "pagination", "auth", "state"],
        }
        return cls(
            [
                CapabilityRequirement(connector, capability)
                for connector, caps in connectors.items()
                for capability in caps
            ]
        )

    def evaluate(self, results: list[CertificationResult] | None = None) -> CertificationReport:
        result_map = {(item.connector, item.capability): item for item in results or []}
        by_connector: dict[str, dict[str, CertificationResult]] = {}
        missing: list[str] = []
        for requirement in self.requirements:
            result = result_map.get((requirement.connector, requirement.capability))
            if result is None:
                result = CertificationResult(requirement.connector, requirement.capability, CertificationStatus.UNKNOWN)
                if requirement.required:
                    missing.append(f"{requirement.connector}.{requirement.capability}")
            elif requirement.required and result.status != CertificationStatus.PASS:
                missing.append(f"{requirement.connector}.{requirement.capability}")
            by_connector.setdefault(requirement.connector, {})[requirement.capability] = result
        return CertificationReport(by_connector, missing)
