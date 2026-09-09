"""Feature parity catalog contracts and constants."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class FeatureTool:
    slug: str
    name: str
    code_comparable: bool
    comparator_note: str


@dataclass(frozen=True)
class FeatureDimension:
    slug: str
    label: str
    weight: int
    enterprise_value: str


@dataclass(frozen=True)
class FeatureRating:
    tool: str
    dimension: str
    level: str
    evidence: str
    sources: tuple[str, ...]


LEVEL_POINTS = {
    "native": 3,
    "managed": 3,
    "strong": 3,
    "supported": 2,
    "partial": 1,
    "external": 1,
    "not_detected": 0,
}

TOOLS = (
    FeatureTool("dpone", "dpone", True, "local framework and feature reference"),
    FeatureTool("airbyte", "Airbyte", True, "open-source code and public docs"),
    FeatureTool("dlt", "dlt", True, "open-source library and public docs"),
    FeatureTool("pentaho-kettle", "Pentaho Kettle", True, "open-source lineage plus current Pentaho docs"),
    FeatureTool("apache-hop", "Apache Hop", True, "open-source platform and public docs"),
    FeatureTool("sling", "Sling", True, "open-source CLI and public docs"),
    FeatureTool("fivetran", "Fivetran", False, "closed-core feature comparator"),
    FeatureTool("informatica", "Informatica", False, "closed-core feature comparator"),
)

DIMENSIONS = (
    FeatureDimension("connectors", "Connector breadth and SDK", 3, "coverage across source and sink systems"),
    FeatureDimension("cdc_incremental", "CDC and incremental capture", 3, "low-latency, low-load replication"),
    FeatureDimension("schema_evolution", "Schema evolution and drift", 3, "safe source changes without rewrites"),
    FeatureDimension("orchestration", "Orchestration and scheduling", 2, "repeatable operational execution"),
    FeatureDimension("retry_resume", "Retry, resume, and recovery", 3, "failure recovery without duplicate loads"),
    FeatureDimension("observability", "Observability and run evidence", 3, "diagnostics, SLOs, and audit trails"),
    FeatureDimension("lineage_catalog", "Lineage and catalog", 2, "trust, impact analysis, and governance"),
    FeatureDimension("governance_security", "Governance, security, and secrets", 3, "enterprise controls"),
    FeatureDimension("deployment_modes", "Deployment modes", 2, "cloud, self-managed, hybrid, embedded, or local"),
    FeatureDimension("certification_evidence", "Certification evidence", 3, "release-grade proof"),
)


def rating(tool: str, dimension: str, level: str, evidence: str, *sources: str) -> FeatureRating:
    return FeatureRating(tool=tool, dimension=dimension, level=level, evidence=evidence, sources=tuple(sources))
