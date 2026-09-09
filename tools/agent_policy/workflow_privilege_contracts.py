"""Immutable contracts shared by the semantic workflow-privilege scanner."""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import asdict, dataclass, field
from enum import StrEnum
from typing import Any

_DEFAULT_RECOVERY = {
    "PRIVILEGE_CONCURRENT_MUTATION": "RERUN_IMMUTABLE_CHECKOUT",
    "PRIVILEGE_INVALID_POLICY": "REPAIR_PRIVILEGE_POLICY",
    "PRIVILEGE_RESOURCE_LIMIT": "REDUCE_OR_PARTITION_WORKFLOWS",
}
_FAIL_RECOVERIES = "PRIVILEGE_PULL_REQUEST_TARGET:REMOVE_PULL_REQUEST_TARGET PRIVILEGE_UNAPPROVED_PR_WRITE:REDUCE_OR_ISOLATE_PR_AUTHORITY PRIVILEGE_PR_SECRET_OR_ENVIRONMENT:REMOVE_PR_SECRET_AUTHORITY PRIVILEGE_PR_SELF_HOSTED:USE_GITHUB_HOSTED_PR_RUNNER PRIVILEGE_WRITE_ALL:REPLACE_WRITE_ALL PRIVILEGE_READ_ALL:REPLACE_READ_ALL PRIVILEGE_CODEQL_PROFILE_DRIFT:RESTORE_CODEQL_PROFILE PRIVILEGE_ADR0037_PROFILE_DRIFT:RESTORE_ADR0037_PROFILE"
_UNVERIFIED_RECOVERIES = "PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE:BOUND_WORKFLOW_EDGE PRIVILEGE_UNKNOWN_EXPRESSION:SIMPLIFY_PRIVILEGE_GUARD PRIVILEGE_UNKNOWN_PERMISSION:UPDATE_PERMISSION_CONTRACT PRIVILEGE_INVALID_POLICY:REPAIR_PRIVILEGE_POLICY PRIVILEGE_INVALID_WORKFLOW:REPAIR_WORKFLOW_SYNTAX PRIVILEGE_RESOURCE_LIMIT:REDUCE_OR_PARTITION_WORKFLOWS PRIVILEGE_CONCURRENT_MUTATION:RERUN_IMMUTABLE_CHECKOUT"
REPORT_OUTCOMES = {
    code: (status, recovery)
    for status, values in (("FAIL", _FAIL_RECOVERIES), ("UNVERIFIED", _UNVERIFIED_RECOVERIES))
    for code, recovery in (entry.split(":", maxsplit=1) for entry in values.split())
}


class CanonicalSizeError(ValueError): ...


class TruthValue(StrEnum):
    """Closed three-valued result used by the expression proof."""

    TRUE = "TRUE"
    FALSE = "FALSE"
    UNKNOWN = "UNKNOWN"


class EdgeKind(StrEnum):
    """Closed graph transitions understood by the v1 scanner."""

    NEEDS = "NEEDS"
    LOCAL_WORKFLOW_CALL = "LOCAL_WORKFLOW_CALL"
    WORKFLOW_RUN = "WORKFLOW_RUN"


class JobResult(StrEnum):
    """Closed job-result values available to status-function proofs."""

    SUCCESS = "success"
    FAILURE = "failure"
    CANCELLED = "cancelled"
    SKIPPED = "skipped"
    UNKNOWN = "unknown"


@dataclass(frozen=True, slots=True)
class ScanLimits:
    """Closed v1 resource envelope shared by acquisition and evaluation."""

    workflow_files: int
    reusable_workflows_including_root: int
    local_call_edges: int
    workflow_run_edges: int
    jobs: int
    graph_edges: int
    roots: int
    routes: int
    authority_records: int
    profile_matches: int
    route_edges: int
    findings: int
    workflow_bytes: int
    total_workflow_bytes: int
    policy_bytes: int
    expression_bytes: int
    expression_tokens: int
    yaml_depth: int
    yaml_nodes: int
    finding_detail_bytes: int
    report_bytes: int
    text_stdout_bytes: int


V1_LIMITS = ScanLimits(
    workflow_files=256,
    reusable_workflows_including_root=50,
    local_call_edges=9,
    workflow_run_edges=1,
    jobs=4096,
    graph_edges=8192,
    roots=512,
    routes=16384,
    authority_records=16384,
    profile_matches=16384,
    route_edges=256,
    findings=4096,
    workflow_bytes=1_048_576,
    total_workflow_bytes=33_554_432,
    policy_bytes=1_048_576,
    expression_bytes=8192,
    expression_tokens=512,
    yaml_depth=32,
    yaml_nodes=100_000,
    finding_detail_bytes=2048,
    report_bytes=16_777_216,
    text_stdout_bytes=16_777_216,
)


@dataclass(frozen=True, slots=True)
class SnapshotReference:
    """Identity of one acquired repository snapshot and its policy parse."""

    policy_sha256: str | None
    policy_schema_version: int | None
    manifest_sha256: str | None
    complete: bool


@dataclass(frozen=True, slots=True)
class SnapshotFile:
    """One byte-exact regular file held by an acquisition lease."""

    path: str
    mode: str
    byte_length: int
    sha256: str
    content: bytes


@dataclass(frozen=True, slots=True)
class _DataclassMapping:
    """Serialize immutable dataclass contracts through one adapter."""

    def to_mapping(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class Finding(_DataclassMapping):
    """One stable, bounded policy finding."""

    code: str
    status: str
    subject: str
    detail: str
    recovery_command_id: str
    route_id: str | None = None


RESOURCE_LIMIT_FINDING = Finding(
    "PRIVILEGE_RESOURCE_LIMIT",
    "UNVERIFIED",
    "findings",
    "observed findings exceed the closed limit",
    "REDUCE_OR_PARTITION_WORKFLOWS",
)


class RouteResolutionLimitError(ValueError):
    """Carry prior route findings across a bounded expression failure."""

    def __init__(self, dimension: str, findings: tuple[Finding, ...]) -> None:
        super().__init__(f"route resolution exceeds {dimension}")
        self.dimension, self.findings = dimension, findings


@dataclass(frozen=True, slots=True)
class Root(_DataclassMapping):
    workflow: str
    event: str


@dataclass(frozen=True, slots=True)
class Edge(_DataclassMapping):
    kind: str
    source_workflow: str
    source_job: str | None
    target_workflow: str
    target_job: str | None


@dataclass(frozen=True, slots=True)
class Route:
    root_index: int
    event_variant: str
    edge_chain: tuple[Edge, ...]
    workflow: str
    job_id: str
    classification: str
    route_id: str = field(init=False)

    def __post_init__(self) -> None:
        object.__setattr__(self, "route_id", canonical_route_id(self.without_id()))

    def without_id(self) -> dict[str, Any]:
        return {
            "root_index": self.root_index,
            "event_variant": self.event_variant,
            "edge_chain": [edge.to_mapping() for edge in self.edge_chain],
            "workflow": self.workflow,
            "job_id": self.job_id,
            "classification": self.classification,
        }

    def to_mapping(self) -> dict[str, Any]:
        return {"route_id": self.route_id, **self.without_id()}


@dataclass(frozen=True, slots=True)
class Result:
    findings: tuple[Finding, ...] = ()


@dataclass(frozen=True, slots=True)
class Snapshot:
    """Bounded repository inputs acquired under one revalidation lease."""

    policy: SnapshotFile | None = None
    workflows: tuple[SnapshotFile, ...] = ()
    complete: bool = False
    manifest_sha256: str | None = None
    workflow_count: int = 0
    overflow_dimensions: tuple[str, ...] = ()
    findings: tuple[Finding, ...] = ()


@dataclass(frozen=True, slots=True)
class SnapshotResult(Result):
    """Finalized snapshot plus the identity consumed by report finalization."""

    snapshot: Snapshot = field(default_factory=Snapshot)
    reference: SnapshotReference = field(default_factory=lambda: SnapshotReference(None, None, None, False))


@dataclass(frozen=True, slots=True)
class GraphResult(Result):
    roots: tuple[Root, ...] = ()
    edges: tuple[Edge, ...] = ()
    job_count: int = 0
    edge_count: int = 0
    root_count: int = 0
    overflow_dimensions: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class RouteExpansion(Result):
    routes: tuple[Route, ...] = ()
    canonical_ids: tuple[str, ...] = ()
    route_count: int = 0
    overflow_dimensions: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class Authority(Result):
    route_id: str = ""
    workflow: str = ""
    workflow_name: str = ""
    job_id: str = ""
    classification: str = "PR_HEAD"
    declared_permissions: Mapping[str, str] = field(default_factory=dict)
    effective_permissions: Mapping[str, str] = field(default_factory=dict)
    permission_source: str = "WORKFLOW"
    runner: Mapping[str, Any] = field(default_factory=dict)
    environment: str | None = None
    secrets: str = "NONE"
    privileged: bool = False
    profile_id: str | None = None
    _source_witness: tuple[tuple[Finding, ...], bool] | None = field(default=None, repr=False, compare=False)

    def to_mapping(self) -> dict[str, Any]:
        return {
            "route_id": self.route_id,
            "workflow": self.workflow,
            "workflow_name": self.workflow_name,
            "job_id": self.job_id,
            "classification": self.classification,
            "declared_permissions": dict(self.declared_permissions),
            "effective_permissions": dict(self.effective_permissions),
            "permission_source": self.permission_source,
            "runner": dict(self.runner),
            "environment": self.environment,
            "secrets": self.secrets,
            "profile_id": self.profile_id,
            "privileged": self.privileged,
        }

    def to_report_mapping(self) -> dict[str, Any]:
        return {key: value for key, value in self.to_mapping().items() if key != "privileged"}


@dataclass(frozen=True, slots=True)
class ProfileMatch(_DataclassMapping):
    route_id: str
    id: str
    workflow: str
    job_id: str
    fingerprint: str
    classification: str


@dataclass(frozen=True, slots=True)
class ReportEvidence:
    identities: tuple[str, ...]
    requires_nonpass: bool


def report_evidence(
    inventory: Mapping[str, Any],
    roots: tuple[Root, ...],
    authorities: tuple[Authority, ...],
    matches: tuple[ProfileMatch, ...],
    findings: tuple[Finding, ...],
) -> ReportEvidence:
    projections = (
        dict(inventory),
        [item.to_mapping() for item in roots],
        [item.to_report_mapping() for item in authorities],
        [item.to_mapping() for item in matches],
        [item.to_mapping() for item in findings],
    )
    source_nonpass = any(
        item.findings
        or (item.privileged and item.profile_id is None and item.classification != "PROVEN_NOT_PR_REACHABLE")
        for item in authorities
    )
    return ReportEvidence(tuple(canonical_sha256(value) for value in projections), bool(findings) or source_nonpass)


@dataclass(frozen=True, slots=True)
class ProfileResult(Result):
    matches: tuple[ProfileMatch, ...] = ()


@dataclass(frozen=True, slots=True)
class MatchResult(Result):
    match: ProfileMatch | None = None


def canonical_json_bytes(value: object, *, max_bytes: int | None = None) -> bytes:
    encoder = json.JSONEncoder(sort_keys=True, separators=(",", ":"), ensure_ascii=True, allow_nan=False)
    result = bytearray()
    for chunk in encoder.iterencode(value):
        encoded = chunk.encode("ascii")
        if max_bytes is not None and len(result) + len(encoded) > max_bytes:
            raise CanonicalSizeError("canonical JSON exceeds its byte limit")
        result.extend(encoded)
    return bytes(result)


def canonical_sha256(value: object) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def canonical_route_id(value: dict[str, Any]) -> str:
    return canonical_sha256(value)


def valid_public_text(value: object, max_bytes: int | None = None, *, allow_layout: bool = False) -> bool:
    """Check the closed report string byte and C0-control envelope."""

    if not isinstance(value, str) or not value:
        return False
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return not any(ord(char) < 32 and (not allow_layout or char not in "\t\n") for char in value) and (
        max_bytes is None or len(encoded) <= max_bytes
    )


def finding_key(item: Finding | Mapping[str, Any]) -> tuple[Any, ...]:
    value = item.to_mapping() if isinstance(item, Finding) else item
    return (value["status"] != "FAIL", value["code"], value["subject"], value["route_id"] or "", value["detail"])


def finding(
    code: str,
    subject: str,
    detail: str,
    *,
    route_id: str | None = None,
    policy: Mapping[str, Any] | None = None,
) -> Finding:
    outcomes = policy.get("recovery", {}).get("code_to_outcome", {}) if policy else {}
    outcome = outcomes.get(code, {})
    return Finding(
        code=code,
        status=str(outcome.get("status", "UNVERIFIED")),
        subject=subject,
        route_id=route_id,
        detail=detail,
        recovery_command_id=str(
            outcome.get("recovery_command_id", _DEFAULT_RECOVERY.get(code, "REPAIR_WORKFLOW_SYNTAX"))
        ),
    )


__all__ = """Authority CanonicalSizeError Edge EdgeKind Finding GraphResult JobResult MatchResult ProfileMatch ProfileResult ReportEvidence REPORT_OUTCOMES RESOURCE_LIMIT_FINDING Result Root Route RouteExpansion RouteResolutionLimitError ScanLimits Snapshot SnapshotFile SnapshotReference SnapshotResult TruthValue V1_LIMITS canonical_json_bytes canonical_route_id canonical_sha256 finding finding_key report_evidence valid_public_text""".split()
