"""Closed V1/V2 policy acquisition without mutating V1 authority."""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path

from tools.agent_policy.workflow_privilege_contracts import (
    ScanLimits,
    Snapshot,
    SnapshotFile,
    SnapshotReference,
    SnapshotResult,
)
from tools.agent_policy.workflow_privilege_snapshot import SnapshotLease, SnapshotReader

V1_POLICY_PATH = ".agents/policy/workflow-security-privileged.yml"
V2_POLICY_PATH = ".agents/policy/workflow-security-privileged-v2.yml"
V3_POLICY_PATH = ".agents/policy/workflow-security-privileged-v3.yml"
V1_POLICY_SHA256 = "0834cb3501e1e803eabec6e9bb8f4bef216c3947252de9e333551b058baa6178"
V2_POLICY_SHA256 = "0aa0e86b816ed365cf390c4bb20907db38f3455779d0aaec6dfefacee8e78059"
V2_AMENDMENT_MERGE = "2bca315cce9249c9a324a65371dfb105cc973d6c"
V3_SPECIFICATION = "docs/feature-design-ci-quality-sharding-v3.md"
V3_APPROVED_BASE = "756a03a88907ad447ea740a7ee73025c81b257c1"
V3_GOVERNANCE_SOURCE_EDGES = tuple(
    ("NEEDS", ".github/workflows/ci.yml", source, ".github/workflows/ci.yml", target)
    for source, target in (
        ("quality-preflight", "quality-shards"),
        ("quality-shards", "quality"),
        ("quality", "governance-source"),
        ("governance-source", "governance-attestation"),
    )
)


@dataclass(slots=True)
class VersionedPolicyLease:
    """Keep V1 open while V2 is selected, preventing a silent historical rewrite."""

    _active: SnapshotLease
    _historical_v1: SnapshotLease | None
    _historical_v2: SnapshotLease | None
    version: int

    @property
    def snapshot(self) -> Snapshot:
        """Return the sole policy/workflow snapshot consumed by the scanner."""

        return self._active.snapshot

    @property
    def historical_v1(self) -> SnapshotFile | None:
        """Expose only the byte-exact V1 policy retained for V2 binding validation."""

        return None if self._historical_v1 is None else self._historical_v1.snapshot.policy

    @property
    def historical_v2(self) -> SnapshotFile | None:
        """Expose byte-exact V2 authority retained for V3 binding validation."""

        return None if self._historical_v2 is None else self._historical_v2.snapshot.policy

    def finalize(self, *, policy_schema_version: int | None) -> SnapshotResult:
        """Finalize every held descriptor before publishing an active-policy identity."""

        active = self._active.finalize(policy_schema_version=policy_schema_version)
        historical = tuple(
            lease.finalize(policy_schema_version=version)
            for lease, version in ((self._historical_v1, 1), (self._historical_v2, 2))
            if lease is not None
        )
        if all(result.snapshot.complete and result.snapshot.policy is not None for result in historical):
            return active
        return SnapshotResult(
            findings=(*active.findings, *(item for result in historical for item in result.findings)),
            snapshot=Snapshot(
                policy=active.snapshot.policy,
                workflows=active.snapshot.workflows,
                complete=False,
                workflow_count=active.snapshot.workflow_count,
                overflow_dimensions=active.snapshot.overflow_dimensions,
                findings=(
                    *active.snapshot.findings,
                    *(item for result in historical for item in result.snapshot.findings),
                ),
            ),
            reference=SnapshotReference(
                active.reference.policy_sha256, active.reference.policy_schema_version, None, False
            ),
        )

    def close(self) -> None:
        """Release both leases when parsing stops before report finalization."""

        self._active.close()
        if self._historical_v1 is not None:
            self._historical_v1.close()
        if self._historical_v2 is not None:
            self._historical_v2.close()


class VersionedPolicyReader:
    """Choose V2 only when its path is present; absence retains V1 exactly."""

    def __init__(self, *, limits: ScanLimits) -> None:
        self._limits = limits

    def acquire(self, root: Path) -> VersionedPolicyLease:
        """Acquire V1 plus a V2 candidate atomically enough for final revalidation."""

        v3_candidate = os.path.lexists(root / V3_POLICY_PATH)
        v2_candidate = os.path.lexists(root / V2_POLICY_PATH)
        if not v2_candidate and not v3_candidate:
            return VersionedPolicyLease(SnapshotReader(limits=self._limits).acquire(root), None, None, 1)
        historical_v1 = SnapshotReader(limits=self._limits, policy_path=V1_POLICY_PATH).acquire(root)
        historical_v2 = (
            SnapshotReader(limits=self._limits, policy_path=V2_POLICY_PATH).acquire(root) if v3_candidate else None
        )
        path, version = (V3_POLICY_PATH, 3) if v3_candidate else (V2_POLICY_PATH, 2)
        active = SnapshotReader(limits=self._limits, policy_path=path).acquire(root)
        return VersionedPolicyLease(active, historical_v1, historical_v2, version)


def valid_v2_binding(policy: Mapping[str, object], historical_v1: SnapshotFile | None) -> bool:
    """Require V2 to name the immutable V1 value and approved amendment merge."""

    if historical_v1 is None or historical_v1.path != V1_POLICY_PATH or historical_v1.sha256 != V1_POLICY_SHA256:
        return False
    supersedes, authority = policy.get("supersedes"), policy.get("authority")
    return (
        isinstance(supersedes, Mapping)
        and isinstance(authority, Mapping)
        and supersedes.get("v1_policy_sha256") == V1_POLICY_SHA256
        and supersedes.get("approved_amendment_merge") == V2_AMENDMENT_MERGE
        and authority.get("amendment_specification") == "docs/feature-design-ci-governance-profile-v2-amendment.md"
        and authority.get("amendment_merge") == V2_AMENDMENT_MERGE
    )


def valid_v3_binding(
    policy: Mapping[str, object], historical_v1: SnapshotFile | None, historical_v2: SnapshotFile | None
) -> bool:
    """Require V3 to retain both immutable predecessors and its approved design base."""

    if historical_v1 is None or historical_v1.path != V1_POLICY_PATH or historical_v1.sha256 != V1_POLICY_SHA256:
        return False
    if historical_v2 is None or historical_v2.path != V2_POLICY_PATH or historical_v2.sha256 != V2_POLICY_SHA256:
        return False
    supersedes, authority = policy.get("supersedes"), policy.get("authority")
    return (
        isinstance(supersedes, Mapping)
        and isinstance(authority, Mapping)
        and supersedes.get("v2_policy_sha256") == V2_POLICY_SHA256
        and supersedes.get("v1_policy_sha256") == V1_POLICY_SHA256
        and supersedes.get("approved_amendment_merge") == V2_AMENDMENT_MERGE
        and supersedes.get("approved_v3_base") == V3_APPROVED_BASE
        and authority.get("amendment_specification") == V3_SPECIFICATION
        and authority.get("amendment_base") == V3_APPROVED_BASE
    )


def valid_policy_binding(
    version: int, policy: Mapping[str, object], historical_v1: SnapshotFile | None, historical_v2: SnapshotFile | None
) -> bool:
    """Validate the predecessor chain required by one selected policy version."""

    return (
        version == 1
        or version == 2
        and valid_v2_binding(policy, historical_v1)
        or version == 3
        and valid_v3_binding(policy, historical_v1, historical_v2)
    )


def policy_binding_failure(
    version: int, policy: Mapping[str, object], historical_v1: SnapshotFile | None, historical_v2: SnapshotFile | None
) -> str | None:
    """Return the selected invalid policy path, never a fallback path."""

    if valid_policy_binding(version, policy, historical_v1, historical_v2):
        return None
    return V2_POLICY_PATH if version == 2 else V3_POLICY_PATH


def policy_binding_failure_for_lease(version: int, policy: Mapping[str, object], lease: object) -> str | None:
    """Apply binding validation to a lease without leaking snapshot internals to the service."""

    return policy_binding_failure(
        version, policy, getattr(lease, "historical_v1", None), getattr(lease, "historical_v2", None)
    )


def report_profile_binding(version: int | None, profile_id: object, binding: object) -> object:
    """Select the exact V3 route coordinates without changing historic report bindings."""

    if version == 3 and profile_id == "ADR0037_GOVERNANCE_SOURCE_ATTESTOR" and isinstance(binding, tuple):
        return (*binding[:6], binding[6], V3_GOVERNANCE_SOURCE_EDGES)
    return binding


__all__ = "V1_POLICY_PATH V2_POLICY_PATH V3_POLICY_PATH V3_GOVERNANCE_SOURCE_EDGES VersionedPolicyLease VersionedPolicyReader policy_binding_failure policy_binding_failure_for_lease report_profile_binding valid_policy_binding valid_v2_binding valid_v3_binding".split()
