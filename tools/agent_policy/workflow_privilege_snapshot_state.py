"""Snapshot lease state and pure report identity projection.

Filesystem acquisition and revalidation stay in the reader. This module owns
pinned-resource records and the transformation of validation outcomes into the
single policy/manifest identity reported to callers.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass, replace

from tools.agent_policy.workflow_privilege_contracts import (
    Snapshot,
    SnapshotFile,
    SnapshotReference,
    SnapshotResult,
    finding,
)
from tools.agent_policy.workflow_privilege_mutation_observer import MutationObserver


@dataclass(slots=True)
class _OpenPath:
    fd: int
    parent_fd: int
    name: str
    metadata: os.stat_result
    track_changes: bool


@dataclass(slots=True)
class _OpenFile(_OpenPath):
    value: SnapshotFile


@dataclass(slots=True)
class _LeaseState:
    observer: MutationObserver
    directory_fds: tuple[int, ...]
    directories: tuple[_OpenPath, ...]
    policy: _OpenFile
    workflows: tuple[_OpenFile, ...]
    workflow_directory: _OpenPath | None
    expected_names: tuple[str, ...]


def finalize_snapshot(snapshot: Snapshot, *, revalidates: bool, policy_schema_version: int | None) -> SnapshotResult:
    """Invalidate only identities whose acquisition lease lost trust."""
    if not revalidates:
        policy_only = not snapshot.complete
        mutation = finding(
            "PRIVILEGE_CONCURRENT_MUTATION",
            snapshot.policy.path if policy_only and snapshot.policy is not None else ".github/workflows",
            "policy changed during incomplete workflow acquisition"
            if policy_only
            else "policy or workflow inventory changed during acquisition",
        )
        snapshot = replace(
            snapshot,
            policy=None if policy_only else snapshot.policy,
            complete=False,
            manifest_sha256=None,
            findings=(*snapshot.findings, mutation) if policy_only else (mutation,),
        )
    policy_sha = snapshot.policy.sha256 if snapshot.policy else None
    reference = SnapshotReference(policy_sha, policy_schema_version, snapshot.manifest_sha256, snapshot.complete)
    return SnapshotResult(findings=snapshot.findings, snapshot=snapshot, reference=reference)


def snapshot_inventory(
    snapshot: Snapshot, reference: SnapshotReference, overrides: Mapping[str, object] | None = None
) -> dict[str, object]:
    """Project one trusted snapshot/reference identity into report inventory."""
    value: dict[str, object] = {
        "complete": reference.complete,
        "manifest_sha256": reference.manifest_sha256,
        "workflow_count": snapshot.workflow_count,
        **dict.fromkeys("job_count edge_count root_count route_count".split(), 0),
        "overflow_dimensions": list(snapshot.overflow_dimensions),
    }
    value.update(overrides or {})
    value["overflow_dimensions"] = sorted(set(value["overflow_dimensions"]))  # type: ignore[arg-type]
    return value
