"""Composition root for approval-bound workload-index promotion."""

from __future__ import annotations

from pathlib import Path

from dpone.adapters.workload_index_promotion import (
    ConfinedWorkloadIndexBaselineStore,
    project_authoring_lock,
)
from dpone.manifest.workload_index_promotion_fs import (
    ConfinedMutationError,
    inspect_project_root,
    replace_file_if_digest,
)
from dpone.ports.workload_index_baseline_store import (
    WorkloadIndexAtomicReplaceError,
    WorkloadIndexAtomicReplaceOutcome,
)
from dpone.services.workload_index_promotion import (
    WorkloadIndexPromotionError,
    WorkloadIndexPromotionService,
)


def build_workload_index_promotion_service(
    root: str | Path,
) -> WorkloadIndexPromotionService:
    """Wire POSIX adapters to one captured project-root identity."""

    try:
        root_identity = inspect_project_root(root)
    except OSError as exc:
        raise WorkloadIndexPromotionError(
            "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
            "Project root could not be opened with a stable confined identity.",
        ) from exc
    assert root_identity is not None
    return WorkloadIndexPromotionService(
        root_identity=root_identity,
        store=ConfinedWorkloadIndexBaselineStore(
            root_identity,
            atomic_replace=_atomic_replace,
        ),
        lock_factory=project_authoring_lock,
    )


def _atomic_replace(
    parent_fd: int,
    name: str,
    replacement_name: str,
    expected_sha256: str,
    max_bytes: int,
) -> WorkloadIndexAtomicReplaceOutcome:
    try:
        outcome = replace_file_if_digest(
            parent_fd,
            name,
            replacement_name,
            expected_sha256=expected_sha256,
            max_bytes=max_bytes,
        )
    except ConfinedMutationError as exc:
        raise WorkloadIndexAtomicReplaceError(
            str(exc),
            committed=exc.committed,
            cleanup_required=exc.cleanup_required,
            recovery_name=exc.recovery_name,
        ) from exc
    return WorkloadIndexAtomicReplaceOutcome(
        committed=outcome.committed,
        cleanup_required=outcome.cleanup_required,
        recovery_name=outcome.recovery_name,
    )


__all__ = [
    "WorkloadIndexPromotionError",
    "build_workload_index_promotion_service",
]
