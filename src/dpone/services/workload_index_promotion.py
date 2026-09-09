"""Application service for approval-bound workload-index promotion."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dpone.manifest.workload_index_io import (
    LoadedWorkloadIndex,
    ProjectRootIdentity,
    WorkloadIndexReadError,
    load_workload_index,
    workload_index_relative_path,
)
from dpone.ports.project_authoring_lock import AuthoringLockFactory, ProjectAuthoringLockError
from dpone.ports.workload_index_baseline_store import (
    WorkloadIndexBaselineStore,
    WorkloadIndexBaselineStoreError,
)
from dpone.services.workload_index_contract import validate_workload_index


class WorkloadIndexPromotionError(RuntimeError):
    """Approval evidence or baseline state no longer matches promotion input."""

    def __init__(
        self,
        code: str,
        message: str,
        *,
        recovery_path: str | None = None,
        recovery_artifacts: tuple[str, ...] = (),
    ) -> None:
        super().__init__(message)
        self.code = code
        self.recovery_path = recovery_path
        self.recovery_artifacts = recovery_artifacts


@dataclass(frozen=True, slots=True)
class WorkloadIndexPromotionReceipt:
    """Exact identities committed by one approval-bound promotion."""

    mode: str
    baseline_path: str
    baseline_before_sha256: str | None
    baseline_before_fingerprint: str | None
    promoted_sha256: str
    promoted_fingerprint: str

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema": "dpone.workload-index-promotion.v1",
            "status": "promoted",
            "mode": self.mode,
            "baseline_path": self.baseline_path,
            "baseline_before_sha256": self.baseline_before_sha256,
            "baseline_before_fingerprint": self.baseline_before_fingerprint,
            "promoted_sha256": self.promoted_sha256,
            "promoted_fingerprint": self.promoted_fingerprint,
        }


class WorkloadIndexPromotionService:
    """Promote captured candidate bytes under project lock and baseline CAS."""

    def __init__(
        self,
        *,
        root_identity: ProjectRootIdentity,
        lock_factory: AuthoringLockFactory,
        store: WorkloadIndexBaselineStore,
    ) -> None:
        self._root_identity = root_identity
        self._lock_factory = lock_factory
        self._store = store

    def promote(
        self,
        *,
        candidate_path: str | Path,
        baseline_path: str | Path,
        approved_candidate_sha256: str,
        approved_current_fingerprint: str,
        expected_baseline_sha256: str | None,
        expected_baseline_fingerprint: str | None,
        expect_baseline_absent: bool,
    ) -> WorkloadIndexPromotionReceipt:
        self._validate_request(
            expected_baseline_sha256=expected_baseline_sha256,
            expected_baseline_fingerprint=expected_baseline_fingerprint,
            expect_baseline_absent=expect_baseline_absent,
        )
        try:
            with self._lock_factory(self._root_identity.path):
                candidate = self._load(candidate_path, label="Candidate")
                candidate_fingerprint = self._approved_candidate_fingerprint(
                    candidate,
                    approved_candidate_sha256=approved_candidate_sha256,
                    approved_current_fingerprint=approved_current_fingerprint,
                )
                baseline_sha256, baseline_fingerprint, baseline_relative = self._approved_baseline(
                    baseline_path,
                    expected_baseline_sha256=expected_baseline_sha256,
                    expected_baseline_fingerprint=expected_baseline_fingerprint,
                    expect_baseline_absent=expect_baseline_absent,
                )
                self._store.promote(
                    baseline_relative,
                    desired=candidate.content,
                    expected_sha256=expected_baseline_sha256,
                )
        except WorkloadIndexBaselineStoreError as exc:
            raise WorkloadIndexPromotionError(
                exc.code,
                str(exc),
                recovery_path=exc.recovery_path,
                recovery_artifacts=exc.recovery_artifacts,
            ) from exc
        except ProjectAuthoringLockError as exc:
            raise WorkloadIndexPromotionError(exc.code, str(exc)) from exc
        return WorkloadIndexPromotionReceipt(
            mode="bootstrap" if expect_baseline_absent else "change",
            baseline_path=baseline_relative,
            baseline_before_sha256=baseline_sha256,
            baseline_before_fingerprint=baseline_fingerprint,
            promoted_sha256=candidate.content_sha256,
            promoted_fingerprint=candidate_fingerprint,
        )

    @staticmethod
    def _validate_request(
        *,
        expected_baseline_sha256: str | None,
        expected_baseline_fingerprint: str | None,
        expect_baseline_absent: bool,
    ) -> None:
        if expect_baseline_absent == (expected_baseline_sha256 is not None):
            raise WorkloadIndexPromotionError(
                "DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID",
                "Choose exactly one baseline guard: expected digest or expected absence.",
            )
        if expect_baseline_absent and expected_baseline_fingerprint is not None:
            raise WorkloadIndexPromotionError(
                "DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID",
                "Bootstrap promotion cannot declare an existing baseline fingerprint.",
            )
        if not expect_baseline_absent and expected_baseline_fingerprint is None:
            raise WorkloadIndexPromotionError(
                "DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID",
                "Existing-baseline promotion requires its approved semantic fingerprint.",
            )

    def _load(self, path: str | Path, *, label: str) -> LoadedWorkloadIndex:
        try:
            loaded = load_workload_index(self._root_identity, path, label=label)
        except WorkloadIndexReadError as exc:
            code = "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT" if exc.confinement else "DPONE_WORKLOAD_INDEX_INVALID"
            raise WorkloadIndexPromotionError(
                code,
                f"{label} workload index could not be loaded safely.",
            ) from exc
        _validate_loaded_index(loaded, label=label)
        return loaded

    @staticmethod
    def _approved_candidate_fingerprint(
        candidate: LoadedWorkloadIndex,
        *,
        approved_candidate_sha256: str,
        approved_current_fingerprint: str,
    ) -> str:
        if candidate.content_sha256 != approved_candidate_sha256:
            raise WorkloadIndexPromotionError(
                "DPONE_WORKLOAD_INDEX_APPROVAL_MISMATCH",
                "Candidate bytes changed after approval.",
            )
        fingerprint = str(candidate.payload["project_fingerprint"])
        if fingerprint != approved_current_fingerprint:
            raise WorkloadIndexPromotionError(
                "DPONE_WORKLOAD_INDEX_APPROVAL_MISMATCH",
                "Candidate semantic fingerprint does not match approval evidence.",
            )
        return fingerprint

    def _approved_baseline(
        self,
        path: str | Path,
        *,
        expected_baseline_sha256: str | None,
        expected_baseline_fingerprint: str | None,
        expect_baseline_absent: bool,
    ) -> tuple[str | None, str | None, str]:
        if expect_baseline_absent:
            try:
                relative_path = workload_index_relative_path(self._root_identity, path)
            except WorkloadIndexReadError as exc:
                raise WorkloadIndexPromotionError(
                    "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
                    "Baseline path is outside the project authority.",
                ) from exc
            return None, None, relative_path
        assert expected_baseline_sha256 is not None
        baseline = self._load(path, label="Baseline")
        fingerprint = str(baseline.payload["project_fingerprint"])
        if baseline.content_sha256 != expected_baseline_sha256 or fingerprint != expected_baseline_fingerprint:
            raise WorkloadIndexPromotionError(
                "DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT",
                "Baseline changed after change impact was approved.",
            )
        return baseline.content_sha256, fingerprint, baseline.relative_path


def _validate_loaded_index(loaded: LoadedWorkloadIndex, *, label: str) -> Mapping[str, Any]:
    try:
        validate_workload_index(loaded.payload)
    except (TypeError, ValueError) as exc:
        raise WorkloadIndexPromotionError(
            "DPONE_WORKLOAD_INDEX_INVALID",
            f"{label} workload index is invalid.",
        ) from exc
    return loaded.payload


__all__ = [
    "WorkloadIndexBaselineStore",
    "WorkloadIndexPromotionError",
    "WorkloadIndexPromotionReceipt",
    "WorkloadIndexPromotionService",
]
