"""Readiness facade for Airflow deployment cache retention planning."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, cast

from dpone.adapters.airflow_desired_state_checkpoint import FileDesiredStateCheckpointStore
from dpone.app.airflow_cache_retention_composition import build_deployment_cache_retention_applier
from dpone.contracts.airflow_deployment import is_canonical_sha256_digest
from dpone.contracts.airflow_loader_ack import (
    MAX_LOADER_ACK_BYTES,
    AirflowLoaderAck,
    AirflowLoaderAckError,
    parse_airflow_loader_ack,
)
from dpone.runtime.deployment_cache import (
    DeploymentCacheError,
    DeploymentCacheRetentionApplyError,
    DeploymentCacheRetentionPlanner,
)
from dpone.runtime.deployment_cache_common import open_regular_file
from dpone.runtime.deployment_cache_retention_contracts import (
    AirflowLoaderAcknowledgement,
    AirflowLoaderAcknowledgementReader,
)

MAX_DEPLOYMENT_EVIDENCE_BYTES = 8 * 1024 * 1024
MAX_DEPLOYMENT_EVIDENCE_DEPTH = 64
MAX_DEPLOYMENT_EVIDENCE_NODES = 100_000


class AirflowCacheRetentionService:
    """Expose plan-first deployment cache retention without command/runtime coupling."""

    def __init__(self, *, cache_root: str | Path = ".dpone-cache") -> None:
        self._cache_root = cache_root

    def plan(
        self,
        *,
        environment: str,
        protected_deployment_ids: tuple[str, ...] = (),
        evidence_files: tuple[str | Path, ...] = (),
    ) -> dict[str, Any]:
        try:
            protected_ids = self._effective_protected_ids(protected_deployment_ids, evidence_files)
            return (
                DeploymentCacheRetentionPlanner(self._cache_root)
                .plan(environment=environment, protected_deployment_ids=protected_ids)
                .to_dict()
            )
        except DeploymentCacheError as exc:
            raise AirflowCacheRetentionError(exc.code, str(exc), path=exc.path) from exc

    def apply(
        self,
        *,
        environment: str,
        confirm_delete: bool,
        promoted_by: str,
        allowed_promoters: tuple[str, ...],
        expected_plan_sha256: str | None = None,
        review_id: str | None = None,
        loader_ack_file: str | Path | None = None,
        evidence_version: str = "v1",
        protected_deployment_ids: tuple[str, ...] = (),
        evidence_files: tuple[str | Path, ...] = (),
    ) -> dict[str, Any]:
        if evidence_version not in {"v1", "v2", "v3"}:
            raise AirflowCacheRetentionError(
                "DPONE_DEPLOYMENT_CACHE_GC_EVIDENCE_VERSION_INVALID",
                "retention evidence version must be v1, v2, or v3",
            )
        try:
            protected_ids = self._effective_protected_ids(protected_deployment_ids, evidence_files)

            loader_ack_reader: AirflowLoaderAcknowledgementReader | None = None
            if loader_ack_file is not None:

                def read_loader_ack() -> AirflowLoaderAcknowledgement | None:
                    return cast(AirflowLoaderAcknowledgement, self._read_loader_ack(loader_ack_file))

                loader_ack_reader = read_loader_ack

            cache_root = Path(self._cache_root).resolve(strict=False)
            report = build_deployment_cache_retention_applier(
                cache_root,
                allowed_promoters=allowed_promoters,
            ).apply(
                environment=environment,
                confirm_delete=confirm_delete,
                promoted_by=promoted_by,
                expected_plan_sha256=expected_plan_sha256,
                review_id=review_id,
                loader_ack_reader=loader_ack_reader,
                checkpoint_reader=FileDesiredStateCheckpointStore(
                    cache_root / "status" / "desired-state-checkpoint.json",
                    root=cache_root,
                ),
                protected_deployment_ids=protected_ids,
            )
            if evidence_version == "v1":
                return report.to_dict()
            if evidence_version == "v2":
                return report.to_v2_dict()
            return report.to_v3_dict()
        except (DeploymentCacheRetentionApplyError, DeploymentCacheError) as exc:
            raise AirflowCacheRetentionError(
                exc.code,
                str(exc),
                path=exc.path,
                details=getattr(exc, "details", None),
            ) from exc

    @staticmethod
    def _read_loader_ack(path_value: str | Path) -> AirflowLoaderAck:
        path = Path(path_value).absolute()
        descriptor = -1
        try:
            descriptor = open_regular_file(
                path,
                missing_code="DPONE_DEPLOYMENT_CACHE_GC_ACK_REQUIRED",
                invalid_code="DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID",
                label="loader acknowledgement",
                root=path.parent,
            )
            if os.fstat(descriptor).st_size > MAX_LOADER_ACK_BYTES:
                raise AirflowLoaderAckError("loader acknowledgement exceeds 64 KiB")
            with os.fdopen(descriptor, "rb") as handle:
                descriptor = -1
                return parse_airflow_loader_ack(handle.read(MAX_LOADER_ACK_BYTES + 1))
        except (AirflowLoaderAckError, OSError, DeploymentCacheError) as exc:
            raise AirflowCacheRetentionError(
                "DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID",
                "loader acknowledgement is missing or invalid",
                path=path.as_posix(),
            ) from exc
        finally:
            if descriptor >= 0:
                os.close(descriptor)

    def _deployment_ids_from_evidence(self, evidence_files: tuple[str | Path, ...]) -> tuple[str, ...]:
        deployment_ids: list[str] = []
        for evidence_file in evidence_files:
            evidence_path = Path(evidence_file)
            try:
                payload = _read_bounded_evidence(evidence_path)
                ids, contains_invalid_id = _inspect_deployment_evidence(payload)
            except (OSError, UnicodeError, json.JSONDecodeError, ValueError, DeploymentCacheError) as exc:
                raise AirflowCacheRetentionError(
                    "DPONE_DEPLOYMENT_EVIDENCE_INVALID",
                    f"deployment evidence file is invalid: {exc}",
                    path=evidence_path.as_posix(),
                ) from exc
            if contains_invalid_id:
                raise AirflowCacheRetentionError(
                    "DPONE_DEPLOYMENT_EVIDENCE_DEPLOYMENT_ID_INVALID",
                    "deployment evidence contains a non-canonical deployment_id",
                    path=evidence_path.as_posix(),
                )
            if not ids:
                raise AirflowCacheRetentionError(
                    "DPONE_DEPLOYMENT_EVIDENCE_DEPLOYMENT_ID_NOT_FOUND",
                    "deployment evidence file does not contain a valid sha256 deployment_id",
                    path=evidence_path.as_posix(),
                )
            deployment_ids.extend(ids)
        return tuple(deployment_ids)

    def _effective_protected_ids(
        self,
        protected_deployment_ids: tuple[str, ...],
        evidence_files: tuple[str | Path, ...],
    ) -> tuple[str, ...]:
        invalid = next((item for item in protected_deployment_ids if not is_canonical_sha256_digest(item)), None)
        if invalid is not None:
            raise AirflowCacheRetentionError(
                "DPONE_DEPLOYMENT_ID_INVALID",
                "protected deployment id must be a sha256 digest",
            )
        return tuple(dict.fromkeys([*protected_deployment_ids, *self._deployment_ids_from_evidence(evidence_files)]))


class AirflowCacheRetentionError(RuntimeError):
    def __init__(
        self,
        code: str,
        message: str,
        *,
        path: str | None = None,
        details: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.path = path
        self.details = dict(details or {})


def _read_bounded_evidence(path: Path) -> Any:
    descriptor = open_regular_file(
        path.absolute(),
        missing_code="DPONE_DEPLOYMENT_EVIDENCE_INVALID",
        invalid_code="DPONE_DEPLOYMENT_EVIDENCE_INVALID",
        label="deployment evidence",
        root=path.absolute().parent,
    )
    try:
        if os.fstat(descriptor).st_size > MAX_DEPLOYMENT_EVIDENCE_BYTES:
            raise ValueError("deployment evidence exceeds its configured byte limit")
        with os.fdopen(descriptor, "rb") as handle:
            descriptor = -1
            payload = handle.read(MAX_DEPLOYMENT_EVIDENCE_BYTES + 1)
        if len(payload) > MAX_DEPLOYMENT_EVIDENCE_BYTES:
            raise ValueError("deployment evidence exceeds its configured byte limit")
        return json.loads(payload.decode("utf-8"))
    finally:
        if descriptor >= 0:
            os.close(descriptor)


def _inspect_deployment_evidence(payload: Any) -> tuple[tuple[str, ...], bool]:
    deployment_ids: list[str] = []
    contains_invalid_id = False
    visited = 0
    stack: list[tuple[Any, int]] = [(payload, 0)]
    while stack:
        value, depth = stack.pop()
        visited += 1
        if visited > MAX_DEPLOYMENT_EVIDENCE_NODES:
            raise ValueError("deployment evidence exceeds its configured node limit")
        if depth > MAX_DEPLOYMENT_EVIDENCE_DEPTH:
            raise ValueError("deployment evidence exceeds its configured nesting limit")
        if isinstance(value, dict):
            for key, item in value.items():
                if key == "deployment_id":
                    if is_canonical_sha256_digest(item):
                        deployment_ids.append(str(item))
                    else:
                        contains_invalid_id = True
                stack.append((item, depth + 1))
        elif isinstance(value, list):
            stack.extend((item, depth + 1) for item in value)
    return tuple(deployment_ids), contains_invalid_id


__all__ = [
    "MAX_DEPLOYMENT_EVIDENCE_BYTES",
    "MAX_DEPLOYMENT_EVIDENCE_DEPTH",
    "MAX_DEPLOYMENT_EVIDENCE_NODES",
    "AirflowCacheRetentionError",
    "AirflowCacheRetentionService",
]
