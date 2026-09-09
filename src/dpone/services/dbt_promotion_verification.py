"""Fail-closed verification for deterministic dbt source-mirror promotion."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.contracts.dbt_project_bundle import DbtProjectBundleArtifact
from dpone.contracts.dbt_promotion import (
    DBT_PROMOTION_SOURCE_DRIFT,
    DBT_PROMOTION_SOURCE_VERIFIED,
    DbtPromotionVerificationReport,
    _drift_report,
    _validated_promotion_source,
)
from dpone.ports.dbt_project_bundle import DbtProjectBundleBuilder


class DbtPromotionVerificationService:
    """Prove a local prod source mirror against immutable release metadata.

    The deterministic bundle builder is injected so this application service
    owns no network, secret, shell, or runtime-adapter construction.
    """

    def __init__(self, *, bundle_builder: DbtProjectBundleBuilder) -> None:
        self._bundle_builder = bundle_builder

    def verify(
        self,
        *,
        project_root: str | Path,
        release_set: Mapping[str, object],
        source_snapshot: Mapping[str, object],
    ) -> DbtPromotionVerificationReport:
        """Rebuild and compare the mirror only after validating pinned identity."""

        try:
            pinned = _validated_promotion_source(release_set, source_snapshot)
        except Exception:
            return _drift_report()

        try:
            artifact = self._bundle_builder.build(Path(project_root))
        except Exception:
            return _drift_report(pinned)
        if not isinstance(artifact, DbtProjectBundleArtifact):
            return _drift_report(pinned)

        observed_sha256 = artifact.bundle.archive_sha256
        if (
            observed_sha256 != pinned.project_bundle_sha256
            or artifact.bundle.archive_bytes != pinned.project_bundle_bytes
        ):
            return _drift_report(pinned, observed_sha256=observed_sha256)
        return DbtPromotionVerificationReport(
            code=DBT_PROMOTION_SOURCE_VERIFIED,
            release_id=pinned.release_id,
            source_snapshot_sha256=pinned.source_snapshot_sha256,
            expected_project_bundle_sha256=pinned.project_bundle_sha256,
            observed_project_bundle_sha256=observed_sha256,
        )


__all__ = [
    "DBT_PROMOTION_SOURCE_DRIFT",
    "DBT_PROMOTION_SOURCE_VERIFIED",
    "DbtProjectBundleBuilder",
    "DbtPromotionVerificationReport",
    "DbtPromotionVerificationService",
]
