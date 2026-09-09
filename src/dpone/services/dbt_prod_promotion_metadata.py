"""Read-only verification of one bot-owned dbt prod promotion descriptor."""

from __future__ import annotations

from pathlib import Path

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.manifest.confined_files import read_confined_file
from dpone.services.dbt_prod_promotion_contract import (
    DbtProdMirrorError,
    DbtPromotionTrustDescriptor,
    dbt_json_object,
    require_dbt_digest,
    safe_dbt_reference,
    validated_relative_path,
    validated_repository_root,
)

_MAX_METADATA_BYTES = 1024 * 1024
_DESCRIPTOR_KEYS_V1 = frozenset(
    {
        "schema",
        "release_id",
        "source_snapshot_path",
        "source_snapshot_sha256",
        "dev_deployment_id",
        "dev_evidence_ref",
        "dev_evidence_subject_sha256",
        "dev_evidence_artifact_name",
        "dev_evidence_producer_workflow",
        "dev_evidence_source_commit",
        "promotion_id",
    }
)
_DESCRIPTOR_KEYS_V2 = _DESCRIPTOR_KEYS_V1 | frozenset(
    {
        "dev_evidence_set_id",
        "dev_evidence_campaign_request_sha256",
    }
)


class DbtProdPromotionMetadataVerifier:
    """Prove review metadata against the release and dev evidence identities."""

    def verify(
        self,
        *,
        compiled_root: Path,
        repository_root: Path,
        descriptor_path: str,
        expected_release_id: str,
        expected_dev_deployment_id: str,
        expected_dev_evidence_ref: str,
        expected_dev_evidence_subject_sha256: str,
        expected_dev_evidence_artifact_name: str,
        expected_dev_evidence_producer_workflow: str,
        expected_dev_evidence_source_commit: str,
        expected_dev_evidence_set_id: str | None = None,
        expected_dev_evidence_campaign_request_sha256: str | None = None,
    ) -> None:
        root = validated_repository_root(repository_root)
        descriptor_relative = validated_relative_path(
            descriptor_path,
            "promotion descriptor path",
        )
        require_dbt_digest(expected_release_id, "release identity")
        require_dbt_digest(
            expected_dev_deployment_id,
            "dev deployment identity",
        )
        evidence_ref = safe_dbt_reference(expected_dev_evidence_ref)
        trust_descriptor = DbtPromotionTrustDescriptor.validated(
            subject_sha256=expected_dev_evidence_subject_sha256,
            artifact_name=expected_dev_evidence_artifact_name,
            producer_workflow=expected_dev_evidence_producer_workflow,
            source_commit=expected_dev_evidence_source_commit,
            evidence_set_id=expected_dev_evidence_set_id,
            campaign_request_sha256=(expected_dev_evidence_campaign_request_sha256),
        )
        try:
            descriptor = dbt_json_object(
                read_confined_file(
                    root,
                    descriptor_relative.as_posix(),
                    max_bytes=_MAX_METADATA_BYTES,
                )
            )
            compiled_snapshot = dbt_json_object(
                read_confined_file(
                    compiled_root,
                    "_dbt/dbt-source-snapshot.json",
                    max_bytes=_MAX_METADATA_BYTES,
                )
            )
        except OSError as exc:
            raise DbtProdMirrorError("prod promotion metadata is unavailable") from exc
        if (
            set(descriptor)
            != (_DESCRIPTOR_KEYS_V2 if expected_dev_evidence_set_id is not None else _DESCRIPTOR_KEYS_V1)
            or descriptor.get("schema")
            != (
                "dpone.dbt-prod-promotion.v2"
                if expected_dev_evidence_set_id is not None
                else "dpone.dbt-prod-promotion.v1"
            )
            or descriptor.get("release_id") != expected_release_id
            or descriptor.get("dev_deployment_id") != expected_dev_deployment_id
            or descriptor.get("dev_evidence_ref") != evidence_ref
            or any(descriptor.get(key) != value for key, value in trust_descriptor.to_dict().items())
        ):
            raise DbtProdMirrorError("prod promotion descriptor differs from reviewed identities")
        source_path = validated_relative_path(
            str(descriptor.get("source_snapshot_path") or ""),
            "source snapshot path",
        )
        try:
            mirrored_snapshot = dbt_json_object(
                read_confined_file(
                    root,
                    source_path.as_posix(),
                    max_bytes=_MAX_METADATA_BYTES,
                )
            )
        except OSError as exc:
            raise DbtProdMirrorError("prod source snapshot is unavailable") from exc
        if mirrored_snapshot != compiled_snapshot or descriptor.get("source_snapshot_sha256") != compiled_snapshot.get(
            "snapshot_sha256"
        ):
            raise DbtProdMirrorError("prod source snapshot differs from the compiled release")
        descriptor_keys = _DESCRIPTOR_KEYS_V2 if expected_dev_evidence_set_id is not None else _DESCRIPTOR_KEYS_V1
        unsigned = {key: descriptor[key] for key in descriptor_keys if key != "promotion_id"}
        if descriptor.get("promotion_id") != canonical_fingerprint(unsigned):
            raise DbtProdMirrorError("prod promotion descriptor fingerprint is invalid")


__all__ = ["DbtProdPromotionMetadataVerifier"]
