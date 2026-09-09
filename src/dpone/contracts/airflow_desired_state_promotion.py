"""Promotion identity contract for an Airflow desired deployment."""

from __future__ import annotations

from dataclasses import dataclass

from dpone.contracts.airflow_desired_state_validation import dag_ids, digest, invalid


@dataclass(frozen=True, slots=True)
class DesiredStatePromotion:
    """Immutable deployment and evidence identities selected for Airflow."""

    registry_scope_id: str
    release_id: str
    deployment_id: str
    airflow_index_sha256: str
    runtime_image_digest: str
    expected_dag_ids: tuple[str, ...]
    publication_evidence_sha256: str
    runtime_image_dbt_digest: str | None = None

    def __post_init__(self) -> None:
        for field in (
            "registry_scope_id",
            "release_id",
            "deployment_id",
            "airflow_index_sha256",
            "runtime_image_digest",
            "publication_evidence_sha256",
        ):
            digest(getattr(self, field), field=f"promotion.{field}")
        if self.runtime_image_dbt_digest is not None:
            digest(self.runtime_image_dbt_digest, field="promotion.runtime_image_dbt_digest")
        if dag_ids(self.expected_dag_ids) != self.expected_dag_ids:
            raise invalid("promotion.expected_dag_ids must be sorted and unique")

    def to_dict(self) -> dict[str, object]:
        """Return the closed promotion mapping, omitting the optional dbt digest."""

        payload: dict[str, object] = {
            "registry_scope_id": self.registry_scope_id,
            "release_id": self.release_id,
            "deployment_id": self.deployment_id,
            "airflow_index_sha256": self.airflow_index_sha256,
            "runtime_image_digest": self.runtime_image_digest,
            "expected_dag_ids": list(self.expected_dag_ids),
            "publication_evidence_sha256": self.publication_evidence_sha256,
        }
        if self.runtime_image_dbt_digest is not None:
            payload["runtime_image_dbt_digest"] = self.runtime_image_dbt_digest
        return payload


__all__ = ["DesiredStatePromotion"]
