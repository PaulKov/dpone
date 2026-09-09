"""Confined create-only journal for dbt dev-evidence campaign authority."""

from __future__ import annotations

from pathlib import Path

from dpone_airflow_pack.dev_evidence_store import (
    DevEvidenceConfinedStore,
    DevEvidenceStoreError,
    evidence_set_directory_parts,
)

from dpone.contracts.dbt_dev_evidence_campaign import (
    DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME,
    DBT_DEV_EVIDENCE_REQUEST_FILENAME,
    DbtDevEvidenceCampaignReceiptContract,
    DbtDevEvidenceRequestContract,
    campaign_request_bytes,
)


class DbtDevEvidenceCampaignJournalError(ValueError):
    """The shared evidence journal cannot be changed safely."""


class ConfinedDbtDevEvidenceCampaignJournal:
    """Persist request first and aggregate terminal receipt last."""

    def __init__(self, root: Path) -> None:
        try:
            self._store = DevEvidenceConfinedStore(root)
        except DevEvidenceStoreError as exc:
            raise DbtDevEvidenceCampaignJournalError("dev evidence campaign journal root is unsafe") from exc

    def open(self, request: DbtDevEvidenceRequestContract) -> None:
        try:
            self._store.install(
                directory_parts=_directory_parts(request),
                filename=DBT_DEV_EVIDENCE_REQUEST_FILENAME,
                payload=campaign_request_bytes(request),
            )
        except DevEvidenceStoreError as exc:
            raise DbtDevEvidenceCampaignJournalError(
                "dev evidence campaign request conflicts with the journal"
            ) from exc

    def close(
        self,
        receipt: DbtDevEvidenceCampaignReceiptContract,
    ) -> None:
        try:
            self._store.install(
                directory_parts=evidence_set_directory_parts(
                    receipt.release_id,
                    receipt.deployment_id,
                    receipt.evidence_set_id,
                ),
                filename=DBT_DEV_EVIDENCE_CAMPAIGN_OUTCOME_FILENAME,
                payload=receipt.to_bytes(),
            )
        except DevEvidenceStoreError as exc:
            raise DbtDevEvidenceCampaignJournalError(
                "dev evidence campaign outcome conflicts with the journal"
            ) from exc


def _directory_parts(
    request: DbtDevEvidenceRequestContract,
) -> tuple[str, ...]:
    return evidence_set_directory_parts(
        request.release_id,
        request.deployment_id,
        request.evidence_set_id,
    )


__all__ = [
    "ConfinedDbtDevEvidenceCampaignJournal",
    "DbtDevEvidenceCampaignJournalError",
]
