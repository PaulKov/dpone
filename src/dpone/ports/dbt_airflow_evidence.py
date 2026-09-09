"""Port used to trigger and observe release-bound Airflow evidence runs."""

from __future__ import annotations

from collections.abc import Mapping
from typing import TYPE_CHECKING, Protocol

if TYPE_CHECKING:
    from dpone.contracts.dbt_dev_evidence_campaign import (
        DbtDevEvidenceCampaignReceiptContract,
        DbtDevEvidenceRequestContract,
    )


class DbtDevEvidenceCampaignError(RuntimeError):
    """The Airflow evidence campaign could not complete safely."""

    code = "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED"


DbtAirflowEvidencePortError = DbtDevEvidenceCampaignError


class DbtAirflowEvidenceConfigurationError(DbtDevEvidenceCampaignError):
    """The configured Airflow evidence endpoint is unsafe."""

    code = "DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_CONFIG_INVALID"


class DbtAirflowEvidencePort(Protocol):
    """Minimal Airflow control-plane surface required by the trusted CI job."""

    def trigger(
        self,
        *,
        dag_id: str,
        dag_run_id: str,
        conf: Mapping[str, object],
        timeout_seconds: float | None = None,
    ) -> None:
        """Create one deterministic DAG run or prove an identical run exists."""

    def state(
        self,
        *,
        dag_id: str,
        dag_run_id: str,
        timeout_seconds: float | None = None,
    ) -> str:
        """Return the current Airflow DAG-run state."""


class DbtDevEvidenceCampaignJournalPort(Protocol):
    """Create-only persistence for campaign authority and terminal closure."""

    def open(self, request: DbtDevEvidenceRequestContract) -> None:
        """Persist the exact request before any DAG run can be triggered."""

    def close(
        self,
        receipt: DbtDevEvidenceCampaignReceiptContract,
    ) -> None:
        """Persist one immutable terminal aggregate receipt."""


__all__ = [
    "DbtAirflowEvidenceConfigurationError",
    "DbtAirflowEvidencePort",
    "DbtAirflowEvidencePortError",
    "DbtDevEvidenceCampaignError",
    "DbtDevEvidenceCampaignJournalPort",
]
