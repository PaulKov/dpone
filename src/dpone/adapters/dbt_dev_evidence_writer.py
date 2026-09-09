"""Compatibility exports for campaign-aware dbt evidence writers."""

from dpone.adapters.dbt_artifacts import (
    DBT_DEV_EVIDENCE_ROOT_ENV as DBT_DEV_EVIDENCE_ROOT_ENV,
)
from dpone.adapters.dbt_artifacts import (
    DBT_DEV_EVIDENCE_SET_ENV as DBT_DEV_EVIDENCE_SET_ENV,
)
from dpone.adapters.dbt_artifacts import (
    CampaignDbtExecutionEvidenceWriter as CampaignDbtExecutionEvidenceWriter,
)

__all__ = [
    "DBT_DEV_EVIDENCE_ROOT_ENV",
    "DBT_DEV_EVIDENCE_SET_ENV",
    "CampaignDbtExecutionEvidenceWriter",
]
