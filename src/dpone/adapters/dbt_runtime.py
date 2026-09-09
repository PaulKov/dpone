"""Stable adapter surface for the dbt runtime composition root."""

from dpone.adapters.dbt_artifacts import (
    DBT_DEV_EVIDENCE_ROOT_ENV,
    DBT_DEV_EVIDENCE_SET_ENV,
    CampaignDbtExecutionEvidenceWriter,
    LocalDbtExecutionEvidenceWriter,
    LocalDbtRunResultsReader,
)
from dpone.adapters.dbt_manifest_schema import OfficialDbtManifestValidator
from dpone.adapters.dbt_run_results_schema import OfficialDbtRunResultsValidator
from dpone.adapters.dbt_runtime_profile import RuntimeDbtProfileRenderer, TemporaryDbtProfileStore
from dpone.adapters.dbt_semantic_refresh_project import (
    SemanticRefreshDbtRuntimeAuthority,
    materialize_semantic_refresh_project,
)
from dpone.adapters.dbt_subprocess import DistributionDbtToolchainInspector, SubprocessDbtCommandRunner
from dpone.adapters.vault_kv_v2 import build_hvac_kubernetes_vault_kv_v2_reader

__all__ = [
    "DBT_DEV_EVIDENCE_ROOT_ENV",
    "DBT_DEV_EVIDENCE_SET_ENV",
    "CampaignDbtExecutionEvidenceWriter",
    "DistributionDbtToolchainInspector",
    "LocalDbtExecutionEvidenceWriter",
    "LocalDbtRunResultsReader",
    "OfficialDbtManifestValidator",
    "OfficialDbtRunResultsValidator",
    "RuntimeDbtProfileRenderer",
    "SemanticRefreshDbtRuntimeAuthority",
    "SubprocessDbtCommandRunner",
    "TemporaryDbtProfileStore",
    "build_hvac_kubernetes_vault_kv_v2_reader",
    "materialize_semantic_refresh_project",
]
