"""Shared, non-mutating manifest semantics for source readers and preflight.

An observation alone is neither an authoritative dbt selection nor SQL execution
evidence. Runtime must still obtain the selected IDs from its actual dbt ls.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_graph_contract import dbt_publish_logical_target, expected_dbt_run_result_ids
from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ERROR_CODES,
    dbt_sqlserver_graph_contract_sha256,
    evaluate_dbt_sqlserver_selected_graph,
)

if TYPE_CHECKING:
    from dpone.contracts.dbt_selection_lock import DbtSelectionLock

DBT_SELECTION_SEMANTIC_ERROR_CODES = frozenset(
    {"DPONE_DBT_SELECTION_DRIFT", "DPONE_DBT_TARGET_IDENTITY_MISMATCH", *DBT_SQLSERVER_GRAPH_POLICY_ERROR_CODES}
)


@dataclass(frozen=True, slots=True)
class DbtSelectedGraphObservation:
    """Manifest-derived identities; actual selected IDs come from the caller."""

    graph_contract_sha256: str
    expected_run_result_unique_ids: tuple[str, ...]

    def require_matches(self, lock: DbtSelectionLock, selected_ids: tuple[str, ...]) -> None:
        if (
            selected_ids != lock.selected_graph_unique_ids
            or self.expected_run_result_unique_ids != lock.expected_run_result_unique_ids
            or self.graph_contract_sha256 != lock.graph_contract_sha256
        ):
            raise DbtPublishingError(
                "DPONE_DBT_SELECTION_DRIFT", "Runtime dbt graph differs from the release selection lock"
            )


def observe_dbt_selected_graph(
    manifest: Mapping[str, Any], *, lock: DbtSelectionLock, logical_target: tuple[str, str]
) -> DbtSelectedGraphObservation:
    """Check target, exact policy, capabilities, then derive graph/result identity.

    Preserve runtime error priority: a policy violation must not be hidden behind
    the graph digest it also changes. Invalid manifest types remain ValueError.
    """

    if dbt_publish_logical_target(manifest, lock.publish_model_unique_ids) != logical_target:
        raise DbtPublishingError(
            "DPONE_DBT_TARGET_IDENTITY_MISMATCH", "Runtime dbt relations differ from the release logical target"
        )
    policy = evaluate_dbt_sqlserver_selected_graph(
        manifest, lock.selected_graph_unique_ids, expected_logical_target=logical_target
    )
    if policy.policy_id != lock.graph_policy_id or policy.policy_sha256 != lock.graph_policy_sha256:
        raise DbtPublishingError(
            "DPONE_DBT_SELECTION_DRIFT", "Runtime dbt graph policy differs from the release selection lock"
        )
    if policy.issues:
        issue = policy.issues[0]
        raise DbtPublishingError(issue.code, issue.message)
    return DbtSelectedGraphObservation(
        dbt_sqlserver_graph_contract_sha256(manifest, lock.selected_graph_unique_ids),
        expected_dbt_run_result_ids(manifest, lock.selected_graph_unique_ids),
    )
