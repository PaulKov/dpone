"""Finite graph-policy dispatch with an exact, unchanged legacy default.

Only the existing SQL Server graph policy has an executable registration. Managed
policies remain unavailable until actual policy and macro authority producers
exist. There is no mutable plugin registry, latest-policy fallback or materialization
name inference. Publish-root selection is outside this graph-only boundary.
"""

from collections.abc import Mapping
from typing import Any

from dpone.contracts.dbt_sqlserver_graph_policy import (
    DBT_SQLSERVER_GRAPH_POLICY_ID,
    DBT_SQLSERVER_GRAPH_POLICY_SHA256,
    DbtSqlServerGraphPolicyReport,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    dbt_sqlserver_graph_contract_sha256 as _legacy_hash,
)
from dpone.contracts.dbt_sqlserver_graph_policy import (
    evaluate_dbt_sqlserver_selected_graph as _legacy_evaluate,
)
from dpone.contracts.dbt_sqlserver_policy_registration import DbtSqlserverGraphRegistration

_LEGACY = DbtSqlserverGraphRegistration(DBT_SQLSERVER_GRAPH_POLICY_ID, DBT_SQLSERVER_GRAPH_POLICY_SHA256)
_UNSUPPORTED = "dbt graph policy does not match the supported SQL Server policy"


def require_graph_registration(*, graph_policy_id: str, graph_policy_sha256: str) -> DbtSqlserverGraphRegistration:
    """Resolve the whole exact pair; unknown or mixed identities never fall back."""
    if (
        not isinstance(graph_policy_id, str)
        or not isinstance(graph_policy_sha256, str)
        or (graph_policy_id, graph_policy_sha256) != (_LEGACY.graph_policy_id, _LEGACY.graph_policy_sha256)
    ):
        raise ValueError(_UNSUPPORTED)
    return _LEGACY


def _registered(selection: DbtSqlserverGraphRegistration | None) -> DbtSqlserverGraphRegistration:
    if selection is None:
        return _LEGACY
    if type(selection) is not DbtSqlserverGraphRegistration:
        raise ValueError(_UNSUPPORTED)
    return require_graph_registration(
        graph_policy_id=selection.graph_policy_id, graph_policy_sha256=selection.graph_policy_sha256
    )


def evaluate_dbt_sqlserver_selected_graph(
    manifest: Mapping[str, Any],
    selected_graph_unique_ids: tuple[str, ...],
    *,
    expected_logical_target: tuple[str, str] | None = None,
    registration: DbtSqlserverGraphRegistration | None = None,
) -> DbtSqlServerGraphPolicyReport:
    """Evaluate only a registered identity, retaining the legacy evaluator call.

    ``None`` selects the exact legacy registration. It never means newest. A
    descriptive caller-created record must resolve to the same registered pair.
    """
    _registered(registration)
    return _legacy_evaluate(manifest, selected_graph_unique_ids, expected_logical_target=expected_logical_target)


def dbt_sqlserver_graph_contract_sha256(
    manifest: Mapping[str, Any],
    selected_graph_unique_ids: tuple[str, ...],
    *,
    registration: DbtSqlserverGraphRegistration | None = None,
) -> str:
    """Hash using the same registered legacy semantics and real macro authority."""
    _registered(registration)
    return _legacy_hash(manifest, selected_graph_unique_ids)
