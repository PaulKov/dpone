"""Build-time Airflow partition-plan policy over the canonical asset graph."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import replace

from dpone.gitops.airflow_asset_graph import AssetGraphReport
from dpone.gitops.airflow_asset_partition import AssetPartitionSpec
from dpone.gitops.airflow_dag_spec import (
    DagAssetRef,
    DagPartitionPlan,
    DagScheduleAssets,
    GitOpsAirflowDagSpec,
)
from dpone.gitops.workload_catalog_models import GitOpsWorkloadCatalogIssue, issue

_PARTITION_PLAN_SOURCE = "dpone gitops airflow asset-graph"


def attach_asset_partition_plans(
    *,
    asset_graph: AssetGraphReport,
    specs: tuple[GitOpsAirflowDagSpec, ...],
) -> tuple[GitOpsAirflowDagSpec, ...]:
    """Attach one deterministic producer or consumer plan where supported."""

    return tuple(_with_partition_plan(spec, asset_graph=asset_graph) for spec in specs)


def asset_partition_plan_blockers(
    *,
    asset_graph: AssetGraphReport,
    specs: tuple[GitOpsAirflowDagSpec, ...],
) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
    """Block DAG plans that Airflow cannot map to one deterministic key."""

    blockers: list[GitOpsWorkloadCatalogIssue] = []
    for spec in specs:
        output_partitions = _output_partitions(spec, asset_graph=asset_graph)
        schedule = spec.declaration.schedule
        schedule_assets = schedule.assets if isinstance(schedule, DagScheduleAssets) else ()
        input_partitions = tuple(asset.partition for asset in schedule_assets if asset.partition is not None)
        blockers.extend(
            _schedule_contract_blockers(
                spec,
                schedule_assets,
                _incoming_partition_edges(spec, asset_graph=asset_graph),
            )
        )
        output = _single_partition(output_partitions)
        if (input_partitions or output is not None) and len(input_partitions) != len(schedule_assets):
            blockers.append(
                _partition_issue(
                    "asset_partition_contract_mismatch",
                    spec,
                    "partitioned asset schedules cannot mix partitioned and unpartitioned assets in v1",
                )
            )
        if len({partition.identity for partition in output_partitions}) > 1:
            blockers.append(
                _partition_issue(
                    "asset_partition_producer_mixed_contracts",
                    spec,
                    "one producer DAG cannot emit multiple partition contracts in v1",
                )
            )
        if len({partition.identity for partition in input_partitions}) > 1:
            blockers.append(
                _partition_issue(
                    "asset_partition_contract_mismatch",
                    spec,
                    "one consumer DAG cannot combine multiple partition contracts in v1",
                )
            )
        incoming = _single_partition(input_partitions)
        if output is not None and schedule is None:
            blockers.append(
                _partition_issue(
                    "asset_partition_producer_schedule_required",
                    spec,
                    "a partitioned producer DAG requires a cron or partitioned asset schedule",
                )
            )
        if output is not None and incoming is not None and output.identity != incoming.identity:
            blockers.append(
                _partition_issue(
                    "asset_partition_contract_mismatch",
                    spec,
                    "consumer and produced asset partitions must use the same v1 contract",
                )
            )
    return tuple(blockers)


def _schedule_contract_blockers(
    spec: GitOpsAirflowDagSpec,
    schedule_assets: Sequence[DagAssetRef],
    incoming_edges: dict[str, AssetPartitionSpec],
) -> tuple[GitOpsWorkloadCatalogIssue, ...]:
    blockers: list[GitOpsWorkloadCatalogIssue] = []
    for asset in schedule_assets:
        expected = incoming_edges.get(asset.uri)
        if expected is not None and asset.partition is not None and expected.identity != asset.partition.identity:
            blockers.append(
                _partition_issue(
                    "asset_partition_contract_mismatch",
                    spec,
                    f"schedule asset {asset.uri!r} conflicts with the inferred producer partition",
                )
            )
    return tuple(blockers)


def _with_partition_plan(spec: GitOpsAirflowDagSpec, *, asset_graph: AssetGraphReport) -> GitOpsAirflowDagSpec:
    schedule = spec.declaration.schedule
    output = _single_partition(_output_partitions(spec, asset_graph=asset_graph))
    if isinstance(schedule, DagScheduleAssets):
        incoming = _single_partition(tuple(asset.partition for asset in schedule.assets if asset.partition is not None))
        if incoming is not None:
            return replace(
                spec,
                partition_plan=DagPartitionPlan(mode="asset_consumer", partition=incoming),
            )
    if isinstance(schedule, str) and output is not None:
        return replace(
            spec,
            partition_plan=DagPartitionPlan(mode="cron_producer", partition=output),
        )
    return spec


def _output_partitions(
    spec: GitOpsAirflowDagSpec,
    *,
    asset_graph: AssetGraphReport,
) -> tuple[AssetPartitionSpec, ...]:
    member_ids = {node.node_id for node in spec.nodes}
    return tuple(
        binding.partition
        for node in asset_graph.nodes
        if node.node_id in member_ids
        for binding in node.sink_partitions
    )


def _incoming_partition_edges(
    spec: GitOpsAirflowDagSpec,
    *,
    asset_graph: AssetGraphReport,
) -> dict[str, AssetPartitionSpec]:
    member_ids = {node.node_id for node in spec.nodes}
    return {
        edge.uri: edge.partition
        for edge in asset_graph.edges
        if edge.consumer_node_id in member_ids and edge.partition is not None
    }


def _single_partition(partitions: Sequence[AssetPartitionSpec]) -> AssetPartitionSpec | None:
    identities = {partition.identity: partition for partition in partitions}
    return next(iter(identities.values())) if len(identities) == 1 else None


def _partition_issue(
    code: str,
    spec: GitOpsAirflowDagSpec,
    message: str,
) -> GitOpsWorkloadCatalogIssue:
    return issue(code=code, message=message, path=spec.dag_id, source=_PARTITION_PLAN_SOURCE)


__all__ = ["asset_partition_plan_blockers", "attach_asset_partition_plans"]
