"""Compatibility shim for ``dpone.ops.dbt_lineage``."""

from dpone.ops.dbt_lineage import DbtLineageEdge, DbtLineageNode, DbtLineageReport, DbtLineageService

__all__ = ["DbtLineageEdge", "DbtLineageNode", "DbtLineageReport", "DbtLineageService"]
