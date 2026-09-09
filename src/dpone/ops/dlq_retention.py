"""Compatibility exports for the canonical DLQ application services."""

from dpone.ops.dlq import DlqRetentionPlan, DlqRetentionResult, DlqRetentionService

__all__ = ["DlqRetentionPlan", "DlqRetentionResult", "DlqRetentionService"]
