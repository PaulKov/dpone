"""Compatibility re-export for CDC replay planning.

The canonical implementation lives in ``dpone.readiness.cdc_replay`` because
replay planning is a credential-free control-plane concern, not a live reader.
"""

from dpone.readiness.cdc_replay import (
    CDCCommitDecision,
    CDCCommitGate,
    CDCOffsetTokenComparator,
    CDCReplayPlan,
    CDCReplayPlanner,
    CDCReplayStep,
)

__all__ = [
    "CDCCommitDecision",
    "CDCCommitGate",
    "CDCOffsetTokenComparator",
    "CDCReplayPlan",
    "CDCReplayPlanner",
    "CDCReplayStep",
]
