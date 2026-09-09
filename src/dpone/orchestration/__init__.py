"""Run orchestration compatibility facade."""

from __future__ import annotations

from importlib import import_module
from typing import Any

__all__ = [
    "SchedulerHandoff",
    "SchedulerHandoffBuilder",
    "LocalRunLockManager",
    "RunLock",
    "ResumeDecision",
    "ResumePolicyPlanner",
    "OrchestratedRunReport",
    "OrchestratedRunRequest",
    "OrchestratedRunService",
    "LocalJobStateStore",
    "NoopJobStateStore",
    "OrchestrationJobState",
]

_EXPORTS: dict[str, str] = {
    "SchedulerHandoff": "dpone.orchestration.handoff:SchedulerHandoff",
    "SchedulerHandoffBuilder": "dpone.orchestration.handoff:SchedulerHandoffBuilder",
    "LocalRunLockManager": "dpone.orchestration.locks:LocalRunLockManager",
    "RunLock": "dpone.orchestration.locks:RunLock",
    "ResumeDecision": "dpone.orchestration.resume:ResumeDecision",
    "ResumePolicyPlanner": "dpone.orchestration.resume:ResumePolicyPlanner",
    "OrchestratedRunReport": "dpone.orchestration.run:OrchestratedRunReport",
    "OrchestratedRunRequest": "dpone.orchestration.run:OrchestratedRunRequest",
    "OrchestratedRunService": "dpone.orchestration.run:OrchestratedRunService",
    "LocalJobStateStore": "dpone.orchestration.state:LocalJobStateStore",
    "NoopJobStateStore": "dpone.orchestration.state:NoopJobStateStore",
    "OrchestrationJobState": "dpone.orchestration.state:OrchestrationJobState",
}


def __getattr__(name: str) -> Any:
    target = _EXPORTS.get(name)
    if target is None:
        raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
    module_name, attr = target.split(":", 1)
    value = getattr(import_module(module_name), attr)
    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(set(globals()) | set(__all__))
