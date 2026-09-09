from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any


@dataclass(frozen=True, slots=True)
class StrategySignal:
    code: str
    severity: str
    message: str
    action: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class AdaptiveBatchingPlan:
    enabled: bool
    initial_batch_size: int
    min_batch_size: int
    max_batch_size: int
    parallel_workers: int
    tuning_metric: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class NativeFastPath:
    path_id: str
    source_type: str
    sink_type: str
    summary: str
    commands: tuple[str, ...]
    required_tools: tuple[str, ...]
    fallback_path: str
    expected_impact: str

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["commands"] = list(self.commands)
        payload["required_tools"] = list(self.required_tools)
        return payload


@dataclass(frozen=True, slots=True)
class StrategyDecision:
    source_type: str
    sink_type: str
    requested_mode: str
    strategy_mode: str
    merge_policy: str | None
    native_fast_path: str
    adaptive_batching: AdaptiveBatchingPlan
    reasons: tuple[StrategySignal, ...] = field(default_factory=tuple)
    safety_gates: tuple[StrategySignal, ...] = field(default_factory=tuple)
    warnings: tuple[StrategySignal, ...] = field(default_factory=tuple)
    native_transfer_plan: dict[str, Any] | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "sink_type": self.sink_type,
            "requested_mode": self.requested_mode,
            "strategy_mode": self.strategy_mode,
            "merge_policy": self.merge_policy,
            "native_fast_path": self.native_fast_path,
            "adaptive_batching": self.adaptive_batching.to_dict(),
            "reasons": [item.to_dict() for item in self.reasons],
            "safety_gates": [item.to_dict() for item in self.safety_gates],
            "warnings": [item.to_dict() for item in self.warnings],
            "native_transfer_plan": self.native_transfer_plan,
        }


@dataclass(frozen=True, slots=True)
class RepairStep:
    action: str
    description: str
    command: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class RepairPlan:
    run_id: str
    safe_to_auto_resume: bool
    steps: tuple[RepairStep, ...]
    commands: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "run_id": self.run_id,
            "safe_to_auto_resume": self.safe_to_auto_resume,
            "steps": [item.to_dict() for item in self.steps],
            "commands": list(self.commands),
        }


@dataclass(frozen=True, slots=True)
class StrategyCertificationEntry:
    source_type: str
    sink_type: str
    strategy_mode: str
    status: str
    native_fast_path: str
    required_evidence: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["required_evidence"] = list(self.required_evidence)
        return payload
