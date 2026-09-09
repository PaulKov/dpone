from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import asdict, dataclass
from typing import Any

from dpone.strategy_intelligence.native_paths import NativeFastPathCatalog


@dataclass(frozen=True, slots=True)
class ProbeCheck:
    name: str
    passed: bool
    message: str
    action: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True, slots=True)
class LivePreflightResult:
    source_type: str
    sink_type: str
    native_fast_path: str
    ready: bool
    checks: dict[str, ProbeCheck]
    actions: tuple[str, ...]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_type": self.source_type,
            "sink_type": self.sink_type,
            "native_fast_path": self.native_fast_path,
            "ready": self.ready,
            "checks": {name: check.to_dict() for name, check in self.checks.items()},
            "actions": list(self.actions),
        }


class LivePreflightProbe(ABC):
    """Target capability probe interface injected by tests or live adapters."""

    @abstractmethod
    def check_odbc(self) -> ProbeCheck:
        raise NotImplementedError

    @abstractmethod
    def check_permissions(self, schema: str) -> ProbeCheck:
        raise NotImplementedError

    @abstractmethod
    def check_staging_schema(self, schema: str) -> ProbeCheck:
        raise NotImplementedError

    @abstractmethod
    def check_lock_risk(self, schema: str, table: str) -> ProbeCheck:
        raise NotImplementedError


class PlanOnlyLivePreflightProbe(LivePreflightProbe):
    """Safe default probe used when no live connection is available."""

    def check_odbc(self) -> ProbeCheck:
        return ProbeCheck(
            "odbc", True, "plan-only: ODBC check skipped", "Run dpone strategy preflight for local tools."
        )

    def check_permissions(self, schema: str) -> ProbeCheck:
        return ProbeCheck("permissions", True, f"plan-only: permissions for {schema} not verified")

    def check_staging_schema(self, schema: str) -> ProbeCheck:
        return ProbeCheck("staging_schema", True, f"plan-only: staging schema {schema} not verified")

    def check_lock_risk(self, schema: str, table: str) -> ProbeCheck:
        return ProbeCheck("lock_risk", True, f"plan-only: lock risk for {schema}.{table} not verified")


class LivePreflightService:
    """Aggregate target preflight checks before native execution."""

    def __init__(
        self,
        *,
        probe: LivePreflightProbe | None = None,
        native_paths: NativeFastPathCatalog | None = None,
    ) -> None:
        self._probe = probe or PlanOnlyLivePreflightProbe()
        self._native_paths = native_paths or NativeFastPathCatalog()

    def check_target(
        self,
        *,
        source_type: str,
        sink_type: str,
        target_schema: str,
        target_table: str,
        staging_schema: str,
    ) -> LivePreflightResult:
        checks = {
            "odbc": self._probe.check_odbc(),
            "permissions": self._probe.check_permissions(staging_schema),
            "staging_schema": self._probe.check_staging_schema(staging_schema),
            "lock_risk": self._probe.check_lock_risk(target_schema, target_table),
        }
        actions = tuple(_action_for(check) for check in checks.values() if not check.passed)
        path = self._native_paths.resolve(source_type, sink_type)
        return LivePreflightResult(
            source_type=source_type,
            sink_type=sink_type,
            native_fast_path=path.path_id,
            ready=all(check.passed for check in checks.values()),
            checks=checks,
            actions=actions,
        )


def _action_for(check: ProbeCheck) -> str:
    if check.action:
        return check.action
    return f"Resolve {check.name.replace('_', ' ')} before executing native fast path."
