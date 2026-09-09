from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

_REPLAY_ARTIFACTS = [
    "cdc_replay_plan.json",
    "state_transition.json",
    "typed_reconciliation.json",
]


@dataclass(frozen=True)
class CdcReplayContract:
    route: str
    strategy: str
    state_boundary: str
    state_commit: str
    delete_mode: str
    idempotency: str
    lossless_replay: bool
    artifacts: list[str] = field(default_factory=lambda: list(_REPLAY_ARTIFACTS))
    warnings: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "route": self.route,
            "strategy": self.strategy,
            "state_boundary": self.state_boundary,
            "state_commit": self.state_commit,
            "delete_mode": self.delete_mode,
            "idempotency": self.idempotency,
            "lossless_replay": self.lossless_replay,
            "artifacts": list(self.artifacts),
            "warnings": list(self.warnings),
        }


class CdcReplayContractBuilder:
    def build(
        self,
        *,
        source_type: str,
        sink_type: str,
        strategy: str,
        unique_key: tuple[str, ...] = (),
        source_options: dict[str, Any] | None = None,
        sink_options: dict[str, Any] | None = None,
    ) -> CdcReplayContract | None:
        route = f"{source_type.lower()}_to_{sink_type.lower()}"
        if route != "postgres_to_mssql" or strategy != "cdc_apply":
            return None

        source_options = source_options or {}
        sink_options = sink_options or {}
        cdc_options = _as_dict(source_options.get("cdc"))
        delete_options = _as_dict(sink_options.get("deletes"))

        state_boundary = str(cdc_options.get("state_boundary") or "unknown")
        delete_mode = str(delete_options.get("mode") or "ignore")
        idempotency = "unique_key" if unique_key else "none"
        warnings = self._warnings(
            state_boundary=state_boundary,
            delete_mode=delete_mode,
            has_unique_key=bool(unique_key),
        )

        return CdcReplayContract(
            route=route,
            strategy=strategy,
            state_boundary=state_boundary,
            state_commit="after_target_finalize_and_quality",
            delete_mode=delete_mode,
            idempotency=idempotency,
            lossless_replay=not warnings,
            warnings=warnings,
        )

    def _warnings(
        self,
        *,
        state_boundary: str,
        delete_mode: str,
        has_unique_key: bool,
    ) -> list[str]:
        warnings: list[str] = []
        if not has_unique_key:
            warnings.append("unique_key is required for idempotent cdc_apply replay")
        if state_boundary == "unknown":
            warnings.append("source.options.cdc.state_boundary is required for durable replay")
        if delete_mode == "ignore":
            warnings.append("sink.options.deletes.mode is not set; defaulting to ignore")
        return warnings


def _as_dict(value: object) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}
