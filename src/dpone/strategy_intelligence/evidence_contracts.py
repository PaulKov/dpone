from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

_CHECKPOINT_KEY = "transfer_partition_id"
_SKIP_RULE = "matching_source_target_strategy_query_schema_and_bounds_hash"


class PartitioningEvidence(Protocol):
    strategy: str
    column: str | None


@dataclass(frozen=True)
class NativeTransferEvidenceContract:
    route: str
    strategy: str
    state_commit_gate: str
    required_artifacts: list[str]
    required_checks: list[str]
    partition_retry: dict[str, Any]
    typed_reconciliation: dict[str, Any] = field(
        default_factory=lambda: {
            "required": True,
            "scope": "source_target_type_aware",
        }
    )

    def to_dict(self) -> dict[str, Any]:
        return {
            "route": self.route,
            "strategy": self.strategy,
            "state_commit_gate": self.state_commit_gate,
            "required_artifacts": list(self.required_artifacts),
            "required_checks": list(self.required_checks),
            "partition_retry": dict(self.partition_retry),
            "typed_reconciliation": dict(self.typed_reconciliation),
        }


class NativeTransferEvidenceContractBuilder:
    def build(
        self,
        *,
        source_type: str,
        sink_type: str,
        strategy: str,
        partitioning: PartitioningEvidence,
    ) -> NativeTransferEvidenceContract:
        partitioned = self._is_partitioned(partitioning)
        artifacts = self._artifacts(strategy=strategy, partitioned=partitioned)
        checks = self._checks(partitioned=partitioned)
        return NativeTransferEvidenceContract(
            route=f"{source_type.lower()}_to_{sink_type.lower()}",
            strategy=strategy,
            state_commit_gate="after_target_finalize_and_quality",
            required_artifacts=artifacts,
            required_checks=checks,
            partition_retry={
                "enabled": partitioned,
                "checkpoint_key": _CHECKPOINT_KEY,
                "skip_rule": _SKIP_RULE,
                "state_scope": "per_partition" if partitioned else "single_partition",
            },
        )

    def _is_partitioned(self, partitioning: PartitioningEvidence) -> bool:
        return bool(partitioning.column and partitioning.strategy != "single")

    def _artifacts(self, *, strategy: str, partitioned: bool) -> list[str]:
        artifacts = [
            "native_transfer_plan.json",
            "transfer_diagnostics.json",
        ]
        if strategy == "cdc_apply":
            artifacts.append("cdc_replay_plan.json")
        if partitioned:
            artifacts.extend(
                [
                    "partition_checkpoints.json",
                    "partition_retry_plan.json",
                ]
            )
        artifacts.extend(
            [
                "typed_reconciliation.json",
                "quality_results.json",
                "state_transition.json",
            ]
        )
        return artifacts

    def _checks(self, *, partitioned: bool) -> list[str]:
        checks: list[str] = []
        if partitioned:
            checks.append("partition_checkpoint_consistency")
        checks.extend(
            [
                "typed_reconciliation",
                "quality_gate",
                "state_transition_after_commit",
            ]
        )
        return checks
