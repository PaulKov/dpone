"""Produce original evidence and validate committed replay without source I/O."""

from __future__ import annotations

from typing import Any

from dpone.governance.quality import QualityProbeSnapshot
from dpone.runtime.governance.quality_execution import QualityGateExecution
from dpone.runtime.governance.quality_replay_acceptance import validate_replay_acceptance
from dpone.runtime.governance.quality_replay_identity import (
    admission_digest,
    prepare_effective_plan,
    validate_effective_plan,
)
from dpone.runtime.governance.validation_snapshot import snapshot_staged_validation_values
from dpone.runtime.quality_replay_contracts import QualityReplayStore
from dpone.runtime.quality_replay_contracts import contracts as quality_contracts

QualityReplayCapsule = quality_contracts.QualityReplayCapsule
ReplayQualityEvidenceError = quality_contracts.ReplayQualityEvidenceError


class QualityReplaySession:
    """One execution's explicit authority-store dependency and frozen admission.

    V1 admits row/hash gates and source/staged acceptance observations. Target
    capture requires a bounded immutable-generation reader; current database
    probes do not supply it, so that selection fails before source extraction.
    """

    def __init__(self, store: QualityReplayStore, config: Any, execution: QualityGateExecution) -> None:
        if not isinstance(store, QualityReplayStore):
            raise ReplayQualityEvidenceError("UNSUPPORTED")
        policy = execution.snapshot
        if any(
            gate.type not in {"row_count_reconciliation", "min_rows", "typed_hash_reconciliation"}
            for gate in policy.gate_policy.gates
        ):
            raise ReplayQualityEvidenceError("UNSUPPORTED")
        if policy.acceptance_policy.enabled and policy.acceptance_policy.capture_target:
            raise ReplayQualityEvidenceError("UNSUPPORTED")
        self.store = store
        self.execution = execution
        self.admission_config = snapshot_staged_validation_values(config)[0]
        self.admission_digest = admission_digest(config)
        store.require_ready(config)

    def prepare(self, *, config: Any, handle: Any, extract_result: Any, receipt: Any, acceptance: Any) -> None:
        """Hand only verified original observations to the publication producer."""
        execution = self.execution
        report = execution.evidence_projection(receipt, load_config=config)
        source, target = execution.replay_probe_projection(receipt, load_config=config)
        plan = prepare_effective_plan(
            self.admission_config,
            config,
            source_schema=getattr(extract_result, "schema", ()) or (),
            payload_schema=handle.payload_schema,
            target_schema=handle.payload_schema,
        )
        snapshots = {side: snapshot.to_jsonable() for side, snapshot in acceptance.snapshots.items()}
        expected = execution.snapshot.acceptance_policy
        if expected.enabled and set(snapshots) != set(expected.requested_sides):
            raise ReplayQualityEvidenceError("INCOMPLETE")
        validate_replay_acceptance(expected, snapshots)
        core = {
            "policy_snapshot_id": execution.snapshot.policy_snapshot_id,
            "admission_digest": self.admission_digest,
            "effective_plan": plan,
            "run_id": execution.run_id,
            "load_id": execution.load_id,
            "source_probe": source,
            "target_probe": target,
            "report": report,
            "acceptance": snapshots,
            "binding": {},
        }
        self.store.stage(config, handle, core)

    def finish_original(self, config: Any) -> None:
        """Durably finish even a policy without a target-capture obligation."""
        self._require_current(config)
        with self.store.committed(config) as capsule:
            self._require_core(capsule)
            if capsule.state == "FAILED":
                raise ReplayQualityEvidenceError("FAILED")
            if capsule.state != "COMPLETE":
                self.store.complete(config, capsule)

    def replay(self, config: Any) -> tuple[Any, dict[str, Any]]:
        """Re-issue a fresh process-local receipt from trusted original probes."""
        self._require_current(config)
        execution = self.execution
        with self.store.committed(config) as capsule:
            core = self._require_core(capsule)
            if capsule.state == "FAILED":
                raise ReplayQualityEvidenceError("FAILED")
            execution.select_boundary("resume_validation", load_config=config)
            receipt = execution.evaluate(
                load_config=config,
                boundary="resume_validation",
                source_snapshot=QualityProbeSnapshot(**core["source_probe"]),
                target_snapshot=QualityProbeSnapshot(**core["target_probe"]),
            )
            projected = execution.evidence_projection(receipt, load_config=config)
            if projected != core["report"]:
                raise ReplayQualityEvidenceError("MISMATCH")
            if capsule.state != "COMPLETE":
                capsule = self.store.complete(config, capsule)
            execution.accept_payload(receipt, load_config=config)
            execution.accept_state(receipt, load_config=config)
            return receipt, {
                "kind": "dpone.quality.replay.result.v1",
                "core_digest": capsule.core_digest,
                "replayed_from": {"run_id": core["run_id"], "load_id": core["load_id"]},
                "acceptance": core["acceptance"],
                "quality_gates": projected,
            }

    def _require_core(self, capsule: QualityReplayCapsule) -> dict[str, Any]:
        core = capsule.core
        if core["report"].get("passed") is not True:
            raise ReplayQualityEvidenceError("FAILED")
        if (
            core["policy_snapshot_id"] != self.execution.snapshot.policy_snapshot_id
            or core["admission_digest"] != self.admission_digest
        ):
            raise ReplayQualityEvidenceError("MISMATCH")
        validate_effective_plan(self.admission_config, core["effective_plan"])
        validate_replay_acceptance(self.execution.snapshot.acceptance_policy, core["acceptance"])
        return core

    def _require_current(self, config: Any) -> None:
        self.execution.assert_current(load_config=config)
        if admission_digest(config) != self.admission_digest:
            raise ReplayQualityEvidenceError("MISMATCH")


def prepare_quality_replay(sink: Any, config: Any, execution: QualityGateExecution) -> None:
    """Negotiate only an explicitly injected store; other routes stay unchanged."""
    store = getattr(sink, "quality_replay_store", None)
    if store is not None and not execution.snapshot.is_inert():
        try:
            execution.replay_session = QualityReplaySession(store, config, execution)
        except Exception as error:
            if getattr(error, "blocks_committed_success", False):
                raise
            raise ReplayQualityEvidenceError("UNSUPPORTED") from error
