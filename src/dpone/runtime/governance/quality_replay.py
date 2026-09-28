"""Produce original evidence and validate committed replay without source I/O."""

from __future__ import annotations

from typing import Any

from dpone.governance.quality import QualityProbeSnapshot
from dpone.ports.target_acceptance import BoundedTargetAcceptanceReader
from dpone.runtime.governance.quality_execution import QualityGateExecution
from dpone.runtime.governance.quality_replay_acceptance import validate_replay_acceptance
from dpone.runtime.governance.quality_replay_identity import (
    admission_digest,
    prepare_effective_plan,
    validate_effective_plan,
)
from dpone.runtime.governance.quality_replay_target import complete_target
from dpone.runtime.governance.quality_target_plan import prepare_target_plan, target_request, validate_target_plan
from dpone.runtime.governance.validation_snapshot import snapshot_staged_validation_values
from dpone.runtime.quality_replay_contracts import QualityReplayStore
from dpone.runtime.quality_replay_contracts import contracts as quality_contracts
from dpone.runtime.sinks.clickhouse_cluster_publication_identity import cluster_name

QualityReplayCapsule = quality_contracts.QualityReplayCapsule
ReplayQualityEvidenceError = quality_contracts.ReplayQualityEvidenceError


class QualityReplaySession:
    """One execution's explicit authority-store dependency and frozen admission.

    V1 preserves source/staged-only bytes. V2 requires an explicitly injected
    bounded target reader and shares completion across original and retry paths.
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
        self.target_requested = policy.acceptance_policy.enabled and policy.acceptance_policy.capture_target
        if self.target_requested:
            selections = (policy.acceptance_policy.null_counts, policy.acceptance_policy.distinct_counts)
            if len({column for selection in selections if isinstance(selection, tuple) for column in selection}) > 256:
                raise ReplayQualityEvidenceError("UNSUPPORTED")
            if not isinstance(store.target_reader, BoundedTargetAcceptanceReader):
                raise ReplayQualityEvidenceError("UNSUPPORTED")
            store.target_reader.require_ready(
                cluster=cluster_name(config), database=str(config.target_schema), table=str(config.target_table)
            )
        self.accepted_receipt: Any = None
        self._prepared_receipt: Any = None
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
        validate_replay_acceptance(expected, snapshots, before_target=self.target_requested)
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
        if self.target_requested:
            core["target_plan"] = prepare_target_plan(config, expected, handle.payload_schema)
            self.store.target_reader.validate_plan(
                target_request(core, quality_contracts.quality_digest(core), "f" * 32)
            )
        self.store.stage(config, handle, core)
        self._prepared_receipt = receipt

    def finish_original(self, config: Any, *, receipt: Any = None) -> dict[str, Any]:
        """Consume original proof under its guard before external state acceptance."""
        _, evidence = self._finish(config, receipt=receipt or self._prepared_receipt)
        return evidence

    def replay(self, config: Any) -> tuple[Any, dict[str, Any]]:
        """Reissue a fresh process-local receipt without extraction or publication."""
        return self._finish(config)

    def _finish(self, config: Any, *, receipt: Any = None) -> tuple[Any, dict[str, Any]]:
        self._require_current(config)
        execution = self.execution
        core: dict[str, Any] | None = None
        proven = False
        try:
            with self.store.committed(config) as capsule:
                proven = True
                core = self._require_core(capsule)
                if capsule.state == "FAILED":
                    raise ReplayQualityEvidenceError("FAILED")
                if receipt is None:
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
                if self.target_requested:
                    capsule = complete_target(self.store, config, capsule, execution.snapshot.acceptance_policy)
                elif capsule.state != "COMPLETE":
                    capsule = self.store.complete(config, capsule)
                execution.accept_payload(receipt, load_config=config)
                execution.accept_state(receipt, load_config=config)
                acceptance = dict(core["acceptance"])
                if self.target_requested:
                    acceptance["target"] = capsule.target
                evidence = {
                    "kind": "dpone.quality.replay.result.v2"
                    if self.target_requested
                    else "dpone.quality.replay.result.v1",
                    "core_digest": capsule.core_digest,
                    "replayed_from": {"run_id": core["run_id"], "load_id": core["load_id"]},
                    "acceptance": acceptance,
                    "quality_gates": projected,
                }
            # Only a verified guard release authorizes downstream consumers to
            # bypass the receipt they would otherwise consume a second time.
            self.accepted_receipt = receipt
            return receipt, evidence
        except BaseException as error:
            details = {
                "target_commit": "proven"
                if proven
                else getattr(error, "replay_details", {}).get("target_commit", "unknown"),
                "governance": "blocked",
                "error_code": safe_replay_error_code(error),
                "current_run_id": execution.run_id,
                "current_load_id": execution.load_id,
            }
            if core is not None:
                details.update(original_run_id=core["run_id"], original_load_id=core["load_id"])
            setattr(error, "replay_details", details)
            raise

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
        if (capsule.version == quality_contracts.TARGET_VERSION) != self.target_requested:
            raise ReplayQualityEvidenceError("MISMATCH")
        validate_replay_acceptance(
            self.execution.snapshot.acceptance_policy, core["acceptance"], before_target=self.target_requested
        )
        if self.target_requested:
            validate_target_plan(self.admission_config, self.execution.snapshot.acceptance_policy, core)
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


def receipt_already_accepted(execution: Any, receipt: Any, *, config: Any) -> bool:
    """Skip only this exact receipt after its guarded acceptance and release."""
    session = getattr(execution, "replay_session", None)
    if receipt is None or session is None or session.accepted_receipt is not receipt:
        return False
    session._require_current(config)
    return True


def safe_replay_error_code(error: BaseException) -> str:
    """Publish only the fixed evidence error vocabulary, never driver messages."""
    code = getattr(error, "code", None)
    allowed = {
        f"DPONE_REPLAY_QUALITY_EVIDENCE_{reason}"
        for reason in ("REQUIRED", "MISMATCH", "INVALID", "FAILED", "INCOMPLETE", "UNSUPPORTED")
    }
    return code if isinstance(code, str) and code in allowed else "DPONE_REPLAY_QUALITY_EVIDENCE_INCOMPLETE"
