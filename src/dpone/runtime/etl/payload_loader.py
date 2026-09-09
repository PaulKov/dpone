"""Single-payload load lifecycle for ``ETLProcessor``."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.governance.quality_execution import QualityGateExecution


from dataclasses import replace
from typing import Any, Protocol

from dpone.governance.quality import QualityGatePolicy
from dpone.runtime.decision_audit import publish_runtime_decision
from dpone.runtime.etl.payload_lifecycle import PayloadLifecyclePreparer
from dpone.runtime.etl.payload_loader_defaults import (
    default_native_transfer_runtime_service,
    default_runtime_lifecycle_service,
    default_schema_evolution_service,
    default_schema_identity_service,
)
from dpone.runtime.etl.payload_loader_invocation import (
    invoke_with_quality_scope,
    invoke_with_target_guard,
)
from dpone.runtime.etl.payload_loader_native_terminal import (
    before_target_mutation,
    commit_checkpoints,
    mark_failed_preserving_primary,
    native_quality_scope,
    publish_success_report,
    raise_commit_unknown_if_needed,
    record_target_success,
    require_committed_success_handling,
)
from dpone.runtime.etl.payload_loader_staged_load import (
    quality_config,
    requested_finalization_phase,
    supports_staged_load,
)
from dpone.runtime.etl.payload_loader_terminal_mixin import PayloadLoadTerminalMixin
from dpone.runtime.governance.acceptance_metrics import AcceptanceMetricPolicy
from dpone.runtime.governance.finalization import LoadGovernanceFinalizationCoordinator
from dpone.runtime.governance.legacy_acceptance import LegacyLoadGovernanceCoordinator
from dpone.runtime.governance.quality_receipt import validate_quality_gate_receipt
from dpone.runtime.governance.service import LoadGovernanceService


class SinkLoadPort(Protocol):
    """Minimal sink contract needed by the payload loader."""

    def load(self, load_config: Any, payload: Any) -> Any: ...


class PayloadLoadService(PayloadLoadTerminalMixin):
    """Apply runtime lifecycle gates and load one prepared payload into a sink."""

    def __init__(
        self,
        *,
        source: Any | None = None,
        sink: SinkLoadPort,
        logger: Any,
        load_identity_service: Any,
        strategy_metadata_enricher: Any,
        schema_identity_service: Any | None = None,
        schema_evolution_service: Any | None = None,
        runtime_lifecycle_service: Any | None = None,
        temporal_fidelity_projector_cls: Any | None = None,
        native_transfer_runtime_service: Any | None = None,
        load_governance_service: Any | None = None,
        finalization_coordinator: Any | None = None,
        legacy_governance_coordinator: Any | None = None,
        snapshot_envelope_lifecycle_policy: Any | None = None,
    ) -> None:
        self.source = source
        self.sink = sink
        self.logger = logger
        self.load_identity_service = load_identity_service
        self.strategy_metadata_enricher = strategy_metadata_enricher
        self.schema_identity_service = schema_identity_service or default_schema_identity_service()
        self.schema_evolution_service = schema_evolution_service or default_schema_evolution_service()
        self.runtime_lifecycle_service = runtime_lifecycle_service or default_runtime_lifecycle_service()
        self.temporal_fidelity_projector_cls = temporal_fidelity_projector_cls
        self.native_transfer_runtime_service = (
            native_transfer_runtime_service or default_native_transfer_runtime_service()
        )
        self.load_governance_service = load_governance_service or LoadGovernanceService()
        self.finalization_coordinator = finalization_coordinator or LoadGovernanceFinalizationCoordinator(
            self.load_governance_service
        )
        self.legacy_governance_coordinator = legacy_governance_coordinator or LegacyLoadGovernanceCoordinator(
            self.load_governance_service
        )
        self.payload_lifecycle_preparer = PayloadLifecyclePreparer(
            schema_identity_service=self.schema_identity_service,
            schema_evolution_service=self.schema_evolution_service,
            runtime_lifecycle_service=self.runtime_lifecycle_service,
            temporal_fidelity_projector_cls=self.temporal_fidelity_projector_cls,
            snapshot_envelope_lifecycle_policy=snapshot_envelope_lifecycle_policy,
        )

    def load_single_payload(
        self,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        *,
        source: Any | None = None,
        update_load_identity: bool = True,
        quality_execution: QualityGateExecution | None = None,
    ) -> Any:
        if quality_execution is not None:
            quality_execution.assert_current(load_config=load_config)
        else:
            validate_quality_config = getattr(self.load_governance_service, "validate_quality_config", None)
            if callable(validate_quality_config):
                validate_quality_config(load_config=load_config)
            else:
                AcceptanceMetricPolicy.from_load_config(load_config)
                QualityGatePolicy.from_config(quality_config(load_config))
        payload = self.strategy_metadata_enricher.enrich_payload(payload, load_config=load_config)
        if quality_execution is not None:
            quality_execution.assert_current(load_config=load_config)
        native_context = self.native_transfer_runtime_service.prepare_before_load(
            load_config=load_config,
            payload=payload,
            run_id=load_record.run_id,
        )
        payload = native_context.payload
        quality_scope = native_quality_scope(self.native_transfer_runtime_service, native_context)
        if native_context.should_skip_load:
            if quality_execution is not None:
                quality_execution.select_boundary("resume_validation", load_config=load_config)
            load_result = invoke_with_quality_scope(
                self.legacy_governance_coordinator.validate_resume_only,
                quality_scope=quality_scope,
                quality_execution=quality_execution,
                source=source if source is not None else self.source,
                sink=self.sink,
                load_config=load_config,
                payload=payload,
                extract_result=extract_result,
                load_record=load_record,
                load_result=native_context.skipped_load_result(),
            )
            scope = getattr(payload, "owned_payload_scope", None)
            if scope is not None:
                scope.success()
            load_result = self._validate_coordinator_receipt(
                load_config,
                load_result,
                quality_execution=quality_execution,
            )
            if quality_execution is not None:
                quality_execution.assert_current(load_config=load_config)
            load_result = publish_success_report(self.native_transfer_runtime_service, native_context, load_result)
            if update_load_identity:
                self._mark_load_committed(load_record, extract_result, load_result)
            return load_result

        native_checkpoint_committed = False
        try:
            if quality_execution is not None:
                quality_execution.select_boundary(
                    "pre_commit" if supports_staged_load(self.sink) else "post_commit",
                    load_config=load_config,
                )
            lifecycle_context = self.payload_lifecycle_preparer.prepare(
                load_config=load_config,
                payload=payload,
                logger=self.logger,
                sink=self.sink,
                run_id=load_record.run_id,
                load_id=load_record.load_id,
                assert_current=(
                    (lambda: quality_execution.assert_current(load_config=load_config))
                    if quality_execution is not None
                    else None
                ),
            )
            payload = lifecycle_context.payload
            if update_load_identity:
                staged_record = self.load_identity_service.mark_staged(
                    load_record, extracted_rows=getattr(extract_result.artifact, "estimated_rows", None)
                )
                load_record = staged_record or load_record

            load_result = self._load_payload(
                load_config,
                payload,
                extract_result,
                load_record,
                source=source,
                before_target_mutation=lambda: before_target_mutation(
                    self.native_transfer_runtime_service,
                    native_context,
                ),
                on_target_committed=lambda result: self._record_target_commit(
                    payload,
                    native_context,
                    result,
                ),
                quality_scope=quality_scope,
                quality_execution=quality_execution,
            )
            load_result = self._validate_coordinator_receipt(
                load_config,
                load_result,
                quality_execution=quality_execution,
            )
            runtime_metrics = self.runtime_lifecycle_service.runtime_metrics(lifecycle_context)
            evidence_path = self.runtime_lifecycle_service.write_evidence(
                load_config=load_config,
                run_id=load_record.run_id,
                context=lifecycle_context,
            )
            if quality_execution is not None:
                quality_execution.assert_current(load_config=load_config)
            load_result, native_checkpoint_committed = commit_checkpoints(
                self.native_transfer_runtime_service,
                native_context,
                load_result,
            )
            try:
                load_result = publish_success_report(self.native_transfer_runtime_service, native_context, load_result)
            except Exception as exc:
                if native_checkpoint_committed:
                    require_committed_success_handling(exc)
                raise
        except Exception as exc:
            if not native_checkpoint_committed:
                mark_failed_preserving_primary(self.native_transfer_runtime_service, native_context, exc)
            raise_commit_unknown_if_needed(
                self.native_transfer_runtime_service,
                native_context,
                exc,
            )
            raise

        load_result = self._with_runtime_metrics(load_result, runtime_metrics, evidence_path)
        if update_load_identity:
            self.load_identity_service.mark_committed(load_record, load_result)
        return load_result

    def _record_target_commit(self, payload: Any, native_context: Any, load_result: Any) -> Any:
        """Freeze source SUCCESS before any post-commit runtime bookkeeping."""

        scope = getattr(payload, "owned_payload_scope", None)
        if scope is not None:
            scope.mark_target_committed(load_result)
        return record_target_success(
            self.native_transfer_runtime_service,
            native_context,
            load_result,
        )

    def _validate_coordinator_receipt(
        self,
        load_config: Any,
        load_result: Any,
        *,
        quality_execution: QualityGateExecution | None,
    ) -> Any:
        validator = getattr(self.load_governance_service, "validate_quality_gate_receipt", None)
        receipt = getattr(load_result, "quality_gate_receipt", None)
        if quality_execution is not None:
            quality_execution.accept_payload(receipt, load_config=load_config)
            return load_result
        if callable(validator):
            validated = validator(load_config=load_config, receipt=receipt)
        else:
            validated = validate_quality_gate_receipt(
                receipt,
                policy=QualityGatePolicy.from_config(quality_config(load_config)),
            )
        return replace(load_result, quality_gate_receipt=validated)

    def _load_payload(
        self,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        *,
        source: Any | None = None,
        before_target_mutation: Any | None = None,
        on_target_committed: Any | None = None,
        quality_scope: Any | None = None,
        quality_execution: QualityGateExecution | None = None,
    ) -> Any:
        if supports_staged_load(self.sink):
            return invoke_with_target_guard(
                self.finalization_coordinator.load,
                before_target_mutation=before_target_mutation,
                quality_scope=quality_scope,
                quality_execution=quality_execution,
                source=source or self.source,
                sink=self.sink,
                load_config=load_config,
                payload=payload,
                extract_result=extract_result,
                load_record=load_record,
                on_target_committed=on_target_committed,
            )
        publish_runtime_decision(
            {
                "requested_backend": requested_finalization_phase(load_config),
                "selected_backend": "legacy_post_finalize",
                "fallback_reason": "staged_load_port_unavailable",
                "release_gate": "warning",
                "warnings": ["staged_load_port_unavailable"],
            },
            decision_id="load_governance.finalization",
            phase="load_governance",
            component="payload_loader",
            category="finalization_phase",
            fallback_allowed=True,
        )
        return invoke_with_target_guard(
            self.legacy_governance_coordinator.load,
            before_target_mutation=before_target_mutation,
            quality_scope=quality_scope,
            quality_execution=quality_execution,
            source=source if source is not None else self.source,
            sink=self.sink,
            load_config=load_config,
            payload=payload,
            extract_result=extract_result,
            load_record=load_record,
            on_target_committed=on_target_committed,
        )

    def prepare_nested_member_payload(
        self,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        *,
        source: Any | None = None,
    ) -> Any:
        """Prepare a nested package member payload without target finalization."""

        del source, extract_result  # Nested package staging uses the bound sink only.
        validate_quality_config = getattr(self.load_governance_service, "validate_quality_config", None)
        if callable(validate_quality_config):
            validate_quality_config(load_config=load_config)
        else:
            AcceptanceMetricPolicy.from_load_config(load_config)
            QualityGatePolicy.from_config(quality_config(load_config))
        payload = self.strategy_metadata_enricher.enrich_payload(payload, load_config=load_config)
        return self.payload_lifecycle_preparer.prepare_nested_member(
            load_config=load_config,
            payload=payload,
            logger=self.logger,
            sink=self.sink,
            run_id=load_record.run_id,
            load_id=load_record.load_id,
        )

    def project_temporal_fidelity(self, load_config: Any, payload: Any) -> Any:
        return self.payload_lifecycle_preparer.project_temporal_fidelity(load_config, payload)


__all__ = [
    "PayloadLoadService",
    "SinkLoadPort",
    "supports_staged_load",
]
