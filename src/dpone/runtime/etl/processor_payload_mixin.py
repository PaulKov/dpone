"""Payload and reconciliation delegation surface for the ETL processor."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from dpone.runtime.governance.quality_execution import QualityExecutionSnapshot, QualityGateExecution

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class ProcessorPayloadMixin:
    """Keep payload delegation separate from the processor run lifecycle."""

    extracted_payload_load_service: Any
    load_governance_service: Any
    reconciliation_service: Any

    @staticmethod
    def _quality_snapshot_from_load_config(load_config: LoadConfig) -> QualityExecutionSnapshot:
        return QualityExecutionSnapshot.from_load_config(load_config)

    def _load_extracted_payload(
        self,
        load_config: LoadConfig,
        extract_result: Any,
        load_record: Any,
        quality_execution: QualityGateExecution,
        owned_payload_scope: Any,
    ) -> tuple[Any, dict[str, Any] | None]:
        return self.extracted_payload_load_service.load_extracted_payload(
            load_config=load_config,
            extract_result=extract_result,
            load_record=load_record,
            reconciliation_service=self.reconciliation_service,
            quality_execution=quality_execution,
            owned_payload_scope=owned_payload_scope,
        )

    def _create_quality_gate_execution(
        self,
        snapshot: QualityExecutionSnapshot,
        load_record: Any,
    ) -> QualityGateExecution:
        create = getattr(self.load_governance_service, "create_quality_gate_execution", None)
        if callable(create):
            return create(
                snapshot=snapshot,
                run_id=str(load_record.run_id),
                load_id=str(load_record.load_id),
            )
        return QualityGateExecution(
            snapshot,
            run_id=str(load_record.run_id),
            load_id=str(load_record.load_id),
        )

    def _load_single_payload(
        self,
        load_config: LoadConfig,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        *,
        update_load_identity: bool = True,
    ) -> Any:
        return self.extracted_payload_load_service.load_single_payload(
            load_config,
            payload,
            extract_result,
            load_record,
            update_load_identity=update_load_identity,
        )

    def _load_nested_table_payload(
        self,
        load_config: LoadConfig,
        payload: Any,
        extract_result: Any,
        load_record: Any,
    ) -> Any:
        return self.extracted_payload_load_service.load_nested_table_payload(
            load_config,
            payload,
            extract_result,
            load_record,
        )

    def _project_temporal_fidelity(self, load_config: LoadConfig, payload: Any) -> Any:
        return self.extracted_payload_load_service.project_temporal_fidelity(load_config, payload)

    def _process_with_batches(self, load_config: LoadConfig, rows: list, schema: list) -> Any:
        raise NotImplementedError("Batch processing deprecated in favor of artifact streaming")

    def _process_reconciliation(self, load_config: LoadConfig, extract_result: Any) -> dict[str, Any]:
        return self.reconciliation_service.process(load_config, extract_result)

    def _get_tech_connector(self):
        return self.reconciliation_service.get_tech_connector()
