"""Load-path coordination for extracted ETL payloads."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.runtime.governance.quality_execution import QualityGateExecution


from dataclasses import replace
from datetime import datetime, timezone
from typing import Any

from dpone.runtime.etl.mssql_schema_preplan import MSSQL_SCHEMA_PREPLAN_OPTION
from dpone.runtime.etl.mssql_transaction_admission import ADMISSION_OPTION
from dpone.runtime.etl.nested_load import NestedLoadService
from dpone.runtime.etl.payload_loader import PayloadLoadService
from dpone.runtime.etl.strategy_policy import is_load_eligible_strategy
from dpone.runtime.governance.service import LoadGovernanceService
from dpone.runtime.lineage import LoadIdentityService, RowLineageEnricher, StrategyMetadataEnricher
from dpone.runtime.lineage.options import LineageOptions
from dpone.runtime.normalization import NestedNormalizationOptions
from dpone.runtime.normalization.load_package import (
    NESTED_PACKAGE_MUTATION_PORT_REQUIRED,
    NestedPackageQualityController,
    NestedPackageStagedMutation,
    require_nested_package_abort,
)
from dpone.runtime.postgres_mssql_r1_execution import r1_execution_from_load_config
from dpone.runtime.sinks.load_payload import LoadPayload

if TYPE_CHECKING:
    from dpone.runtime.etl.owned_payload_scope import OwnedPayloadScope


class ExtractedPayloadLoadService:
    """Select and execute the correct sink load path for an extract result."""

    def __init__(
        self,
        *,
        source: Any | None = None,
        sink: Any,
        logger: Any,
        load_identity_service: Any,
        row_lineage_enricher: Any | None = None,
        nested_normalization_service: Any | None = None,
        nested_load_service: Any | None = None,
        strategy_metadata_enricher: Any | None = None,
        payload_load_service: Any | None = None,
        schema_evolution_service: Any | None = None,
        runtime_lifecycle_service: Any | None = None,
        temporal_fidelity_projector_cls: Any | None = None,
        native_transfer_runtime_service: Any | None = None,
        load_governance_service: Any | None = None,
    ) -> None:
        self.source = source
        self.sink = sink
        self.logger = logger
        self.load_identity_service = load_identity_service
        self.row_lineage_enricher = row_lineage_enricher or RowLineageEnricher()
        self.nested_load_service = nested_load_service or NestedLoadService(
            normalization_service=nested_normalization_service,
            load_identity_service=load_identity_service,
        )
        self.strategy_metadata_enricher = strategy_metadata_enricher or StrategyMetadataEnricher()
        self.payload_load_service = payload_load_service or PayloadLoadService(
            source=source,
            sink=sink,
            logger=logger,
            load_identity_service=load_identity_service,
            strategy_metadata_enricher=self.strategy_metadata_enricher,
            schema_evolution_service=schema_evolution_service,
            runtime_lifecycle_service=runtime_lifecycle_service,
            temporal_fidelity_projector_cls=temporal_fidelity_projector_cls,
            native_transfer_runtime_service=native_transfer_runtime_service,
            load_governance_service=load_governance_service,
        )
        self.load_governance_service = (
            getattr(
                self.payload_load_service,
                "load_governance_service",
                None,
            )
            or load_governance_service
            or LoadGovernanceService()
        )

    def load_extracted_payload(
        self,
        *,
        load_config: Any,
        extract_result: Any,
        load_record: Any,
        reconciliation_service: Any,
        quality_execution: QualityGateExecution | None = None,
        owned_payload_scope: OwnedPayloadScope | None = None,
    ) -> tuple[Any, dict[str, Any] | None]:
        """Load an extract result and return load result plus runtime metrics."""

        if not is_load_eligible_strategy(load_config.load_strategy):
            return self.payload_load_service.empty_load_result(), None

        self.logger.log_etl_progress(
            "LOAD_START",
            {"Strategy": load_config.load_strategy.value},
        )

        extraction_lifecycle = getattr(extract_result, "extraction_lifecycle", None)
        load_config = _with_load_identity(load_config, load_record, extraction_lifecycle)
        reconciliation_metrics = reconciliation_service.run_if_enabled(load_config, extract_result)
        payload = LoadPayload(
            artifact=extract_result.artifact,
            schema=extract_result.schema,
            relation_schema=getattr(extract_result, "relation_schema", None),
            relation_metadata=getattr(extract_result, "relation_metadata", None),
            relation_dialect=getattr(extract_result, "relation_dialect", None),
            target_projection=getattr(extract_result, "target_projection", None),
            mssql_transaction_admission=(getattr(load_config, "options", {}) or {}).get(ADMISSION_OPTION),
            mssql_target_mutation_plan=getattr(
                (getattr(load_config, "options", {}) or {}).get(MSSQL_SCHEMA_PREPLAN_OPTION),
                "target_mutation_plan",
                None,
            ),
            postgres_mssql_r1_execution=r1_execution_from_load_config(load_config),
            extraction_lifecycle=extraction_lifecycle,
            owned_payload_scope=owned_payload_scope,
        )
        lineage_options = LineageOptions.from_config((load_config.options or {}).get("lineage"))
        nested_options = NestedNormalizationOptions.from_config((load_config.options or {}).get("normalization"))
        if nested_options.enabled:
            require_nested_package_abort(self.sink)
            package_quality = NestedPackageQualityController.for_load(
                governance_service=self.load_governance_service,
                quality_execution=quality_execution,
                load_config=load_config,
                load_record=load_record,
            )

            def record_nested_failure(error: BaseException, details: Any) -> None:
                self.load_governance_service.record_load_step(
                    load_record=load_record,
                    step_id="load_governance_failed",
                    phase="load_governance",
                    kind="nested_package_finalization",
                    status="failed",
                    error_message=str(getattr(error, "code", type(error).__name__)),
                    details=details,
                )

            staged_mutation = NestedPackageStagedMutation(
                self.sink,
                failure_recorder=record_nested_failure,
            )

            def prepare_member(
                table_config: Any,
                table_payload: Any,
                table_extract_result: Any,
                table_load_record: Any,
            ) -> tuple[Any, Any]:
                member_config = package_quality.member_load_config(table_config)
                prepared = self.payload_load_service.prepare_nested_member_payload(
                    member_config,
                    table_payload,
                    table_extract_result,
                    table_load_record,
                    source=self.source,
                )
                return member_config, prepared

            def evaluate_package(package_result: Any) -> Any:
                return package_quality.finalize(
                    load_config=load_config,
                    extract_result=extract_result,
                    load_result=package_result,
                )

            load_result = self.nested_load_service.load(
                load_config=load_config,
                payload=payload,
                extract_result=extract_result,
                load_record=load_record,
                lineage_options=lineage_options,
                nested_options=nested_options,
                package_mutation=staged_mutation,
                prepare_member=prepare_member,
                evaluate_package=evaluate_package,
            )
            return load_result, merge_runtime_metrics(reconciliation_metrics, load_result.reconciliation_metrics)

        if not _sink_owns_native_lineage(self.sink):
            payload = self.row_lineage_enricher.enrich_payload(
                payload,
                options=lineage_options,
                run_id=load_record.run_id,
                load_id=load_record.load_id,
                source_type=str((load_config.options or {}).get("source_type", "")),
                source_schema=load_config.source_schema,
                source_table=load_config.source_table,
                unique_key=load_config.unique_key,
                extracted_at=_extraction_started_at(extraction_lifecycle),
            )
        load_result = self.load_single_payload(
            load_config,
            payload,
            extract_result,
            load_record,
            quality_execution=quality_execution,
        )
        return load_result, merge_runtime_metrics(reconciliation_metrics, load_result.reconciliation_metrics)

    def load_single_payload(
        self,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        *,
        update_load_identity: bool = True,
        quality_execution: QualityGateExecution | None = None,
    ) -> Any:
        """Load one already prepared payload into the sink."""

        return self.payload_load_service.load_single_payload(
            load_config,
            payload,
            extract_result,
            load_record,
            source=self.source,
            update_load_identity=update_load_identity,
            quality_execution=quality_execution,
        )

    def load_nested_table_payload(
        self,
        load_config: Any,
        payload: Any,
        extract_result: Any,
        load_record: Any,
        *,
        quality_execution: QualityGateExecution | None = None,
        package_quality: NestedPackageQualityController | None = None,
    ) -> Any:
        """Fail closed: nested members cannot commit outside the package mutation port."""

        del load_config, payload, extract_result, load_record, quality_execution, package_quality
        raise RuntimeError(NESTED_PACKAGE_MUTATION_PORT_REQUIRED)

    def project_temporal_fidelity(self, load_config: Any, payload: Any) -> Any:
        """Project payload temporal fidelity through the shared payload loader."""

        return self.payload_load_service.project_temporal_fidelity(load_config, payload)


def create_load_identity_service() -> Any:
    """Create the default load identity service for ETL processor runs."""

    return LoadIdentityService()


def create_load_governance_service() -> Any:
    """Create the default load governance service for ETL processor runs."""

    return LoadGovernanceService()


def _with_load_identity(load_config: Any, load_record: Any, extraction_lifecycle: Any | None) -> Any:
    options = dict(getattr(load_config, "options", {}) or {})
    identity = {
        "run_id": getattr(load_record, "run_id", ""),
        "load_id": getattr(load_record, "load_id", ""),
    }
    started_at = _extraction_started_at(extraction_lifecycle)
    if started_at is not None:
        identity["extracted_at"] = _isoformat_utc(started_at)
    options["__dpone_load_identity"] = identity
    return replace(load_config, options=options)


def _sink_owns_native_lineage(sink: Any) -> bool:
    capability = getattr(sink, "native_lineage_projection_capability", None)
    return callable(capability) and capability() == "mssql_native_lineage_v1"


def _extraction_started_at(extraction_lifecycle: Any | None) -> datetime | None:
    receipt = getattr(extraction_lifecycle, "receipt", None)
    value = getattr(receipt, "extraction_started_at", None)
    return value if isinstance(value, datetime) else None


def _isoformat_utc(value: Any) -> str:
    if isinstance(value, datetime):
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)  # noqa: UP017
        return value.astimezone(timezone.utc).isoformat()  # noqa: UP017
    return datetime.now(timezone.utc).isoformat()  # noqa: UP017


def merge_runtime_metrics(
    reconciliation_metrics: dict[str, Any] | None,
    load_metrics: dict[str, Any] | None,
) -> dict[str, Any] | None:
    """Merge optional reconciliation and load lifecycle metrics."""

    if not reconciliation_metrics and not load_metrics:
        return None
    return {**(reconciliation_metrics or {}), **(load_metrics or {})}


__all__ = [
    "ExtractedPayloadLoadService",
    "create_load_governance_service",
    "create_load_identity_service",
    "merge_runtime_metrics",
]
