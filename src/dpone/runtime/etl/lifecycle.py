"""Runtime lifecycle gates for ``ETLProcessor``.

The service keeps production gates out of the processor itself:

- row-level schema contract enforcement;
- target type compatibility checks;
- optional physical DDL apply;
- auditable data-contract evidence.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any, cast

from dpone.readiness.schema_contracts import SchemaContract
from dpone.readiness.type_compatibility import TypeCompatibilityGate, TypeCompatibilityReport
from dpone.runtime.dbt_schema_readiness import DbtSchemaReadinessGate
from dpone.runtime.etl.contract_artifacts import (
    ContractEnforcedStreamingArtifact,
    ContractValidatedFileArtifact,
    ContractValidationSummary,
)
from dpone.runtime.etl.physical_design_lifecycle import RuntimePhysicalDesignService
from dpone.runtime.in_memory_rows import InMemoryRowsArtifact
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.type_system import ContractEnforcementResult, ContractEnforcementService
from dpone.type_system.models import ConflictPolicy


@dataclass(frozen=True, slots=True)
class RuntimeLifecycleContext:
    payload: LoadPayload
    enforcement: ContractEnforcementResult | None = None
    compatibility: TypeCompatibilityReport | None = None
    ddl_apply: Mapping[str, Any] | None = None
    evidence_path: str | None = None


class RuntimeLifecycleService:
    """Coordinates production gates around staging and finalization."""

    def prepare_before_schema_evolution(
        self,
        *,
        load_config: Any,
        payload: LoadPayload,
        run_id: str,
        load_id: str,
    ) -> RuntimeLifecycleContext:
        self._validate_dbt_schema_readiness(load_config, payload)
        contract = self._schema_contract(load_config)
        if not contract.columns:
            return RuntimeLifecycleContext(payload=payload)
        schema = (
            list(payload.schema)
            if payload.target_projection is not None
            else self._contract_schema(payload.schema, contract)
        )
        if self._is_streaming(payload):
            return RuntimeLifecycleContext(
                payload=payload.rebind(
                    artifact=ContractEnforcedStreamingArtifact(
                        payload.artifact,
                        contract=contract,
                        run_id=run_id,
                        load_id=load_id,
                        quarantine=self._quarantine(load_config),
                        conflict_policy=self._conflict_policy(load_config),
                    ),
                    schema=schema,
                )
            )
        if self._is_file(payload):
            return RuntimeLifecycleContext(
                payload=payload.rebind(
                    artifact=ContractValidatedFileArtifact(
                        payload.artifact,
                        contract=contract,
                        schema=tuple((str(name), str(dtype)) for name, dtype in payload.schema),
                        run_id=run_id,
                        load_id=load_id,
                    ),
                    schema=schema,
                )
            )
        enforcement = ContractEnforcementService(quarantine=self._quarantine(load_config)).enforce(
            rows=self._rows(payload),
            contract=contract,
            run_id=run_id,
            load_id=load_id,
            conflict_policy=self._conflict_policy(load_config),
        )
        if not enforcement.passed:
            raise RuntimeError("data contract enforcement failed")
        return RuntimeLifecycleContext(
            payload=payload.rebind(
                artifact=InMemoryRowsArtifact(enforcement.target_rows),
                schema=schema,
            ),
            enforcement=enforcement,
        )

    def prepare_after_schema_evolution(
        self,
        *,
        load_config: Any,
        sink: Any,
        context: RuntimeLifecycleContext,
    ) -> RuntimeLifecycleContext:
        compatibility = self._type_compatibility(load_config, sink, context.payload)
        if compatibility is not None and not compatibility.passed:
            raise RuntimeError("target type compatibility gate failed")
        physical = RuntimePhysicalDesignService().prepare(load_config, sink, context.payload)
        ddl_apply = physical.report
        blockers = tuple(str(item) for item in (ddl_apply or {}).get("blockers", ()) if str(item))
        if blockers:
            raise RuntimeError(f"physical DDL apply blocked: {', '.join(blockers)}")
        return RuntimeLifecycleContext(
            payload=physical.payload,
            enforcement=context.enforcement,
            compatibility=compatibility,
            ddl_apply=ddl_apply,
            evidence_path=context.evidence_path,
        )

    def write_evidence(
        self,
        *,
        load_config: Any,
        run_id: str,
        context: RuntimeLifecycleContext,
    ) -> str | None:
        evidence_dir = self._evidence_dir(load_config)
        enforcement = context.enforcement or getattr(context.payload.artifact, "enforcement_result", None)
        if evidence_dir is None or enforcement is None:
            return None
        module = import_module("dpone.ops.data_contract_evidence")
        writer = module.DataContractEvidenceBundleWriter(evidence_dir)
        artifact = writer.write(
            run_id=run_id,
            pipeline=f"{load_config.source_schema}.{load_config.source_table}->{load_config.target_schema}.{load_config.target_table}",
            enforcement=enforcement,
            ddl_apply=dict(context.ddl_apply or {}),
            compatibility=context.compatibility.to_dict() if context.compatibility is not None else {"passed": True},
        )
        return str(artifact.json_path)

    def runtime_metrics(self, context: RuntimeLifecycleContext) -> dict[str, Any]:
        summary = self._validation_summary(context)
        if summary is None:
            return {}
        return {"contract_validation": summary.to_dict()}

    def _schema_contract(self, load_config: Any) -> SchemaContract:
        options = self._options(load_config)
        return SchemaContract.from_config(options.get("schema_contract", {}))

    def _validate_dbt_schema_readiness(self, load_config: Any, payload: LoadPayload) -> None:
        options = self._options(load_config)
        readiness = options.get("dbt_schema_readiness")
        if not isinstance(readiness, Mapping):
            return
        contract = options.get("schema_contract")
        DbtSchemaReadinessGate().validate(
            readiness=readiness,
            schema_contract=(contract if isinstance(contract, Mapping) else {}),
            source_database=str(getattr(load_config, "source_database", "") or ""),
            source_schema=str(getattr(load_config, "source_schema", "") or ""),
            source_table=str(getattr(load_config, "source_table", "") or ""),
            observed_schema=payload.relation_schema or (),
        )

    def _quarantine(self, load_config: Any) -> Any | None:
        options = self._options(load_config)
        legacy = options.get("quarantine")
        dlq = options.get("dlq")
        if dlq is None and isinstance(legacy, Mapping) and legacy.get("dir"):
            module = import_module("dpone.ops.quarantine")
            return module.QuarantineService(legacy["dir"])
        if self._schema_contract(load_config).enforcement != "quarantine" and dlq is None:
            return None
        raw = dlq if isinstance(dlq, Mapping) else {}
        contracts = import_module("dpone.contracts.dlq")
        storage = import_module("dpone.ops.dlq_store")
        services = import_module("dpone.ops.dlq")
        environment = str(options.get("environment") or getattr(load_config, "environment", "") or "")
        policy = contracts.DlqPolicy.from_config(raw, environment=environment)
        if not policy.enabled:
            raise RuntimeError("DPONE_DLQ_STORE_REQUIRED: quarantine enforcement cannot disable the DLQ")
        store = storage.DlqFileStore(
            policy.directory,
            max_record_bytes=policy.max_record_bytes,
            max_diagnostic_bytes=policy.max_diagnostic_bytes,
            max_index_bytes=policy.max_index_bytes,
        )
        return services.DlqService(store, policy=policy)

    def _rows(self, payload: LoadPayload) -> Sequence[Mapping[str, Any]]:
        rows = getattr(payload.artifact, "_rows", None)
        if rows is None:
            raise RuntimeError("data contract enforcement requires a row-addressable artifact")
        return rows

    def _is_streaming(self, payload: LoadPayload) -> bool:
        return getattr(payload.artifact, "_iterator", None) is not None

    def _is_file(self, payload: LoadPayload) -> bool:
        return getattr(payload.artifact, "file_path", None) is not None

    def _validation_summary(self, context: RuntimeLifecycleContext) -> ContractValidationSummary | None:
        if context.enforcement is not None:
            return ContractValidationSummary(
                accepted_rows=len(context.enforcement.target_rows),
                rejected_rows=context.enforcement.rejected_rows,
                quarantined_rows=context.enforcement.quarantined_rows,
                validation_mode="rows",
            )
        summary = getattr(context.payload.artifact, "validation_summary", None)
        return summary if isinstance(summary, ContractValidationSummary) else None

    def _contract_schema(self, schema: Sequence[tuple[str, str]], contract: SchemaContract) -> list[tuple[str, str]]:
        return [(name, self._contract_dtype(name, dtype, contract)) for name, dtype in schema]

    def _contract_dtype(self, name: str, dtype: str, contract: SchemaContract) -> str:
        column = contract.column(name)
        if column is None:
            return dtype
        logical = column.logical_type.lower()
        if logical == "decimal":
            return f"decimal({column.precision or 38},{column.scale or 9})"
        if logical == "timestamp":
            return "timestamp"
        if logical in {"integer", "bigint"}:
            return "bigint"
        if logical == "boolean":
            return "boolean"
        if logical == "date":
            return "date"
        if logical == "time":
            return "time"
        if logical == "json":
            return "json"
        if logical == "binary":
            return "binary"
        return dtype

    def _type_compatibility(self, load_config: Any, sink: Any, payload: LoadPayload) -> TypeCompatibilityReport | None:
        if not hasattr(sink, "get_target_schema"):
            return None
        target_schema = sink.get_target_schema(load_config)
        if not target_schema:
            return None
        return TypeCompatibilityGate().evaluate(
            sink_type=self._sink_type(load_config),
            source_columns={
                name: dtype
                for name, dtype in (
                    payload.target_projection.target_schema if payload.target_projection is not None else payload.schema
                )
            },
            target_columns={name: dtype for name, dtype in target_schema},
            conflict_policy=self._conflict_policy(load_config),
        )

    def _apply_physical_design(self, load_config: Any, sink: Any, payload: LoadPayload) -> Mapping[str, Any] | None:
        return RuntimePhysicalDesignService().apply_legacy(load_config, sink, payload)

    def _conflict_policy(self, load_config: Any) -> ConflictPolicy:
        options = self._options(load_config)
        raw = options.get("type_inference", {})
        value = raw.get("conflict_policy", "fail") if isinstance(raw, Mapping) else "fail"
        if value in {"fail", "variant_column", "quarantine"}:
            return cast(ConflictPolicy, value)
        return "fail"

    def _evidence_dir(self, load_config: Any) -> Path | None:
        raw = self._options(load_config).get("runtime_evidence", {})
        if not isinstance(raw, Mapping):
            return None
        output = raw.get("output_dir")
        return Path(output) if output else None

    def _sink_type(self, load_config: Any) -> str:
        options = self._options(load_config)
        return str(options.get("sink_type") or options.get("target_type") or "unknown")

    def _options(self, load_config: Any) -> Mapping[str, Any]:
        options = getattr(load_config, "options", {}) or {}
        return options if isinstance(options, Mapping) else {}


__all__ = ["RuntimeLifecycleContext", "RuntimeLifecycleService"]
