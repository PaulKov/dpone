"""Derive the portable Binding core: target mapping, stage calls and modules."""

from dataclasses import dataclass
from hashlib import sha256
from typing import Any, Literal
from uuid import UUID

from dpone._compat import StrEnum
from dpone.contracts.mssql_r1_v3_codec import canonical_bytes
from dpone.contracts.mssql_r1_v3_physical_descriptor_enums import MssqlR1BindingModuleKindV1
from dpone.contracts.mssql_r1_v3_physical_descriptor_resources import (
    MssqlR1AccessKindV1,
    MssqlR1PhysicalResourceAccessV1,
    MssqlR1PhysicalResourceRefV1,
    MssqlR1ResourceKindV1,
)
from dpone.contracts.mssql_r1_v3_registered_target_catalog import MssqlR1RegisteredTargetColumnRefV1
from dpone.contracts.mssql_r1_v3_schema_primitives import MssqlR1ResultColumnV3
from dpone.contracts.mssql_r1_v3_stage_evidence import R1StageArtifactKindV1
from dpone.contracts.postgres_mssql_type_authority import PostgresMssqlSourceColumnRefV1


class MssqlR1StageBufferSymbolV2(StrEnum):
    BATCH_PAYLOAD = "batch_payload"
    XMIN_DELTA = "xmin_delta"
    XMIN_COMPLETE_KEYS = "xmin_complete_keys"


class MssqlR1StageEnvelopeSourceV2(StrEnum):
    OPEN_STAGE_PLAN_CANONICAL_BYTES = "open_stage_plan_canonical_bytes"
    SHA256_REQUEST_PAYLOAD = "sha256_request_payload"
    PROJECTION_JSON_FROM_OPEN_STAGE_PLAN = "projection_json_from_open_stage_plan"


def _raw(value: Any) -> Any:
    return value.canonical_bytes if hasattr(value, "canonical_bytes") else value


class _Canonical:
    @property
    def canonical_bytes(self) -> bytes:
        fields = getattr(type(self), "__dataclass_fields__")
        return canonical_bytes(getattr(type(self), "_domain"), tuple(_raw(getattr(self, name)) for name in fields))

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


@dataclass(frozen=True, slots=True)
class MssqlR1RegisteredTargetRefV2(_Canonical):
    contract_version: Literal["dpone-mssql-r1-registered-target-ref-2"]
    target_binding_uuid: UUID
    target_object_uuid: UUID
    resource_ref: MssqlR1PhysicalResourceRefV1
    stable_target_authority_digest: bytes
    target_catalog_digest: bytes
    _domain = b"dpone-mssql-r1-registered-target-ref-v2\0"


@dataclass(frozen=True, slots=True)
class MssqlR1BusinessColumnMappingV2(_Canonical):
    contract_version: Literal["dpone-mssql-r1-business-column-mapping-2"]
    ordinal: int
    source_column_ref: PostgresMssqlSourceColumnRefV1
    target_column_ref: MssqlR1RegisteredTargetColumnRefV1
    type_decision_id: str
    key_ordinal: Literal[1] | None
    _domain = b"dpone-mssql-r1-business-column-mapping-v2\0"


@dataclass(frozen=True, slots=True)
class MssqlR1BindingTargetMappingV2(_Canonical):
    contract_version: Literal["dpone-mssql-r1-binding-target-mapping-2"]
    registered_target: MssqlR1RegisteredTargetRefV2
    source_schema_authority_digest: bytes
    route_source_authority_sha256: bytes
    type_policy_authority_digest: bytes
    ordered_columns: tuple[MssqlR1BusinessColumnMappingV2, ...]
    _domain = b"dpone-mssql-r1-binding-target-mapping-v2\0"

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            self._domain,
            (
                self.contract_version,
                self.registered_target.canonical_bytes,
                self.source_schema_authority_digest,
                self.route_source_authority_sha256,
                self.type_policy_authority_digest,
                tuple(item.canonical_bytes for item in self.ordered_columns),
            ),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1StageScanProjectionV2(_Canonical):
    contract_version: Literal["dpone-mssql-r1-stage-scan-projection-2"]
    artifact_kind: R1StageArtifactKindV1
    core_scan_procedure_ref: MssqlR1PhysicalResourceRefV1
    stage_scan_template_digest: bytes
    ordered_business_columns: tuple[MssqlR1ResultColumnV3, ...]
    ordered_result_columns: tuple[MssqlR1ResultColumnV3, ...]
    _domain = b"dpone-mssql-r1-stage-scan-projection-v2\0"

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            self._domain,
            (
                self.contract_version,
                self.artifact_kind.value,
                self.core_scan_procedure_ref.canonical_bytes,
                self.stage_scan_template_digest,
                tuple(x.canonical_bytes for x in self.ordered_business_columns),
                tuple(x.canonical_bytes for x in self.ordered_result_columns),
            ),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1StageScanInvocationV2(_Canonical):
    contract_version: Literal["dpone-mssql-r1-stage-scan-invocation-2"]
    ordinal: int
    artifact_kind: R1StageArtifactKindV1
    buffer_symbol: MssqlR1StageBufferSymbolV2
    projection_digest: bytes
    scan_procedure_digest: bytes
    ordered_envelope_sources: tuple[MssqlR1StageEnvelopeSourceV2, ...]
    _domain = b"dpone-mssql-r1-stage-scan-invocation-v2\0"

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            self._domain,
            (
                self.contract_version,
                self.ordinal,
                self.artifact_kind.value,
                self.buffer_symbol.value,
                self.projection_digest,
                self.scan_procedure_digest,
                tuple(x.value for x in self.ordered_envelope_sources),
            ),
        )


@dataclass(frozen=True, slots=True)
class MssqlR1ModuleStageBufferPlanV2:
    contract_version: Literal["dpone-mssql-r1-module-stage-buffer-plan-2"]
    module_kind: MssqlR1BindingModuleKindV1
    ordered_inputs: tuple[MssqlR1StageScanInvocationV2, ...]

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-mssql-r1-module-stage-buffer-plan-v2\0",
            (self.contract_version, self.module_kind.value, tuple(x.canonical_bytes for x in self.ordered_inputs)),
        )

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


@dataclass(frozen=True, slots=True)
class MssqlR1InstantiatedBindingModuleV2:
    contract_version: Literal["dpone-mssql-r1-instantiated-binding-module-2"]
    module_kind: MssqlR1BindingModuleKindV1
    schema_name: Literal["dpone_authority"]
    object_name: str
    descriptor_template_digest: bytes
    target_mapping_digest: bytes
    buffer_plan: MssqlR1ModuleStageBufferPlanV2
    ordered_target_access_intents: tuple[MssqlR1PhysicalResourceAccessV1, ...]

    @property
    def canonical_bytes(self) -> bytes:
        return canonical_bytes(
            b"dpone-mssql-r1-instantiated-binding-module-v2\0",
            (
                self.contract_version,
                self.module_kind.value,
                self.schema_name,
                self.object_name,
                self.descriptor_template_digest,
                self.target_mapping_digest,
                self.buffer_plan.canonical_bytes,
                tuple(x.canonical_bytes for x in self.ordered_target_access_intents),
            ),
        )

    @property
    def digest(self) -> bytes:
        return sha256(self.canonical_bytes).digest()


def derive_binding_core(
    descriptor: Any, source: Any, stable: Any, catalog: Any, payloads: tuple[bytes, ...]
) -> tuple[Any, ...]:
    scan = next(p for p in descriptor.ordered_procedures if p.portable_object.object_name == "dpone_scan_stage_v3")
    target_resource = MssqlR1PhysicalResourceRefV1(
        MssqlR1ResourceKindV1.REGISTERED_TARGET, None, None, "registered_target"
    )
    target_ref = MssqlR1RegisteredTargetRefV2(
        "dpone-mssql-r1-registered-target-ref-2",
        catalog.target_binding_uuid,
        catalog.target_object_uuid,
        target_resource,
        sha256(payloads[4]).digest(),
        catalog.digest,
    )
    decisions = {x.source_shape.canonical_bytes: x for x in source.type_policy_authority.ordered_decisions}
    columns = tuple(
        MssqlR1BusinessColumnMappingV2(
            "dpone-mssql-r1-business-column-mapping-2",
            i,
            item.source_column_ref,
            MssqlR1RegisteredTargetColumnRefV1.from_catalog(catalog, i),
            decisions[item.source_shape.canonical_bytes].decision_id,
            1 if i == catalog.primary_key.ordered_key_column_ordinals[0] else None,
        )
        for i, item in enumerate(source.ordered_columns, 1)
    )
    mapping = MssqlR1BindingTargetMappingV2(
        "dpone-mssql-r1-binding-target-mapping-2",
        target_ref,
        sha256(payloads[3]).digest(),
        source.selected_source_authority_sha256,
        source.type_policy_authority.digest,
        columns,
    )
    scan_ref = MssqlR1PhysicalResourceRefV1(
        MssqlR1ResourceKindV1.STATIC_OBJECT, scan.portable_object.schema_name, scan.portable_object.object_name, None
    )
    business = tuple(
        MssqlR1ResultColumnV3(
            c.ordinal,
            c.name,
            c.system_type_name,
            c.max_length,
            c.precision,
            c.scale,
            c.nullable,
            c.scalar_shape.collation,
        )
        for c in catalog.ordered_columns
    )
    key_column = catalog.ordered_columns[catalog.primary_key.ordered_key_column_ordinals[0] - 1]
    complete_key = MssqlR1ResultColumnV3(
        1,
        key_column.name,
        key_column.system_type_name,
        key_column.max_length,
        key_column.precision,
        key_column.scale,
        key_column.nullable,
        key_column.scalar_shape.collation,
    )
    projections = []
    for kind in R1StageArtifactKindV1:
        selected = (complete_key,) if kind is R1StageArtifactKindV1.XMIN_COMPLETE_KEYS else business
        results = selected + tuple(
            MssqlR1ResultColumnV3(
                len(selected) + s.ordinal,
                s.name,
                s.sql_type,
                s.maximum_length,
                s.precision,
                s.scale,
                s.nullable,
                s.collation,
            )
            for s in scan.portable_object.result_contract.ordered_fixed_suffix_columns
        )
        projections.append(
            MssqlR1StageScanProjectionV2(
                "dpone-mssql-r1-stage-scan-projection-2",
                kind,
                scan_ref,
                sha256(scan.portable_object.result_contract.canonical_bytes).digest(),
                selected,
                results,
            )
        )
    projections_t = tuple(projections)
    by_kind = {x.artifact_kind: x for x in projections_t}
    row_resource = MssqlR1PhysicalResourceRefV1(
        MssqlR1ResourceKindV1.STATIC_OBJECT, "dpone_authority", "dpone_target_row_hash_v3", None
    )

    def accesses(resource: Any) -> tuple[Any, ...]:
        return tuple(
            MssqlR1PhysicalResourceAccessV1(resource, x)
            for x in (
                MssqlR1AccessKindV1.READ,
                MssqlR1AccessKindV1.INSERT,
                MssqlR1AccessKindV1.UPDATE,
                MssqlR1AccessKindV1.DELETE,
            )
        )

    target_access, row_access = accesses(target_resource), accesses(row_resource)
    modules = []
    for template in descriptor.ordered_binding_module_templates:
        kind = template.module_kind
        artifacts = (
            (R1StageArtifactKindV1.BATCH_PAYLOAD,)
            if kind.value.startswith("batch_")
            else (R1StageArtifactKindV1.XMIN_DELTA, R1StageArtifactKindV1.XMIN_COMPLETE_KEYS)
        )
        inputs = tuple(
            MssqlR1StageScanInvocationV2(
                "dpone-mssql-r1-stage-scan-invocation-2",
                i,
                artifact,
                MssqlR1StageBufferSymbolV2(artifact.value),
                by_kind[artifact].digest,
                sha256(scan.canonical_bytes).digest(),
                tuple(MssqlR1StageEnvelopeSourceV2),
            )
            for i, artifact in enumerate(artifacts, 1)
        )
        plan = MssqlR1ModuleStageBufferPlanV2("dpone-mssql-r1-module-stage-buffer-plan-2", kind, inputs)
        intents = (
            target_access
            if kind.value.endswith("mutate")
            else row_access
            if kind.value.endswith("row_hash")
            else (target_access[0], row_access[0])
        )
        modules.append(
            MssqlR1InstantiatedBindingModuleV2(
                "dpone-mssql-r1-instantiated-binding-module-2",
                kind,
                "dpone_authority",
                template.name_template.replace("{binding_uuid_hex}", catalog.target_binding_uuid.hex),
                sha256(template.canonical_bytes).digest(),
                mapping.digest,
                plan,
                intents,
            )
        )
    return scan, mapping, projections_t, tuple(modules)
