"""Canonical pre-release compiler for actual dbt semantic-refresh reports."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Protocol

from dpone.contracts.dbt_publish_models import (
    CompiledDbtModel,
    DbtCompileReport,
    DbtModelArtifact,
)
from dpone.contracts.dbt_semantic_refresh_catalog_proof import (
    SemanticRefreshCatalogProofReceipt,
)
from dpone.contracts.dbt_semantic_refresh_certification import (
    SemanticRefreshCertificationDecision,
    SemanticRefreshCertificationRequest,
)
from dpone.contracts.dbt_semantic_refresh_lifecycle import (
    SemanticRefreshLifecycleReport,
)
from dpone.contracts.dbt_semantic_refresh_plan_policy import (
    SemanticRefreshResourcePolicy,
    SemanticRefreshWritableColumn,
)
from dpone.contracts.dbt_semantic_refresh_pre_release_schema import (
    SemanticRefreshTypeMapperPort,
    compile_pre_release_schema,
)
from dpone.contracts.dbt_semantic_refresh_source_proof import prove_raw_jinja_closure
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_effective_key_identity import EffectiveKeyTemplateColumn
from dpone.contracts.semantic_refresh_lifecycle_policy import (
    SemanticRefreshSqlServerLifecyclePolicy,
)
from dpone.contracts.semantic_refresh_model_proof import (
    SemanticRefreshModelDefinitionProof,
)
from dpone.contracts.semantic_refresh_mutation_closure import (
    SemanticRefreshMutationClosure,
)
from dpone.contracts.semantic_refresh_read_dependency import SemanticRefreshReadDependencyProof
from dpone.contracts.semantic_refresh_types import ClosureStatus

_TARGET_INDEPENDENCE_POLICY = semantic_refresh_sha256(
    {
        "minimum_named_target_compilations": 2,
        "normalized_sql_must_match": True,
        "schema": "dpone.dbt-semantic-refresh-target-independence-policy.v1",
    }
)


class _TargetIndependentSqlProofResult(Protocol):
    status: str
    compiled_sql_sha256: str | None
    read_relations: tuple[tuple[str, str, str], ...]


class TargetIndependentSqlProofPort(Protocol):
    """Consumer-owned target-independent SQL proof capability."""

    def prove(
        self,
        compiled_sql_by_target: dict[str, str],
        *,
        forbidden_relations: tuple[tuple[str, str, str], ...],
    ) -> _TargetIndependentSqlProofResult: ...


class SemanticRefreshPreReleaseCompileError(SemanticRefreshContractError):
    """Fail-closed V2 build-plane proof projection error."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


@dataclass(frozen=True, slots=True)
class SemanticRefreshPreReleaseCompileRequest:
    """Protected non-model inputs combined with one actual compile report."""

    report: DbtCompileReport
    workflow_name: str
    environment: str
    mutation_closure: SemanticRefreshMutationClosure
    lifecycle_policy: SemanticRefreshSqlServerLifecyclePolicy
    lifecycle_report: SemanticRefreshLifecycleReport
    certification_request: SemanticRefreshCertificationRequest
    certification_decision: SemanticRefreshCertificationDecision
    platform_policy_digest: str
    resource_policy: SemanticRefreshResourcePolicy
    catalog_proofs: tuple[SemanticRefreshCatalogProofReceipt, ...]


@dataclass(frozen=True, slots=True)
class SemanticRefreshPreReleaseModelProof:
    """Domain proof projection before canonical bundle assembly."""

    model_unique_id: str
    scope_family_id: str
    event_time_column: str
    effective_key_templates: tuple[EffectiveKeyTemplateColumn, ...]
    writable_columns: tuple[SemanticRefreshWritableColumn, ...]
    model_definition_proof: SemanticRefreshModelDefinitionProof
    read_dependency_proof: SemanticRefreshReadDependencyProof


@dataclass(frozen=True, slots=True)
class SemanticRefreshPreReleaseDomainResult:
    """Proven build-plane values consumed by the application compiler."""

    request: SemanticRefreshPreReleaseCompileRequest
    manifest_sha256: str
    profile_sha256: str
    models: tuple[SemanticRefreshPreReleaseModelProof, ...]


class SemanticRefreshPreReleaseDomainCompiler:
    """Derive keys/schema/model proofs without accepting authored plan inputs."""

    def __init__(
        self,
        *,
        sql_proof: TargetIndependentSqlProofPort,
        type_mapper: SemanticRefreshTypeMapperPort,
    ) -> None:
        self._sql_proof = sql_proof
        self._type_mapper = type_mapper

    def compile(self, request: SemanticRefreshPreReleaseCompileRequest) -> SemanticRefreshPreReleaseDomainResult:
        report = request.report
        if not isinstance(report, DbtCompileReport) or not report.passed or report.manifest_sha256 is None:
            raise _error(
                "COMPILE_UNVERIFIED",
                "actual dbt compile report is not successful and immutable",
            )
        _validate_lifecycle(request)
        models = _workflow_models(report, request.workflow_name)
        proofs = {item.model_unique_id: item for item in request.catalog_proofs}
        if len(proofs) != len(request.catalog_proofs) or set(proofs) != {item.model.unique_id for item in models}:
            raise _error(
                "CATALOG_UNVERIFIED",
                "catalog proof closure differs from the compiled workflow",
            )
        route_receipt = _route_receipt(request.certification_decision)
        if any(
            item.route_capability.get("status") != "CERTIFIED"
            or item.route_capability.get("certification_receipt_sha256") != route_receipt
            for item in models
        ):
            raise _error(
                "LIVE_UNVERIFIED",
                "compiled workflow differs from the protected route receipt",
            )
        inputs = tuple(
            self._model_input(
                compiled,
                manifest_sha256=report.manifest_sha256,
                toolchain_sha256=request.certification_request.toolchain_sha256,
                catalog=proofs[compiled.model.unique_id],
            )
            for compiled in models
        )
        return SemanticRefreshPreReleaseDomainResult(
            request,
            report.manifest_sha256,
            _profile_sha256(models),
            inputs,
        )

    def _model_input(
        self,
        compiled: CompiledDbtModel,
        *,
        manifest_sha256: str,
        toolchain_sha256: str,
        catalog: SemanticRefreshCatalogProofReceipt,
    ) -> SemanticRefreshPreReleaseModelProof:
        model = compiled.model
        sql = self._sql_proof.prove(
            dict(model.compiled_code_by_target),
            forbidden_relations=(_target_relation(model),),
        )
        dependency = catalog.dependency_proof
        if (
            sql.status != "PROVEN"
            or sql.compiled_sql_sha256 is None
            or dependency.compiled_sql_sha256 != sql.compiled_sql_sha256
            or dependency.model_unique_id != model.unique_id
        ):
            raise _error(
                "COMPILE_UNVERIFIED",
                "catalog proof differs from the actual target-independent SQL",
            )
        if model.semantic_refresh_macro_closure_complete is not True:
            raise _error(
                "MACRO_CLOSURE_UNVERIFIED",
                "transitive manifest macro source closure is unavailable or outside its budget",
            )
        macro_sources = model.semantic_refresh_macro_sources
        source = prove_raw_jinja_closure(
            model_raw_sql=model.raw_code,
            macro_sources=macro_sources,
            required_macro_ids=tuple(sorted(macro_sources)),
            allowed_vars=("dpone_data_interval_end", "dpone_data_interval_start"),
            maximum_source_bytes=1024 * 1024,
        )
        if source.status != "PROVEN":
            code = source.issues[0].code if source.issues else "DPONE_DBT_V2_MACRO_CLOSURE_UNVERIFIED"
            raise SemanticRefreshPreReleaseCompileError(code, "raw model/macro closure is not PROVEN")
        try:
            schema = compile_pre_release_schema(model, self._type_mapper)
        except SemanticRefreshContractError as exc:
            message = str(exc)
            suffix = (
                "EVENT_TIME_INVALID"
                if "exactly one date or datetime2(6)" in message
                else "CONTRACT_TYPE_UNSUPPORTED"
                if "writable MSSQL type" in message
                else "EFFECTIVE_KEY_INVALID"
            )
            raise _error(suffix, message) from exc
        definition = SemanticRefreshModelDefinitionProof.build(
            status=ClosureStatus.PROVEN,
            model_unique_id=model.unique_id,
            manifest_sha256=manifest_sha256,
            raw_code_sha256="sha256:" + hashlib.sha256(model.raw_code.encode("utf-8")).hexdigest(),
            compiled_sql_sha256=sql.compiled_sql_sha256,
            macro_closure_sha256=source.proof_sha256,
            toolchain_sha256=toolchain_sha256,
            resolved_relation_dependency_digest=dependency.catalog_snapshot_sha256,
            resolved_module_dependency_digest=dependency.normalized_definitions_sha256,
            catalog_observation_digest=catalog.catalog_observation_sha256,
            parser_runtime_policy_digest=dependency.policy_sha256,
            target_independence_policy_digest=_TARGET_INDEPENDENCE_POLICY,
        )
        return SemanticRefreshPreReleaseModelProof(
            model_unique_id=model.unique_id,
            scope_family_id=semantic_refresh_sha256(
                {
                    "model_unique_id": model.unique_id,
                    "schema": "dpone.dbt-semantic-refresh-scope-family.v1",
                }
            ),
            event_time_column=schema.event_time_column,
            effective_key_templates=schema.effective_key_templates,
            writable_columns=schema.writable_columns,
            model_definition_proof=definition,
            read_dependency_proof=dependency,
        )


def _workflow_models(report: DbtCompileReport, workflow_name: str) -> tuple[CompiledDbtModel, ...]:
    matches = tuple(item for item in report.workflows if item.workflow == workflow_name)
    if len(matches) != 1 or not matches[0].models:
        raise _error(
            "WORKFLOW_INVALID",
            "compiled report lacks one exact semantic-refresh workflow",
        )
    models = tuple(sorted(matches[0].models, key=lambda item: item.model.unique_id))
    if any(item.profile.semantic_refresh is None for item in models):
        raise _error(
            "WORKFLOW_INVALID",
            "V1 and V2 models cannot share a semantic-refresh workflow",
        )
    return models


def _profile_sha256(models: tuple[CompiledDbtModel, ...]) -> str:
    values = {item.profile.semantic_refresh.profile_sha256 for item in models if item.profile.semantic_refresh}
    if len(values) != 1:
        raise _error(
            "PROFILE_INVALID",
            "semantic-refresh workflow must use one exact protected profile",
        )
    return next(iter(values))


def _route_receipt(decision: SemanticRefreshCertificationDecision) -> str:
    if decision.receipt_sha256 is None:
        raise _error("LIVE_UNVERIFIED", "protected route certification receipt is absent")
    return decision.receipt_sha256


def _validate_lifecycle(request: SemanticRefreshPreReleaseCompileRequest) -> None:
    report = request.lifecycle_report
    if (
        not isinstance(report, SemanticRefreshLifecycleReport)
        or report.status != "PROVEN"
        or report.issues
        or report.lifecycle_policy_sha256 != request.lifecycle_policy.sqlserver_lifecycle_policy_sha256
        or report.certification_coordinate_sha256 != request.certification_request.certification_coordinate_sha256
    ):
        raise _error(
            "LIFECYCLE_UNVERIFIED",
            "pinned lifecycle report does not authorize the canonical policy and certification coordinate",
        )


def _target_relation(model: DbtModelArtifact) -> tuple[str, str, str]:
    if model.database is None:
        raise _error("TARGET_IDENTITY_INVALID", "compiled target database is absent")
    return model.database, model.schema, model.alias


def _error(suffix: str, message: str) -> SemanticRefreshPreReleaseCompileError:
    return SemanticRefreshPreReleaseCompileError(f"DPONE_DBT_V2_{suffix}", message)


__all__ = [
    "SemanticRefreshPreReleaseCompileError",
    "SemanticRefreshPreReleaseCompileRequest",
    "SemanticRefreshPreReleaseDomainCompiler",
    "SemanticRefreshPreReleaseDomainResult",
    "SemanticRefreshPreReleaseModelProof",
    "SemanticRefreshTypeMapperPort",
    "TargetIndependentSqlProofPort",
]
