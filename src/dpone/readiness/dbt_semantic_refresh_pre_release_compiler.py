"""Application composition for the canonical V2 pre-release compiler."""

from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshPreReleaseModelInput,
    SemanticRefreshPreReleaseProofBundle,
)
from dpone.contracts.dbt_semantic_refresh_pre_release_domain import (
    SemanticRefreshPreReleaseCompileError,
    SemanticRefreshPreReleaseCompileRequest,
    SemanticRefreshPreReleaseDomainCompiler,
    TargetIndependentSqlProofPort,
)
from dpone.type_system.source_sink.mssql_clickhouse import MssqlClickHouseMatrixMapper


class SemanticRefreshPreReleaseCompiler:
    """Inject dpone's canonical lossless MSSQL-to-ClickHouse type mapper."""

    def __init__(self, *, sql_proof: TargetIndependentSqlProofPort) -> None:
        self._domain = SemanticRefreshPreReleaseDomainCompiler(
            sql_proof=sql_proof,
            type_mapper=MssqlClickHouseMatrixMapper(),
        )

    def compile(self, request: SemanticRefreshPreReleaseCompileRequest) -> SemanticRefreshPreReleaseProofBundle:
        """Assemble the canonical proof bundle from proven domain values."""

        result = self._domain.compile(request)
        models = tuple(
            SemanticRefreshPreReleaseModelInput(
                model_unique_id=item.model_unique_id,
                scope_family_id=item.scope_family_id,
                event_time_column=item.event_time_column,
                effective_key_templates=item.effective_key_templates,
                writable_columns=item.writable_columns,
                model_definition_proof=item.model_definition_proof,
                read_dependency_proof=item.read_dependency_proof,
            )
            for item in result.models
        )
        return SemanticRefreshPreReleaseProofBundle.build(
            workflow_name=request.workflow_name,
            environment=request.environment,
            manifest_sha256=result.manifest_sha256,
            profile_sha256=result.profile_sha256,
            toolchain_sha256=request.certification_request.toolchain_sha256,
            certification_request=request.certification_request,
            certification_decision=request.certification_decision,
            platform_policy_digest=request.platform_policy_digest,
            resource_policy=request.resource_policy,
            mutation_closure=request.mutation_closure,
            lifecycle_policy=request.lifecycle_policy,
            lifecycle_report=request.lifecycle_report,
            models=models,
        )


__all__ = [
    "SemanticRefreshPreReleaseCompileError",
    "SemanticRefreshPreReleaseCompileRequest",
    "SemanticRefreshPreReleaseCompiler",
]
