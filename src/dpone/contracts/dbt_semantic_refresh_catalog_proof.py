"""Protected SQL Server catalog-observation contracts for dbt semantic refresh."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from typing import Protocol, TypeVar

from dpone.contracts.dbt_semantic_refresh_catalog_observation import (
    RelationIdentity,
    SemanticRefreshCatalogAuthority,
    SemanticRefreshCatalogObservation,
    build_sqlserver_catalog_observation,
)
from dpone.contracts.dbt_semantic_refresh_catalog_types import (
    SqlServerCatalogSnapshot,
    SqlServerDependencyLimits,
    SqlServerObjectMetadata,
)
from dpone.contracts.semantic_refresh_core import (
    SemanticRefreshContractError,
    require_digest,
    require_text,
    semantic_refresh_sha256,
)
from dpone.contracts.semantic_refresh_read_dependency import (
    ReadDependencyEdge,
    ReadDependencyKind,
    SemanticRefreshReadDependencyProof,
)
from dpone.contracts.semantic_refresh_types import ClosureStatus

_T = TypeVar("_T")


class _SqlProofIssue(Protocol):
    code: str


class _ReadDependencyProofResult(Protocol):
    status: str
    resolved_base_object_ids: tuple[int, ...]
    ordered_edges: tuple[tuple[int, int], ...]
    module_definition_digests: tuple[tuple[int, str], ...]
    catalog_sha256: str
    issues: tuple[_SqlProofIssue, ...]


class _TargetIndependentSqlProofResult(Protocol):
    status: str
    compiled_sql_sha256: str | None
    read_relations: tuple[RelationIdentity, ...]
    issues: tuple[_SqlProofIssue, ...]


class TargetIndependentSqlProofPort(Protocol):
    """Consumer-owned target-independent SQL proof capability."""

    def prove(
        self,
        compiled_sql_by_target: dict[str, str],
        *,
        forbidden_relations: tuple[RelationIdentity, ...],
    ) -> _TargetIndependentSqlProofResult: ...


class SemanticRefreshReadDependencyProverPort(Protocol):
    """Prove a bounded closure from one immutable catalog snapshot."""

    def prove(
        self,
        snapshot: SqlServerCatalogSnapshot,
        *,
        root_object_ids: tuple[int, ...],
        target_object_id: int,
        limits: SqlServerDependencyLimits,
    ) -> _ReadDependencyProofResult: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshCatalogProofReceipt:
    """Canonical proof accepted for compile and exact runtime recheck."""

    model_unique_id: str
    catalog_authority_sha256: str
    catalog_observation_sha256: str
    dependency_proof: SemanticRefreshReadDependencyProof
    proof_receipt_sha256: str

    @classmethod
    def build(
        cls,
        *,
        model_unique_id: str,
        catalog_authority_sha256: str,
        catalog_observation_sha256: str,
        dependency_proof: SemanticRefreshReadDependencyProof,
    ) -> SemanticRefreshCatalogProofReceipt:
        require_text(model_unique_id, "model_unique_id")
        require_digest(catalog_authority_sha256, "catalog_authority_sha256")
        require_digest(catalog_observation_sha256, "catalog_observation_sha256")
        if not isinstance(dependency_proof, SemanticRefreshReadDependencyProof):
            raise SemanticRefreshContractError("dependency_proof must be canonical and typed")
        unsigned = {
            "catalog_authority_sha256": catalog_authority_sha256,
            "catalog_observation_sha256": catalog_observation_sha256,
            "dependency_proof": dependency_proof.to_dict(),
            "model_unique_id": model_unique_id,
            "schema": "dpone.dbt-semantic-refresh-catalog-proof-receipt.v1",
        }
        return cls(
            model_unique_id,
            catalog_authority_sha256,
            catalog_observation_sha256,
            dependency_proof,
            semantic_refresh_sha256(unsigned),
        )


class SemanticRefreshCatalogAuthorityVerifierPort(Protocol):
    """Authenticate the complete catalog/DDL authority in protected state."""

    def verify(self, authority: SemanticRefreshCatalogAuthority) -> bool: ...


class SemanticRefreshCatalogObserverPort(Protocol):
    """Observe live SQL Server catalogs; callers cannot provide metadata rows."""

    def observe(
        self,
        *,
        authority: SemanticRefreshCatalogAuthority,
        read_relations: tuple[RelationIdentity, ...],
        target_relation: RelationIdentity,
        limits: SqlServerDependencyLimits,
    ) -> SemanticRefreshCatalogObservation: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshCatalogProofRequest:
    """Primitive compile input; no caller-supplied catalog metadata is accepted."""

    model_unique_id: str
    compiled_sql_by_target: dict[str, str]
    forbidden_relations: tuple[RelationIdentity, ...]
    target_relation: RelationIdentity
    limits: SqlServerDependencyLimits
    policy_sha256: str
    authority: SemanticRefreshCatalogAuthority


class SemanticRefreshCatalogProofError(RuntimeError):
    """Fail-closed compile/runtime dependency proof failure."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code


class SemanticRefreshCatalogProofService:
    """Build a proof only from live catalog rows under protected authority."""

    def __init__(
        self,
        *,
        sql_proof: TargetIndependentSqlProofPort,
        dependency_prover: SemanticRefreshReadDependencyProverPort,
        catalog: SemanticRefreshCatalogObserverPort,
        authority_verifier: SemanticRefreshCatalogAuthorityVerifierPort,
    ) -> None:
        self._sql_proof = sql_proof
        self._dependency_prover = dependency_prover
        self._catalog = catalog
        self._authority_verifier = authority_verifier

    def prove(self, request: SemanticRefreshCatalogProofRequest) -> SemanticRefreshCatalogProofReceipt:
        require_text(request.model_unique_id, "model_unique_id")
        require_digest(request.policy_sha256, "policy_sha256")
        if self._authority_verifier.verify(request.authority) is not True:
            raise SemanticRefreshCatalogProofError(
                "DPONE_DBT_V2_CATALOG_UNVERIFIED",
                "catalog/DDL authority is not protected and exact",
            )
        sql = self._sql_proof.prove(
            request.compiled_sql_by_target,
            forbidden_relations=request.forbidden_relations,
        )
        if sql.status != "PROVEN" or sql.compiled_sql_sha256 is None or not sql.read_relations:
            code = sql.issues[0].code if sql.issues else "DPONE_DBT_V2_COMPILE_UNVERIFIED"
            raise SemanticRefreshCatalogProofError(code, "target-independent compiled SQL is not PROVEN")
        try:
            observation = self._catalog.observe(
                authority=request.authority,
                read_relations=sql.read_relations,
                target_relation=request.target_relation,
                limits=request.limits,
            )
        except Exception as exc:
            raise SemanticRefreshCatalogProofError(
                "DPONE_DBT_V2_CATALOG_UNVERIFIED",
                "SQL Server catalog observation is unavailable under protected authority",
            ) from exc
        snapshot = replace(observation.snapshot, ddl_exclusivity_proven=True)
        raw = self._dependency_prover.prove(
            snapshot,
            root_object_ids=tuple(object_id for _relation, object_id in observation.resolved_relations),
            target_object_id=observation.target_object_id,
            limits=request.limits,
        )
        if raw.status != "PROVEN":
            code = raw.issues[0].code if raw.issues else "DPONE_DBT_V2_CATALOG_UNVERIFIED"
            raise SemanticRefreshCatalogProofError(code, "SQL Server read-dependency closure is not PROVEN")
        dependency = _canonical_dependency_proof(
            request,
            observation,
            raw,
            compiled_sql_sha256=sql.compiled_sql_sha256,
        )
        return SemanticRefreshCatalogProofReceipt.build(
            model_unique_id=request.model_unique_id,
            catalog_authority_sha256=request.authority.authority_sha256,
            catalog_observation_sha256=_observation_sha256(request, observation, raw),
            dependency_proof=dependency,
        )

    def recheck_then_execute(
        self,
        *,
        request: SemanticRefreshCatalogProofRequest,
        expected: SemanticRefreshCatalogProofReceipt,
        execute: Callable[[], _T],
    ) -> _T:
        """Recheck immediately before the injected dbt action and reject drift."""

        observed = self.prove(request)
        if (
            observed.proof_receipt_sha256 != expected.proof_receipt_sha256
            or observed.catalog_observation_sha256 != expected.catalog_observation_sha256
            or observed.dependency_proof.read_dependency_proof_sha256
            != expected.dependency_proof.read_dependency_proof_sha256
        ):
            raise SemanticRefreshCatalogProofError(
                "DPONE_DBT_V2_CATALOG_DRIFT",
                "SQL Server relation/module/catalog proof drifted before dbt",
            )
        return execute()


def _canonical_dependency_proof(
    request: SemanticRefreshCatalogProofRequest,
    observation: SemanticRefreshCatalogObservation,
    raw: _ReadDependencyProofResult,
    *,
    compiled_sql_sha256: str,
) -> SemanticRefreshReadDependencyProof:
    objects = {item.object_id: item for item in observation.snapshot.objects}
    edges = tuple(
        ReadDependencyEdge(source, target, _dependency_kind(objects[target])) for source, target in raw.ordered_edges
    )
    definitions_sha256 = semantic_refresh_sha256(
        {"module_definition_digests": [list(item) for item in raw.module_definition_digests]}
    )
    return SemanticRefreshReadDependencyProof.build(
        status=ClosureStatus.PROVEN,
        model_unique_id=request.model_unique_id,
        database_name=observation.snapshot.database,
        compiled_sql_sha256=compiled_sql_sha256,
        catalog_snapshot_sha256=raw.catalog_sha256,
        normalized_definitions_sha256=definitions_sha256,
        policy_sha256=request.policy_sha256,
        dependency_edges=edges,
        base_relation_object_ids=raw.resolved_base_object_ids,
        max_depth=request.limits.max_depth,
        max_nodes=request.limits.max_nodes,
        max_edges=request.limits.max_edges,
        max_definition_bytes=request.limits.max_definition_bytes,
    )


def _dependency_kind(metadata: SqlServerObjectMetadata) -> ReadDependencyKind:
    if metadata.object_type == "USER_TABLE":
        return ReadDependencyKind.BASE_TABLE
    if metadata.object_type == "VIEW":
        return ReadDependencyKind.SCHEMABOUND_VIEW
    if metadata.object_type == "SQL_INLINE_TABLE_VALUED_FUNCTION":
        return ReadDependencyKind.SCHEMABOUND_INLINE_TVF
    raise SemanticRefreshCatalogProofError(
        "DPONE_DBT_V2_OBJECT_TYPE_UNSUPPORTED",
        "resolved dependency kind is outside the admitted policy",
    )


def _observation_sha256(
    request: SemanticRefreshCatalogProofRequest,
    observation: SemanticRefreshCatalogObservation,
    raw: _ReadDependencyProofResult,
) -> str:
    return semantic_refresh_sha256(
        {
            "catalog_authority_sha256": request.authority.authority_sha256,
            "catalog_sha256": raw.catalog_sha256,
            "database_name": observation.snapshot.database.casefold(),
            "resolved_relations": [[*relation, object_id] for relation, object_id in observation.resolved_relations],
            "target_object_id": observation.target_object_id,
            "target_relation": list(observation.target_relation),
        }
    )


__all__ = [
    "RelationIdentity",
    "SemanticRefreshCatalogAuthority",
    "SemanticRefreshCatalogAuthorityVerifierPort",
    "SemanticRefreshCatalogObservation",
    "SemanticRefreshCatalogProofError",
    "SemanticRefreshCatalogObserverPort",
    "SemanticRefreshCatalogProofReceipt",
    "SemanticRefreshCatalogProofRequest",
    "SemanticRefreshCatalogProofService",
    "SemanticRefreshReadDependencyProverPort",
    "SqlServerDependencyLimits",
    "TargetIndependentSqlProofPort",
    "build_sqlserver_catalog_observation",
]
