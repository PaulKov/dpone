"""Typed authority and pure policy for worker-time dbt proof rechecks."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass, replace
from typing import Any, Protocol

from dpone.contracts.dbt_semantic_refresh_catalog_proof import (
    SemanticRefreshCatalogProofReceipt,
    SemanticRefreshCatalogProofRequest,
)
from dpone.contracts.dbt_semantic_refresh_lifecycle import (
    DBT_CORE_VERSION,
    SemanticRefreshLifecycleLock,
    SemanticRefreshLifecycleObservation,
    evaluate_semantic_refresh_lifecycle,
)
from dpone.contracts.dbt_semantic_refresh_plan_contracts import (
    SemanticRefreshPreReleaseModelInput,
    SemanticRefreshPreReleaseProofBundle,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    SemanticRefreshRuntimeProofError,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    drift as _drift,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    mapping as _mapping,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    model_ids as _model_ids,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    model_node as _model_node,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    relation as _relation,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    required_text as _required_text,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    status_error as _status_error,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    string_array as _string_array,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    unverified as _unverified,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof_support import (
    validate_manifest_toolchain as _validate_manifest_toolchain_impl,
)
from dpone.contracts.dbt_semantic_refresh_source_proof import (
    prove_raw_jinja_closure,
    resolve_manifest_macro_source_closure,
)
from dpone.contracts.semantic_refresh_core import semantic_refresh_sha256
from dpone.contracts.semantic_refresh_model_proof import (
    SemanticRefreshModelDefinitionProof,
)
from dpone.contracts.semantic_refresh_read_dependency import (
    SemanticRefreshReadDependencyProof,
)
from dpone.contracts.semantic_refresh_types import ClosureStatus

_RUNTIME_TARGETS = ("runtime_compile", "runtime_recheck")
_ALLOWED_VARS = ("dpone_data_interval_end", "dpone_data_interval_start")
_MAX_SOURCE_BYTES = 1024 * 1024


class _SqlProofResult(Protocol):
    status: str
    compiled_sql_sha256: str | None


class SemanticRefreshRuntimeSqlProofPort(Protocol):
    """Parse and normalize the exact SQL emitted by the current dbt compile."""

    def prove(
        self,
        compiled_sql_by_target: dict[str, str],
        *,
        forbidden_relations: tuple[tuple[str, str, str], ...],
    ) -> _SqlProofResult: ...


class SemanticRefreshRuntimeCatalogProofPort(Protocol):
    """Re-observe the protected SQL Server catalog from one exact request."""

    def prove(self, request: SemanticRefreshCatalogProofRequest) -> SemanticRefreshCatalogProofReceipt: ...


class SemanticRefreshRuntimeLifecycleObserverPort(Protocol):
    """Observe current pinned adapter/target lifecycle facts without mutation."""

    def observe(
        self,
        *,
        authority: SemanticRefreshImmutableProofAuthority,
        manifest: Mapping[str, Any],
        selected_model_unique_ids: tuple[str, ...],
    ) -> SemanticRefreshLifecycleObservation: ...


class SemanticRefreshImmutableProofAuthorityLoaderPort(Protocol):
    """Load the full proof authority by exact promoted deployment coordinates."""

    def load_exact(
        self,
        *,
        release_id: str,
        deployment_id: str,
        plan_bundle_sha256: str,
        pre_release_bundle_sha256: str,
        package_artifacts_sha256: str,
    ) -> SemanticRefreshImmutableProofAuthority: ...


@dataclass(frozen=True, slots=True)
class SemanticRefreshImmutableProofAuthority:
    """Full typed build proof plus protected catalog/lifecycle recheck inputs."""

    pre_release_bundle: SemanticRefreshPreReleaseProofBundle
    lifecycle_lock: SemanticRefreshLifecycleLock
    catalog_requests: tuple[SemanticRefreshCatalogProofRequest, ...]

    def __post_init__(self) -> None:
        pre_release = self.pre_release_bundle
        if not isinstance(pre_release, SemanticRefreshPreReleaseProofBundle):
            raise TypeError("pre_release_bundle must be canonical and typed")
        payload = pre_release.to_dict()
        supplied_digest = payload.pop("pre_release_bundle_sha256", None)
        if (
            supplied_digest != pre_release.pre_release_bundle_sha256
            or semantic_refresh_sha256(payload) != supplied_digest
        ):
            raise ValueError("pre-release proof bundle digest differs from its content")
        if not isinstance(self.lifecycle_lock, SemanticRefreshLifecycleLock):
            raise TypeError("lifecycle_lock must be canonical and typed")
        if (
            self.lifecycle_lock.certification_coordinate_sha256 != pre_release.certification_coordinate_sha256
            or self.lifecycle_lock.package_artifacts_sha256 != pre_release.lifecycle_policy.package_artifacts_digest
        ):
            raise ValueError("lifecycle lock differs from the pre-release proof authority")
        models = {item.model_unique_id: item for item in pre_release.models}
        requests = {item.model_unique_id: item for item in self.catalog_requests}
        if len(requests) != len(self.catalog_requests) or tuple(sorted(requests)) != tuple(sorted(models)):
            raise ValueError("runtime catalog request closure differs from the pre-release models")
        for model_id, model in models.items():
            _validate_catalog_request(requests[model_id], model)


@dataclass(frozen=True, slots=True)
class SemanticRefreshImmutableProofResult:
    """Structurally compatible observation returned to runtime preflight."""

    observed_selected_unique_ids: tuple[str, ...]
    observed_proof_digests: tuple[str, ...]
    proof_statuses: tuple[str, ...]


def recheck_semantic_refresh_immutable_sources(
    *,
    manifest: Mapping[str, Any],
    authority: SemanticRefreshImmutableProofAuthority,
    selected_model_unique_ids: tuple[str, ...],
) -> None:
    """Reject raw model or macro drift before dbt is allowed to compile."""

    selected, expected_models = _proof_models(authority, selected_model_unique_ids)
    _validate_manifest_toolchain_impl(manifest, expected_dbt_version=DBT_CORE_VERSION)
    nodes = _mapping(manifest.get("nodes"), "manifest.nodes")
    macros = _mapping(manifest.get("macros"), "manifest.macros")
    for model_id in selected:
        expected = expected_models[model_id].model_definition_proof
        raw_code_sha256, macro_closure_sha256 = _recheck_source(
            node=_model_node(nodes, model_id),
            macros=macros,
        )
        if raw_code_sha256 != expected.raw_code_sha256 or macro_closure_sha256 != expected.macro_closure_sha256:
            raise _drift("current raw model or macro closure differs from the promoted release")


def recheck_semantic_refresh_immutable_proofs(
    *,
    manifest: Mapping[str, Any],
    authority: SemanticRefreshImmutableProofAuthority,
    selected_model_unique_ids: tuple[str, ...],
    sql_proof: SemanticRefreshRuntimeSqlProofPort,
    catalog_proof_service: SemanticRefreshRuntimeCatalogProofPort,
    lifecycle_observer: SemanticRefreshRuntimeLifecycleObserverPort,
) -> SemanticRefreshImmutableProofResult:
    """Recompute every mutable observation while retaining immutable release identity."""

    selected, expected_models = _proof_models(authority, selected_model_unique_ids)
    pre_release = authority.pre_release_bundle
    _validate_manifest_toolchain_impl(manifest, expected_dbt_version=DBT_CORE_VERSION)
    nodes = _mapping(manifest.get("nodes"), "manifest.nodes")
    macros = _mapping(manifest.get("macros"), "manifest.macros")
    requests = {item.model_unique_id: item for item in authority.catalog_requests}
    observed_digests: list[str] = []
    for model_id in selected:
        model = expected_models[model_id]
        node = _model_node(nodes, model_id)
        request = requests[model_id]
        current_definition, current_dependency = _recheck_model(
            node=node,
            macros=macros,
            expected=model,
            request=request,
            sql_proof=sql_proof,
            catalog_proof_service=catalog_proof_service,
        )
        if current_definition != model.model_definition_proof or current_dependency != model.read_dependency_proof:
            raise _drift("current dbt SQL or SQL Server catalog proof differs from the promoted release")
        observed_digests.extend(
            (
                current_definition.model_definition_proof_sha256,
                current_dependency.read_dependency_proof_sha256,
                pre_release.mutation_closure.mutation_closure_sha256,
                pre_release.lifecycle_policy.sqlserver_lifecycle_policy_sha256,
            )
        )
    _recheck_lifecycle(
        manifest=manifest,
        authority=authority,
        selected_model_unique_ids=selected,
        lifecycle_observer=lifecycle_observer,
    )
    return SemanticRefreshImmutableProofResult(
        observed_selected_unique_ids=selected,
        observed_proof_digests=tuple(observed_digests),
        proof_statuses=("PROVEN",) * len(observed_digests),
    )


def _recheck_model(
    *,
    node: Mapping[str, Any],
    macros: Mapping[str, Any],
    expected: SemanticRefreshPreReleaseModelInput,
    request: SemanticRefreshCatalogProofRequest,
    sql_proof: SemanticRefreshRuntimeSqlProofPort,
    catalog_proof_service: SemanticRefreshRuntimeCatalogProofPort,
) -> tuple[SemanticRefreshModelDefinitionProof, SemanticRefreshReadDependencyProof]:
    current_target = (
        _required_text(node.get("database"), "model.database"),
        _required_text(node.get("schema"), "model.schema"),
        _required_text(node.get("alias") or node.get("name"), "model.alias"),
    )
    if _relation(current_target) != _relation(request.target_relation):
        raise _drift("current dbt target relation differs from protected catalog authority")
    compiled_code = _required_text(node.get("compiled_code"), "model.compiled_code")
    raw_code_sha256, macro_closure_sha256 = _recheck_source(node=node, macros=macros)
    compilations = {target: compiled_code for target in _RUNTIME_TARGETS}
    sql = sql_proof.prove(compilations, forbidden_relations=request.forbidden_relations)
    if sql.status != "PROVEN" or sql.compiled_sql_sha256 is None:
        raise _status_error(sql.status, "current compiled SQL is not a proven read-only query")
    live_request = replace(request, compiled_sql_by_target=compilations)
    try:
        catalog = catalog_proof_service.prove(live_request)
    except Exception as exc:
        code = str(getattr(exc, "code", ""))
        if "UNVERIFIED" in code:
            raise _unverified("current SQL Server catalog proof is unavailable") from exc
        raise _drift("current SQL Server catalog closure is outside the protected proof") from exc
    dependency = catalog.dependency_proof
    definition = expected.model_definition_proof
    current = SemanticRefreshModelDefinitionProof.build(
        status=ClosureStatus.PROVEN,
        model_unique_id=expected.model_unique_id,
        manifest_sha256=definition.manifest_sha256,
        raw_code_sha256=raw_code_sha256,
        compiled_sql_sha256=sql.compiled_sql_sha256,
        macro_closure_sha256=macro_closure_sha256,
        toolchain_sha256=definition.toolchain_sha256,
        resolved_relation_dependency_digest=dependency.catalog_snapshot_sha256,
        resolved_module_dependency_digest=dependency.normalized_definitions_sha256,
        catalog_observation_digest=catalog.catalog_observation_sha256,
        parser_runtime_policy_digest=dependency.policy_sha256,
        target_independence_policy_digest=definition.target_independence_policy_digest,
    )
    return current, dependency


def _recheck_source(
    *,
    node: Mapping[str, Any],
    macros: Mapping[str, Any],
) -> tuple[str, str]:
    raw_code = _required_text(node.get("raw_code"), "model.raw_code")
    required_macros = _string_array(_mapping(node.get("depends_on"), "model.depends_on").get("macros"))
    try:
        macro_sources = resolve_manifest_macro_source_closure(
            root_macro_ids=required_macros,
            macros=macros,
        )
    except ValueError as exc:
        raise _unverified("current manifest macro dependency closure is unavailable") from exc
    source = prove_raw_jinja_closure(
        model_raw_sql=raw_code,
        macro_sources=macro_sources,
        required_macro_ids=tuple(macro_sources),
        allowed_vars=_ALLOWED_VARS,
        maximum_source_bytes=_MAX_SOURCE_BYTES,
    )
    if source.status != "PROVEN":
        raise _status_error(source.status, "current raw model or macro closure is not PROVEN")
    return "sha256:" + hashlib.sha256(raw_code.encode("utf-8")).hexdigest(), source.proof_sha256


def _proof_models(
    authority: SemanticRefreshImmutableProofAuthority,
    selected_model_unique_ids: tuple[str, ...],
) -> tuple[tuple[str, ...], dict[str, SemanticRefreshPreReleaseModelInput]]:
    selected = _model_ids(selected_model_unique_ids)
    pre_release = authority.pre_release_bundle
    expected_models = {item.model_unique_id: item for item in pre_release.models}
    if selected != tuple(sorted(expected_models)) or (
        pre_release.mutation_closure.status is not ClosureStatus.PROVEN
        or pre_release.mutation_closure.selected_mutating_node_ids != selected
    ):
        raise _unverified("selected model closure differs from the protected proof authority")
    return selected, expected_models


def _recheck_lifecycle(
    *,
    manifest: Mapping[str, Any],
    authority: SemanticRefreshImmutableProofAuthority,
    selected_model_unique_ids: tuple[str, ...],
    lifecycle_observer: SemanticRefreshRuntimeLifecycleObserverPort,
) -> None:
    try:
        observation = lifecycle_observer.observe(
            authority=authority,
            manifest=manifest,
            selected_model_unique_ids=selected_model_unique_ids,
        )
    except Exception as exc:
        raise _unverified("current dbt/SQL Server lifecycle observation is unavailable") from exc
    if not isinstance(observation, SemanticRefreshLifecycleObservation):
        raise _unverified("current lifecycle observer returned an invalid authority")
    pre_release = authority.pre_release_bundle
    report = evaluate_semantic_refresh_lifecycle(
        authority.lifecycle_lock,
        observation,
        lifecycle_policy=pre_release.lifecycle_policy,
    )
    if report.status != "PROVEN":
        raise _status_error(report.status, "current pinned adapter lifecycle is not PROVEN")
    if report != pre_release.lifecycle_report:
        raise _drift("current pinned adapter lifecycle differs from the promoted release")


def _validate_catalog_request(
    request: SemanticRefreshCatalogProofRequest,
    model: SemanticRefreshPreReleaseModelInput,
) -> None:
    if not isinstance(request, SemanticRefreshCatalogProofRequest) or request.model_unique_id != model.model_unique_id:
        raise TypeError("runtime catalog request must be canonical, typed, and model-bound")
    proof = model.read_dependency_proof
    limits = request.limits
    if (
        request.policy_sha256 != proof.policy_sha256
        or request.authority.database_name.casefold() != proof.database_name.casefold()
        or (
            limits.max_depth,
            limits.max_nodes,
            limits.max_edges,
            limits.max_definition_bytes,
        )
        != (proof.max_depth, proof.max_nodes, proof.max_edges, proof.max_definition_bytes)
        or _relation(request.target_relation) not in {_relation(item) for item in request.forbidden_relations}
    ):
        raise ValueError("runtime catalog request differs from the immutable dependency proof policy")


__all__ = [
    "SemanticRefreshImmutableProofAuthority",
    "SemanticRefreshImmutableProofAuthorityLoaderPort",
    "SemanticRefreshImmutableProofResult",
    "SemanticRefreshRuntimeCatalogProofPort",
    "SemanticRefreshRuntimeLifecycleObserverPort",
    "SemanticRefreshRuntimeProofError",
    "SemanticRefreshRuntimeSqlProofPort",
    "recheck_semantic_refresh_immutable_sources",
    "recheck_semantic_refresh_immutable_proofs",
]
