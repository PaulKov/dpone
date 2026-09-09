"""Concrete worker-time recheck of immutable semantic-refresh dbt proofs."""

from __future__ import annotations

from dataclasses import replace

import pytest

from dpone.adapters.dbt_semantic_refresh_sql_proof import (
    SqlglotSemanticRefreshSqlProof,
)
from dpone.contracts.dbt_semantic_refresh_catalog_proof import (
    SemanticRefreshCatalogProofRequest,
)
from dpone.contracts.dbt_semantic_refresh_catalog_types import (
    SqlServerDependencyLimits,
)
from dpone.contracts.dbt_semantic_refresh_certification import (
    SemanticRefreshCertificationDecision,
    SemanticRefreshCertificationRequest,
)
from dpone.contracts.dbt_semantic_refresh_lifecycle import (
    SemanticRefreshLifecycleObservation,
    evaluate_semantic_refresh_lifecycle,
)
from dpone.contracts.dbt_semantic_refresh_runtime_proof import (
    SemanticRefreshImmutableProofAuthority,
    SemanticRefreshRuntimeProofError,
)
from dpone.readiness.dbt_semantic_refresh_pre_release_compiler import (
    SemanticRefreshPreReleaseCompiler,
)
from dpone.readiness.dbt_semantic_refresh_runtime_proof import (
    SemanticRefreshImmutableProofRechecker,
)
from dpone.runtime.dbt_semantic_refresh_run_authority import (
    SemanticRefreshDbtStaticProjectionIdentity,
)
from tests.test_dbt_semantic_refresh_catalog_proof import (
    _authority,
    _CatalogRows,
    _rows,
    _service,
)
from tests.test_dbt_semantic_refresh_lifecycle import _lock, _observation
from tests.test_dbt_semantic_refresh_pre_release_compiler import (
    _request as _compile_request,
)

_MODEL_ID = "model.analytics.events"
_SOURCE_SQL = "select event_date, event_id, payload from DWH.raw.events"
_DIGESTS = tuple("sha256:" + character * 64 for character in "123456789abcdef")


class _AuthorityLoader:
    def __init__(self, authority: SemanticRefreshImmutableProofAuthority) -> None:
        self.authority = authority
        self.calls: list[dict[str, str]] = []

    def load_exact(self, **coordinates: str) -> SemanticRefreshImmutableProofAuthority:
        self.calls.append(coordinates)
        return self.authority


class _LifecycleObserver:
    def __init__(self, observation: SemanticRefreshLifecycleObservation) -> None:
        self.observation = observation

    def observe(self, **_coordinates: object) -> SemanticRefreshLifecycleObservation:
        return self.observation


def test_concrete_runtime_rechecker_reproves_exact_sql_catalog_and_lifecycle() -> None:
    fixture = _fixture()

    observed = fixture.rechecker.recheck(
        manifest=fixture.manifest,
        plan_bundle=fixture.plan,
        projection_identity=fixture.projection,
        selected_model_unique_ids=(_MODEL_ID,),
    )

    model = fixture.authority.pre_release_bundle.models[0]
    assert observed.observed_selected_unique_ids == (_MODEL_ID,)
    assert observed.observed_proof_digests == (
        model.model_definition_proof.model_definition_proof_sha256,
        model.read_dependency_proof.read_dependency_proof_sha256,
        fixture.authority.pre_release_bundle.mutation_closure.mutation_closure_sha256,
        fixture.authority.pre_release_bundle.lifecycle_policy.sqlserver_lifecycle_policy_sha256,
    )
    assert observed.proof_statuses == ("PROVEN",) * 4
    assert fixture.loader.calls == [
        {
            "deployment_id": fixture.projection.deployment_id,
            "package_artifacts_sha256": fixture.projection.package_artifacts_sha256,
            "plan_bundle_sha256": fixture.projection.plan_bundle_sha256,
            "pre_release_bundle_sha256": fixture.projection.pre_release_bundle_sha256,
            "release_id": fixture.projection.release_id,
        }
    ]


def test_concrete_runtime_rechecker_accepts_exact_sources_before_compile() -> None:
    fixture = _fixture()

    fixture.rechecker.recheck_sources(
        manifest=fixture.manifest,
        plan_bundle=fixture.plan,
        projection_identity=fixture.projection,
        selected_model_unique_ids=(_MODEL_ID,),
    )

    assert fixture.loader.calls == [
        {
            "deployment_id": fixture.projection.deployment_id,
            "package_artifacts_sha256": fixture.projection.package_artifacts_sha256,
            "plan_bundle_sha256": fixture.projection.plan_bundle_sha256,
            "pre_release_bundle_sha256": fixture.projection.pre_release_bundle_sha256,
            "release_id": fixture.projection.release_id,
        }
    ]


def test_runtime_source_rechecker_rejects_query_macro_before_compile() -> None:
    fixture = _fixture()
    node = dict(fixture.manifest["nodes"][_MODEL_ID])
    node["raw_code"] = "{{ run_query('select 1') }}\n" + str(node["raw_code"])
    manifest = {**fixture.manifest, "nodes": {_MODEL_ID: node}}

    with pytest.raises(SemanticRefreshRuntimeProofError) as raised:
        fixture.rechecker.recheck_sources(
            manifest=manifest,
            plan_bundle=fixture.plan,
            projection_identity=fixture.projection,
            selected_model_unique_ids=(_MODEL_ID,),
        )

    assert raised.value.code == "DPONE_DBT_V2_PROOF_DRIFT"


def test_runtime_source_rechecker_rejects_missing_transitive_macro_before_compile() -> None:
    fixture = _fixture()
    node = dict(fixture.manifest["nodes"][_MODEL_ID])
    node["depends_on"] = {
        "macros": ["macro.analytics.direct"],
        "nodes": ["source.analytics.events"],
    }
    manifest = {
        **fixture.manifest,
        "macros": {
            "macro.analytics.direct": {
                "depends_on": {"macros": ["macro.analytics.missing"]},
                "macro_sql": "{% macro direct() %}{{ missing() }}{% endmacro %}",
            }
        },
        "nodes": {_MODEL_ID: node},
    }

    with pytest.raises(SemanticRefreshRuntimeProofError) as raised:
        fixture.rechecker.recheck_sources(
            manifest=manifest,
            plan_bundle=fixture.plan,
            projection_identity=fixture.projection,
            selected_model_unique_ids=(_MODEL_ID,),
        )

    assert raised.value.code == "DPONE_DBT_V2_PROOF_UNVERIFIED"


def test_runtime_source_rechecker_rejects_forbidden_transitive_macro_before_compile() -> None:
    fixture = _fixture()
    node = dict(fixture.manifest["nodes"][_MODEL_ID])
    node["depends_on"] = {
        "macros": ["macro.analytics.direct"],
        "nodes": ["source.analytics.events"],
    }
    manifest = {
        **fixture.manifest,
        "macros": {
            "macro.analytics.direct": {
                "depends_on": {"macros": ["macro.analytics.transitive"]},
                "macro_sql": "{% macro direct() %}{{ transitive() }}{% endmacro %}",
            },
            "macro.analytics.transitive": {
                "depends_on": {"macros": []},
                "macro_sql": "{% macro transitive() %}{{ run_query('delete from dbo.x') }}{% endmacro %}",
            },
        },
        "nodes": {_MODEL_ID: node},
    }

    with pytest.raises(SemanticRefreshRuntimeProofError) as raised:
        fixture.rechecker.recheck_sources(
            manifest=manifest,
            plan_bundle=fixture.plan,
            projection_identity=fixture.projection,
            selected_model_unique_ids=(_MODEL_ID,),
        )

    assert raised.value.code == "DPONE_DBT_V2_PROOF_DRIFT"


def test_runtime_rechecker_rejects_same_id_compiled_sql_drift() -> None:
    fixture = _fixture()
    node = dict(fixture.manifest["nodes"][_MODEL_ID])
    node["compiled_code"] = _SOURCE_SQL + " where event_id > 0"
    manifest = {**fixture.manifest, "nodes": {_MODEL_ID: node}}

    with pytest.raises(SemanticRefreshRuntimeProofError) as raised:
        fixture.rechecker.recheck(
            manifest=manifest,
            plan_bundle=fixture.plan,
            projection_identity=fixture.projection,
            selected_model_unique_ids=(_MODEL_ID,),
        )

    assert raised.value.code == "DPONE_DBT_V2_PROOF_DRIFT"


def test_runtime_rechecker_rejects_lifecycle_drift() -> None:
    fixture = _fixture()
    fixture.lifecycle_observer.observation = replace(
        fixture.lifecycle_observer.observation,
        full_refresh=True,
    )

    with pytest.raises(SemanticRefreshRuntimeProofError) as raised:
        fixture.rechecker.recheck(
            manifest=fixture.manifest,
            plan_bundle=fixture.plan,
            projection_identity=fixture.projection,
            selected_model_unique_ids=(_MODEL_ID,),
        )

    assert raised.value.code == "DPONE_DBT_V2_PROOF_DRIFT"


def test_runtime_rechecker_maps_missing_catalog_visibility_to_unverified() -> None:
    fixture = _fixture()
    fixture.catalog_rows.metadata_visible = False

    with pytest.raises(SemanticRefreshRuntimeProofError) as raised:
        fixture.rechecker.recheck(
            manifest=fixture.manifest,
            plan_bundle=fixture.plan,
            projection_identity=fixture.projection,
            selected_model_unique_ids=(_MODEL_ID,),
        )

    assert raised.value.code == "DPONE_DBT_V2_PROOF_UNVERIFIED"


class _Fixture:
    def __init__(
        self,
        *,
        authority: SemanticRefreshImmutableProofAuthority,
        loader: _AuthorityLoader,
        lifecycle_observer: _LifecycleObserver,
        rechecker: SemanticRefreshImmutableProofRechecker,
        catalog_rows: _CatalogRows,
        manifest: dict[str, object],
        plan: dict[str, object],
        projection: SemanticRefreshDbtStaticProjectionIdentity,
    ) -> None:
        self.authority = authority
        self.loader = loader
        self.lifecycle_observer = lifecycle_observer
        self.rechecker = rechecker
        self.catalog_rows = catalog_rows
        self.manifest = manifest
        self.plan = plan
        self.projection = projection


def _fixture() -> _Fixture:
    compile_request = _compile_request()
    lifecycle_lock = _lock(compile_request.lifecycle_policy)
    previous_certification = compile_request.certification_request
    certification_request = SemanticRefreshCertificationRequest.build(
        certification_coordinate_sha256=lifecycle_lock.certification_coordinate_sha256,
        manifest_sha256=previous_certification.manifest_sha256,
        profile_sha256=previous_certification.profile_sha256,
        toolchain_sha256=previous_certification.toolchain_sha256,
        verification_time=previous_certification.verification_time,
    )
    previous_decision = compile_request.certification_decision
    certification_decision = SemanticRefreshCertificationDecision(
        certification_request.request_sha256,
        previous_decision.status,
        previous_decision.receipt_sha256,
        previous_decision.certified_at,
        previous_decision.expires_at,
    )
    compile_request = replace(
        compile_request,
        certification_request=certification_request,
        certification_decision=certification_decision,
    )
    lifecycle_observation = _observation(lifecycle_lock)
    lifecycle_report = evaluate_semantic_refresh_lifecycle(
        lifecycle_lock,
        lifecycle_observation,
        lifecycle_policy=compile_request.lifecycle_policy,
    )
    rows = _CatalogRows(_rows())
    catalog_service = _service(rows)
    dependency = compile_request.catalog_proofs[0].dependency_proof
    catalog_request = SemanticRefreshCatalogProofRequest(
        model_unique_id=_MODEL_ID,
        compiled_sql_by_target={"certified_a": _SOURCE_SQL, "certified_b": _SOURCE_SQL},
        forbidden_relations=(("DWH", "mart", "events"),),
        target_relation=("DWH", "mart", "events"),
        limits=SqlServerDependencyLimits(
            dependency.max_depth,
            dependency.max_nodes,
            dependency.max_edges,
            dependency.max_definition_bytes,
        ),
        policy_sha256=dependency.policy_sha256,
        authority=_authority(),
    )
    catalog_receipt = catalog_service.prove(catalog_request)
    compile_request = replace(
        compile_request,
        catalog_proofs=(catalog_receipt,),
        lifecycle_report=lifecycle_report,
    )
    pre_release = SemanticRefreshPreReleaseCompiler(sql_proof=SqlglotSemanticRefreshSqlProof()).compile(compile_request)
    authority = SemanticRefreshImmutableProofAuthority(
        pre_release_bundle=pre_release,
        lifecycle_lock=lifecycle_lock,
        catalog_requests=(catalog_request,),
    )
    loader = _AuthorityLoader(authority)
    lifecycle_observer = _LifecycleObserver(lifecycle_observation)
    rechecker = SemanticRefreshImmutableProofRechecker(
        authority_loader=loader,
        sql_proof=SqlglotSemanticRefreshSqlProof(),
        catalog_proof_service=catalog_service,
        lifecycle_observer=lifecycle_observer,
    )
    projection = SemanticRefreshDbtStaticProjectionIdentity(
        dag_projection_sha256=_DIGESTS[0],
        release_id=_DIGESTS[1],
        deployment_id=_DIGESTS[2],
        plan_bundle_sha256=_DIGESTS[3],
        workflow_plan_sha256=_DIGESTS[4],
        topology_sha256=_DIGESTS[5],
        pre_release_bundle_sha256=pre_release.pre_release_bundle_sha256,
        package_artifacts_sha256=pre_release.lifecycle_policy.package_artifacts_digest,
        template_pack_fingerprint=_DIGESTS[6],
    )
    model = pre_release.models[0]
    plan: dict[str, object] = {
        "operation_plans": [
            {
                "model_definition_proof_sha256": model.model_definition_proof.model_definition_proof_sha256,
                "model_unique_id": _MODEL_ID,
                "mutation_closure_sha256": pre_release.mutation_closure.mutation_closure_sha256,
                "read_dependency_proof_sha256": model.read_dependency_proof.read_dependency_proof_sha256,
                "sqlserver_lifecycle_policy_sha256": pre_release.lifecycle_policy.sqlserver_lifecycle_policy_sha256,
            }
        ],
        "package_artifacts_sha256": projection.package_artifacts_sha256,
        "plan_bundle_sha256": projection.plan_bundle_sha256,
        "pre_release_bundle_sha256": projection.pre_release_bundle_sha256,
        "release_deployment_authority": {
            "deployment_id": projection.deployment_id,
            "release_id": projection.release_id,
        },
    }
    manifest: dict[str, object] = {
        "macros": {},
        "metadata": {
            "adapter_type": "sqlserver",
            "dbt_version": "1.12.3",
        },
        "nodes": {
            _MODEL_ID: {
                "alias": "events",
                "compiled_code": _SOURCE_SQL,
                "database": "DWH",
                "depends_on": {"macros": [], "nodes": ["source.analytics.events"]},
                "raw_code": compile_request.report.models[0].model.raw_code,
                "resource_type": "model",
                "schema": "mart",
                "unique_id": _MODEL_ID,
            }
        },
    }
    return _Fixture(
        authority=authority,
        loader=loader,
        lifecycle_observer=lifecycle_observer,
        rechecker=rechecker,
        catalog_rows=rows,
        manifest=manifest,
        plan=plan,
        projection=projection,
    )
