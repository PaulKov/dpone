# Exercise semantic-refresh V2 baseline adoption locally

This platform how-to exercises adoption of one existing complete SQL
Server/ClickHouse baseline in a disposable local 0.74 preview environment. It
is not a dbt-author or production command. Production activation remains
unavailable even after this receipt is created. Start with the
[V2 fit and author journey](dbt-semantic-refresh-v2.md).

## Prerequisites

Use only promoted, protected typed inputs. The platform must provide certified
source and ClickHouse observers, an authority verifier, an assurance verifier,
an exact UTC clock, and the create-only MSSQL receipt store. Caller-built
evidence mappings or digest bags are not accepted.

## Build the closed subject

The helper below projects every value available in the promoted artifacts and
requires the remaining source relation, coverage, codec, and issuance-policy
coordinates explicitly. It fills all 28 public subject fields.

```python
from dpone.contracts.dbt_semantic_refresh_baseline_types import (
    SemanticRefreshBaselineIssuanceSubject,
)
from dpone.contracts.semantic_refresh_baseline_receipt import (
    BaselineAssuranceKind,
)


def baseline_subject_from_promoted_artifacts(
    *,
    pre_release,
    deployment_authority,
    deployment_model,
    source_relation_id: str,
    coverage_start: str,
    coverage_end: str,
    certified_codec_mapping_sha256: str,
    issuance_policy_sha256: str,
) -> SemanticRefreshBaselineIssuanceSubject:
    model = next(
        item
        for item in pre_release.models
        if item.model_unique_id == deployment_model.model_unique_id
    )
    return SemanticRefreshBaselineIssuanceSubject(
        baseline_kind=BaselineAssuranceKind.ADOPTED_COMPLETE_RELATION_CONFORMANT,
        model_unique_id=model.model_unique_id,
        release_id=deployment_authority.release_id,
        deployment_id=deployment_authority.deployment_id,
        environment=pre_release.environment,
        source_relation_id=source_relation_id,
        mssql_relation_id=deployment_model.target_resource_id,
        mssql_connection_authority_id=(
            deployment_model.mssql_connection_authority_id
        ),
        mssql_target_authority_id=deployment_model.mssql_target_authority_id,
        clickhouse_relation_id=(
            f"{deployment_model.publication_database}."
            f"{deployment_model.publication_target_table}"
        ),
        clickhouse_cluster_authority_id=(
            deployment_model.clickhouse_cluster_authority_id
        ),
        clickhouse_target_authority_id=(
            deployment_model.clickhouse_target_authority_id
        ),
        coverage_start=coverage_start,
        coverage_end=coverage_end,
        model_definition_proof_sha256=(
            model.model_definition_proof.model_definition_proof_sha256
        ),
        effective_key_template_sha256=model.effective_key_template_sha256,
        writable_schema_sha256=model.writable_schema_sha256,
        sqlserver_lifecycle_policy_sha256=(
            pre_release.lifecycle_policy.sqlserver_lifecycle_policy_sha256
        ),
        certification_coordinate_sha256=(
            pre_release.certification_coordinate_sha256
        ),
        route_certification_receipt_sha256=(
            pre_release.route_certification_receipt_sha256
        ),
        certified_codec_mapping_sha256=certified_codec_mapping_sha256,
        mssql_control_database=deployment_model.mssql_control_database,
        mssql_control_schema=deployment_model.mssql_control_schema,
        mssql_image_schema=deployment_model.mssql_image_schema,
        scope_image_namespace_policy_sha256=(
            deployment_model.scope_image_namespace_policy_sha256
        ),
        event_time_column=model.event_time_column,
        utc_assurance_required=True,
        issuance_policy_sha256=issuance_policy_sha256,
    )
```

The `promoted_*` and `protected_*` values are typed authority results, not
operator strings. `mssql_relation_id` is the exact `database.schema.table`
target resource. The ClickHouse relation is derived from the protected
publication database and table.

## Plan, review, and apply

```python
from dpone.app.semantic_refresh_baseline_composition import (
    build_semantic_refresh_baseline_issuance_runtime,
)

baseline = build_semantic_refresh_baseline_issuance_runtime(
    mssql_connection_factory=platform_mssql_connection_factory,
    evidence_provider=platform_cross_engine_baseline_observer,
    authority_verifier=platform_baseline_authority_verifier,
    assurance_verifier=platform_assurance_verifier,
    clock=platform_utc_clock,
)
subject = baseline_subject_from_promoted_artifacts(
    pre_release=promoted_pre_release_bundle,
    deployment_authority=protected_deployment_authority,
    deployment_model=protected_deployment_model,
    source_relation_id=protected_source_relation_id,
    coverage_start=authorized_coverage_start,
    coverage_end=authorized_coverage_end,
    certified_codec_mapping_sha256=(
        protected_codec_authority.certified_codec_mapping_sha256
    ),
    issuance_policy_sha256=protected_issuance_policy.policy_sha256,
)
plan = baseline.plan(subject)
# Review and authorize this typed object inside the protected controller.
receipt = baseline.apply(plan)
print(receipt.baseline_adoption_receipt_sha256)
```

`plan()` is non-mutating and queries no engine. `apply()` obtains source,
MSSQL, ClickHouse, route, and runtime observations from injected protected
capabilities. It verifies release/deployment, generations, schemas, keys,
physical identities, coverage, ClickHouse UUID/internal multiset,
writer/DDL/UTC assurances, and route certification before create-only storage.

Exact replay returns the same receipt; a different current baseline is a
conflict. Keep the typed plan inside the protected process. Do not reconstruct
it from `plan.to_dict()` and never edit a receipt by hand.

## Verify and continue

Preserve `baseline_adoption_receipt_sha256` with the deployment evidence and
load the same receipt from the MSSQL store by its protected MSSQL relation
identity. A consuming platform remains `UNVERIFIED` until its concrete
cross-engine observers/verifiers and full route campaign are current.

Continue by verifying the fail-closed
[production-activation boundary](dbt-semantic-refresh-v2-platform.md#verify-that-production-activation-is-unavailable).
