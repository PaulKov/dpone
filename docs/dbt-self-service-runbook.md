# dbt self-service operations runbook

> **Semantic Refresh V2 status in 0.74:** local diagnostic preview only.
> Production activation fails before persistence or target mutation with
> `DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE`. The V2 recovery
> branches below apply only to disposable local preview evidence and future
> certified deployments; they do not authorize an operator bypass.

This runbook is for Airflow operators diagnosing and recovering a pinned dbt
publishing workflow. It assumes the DAG was created through the immutable
[promotion flow](dbt-self-service-promotion.md).

The operator environment must use the same released dpone/provider version as
the release jobs, an exact supported Airflow/Python pair, an absolute
scheduler-local cache, digest-pinned runtime and artifact references, workload
identity, and runtime-only Vault access. It must also have a platform-owned
campaign/evidence root shared by the campaign controller and Airflow provider,
a protected Airflow API origin/version, and a short-lived create/read token.
The reusable protected-dev workflow derives the immutable request, triggers and
observes the exact DAG runs, requires the terminal provider task to export
attempt evidence, records the campaign outcome, then finalizes and attests it.
Any missing stage remains `UNVERIFIED`.

## Observe before acting

Record:

- Airflow DAG, run, task, and bundle identities;
- release and deployment digests;
- dbt invocation, manifest, selection, graph/adapter policy, adapter runtime,
  project-bundle, and toolchain digests;
- effective query, dbt process, and Airflow task timeout values;
- dbt model/test outcomes and transfer outcomes;
- the final durable workflow evidence status.

Evidence may contain safe credential-version metadata, but never secret values.
A green Airflow task without required durable dpone evidence is not proof of a
successful publication.

Start from the scheduler cache, not from a copied DAG or mutable source tree:

```bash
export DPONE_SCHEDULER_CACHE_ROOT=/opt/airflow/.dpone-cache
export DPONE_ENVIRONMENT=prod

dpone airflow cache-recovery-plan \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --environment "${DPONE_ENVIRONMENT}" \
  --format json
```

Record `current_path_deployment_id`, `current_deployment_id`, `release_id`,
`status`, every issue code and the exact evidence artifact URI. In Airflow,
record the DAG ID, run ID, task ID, try number and data interval before clearing
or rerunning anything.

## Diagnose

| Signal | Meaning | First action |
| --- | --- | --- |
| `DPONE_DBT_NO_PUBLISH_MODELS` | No enabled publishing model was selected. | Check resolved `meta`, selection, and whether `--allow-empty` was used only for reporting. |
| `DPONE_DBT_MANIFEST_*` or `DPONE_DBT_INTENT_*` | dbt artifact or authoring metadata is invalid. | Run `dbt parse`, then `dpone dbt check` and `dpone dbt explain MODEL` on the same source. |
| `DPONE_DBT_SQLSERVER_PROJECT_POLICY_INVALID` | The extracted project's exact adapter flags, macro dispatch, or bounded YAML input violate release policy. | Confirm the four literal flags, remove top-level `dispatch`, regenerate the manifest and immutable release, and never edit an extracted runtime bundle. |
| `DPONE_DBT_SQLSERVER_GRAPH_CAPABILITY_UNSUPPORTED` | The exact selected closure uses behavior outside the documented SQL Server graph and config boundary. | Use the reported node and field to remove the unsupported capability, then publish a new release; do not remove the node from the lock by hand. |
| `DPONE_DBT_SQLSERVER_PHYSICAL_CONSTRAINT_UNSUPPORTED` | A model or column declares an unsupported physical constraint. | Keep model constraints empty and column constraints empty or `not_null` only; move other assertions to admitted tests, then publish a new release. |
| `DPONE_DBT_SQLSERVER_MACRO_AUTHORITY_INVALID` | Runtime or compile cannot reproduce the exact generated framework/invocation macro authority. | Restore the pinned toolchain and reviewed package source, run `dbt deps` when declarations changed, then `dbt parse` and publish a new release; never edit the manifest/baseline. |
| `DPONE_DBT_V2_SQL_MODULE_DEPENDENCY_UNSUPPORTED` | A V2 model reads through an unsupported or unverifiable SQL Server module. | Move deterministic logic into the target-independent dbt query or an admitted same-database schema-bound view/inline TVF, then compile a new release. |
| `DPONE_DBT_V2_READ_DEPENDENCY_DRIFT` | The runtime catalog closure differs from the approved model-definition proof. | Keep dbt and target mutation blocked; restore the governed module definitions or compile and certify a new release. |
| `DPONE_DBT_ADAPTER_LIFECYCLE_DRIFT` | The installed dbt/adapter/materialization lifecycle differs from its frozen V2 tuple. | Restore the exact runtime image and package artifacts or recertify a new release; do not allow the drift at runtime. |
| `DPONE_DBT_MSSQL_OUTCOME_UNVERIFIED` | The SQL Server model transaction started but durable evidence cannot prove rollback or committed images. | Preserve the exact operation/session/image identities and reconcile SQL Server. Keep artifact transfer and every replacement blocked. |
| `DPONE_DBT_UNIQUE_KEY_*` | The merge key is missing, malformed, an expression, outside the enforced contract, duplicated, mismatched, or nullable. | Use exact distinct contract identifiers, add structural `not_null` to every key column, align/remove the metadata key, parse, and publish a new release. |
| `DPONE_DBT_TEST_DEPENDENCY_OUTSIDE_WORKFLOW` | An eager-selected test reads a model outside its locked workflow. | Put all tested models in one workflow closure or rewrite the assertion across a governed source boundary. |
| `DPONE_DBT_CROSS_WORKFLOW_DEPENDENCY_UNSUPPORTED` | A workflow closure contains a publish model owned by another workflow. | Merge the dependent publish models into one workflow or use a governed source boundary, then compile a new release. |
| `DPONE_DBT_WORKFLOW_GRAPH_OVERLAP` | Materialized upstream model ownership overlaps across workflows. | Make closures disjoint or merge the workflows; compile into a new empty output directory rather than editing locks. |
| `DPONE_DBT_SQLSERVER_RUNTIME_POLICY_INVALID` | Pack/runtime values or policy digests do not prove `pyodbc`, one total SQL execute attempt, and the required timeout hierarchy. | Quarantine mutated bytes, fetch the exact release again, or rebuild through dev CI; do not override provider kwargs. |
| `DPONE_DBT_ROUTE_NOT_CERTIFIED` | Project-authorized route evidence is absent, stale, foreign, or below production level. | Inspect `dpone.yaml` `capability_discovery.certification_evidence` and rerun the exact certifier; do not use demo/check output. |
| `DPONE_DBT_STRATEGY_UNRESOLVED` | Policy and certified capability cannot prove a safe load strategy. | Ask the platform owner to approve a named policy/capability; do not force an ungoverned override. |
| `DPONE_DBT_PROMOTION_SOURCE_DRIFT` | The prod source mirror differs from the approved dev snapshot. | Reject the prod MR and recreate it from the approved dev source; do not rebuild in prod. |
| `DPONE_DBT_DEV_EVIDENCE_INTEGRITY_INVALID` | Finalized evidence bytes, provenance, identities, or checksum subject are invalid. | Re-export from the exact dev run and rerun the protected evidence finalizer; never edit the attested bundle. |
| `DPONE_DBT_DEV_EVIDENCE_REQUEST_INVALID` | The release cannot produce one bounded, exact campaign request. | Regenerate the request from the exact compiled release; do not hand-edit JSON. |
| `DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_CONFIG_INVALID` | Airflow origin/token or the shared campaign root violates platform policy. | Restore protected variables/secrets and root ownership; never replace them with caller inputs. |
| `DPONE_DBT_DEV_EVIDENCE_CAMPAIGN_FAILED` | At least one exact DAG run failed, timed out, conflicted, or did not produce terminal evidence. | Inspect the campaign request/outcome and the exact Airflow run before replaying the same request. |
| `DPONE_DBT_DEV_EVIDENCE_EXPORT_FAILED` or `DPONE_DBT_WORKFLOW_EVIDENCE_EXPORT_FAILED` | The terminal provider could not validate or atomically store the requested attempt evidence. | Repair the shared root/provider access, then rerun the same pinned workload; do not fabricate evidence or mark the XCom passed. |
| `DPONE_DBT_RELEASE_INTEGRITY_INVALID` | Downloaded release bytes differ from the signed checksum subject. | Fetch the exact dev artifact again and verify the trusted signer; never repair immutable bytes. |
| `DPONE_DBT_INVOCATION_CONTEXT_INVALID` or `DPONE_DBT_SELECTION_DRIFT` | Invocation, graph, or selected nodes differ from the compiled release. | Confirm `build_started: false`, then rebuild through the editable dev repository. Do not retry the old release. |
| `DPONE_DBT_TARGET_IDENTITY_MISMATCH` | Deployment binding resolves a different adapter, database, or schema. | Confirm `build_started: false`, then correct the environment binding or promote a matching deployment. |
| `DPONE_DBT_SCHEMA_DRIFT` | The live source relation differs from the compiled contract. | Correct source/contract drift before staging and publish a new release when source semantics changed. |
| `DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_NULL` or `DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_DUPLICATE` | The exact post-lineage finalization table failed the key-integrity gate before finalizer target lookup/mutation. | Verify failed-before-target evidence and every named attempt table with the procedure below, correct the source/transform rows, and start a new pinned attempt. Escalate if any target mutation signal exists. |
| `nested_package_partial_finalize` | A nested member finalizer was invoked, or an earlier member committed, without a complete package outcome. Current and not-yet-invoked member staging is retained. | Freeze retries, read `finalized_tables`, `retained_members`, and `operation_tables`, then follow the retained-staging reconciliation procedure below. |
| `staged_cleanup_failed` with `target_outcome: committed` | Target finalization succeeded, but attempt-table cleanup did not complete. Replaying can duplicate an append or repeat another target mutation. | Do not retry. Preserve the failure step, verify the committed target, then remove only the exact retained operation tables across every replica. |
| `DPONE_DBT_EXECUTION_FAILED` | The workflow's dbt gate has a complete, proven failed result, or failed before mutation. | Fix the dbt source or platform cause; transfer tasks must not start. |
| `DPONE_DBT_RUN_RESULTS_INVALID` | A non-mutating result validation failed. A missing or invalid artifact after build start is promoted to `COMMIT_UNKNOWN`. | Keep transfers blocked and fix the artifact source before a safe new attempt. |
| Transfer or quality failure | One model publication failed. | Inspect that transfer's staging/finalization evidence; do not assume workflow-wide rollback. |
| `COMMIT_UNKNOWN` | A dbt or transfer target commit may have occurred without durable proof, including a post-build result-validator, evidence-writer, or staged-finalizer failure. | Do not retry automatically. For ClickHouse, require `operation_tables`, `cleanup_attempted: false`, and `cleanup_status: retained_for_reconciliation`; reconcile the MSSQL/ClickHouse target and evidence, then escalate. |

## Verify ClickHouse attempt cleanup

For `DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_NULL` and
`DPONE_CLICKHOUSE_STAGING_UNIQUE_KEY_DUPLICATE`, do not retry from the error
code alone. The failed `load_governance_failed` step must contain:

```json
{
  "failure_boundary": "pre_commit",
  "target_outcome": "failed_before_target",
  "operation_tables": {
    "staging": "ops.orders__dpone_staging_<attempt>",
    "decoded": "ops.orders__dpone_decoded_<attempt>",
    "projected": "ops.orders__dpone_projected_<attempt>"
  },
  "cleanup_attempted": true,
  "cleanup_status": "succeeded",
  "cleanup_verification_required": true
}
```

Roles that were not created are absent. Read the exact record from compact
runtime evidence or, when the default ClickHouse audit store is enabled, from
the configured `__dpone__load_steps` table:

```sql
SELECT run_id, load_id, details_json
FROM `etl_state`.`__dpone__load_steps`
WHERE run_id = '<run-id>'
  AND load_id = '<load-id>'
  AND step_id = 'load_governance_failed'
ORDER BY started_at DESC
LIMIT 1;
```

`cleanup_status` reports whether dpone's DROP requests completed; it is not an
absence proof. Copy only the exact fully qualified values from
`operation_tables` into the verification query:

```sql
SELECT database, name
FROM system.tables
WHERE concat(database, '.', name) IN (
  'ops.orders__dpone_staging_<attempt>',
  'ops.orders__dpone_decoded_<attempt>',
  'ops.orders__dpone_projected_<attempt>'
)
ORDER BY database, name;
```

On a single-node deployment, zero rows proves that the named attempt tables are
absent from that server. If rows remain, a ClickHouse platform owner may issue
reviewed `DROP TABLE IF EXISTS` statements for those exact evidence identities
only:

```sql
DROP TABLE IF EXISTS `ops`.`orders__dpone_staging_<attempt>`;
```

Repeat per remaining `operation_tables` value and rerun the `system.tables`
query.

For a clustered deployment, a local `system.tables` result is not proof. Use
the deployment's exact reviewed cluster name to inspect every replica:

```sql
SELECT hostName() AS host, database, name
FROM clusterAllReplicas('<reviewed-cluster-name>', system.tables)
WHERE concat(database, '.', name) IN (
  'ops.orders__dpone_staging_<attempt>',
  'ops.orders__dpone_decoded_<attempt>',
  'ops.orders__dpone_projected_<attempt>'
)
ORDER BY host, database, name;
```

If `clusterAllReplicas` is unavailable, run the local query on every replica
and retain the complete host list with the result. Only zero rows across all
replicas proves cluster-wide absence. Use the same exact reviewed cluster in
`DROP TABLE IF EXISTS ... ON CLUSTER '<reviewed-cluster-name>'`, then repeat
the all-replica query. Never guess a cluster, use a wildcard/prefix cleanup, or
drop the business target. If the failure record is missing, its
`target_outcome` differs, or any target-mutation signal exists, stop: cleanup
and retry are unproven, so follow the `COMMIT_UNKNOWN` escalation path.

## Reconcile retained ClickHouse staging

For `COMMIT_UNKNOWN` and `nested_package_partial_finalize`, the failure step is
the recovery authority. A nested failure has this bounded shape:

```json
{
  "failure_boundary": "commit_unknown",
  "target_outcome": "commit_unknown",
  "error_code": "nested_package_partial_finalize",
  "finalized_tables": ["orders__items"],
  "retained_members": [
    {
      "target": "landing.orders",
      "target_outcome": "commit_unknown",
      "operation_tables": {
        "staging": "ops.orders__dpone_staging_<attempt>",
        "projected": "ops.orders__dpone_projected_<attempt>"
      }
    }
  ],
  "operation_tables": {
    "landing.orders:staging": "ops.orders__dpone_staging_<attempt>",
    "landing.orders:projected": "ops.orders__dpone_projected_<attempt>"
  },
  "cleanup_attempted": false,
  "cleanup_status": "retained_for_reconciliation",
  "cleanup_verification_required": true,
  "safe_to_retry": false
}
```

Use this recovery order:

1. Pause the route and every automatic retry. Preserve the run/load IDs,
   manifest/release digest, failure step, and target-side query/audit history.
2. Copy only exact identities from `operation_tables`. Use the single-node or
   all-replica `system.tables` query above to prove which attempt tables remain;
   do not drop them yet.
3. Reconcile each `retained_members[].target` and `finalized_tables` entry
   against the pinned strategy and attempt. For append, prove whether the
   attempt's rows or load identity reached the target. For merge, snapshot,
   partition replace, and SCD2, compare the affected keys/partitions and
   strategy metadata with the retained projected table. Record the query text,
   host/replica set, result, and reviewer.
4. If the mutation committed, do not replay it. Reconcile checkpoint/state
   through the approved recovery process or create a reviewed compensating
   attempt, then clean only the exact operation tables. If it provably did not
   commit, cleanup and retry are allowed only after target non-application and
   cluster-wide attempt-table cleanup are both recorded. If the outcome remains
   ambiguous, keep staging and escalate to the route owner and ClickHouse
   platform owner.
5. For `staged_cleanup_failed` with `target_outcome: committed`, skip outcome
   discovery: the target is already confirmed and `safe_to_retry` is false.
   Preserve the committed evidence, clean the exact retained tables on every
   replica, and do not rerun the transfer.

Never infer safety from a raw exception message, an empty local `system.tables`
result on one replica, or the absence of a checkpoint. Never use a table-name
prefix/wildcard for recovery cleanup.

JSON-mode command failures use the `dpone.error.v1` envelope. The internal
`dpone dbt execute-pack` path instead returns
`dpone.dbt-execution-evidence.v1` for every completed runtime outcome, including
`COMMIT_UNKNOWN`; it uses `dpone.error.v1` only when no runtime outcome can be
created. Use stable codes and actionable fields rather than raw exception text.
See the [error catalog](dbt-self-service-errors.md) for the complete action
matrix.

## Retry decision

Automatic retries default to zero. Before rerunning:

1. Preserve the original release, deployment, Airflow bundle, and data-interval
   identities.
2. Confirm the failure happened before target mutation, or reconcile the target
   against durable evidence.
3. Confirm the selected route and target fence are certified replay-safe.
4. Rerun only the approved scope and verify fresh durable evidence.

For a configured dbt process budget `P`, require
`query_timeout_seconds = P - 300` and Airflow
`execution_timeout_seconds = P + 300`, where `600 <= P <= 86400`. A timeout
after `build_started: true` remains `COMMIT_UNKNOWN`. The Airflow value bounds
the whole task, including init-fetch and both preflight commands; it does not
guarantee a 300-second post-build evidence reserve and never authorizes an
automatic retry.

For a V2 operation, clearing an Airflow task is safe only within the same
logical DagRun and after the durable operation has been reconciled. A new
DagRun cannot resume the old operation: use a failed-precommit workflow
replacement or a completed-scope revision. Clearing an Airflow task does not
create a new release and must not change the
locked dbt selection. A failed model does not roll back successful dbt models
or other already-finalized transfers; the workflow does not claim one global
transaction.

In 0.74 the protected controller may create/replay continuation receipt v1 only
for the immediate successor try. It must first prove the original attempt's
trusted termination, engine quiescence, bounded target state, and exact
after-image. A second successor fails with
`DPONE_SEMANTIC_REFRESH_CONTINUATION_CHAIN_UNSUPPORTED`; do not clear the task
again or synthesize a receipt. `Retry` elsewhere in this runbook never means an
automatic V2 mutation retry.

If admission fails with
`DPONE_SEMANTIC_REFRESH_WORKER_ADMISSION_COMMIT_UNKNOWN`, keep the DAG paused
and preserve the original run/attempt evidence. Do not infer rollback from the
exception and do not enter continuation, which requires a completed build
receipt. Have the incident commander reconcile the canonical authority,
workflow/resource guards, every `PREPARING` journal, and the admission receipt
closure in SQL Server. Resume only through a reviewed recovery decision; 0.74
has no automatic commit-outcome classifier.

For the manual readback, dispose of the failed client handle and use a
provider-owned non-pooled factory that opens a new SQL Server session. Call
`MssqlSemanticRefreshWorkerRunAuthority.locate(workflow_plan_sha256,
workflow_execution_id)` with the immutable original values. `ADMITTED` means
the exact guard/reservation/journal/attempt closure committed and must be loaded
rather than admitted again. `REGISTERED` is accepted only when the reader also
proves there are no partial journals, reservation, or held guards; a reviewed
operator may then replay the exact original admission. Any exception,
unavailable session, mixed closure, or identity mismatch remains
`COMMIT_UNKNOWN` and keeps the DAG paused. This is manual reconciliation, not
the post-0.74 automatic classifier.

Do not recover by running `dbt build` from the DAG repository checkout. Runtime
must use the project bundle pinned by the original release. Do not substitute a
new Vault credential reference, image, selection, or data interval during a
replay.

For the exact preflight ordering and identity fingerprints, see
[Invocation, selection, and target identity](dbt-self-service-runtime-identity.md).

Pause the affected DAG through the approved Airflow operator interface before
changing deployment state. Do not pause unrelated DAGs and do not delete the
failed run: its task logs and identities are part of the recovery record.

For an evidence-campaign incident, retain and compare these immutable files:

```text
evidence-request.json
evidence-campaign.json
<evidence-root>/releases/<release>/deployments/<deployment>/sets/<evidence-set>/campaign-request.json
<evidence-root>/releases/<release>/deployments/<deployment>/sets/<evidence-set>/campaign-outcome.json
<evidence-root>/releases/<release>/deployments/<deployment>/sets/<evidence-set>/airflow/*.json
<evidence-root>/dbt-spool/releases/<release>/deployments/<deployment>/sets/<evidence-set>/dbt/*.json
```

The request and receipt must agree on evidence-set, release, deployment, DAG,
run, and workflow identities. A rerun with the same request is idempotent only
when the existing journal bytes and Airflow `conf` are identical. A byte
conflict is an incident, not a reason to delete the journal. The single
campaign timeout includes request journaling, every trigger/reconciliation
call, polling, and receipt creation; a partially triggered campaign therefore
remains failed until the same request is safely reconciled.

The `dbt-spool` subtree is a runtime handoff, not the promotion source. KPO sees
only that subtree through a confined PVC `subPath`; the terminal provider task
reads the exact descriptor, validates it against the Airflow attempt, and
copies the accepted dbt evidence into the final evidence-set directory. Never
move or promote a raw spool file directly.

## Recovery by failure boundary

The Semantic Refresh V2 branches in this section describe disposable 0.74
fault testing and the reviewed shape of a future certified route. They are not
an executable 0.74 production recovery path. Keep production on V1; every V2
initial or successor persistence attempt must fail with
`DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE`.

Before the first V2 deployment, adopt an existing complete baseline through
the protected plan/apply controller documented in
[V2 baseline adoption](dbt-semantic-refresh-v2-baseline.md).
The operator reviews the typed plan, while `apply()` itself obtains all
cross-engine observations from injected protected capabilities. Never pass
caller-built evidence, infer a generation from table names, or use the MSSQL
receipt store as an evidence verifier. Preserve the returned
`baseline_adoption_receipt_sha256` with the deployment evidence.

For a V2 failed-precommit workflow, run reconciliation through the supported
application controller. The controller accepts only the workflow execution ID;
it never accepts a claimed model outcome or replacement action:

```python
from dpone.app.semantic_refresh_recovery_composition import (
    build_semantic_refresh_recovery_runtime,
)

recovery = build_semantic_refresh_recovery_runtime(
    mssql_connection_factory=platform_mssql_connection_factory,
)
decision = recovery.reconcile(failed_workflow_execution_id)
```

Record `decision.summary.terminal_summary_sha256` in the incident. The durable
copy is already stored in `semantic_refresh_workflow_executions`; each exact
model outcome/evidence digest is stored in `semantic_refresh_journals`. If
`reconcile()` raises because evidence is `COMMIT_UNKNOWN`, stop: do not create
a successor, clear the task or start a new DagRun.

After the exact protected predecessor release has produced a canonical
replacement workflow, clean the failed attempt before admitting a successor.
The replacement must retain the same release, deployment, pre-release bundle,
package, model closure and scope identities; it is not a corrected release.
The cleanup controller
loads only durable `FAILED_PRE_COMMIT` authority, verifies the unchanged target
UUID and exact operation-owned staging/shadow relations, then proves both
relations absent after idempotent DROP reconciliation. It releases the exact
five-allocation aggregate closure only after that absence proof and persists a
create-only cleanup acknowledgement. A successor is inadmissible until MSSQL
locks and verifies that acknowledgement plus the `RELEASED` allocation history.
`COMMIT_UNKNOWN` never issues cleanup authority.

Construct that controller from the same protected MSSQL connection authority,
ClickHouse endpoint/topology authority and UTC clock used by the publication
runtime, then clean every failed operation returned by the durable predecessor
decision:

```python
from dpone.app.semantic_refresh_recovery_composition import (
    build_semantic_refresh_failed_scratch_cleanup_runtime,
)

cleanup = build_semantic_refresh_failed_scratch_cleanup_runtime(
    mssql_connection_factory=platform_mssql_connection_factory,
    clickhouse_http_client=platform_clickhouse_http_client,
    clickhouse_connection_authority=platform_clickhouse_connection_authority,
    now=platform_utc_clock,
)
for operation_id in decision.summary.expected_operation_ids:
    cleanup_ack = cleanup.cleanup(
        workflow_execution_binding_sha256=(
            decision.summary.workflow_execution_binding_sha256
        ),
        operation_id=operation_id,
    )
    incident_cleanup_receipts.append(cleanup_ack.cleanup_receipt_sha256)
```

The `platform_*` values above are injected platform capabilities, not values
copied from an Airflow task, incident ticket or CLI argument. If any cleanup
call fails, keep successor admission blocked and rerun the same idempotent
cleanup only after reconciliation.

For a future certified release, the exact-predecessor replacement compiler must build
`SemanticRefreshFailedPrecommitReplacementAuthority` from the protected
predecessor plan, `decision.summary`, `decision.replacement_actions` and
`decision.summary.workflow_id`. It then passes that authority to
`SemanticRefreshPostDeploymentPlanCompiler.compile(...)`. Its deployment
controller freezes the resulting run-neutral `successor_plan` with
`SemanticRefreshActivationRuntime.plan_deployment_authorities(...)`, retains
that typed authority and its one `persisted_at` value for acknowledgement-loss
replay, then calls a guarded
`SemanticRefreshActivationRuntime.persist_planned_deployment_authorities(...)`.
Version 0.74 raises before this persistence. A future certified guard may create
a plan-specific receipt without replaying initial physical target activation.
The controller then
publishes its authenticated DAG projection/index before Airflow starts the
successor. The initial/recovery activation distinction is summarized in the
[platform guide](dbt-semantic-refresh-v2-platform.md#recover-without-reactivating-the-deployment).
The compiler's `workflow_id` is the one exact value shared by every protected
predecessor operation; do not substitute `workflow_plan.workflow_name`.
The route certification and runtime-assurance receipts must be the exact
predecessor bytes and remain current. Do not renew or replace them under the
same deployment during recovery; create-only authority persistence will reject
that drift.

The protected controller executes the complete middle path below. Every
`protected_*` value is loaded from immutable predecessor authority; none comes
from the operator or incident ticket:

```python
from dpone.app.semantic_refresh_activation_composition import (
    SemanticRefreshProductionActivationUnavailableError,
    build_semantic_refresh_activation_runtime,
)
from dpone.contracts.dbt_semantic_refresh_plan_compiler import (
    SemanticRefreshPostDeploymentPlanCompiler,
)
from dpone.contracts.dbt_semantic_refresh_recovery_authority import (
    SemanticRefreshFailedPrecommitReplacementAuthority,
)

replacement_authority = SemanticRefreshFailedPrecommitReplacementAuthority.build(
    predecessor_plan=protected_predecessor_plan,
    predecessor_summary=decision.summary,
    predecessor_workflow_execution_id=decision.summary.workflow_id,
    replacement_actions=decision.replacement_actions,
)
predecessor_workflow_ids = {
    operation.workflow_id
    for operation in protected_predecessor_plan.operation_plans
}
if len(predecessor_workflow_ids) != 1:
    raise RuntimeError("protected predecessor workflow identity is ambiguous")
predecessor_workflow_id = next(iter(predecessor_workflow_ids))

successor_plan = SemanticRefreshPostDeploymentPlanCompiler(
    protected_release_deployment_verifier,
    protected_assurance_verifier,
    recovery.plan_verifier,
).compile(
    pre_release=protected_predecessor_pre_release_bundle,
    authority=protected_predecessor_plan.release_deployment_authority,
    workflow_id=predecessor_workflow_id,
    scope_start=protected_predecessor_plan.workflow_plan.scope_start,
    scope_end=protected_predecessor_plan.workflow_plan.scope_end,
    deployment_models=protected_deployment_models,
    route_certification=protected_predecessor_route_certification,
    runtime_assurances=protected_predecessor_runtime_assurances,
    verification_time=platform_utc_clock(),
    recovery_authority=replacement_authority,
)
activation = build_semantic_refresh_activation_runtime(
    mssql_connection_factory=platform_mssql_connection_factory,
    release_deployment_verifier=protected_release_deployment_verifier,
    release_template_verifier=protected_release_template_verifier,
    authority_store_ref=protected_predecessor_authority_store_ref,
)
planned_successor_authority = activation.plan_deployment_authorities(
    template_pack=protected_predecessor_template_pack,
    plan_bundle=successor_plan,
    route_certification=protected_predecessor_route_certification,
    runtime_assurances=protected_predecessor_runtime_assurances,
    persisted_at=successor_authority_persisted_at,
)
try:
    activation.persist_planned_deployment_authorities(
        template_pack=protected_predecessor_template_pack,
        plan_bundle=successor_plan,
        authority=planned_successor_authority,
    )
except SemanticRefreshProductionActivationUnavailableError as exc:
    assert exc.code == "DPONE_SEMANTIC_REFRESH_PRODUCTION_ACTIVATION_UNAVAILABLE"
else:
    raise AssertionError("dpone 0.74 must not persist a V2 successor")
```

The 0.74 snippet proves the block and creates no receipt. In a future certified
release, sample `successor_authority_persisted_at` once in canonical
`YYYY-MM-DDTHH:MM:SS[.ffffff]Z` form when the typed authority is planned. Store
that typed plan and replay the same object after acknowledgement loss. This
path stores a plan-specific receipt only; it must not call initial physical
deployment activation.

The first dbt task admits the actual `dag_run.run_id` through
`SemanticRefreshMssqlDbtRunAdmission`; that one MSSQL transaction creates the
canonical binding and performs the replacement CAS. Reload the binding by its
workflow-plan digest and actual DagRun ID, then pass only that persisted digest
to the recovery replay boundary:

```python
from dpone.adapters.semantic_refresh_mssql_run_binding import (
    MssqlSemanticRefreshWorkerRunBindingAuthority,
)

successor_binding_authority = MssqlSemanticRefreshWorkerRunBindingAuthority(
    platform_mssql_connection_factory,
)
persisted_successor = successor_binding_authority.locate_binding(
    successor_plan.workflow_plan.workflow_plan_sha256,
    actual_successor_dag_run_id,
)
receipt = recovery.admit_successor(
    persisted_successor.record.workflow_execution_binding_sha256
)
```

The returned `MssqlAdmissionReceipt` identifies the successor workflow and
exact guard/journal closure. A competing or byte-different successor fails the
MSSQL CAS; there is no force override. Preserve the predecessor failed summary,
replacement plan, successor execution binding and admission receipt together
in the incident evidence package.

| Boundary | Safe recovery |
| --- | --- |
| Author check or compile | Fix source or platform policy and rerun CI; no release should have been published. |
| dbt build/test | Fix dbt source and publish a new release. Existing transfer tasks remain blocked. |
| V2 failed-precommit dbt workflow | Reconcile every model to `NOT_INVOKED`, `ROLLED_BACK`, `COMMITTED_WITH_IMAGES`, or `COMMIT_UNKNOWN`. Create one workflow replacement only when no model is unknown. |
| V2 failed-precommit scratch/allocation cleanup | Failure terminalization may release the predecessor guard, but any later target-guard reacquisition is blocked until exact staging/shadow absence and all five released allocations are durably acknowledged. Never treat DROP request success as absence proof. |
| V2 replacement model with committed images | Verify the current scope equals the retained after-image; restore the retained preimage and rebuild in one fenced transaction. Never run an ordinary upsert as a substitute. |
| Preflight schema drift | Correct source/contract drift and rebuild; no target mutation should have begun. |
| Staging or quality | For NULL/duplicate key codes, confirm failed-before-target evidence and verify every evidence-named attempt table is absent, then correct data and retry the pinned work. Otherwise abort governed staging only where evidence proves no final commit and follow route-specific recovery. |
| V2 empty staged scope | Require the exact `NOT_REQUIRED_EMPTY_SCOPE` terminal receipt, unchanged target UUID/generation, and successor scope revision/checkpoint. Replay only the same durable PREPARED/terminal documents; a non-empty or byte-different retry is a conflict. |
| Finalization with durable failure | Follow the certified load-strategy recovery procedure for that model. |
| `COMMIT_UNKNOWN` | Freeze automated retries, reconcile target state and evidence, and escalate to the route owner. |
| Bad deployment with known-good predecessor | Compare-and-swap the environment pointer to the previous deployment using the rollback guide. |
| Any `DPONE_ARTIFACT_*` attestation failure in prod CI or runtime | Keep the candidate inactive. Compare the stable code with the error catalog, then restore the exact policy/bundle/image input through reviewed immutable publication. Never bypass or downgrade to checksum-only success. |

If the deployment pointer changed after the operator read it, stop and rebuild
the rollback plan from the new current identity. Never force-update `current`.

For a reviewed rollback, first materialize the known-good deployment from
trusted immutable storage if it is not already present. Then activate it with
the incident deployment as the CAS guard:

```bash
export DPONE_ROLLBACK_DEPLOYMENT_DIR=/opt/airflow/.dpone-cache/deployments/prod/sha256-<known-good>
export DPONE_INCIDENT_DEPLOYMENT_ID=sha256:<incident>
export DPONE_OPERATOR_ID=ci://github-actions/airflow-prod-rollback

dpone airflow cache-sync \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --deployment-dir "${DPONE_ROLLBACK_DEPLOYMENT_DIR}" \
  --environment "${DPONE_ENVIRONMENT}" \
  --promoted-by "${DPONE_OPERATOR_ID}" \
  --allowed-promoter "${DPONE_ALLOWED_ROLLBACK_PROMOTER}" \
  --expected-current-deployment-id "${DPONE_INCIDENT_DEPLOYMENT_ID}" \
  --confirm-promote \
  --format json
```

The protected production environment must pre-provision
`DPONE_ALLOWED_ROLLBACK_PROMOTER`; the incident operator must not export or
derive it from `DPONE_OPERATOR_ID`. The operator reports `DPONE_OPERATOR_ID`,
but cannot define its own allowlist. Command arguments alone are not identity
proof. Immediately
rerun `cache-recovery-plan` and require both logical and physical current IDs to
equal the known-good deployment before resuming scheduling.

## Control schema v20/v21 to v22

Use this procedure before a runtime that can persist failed-precommit successor
plans or same-DagRun continuation receipts. Version 21 preserves every existing activation receipt and replaces the
deployment-only primary key with
`(deployment_id, plan_bundle_sha256)`. It never selects or creates a `latest`
receipt. Version 22 adds the create-only continuation-receipt v1 table and the protected
monotonic DDL epoch/trigger used to prevent catalog ABA during dbt execution.
It does not install or imply a continuation receipt v2.

1. Pause semantic-refresh schedules and wait until no dbt, PREPARE, COMMIT,
   summary or recovery worker owns the control database. Keep ordinary ETL
   policy unchanged.
2. Take the protected SQL Server backup required by the environment runbook.
   Record the `semantic_refresh_v2` row from
   `semantic_refresh_schema_versions`, the current activation-authority PK
   columns, row count, and activation receipt digests. Do not export receipt
   JSON into the incident ticket.
3. Continue only when the installed version is 20 with a deployment-only PK,
   21 with the exact composite PK, or 22 with the composite PK and exact DDL
   epoch/continuation objects. A different version/key shape is an escalation,
   not permission for manual DDL.
4. Run the matching application migration through the protected connection:

   ```python
   from dpone.adapters.semantic_refresh_mssql_schema import (
       MssqlSemanticRefreshSchemaMigration,
   )

   MssqlSemanticRefreshSchemaMigration(
       platform_mssql_connection_factory,
       control_schema="dpone_control",
   ).apply()
   ```

5. Require version 22, primary-key order
   `(deployment_id, plan_bundle_sha256)`, unchanged pre-upgrade row count and
   receipt digests, one positive singleton epoch, the database DDL trigger and
   the continuation table, then run the same `apply()` once more to prove
   idempotency.
   Resume scheduling only after the normal worker-admission and recovery
   smoke checks pass.

The migration uses `XACT_ABORT`, `SERIALIZABLE`, an exclusive schema application
lock and one transaction. If it emits
`DPONE_SEMANTIC_REFRESH_ACTIVATION_AUTHORITY_KEY_MIGRATION_FAILED`,
`DPONE_SEMANTIC_REFRESH_SCHEMA_BUSY`, `DPONE_SEMANTIC_REFRESH_SCHEMA_VERSION_CONFLICT`
or another SQL error, keep admission stopped and verify that the exact
pre-migration version, key shape and all receipt rows remain unchanged. Never
repair the key or receipt rows manually. Escalate with the dpone version,
stable code, redacted SQL error, schema-version row, PK column metadata,
pre/post row counts and digest
set. Use the reviewed database restore only if transaction rollback itself was
not clean.

## Verify recovery

Recovery is complete only when:

- the expected release and deployment identities are active;
- required dbt model/tests and transfers have durable outcomes;
- the workflow evidence links the same Airflow and dbt identities;
- source and target reconciliation matches the route contract;
- no secret or generated artifact entered a source repository;
- the Airflow DAG and runtime used the expected pinned project bundle rather
  than a repository filesystem path.

## Escalation package

Provide identities, stable error codes, redacted logs, artifact checksums,
evidence locations, target reconciliation, and actions already attempted. Never
attach rendered `profiles.yml`, credentials, raw Vault responses, or secret
values.

## Operational ownership and alerts

| Signal | Initial owner | Required response |
| --- | --- | --- |
| DAG parse error or index identity mismatch | Airflow platform | Page immediately; keep the last valid deployment active. |
| Missing/failed dbt evidence | Analytics/dbt owner | Block transfers and promotion; fix source or runtime evidence. |
| Campaign controller or Airflow API unavailable | Dev platform owner | Keep promotion blocked; restore protected API/root access and replay only the same immutable request. |
| Provider attempt-evidence export absent/failed | Airflow platform owner | Repair provider/shared-root access and rerun the pinned workload; passed XCom must remain absent. |
| Dev evidence finalization/attestation failed | Release platform | Verify request, terminal receipt, and provider attempts, then rerun the protected finalizer with the same identities. |
| Transfer quality/staging failure | Route owner | Reconcile staging and follow the certified route runbook. |
| `COMMIT_UNKNOWN` | Incident commander + route owner | Page immediately, freeze retries and reconcile target/evidence. |
| CAS conflict | Release engineer | Refresh the recovery plan; never force the pointer. |
| Artifact/Vault/runtime dependency unavailable | Platform on-call | Restore dependency and retry only the same pinned identities. |

Platform policy owns Airflow pools, `max_active_tasks`, the capacity-one dbt
project/target pool and alert thresholds. Alert on any terminal workflow
without final evidence, any `COMMIT_UNKNOWN`, repeated schema drift, cache
recovery-required status, or a dev-to-prod promotion lead time beyond the
platform SLO. The initial product target is a dev DAG within ten minutes after
merge; environment-specific paging thresholds and escalation contacts belong
in the consuming platform repository.

Detailed cache corruption and pointer recovery commands are in
[Airflow cache sync and recovery](airflow-cache-sync.md). Route-specific
staging/finalization procedures are linked from the certified route evidence;
do not invent a generic cleanup for an uncertified route.

Astronomer Cosmos is not a recovery dependency. Co-installed Cosmos DAGs are
operated separately and do not replace dpone release, deployment, retry, or
evidence authority.

This feature remains preview-only until strict runtime and live
MSSQL-to-ClickHouse certification pass **and** an exact-UUID predecessor
retention plan/apply controller is implemented and certified. The current
preview retains predecessor generations; operators must not delete by table
name, wildcard, or best effort. A skipped live check is `SKIP` or `UNVERIFIED`,
never `PASS`.

Return to the [five-minute tutorial](dbt-inline-publishing.md), the
[author and platform reference](dbt-self-service-reference.md), or the
[dbt integration hub](dbt.md).
