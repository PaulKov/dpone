# Domain-first operations and recovery

This operational guide covers Airflow exclusion, custom authoring roots,
compatibility, and common recovery actions. Use the
[domain-first tutorial](getting-started/domain-first-airflow.md) for initial
scaffolding.

## Pipelines outside Airflow

Use `--no-airflow` for a valid dpone workload that must not produce an Airflow
DAG:

```bash
dpone init pipeline runtime_only \
  --domain crm \
  --route mssql:clickhouse:incremental_merge \
  --from mssql_dev:dbo.runtime_only \
  --to clickhouse_dev:analytics.runtime_only \
  --key id \
  --no-airflow
```

The primary source records `metadata.airflow: false`. Static check can pass,
but project selection emits no DAG and `dpone airflow preview runtime_only`
returns [`DPONE_AIRFLOW_DISABLED`](errors/DPONE_AIRFLOW_DISABLED.md) with exit
`1`.

A first disabled preview creates no cache. If the verified current local
preview advertises only that pipeline, dpone atomically promotes an empty,
immutable compensating projection before reporting the error. The next
provider parse sees no stale DAG.

If current advertises several workloads, dpone preserves it so an individual
command cannot remove unrelated DAGs. The error supplies the exact bounded
refresh:

```bash
dpone airflow preview . --exclude id:runtime_only
```

The text response links to the hosted error catalog and names the manual fix.
It does not invent a deployment id.

## Custom authoring root

The default root is `workloads`. A platform team may choose another confined,
project-relative root:

```yaml
schema: dpone.project.v1
layout:
  mode: domain_first
  root: data-products
  pipeline_id_scope: project
```

Absolute paths, `..`, backslashes, NUL bytes, and symlink escapes are rejected.
Changing the root moves authority; dpone never moves files automatically.

## Recovery table

| Symptom | Meaning | Action |
| --- | --- | --- |
| [`DPONE_DOMAIN_REQUIRED`](errors/DPONE_DOMAIN_REQUIRED.md) | A pipeline has no owning domain. | Add `--domain <id>`. |
| [`DPONE_DOMAIN_OWNERSHIP_MISSING`](errors/DPONE_DOMAIN_OWNERSHIP_MISSING.md) | The domain was not initialized. | Create explicit owner and approver authority. |
| [`DPONE_DOMAIN_CATALOG_MEMBERSHIP_CONFLICT`](errors/DPONE_DOMAIN_CATALOG_MEMBERSHIP_CONFLICT.md) | Catalog membership could not be written safely. | Fix or remove the conflicting `workloads:` entry, then rerun `init pipeline`. |
| Reconcile warning `workload_catalog_membership_missing` | A discovered Airflow-eligible pipeline is not part of the resolved workload-set catalog membership. | Add the suggested `workloads:` entry to the named `.dpone/config/domains/<domain>.yaml` file (or the catalog file that owns the domain bundle). Reconcile continues; the pipeline is not packed until membership is present. |
| [`DPONE_PIPELINE_ID_DUPLICATE`](errors/DPONE_PIPELINE_ID_DUPLICATE.md) | The id exists in another domain. | Choose a project-unique id or migrate deliberately. |
| [`DPONE_PIPELINE_DOMAIN_MISMATCH`](errors/DPONE_PIPELINE_DOMAIN_MISMATCH.md) | Source metadata conflicts with its path. | Match the source domain to its owning directory. |
| [`DPONE_DISCOVERY_PATH_INVALID`](errors/DPONE_DISCOVERY_PATH_INVALID.md) | Discovery found an unsafe or unexpected path. | Remove the symlink or nesting and retry. |
| [`DPONE_PROJECT_CONFIG_INVALID`](errors/DPONE_PROJECT_CONFIG_INVALID.md) | `dpone.yaml` is malformed or unsafe. | Repair `dpone.project.v1`, then rerun static check. |
| [`DPONE_PROJECT_AUTHORING_LOCK_FAILED`](errors/DPONE_PROJECT_AUTHORING_LOCK_FAILED.md) | The project lock is unavailable or timed out. | Stop the competing authoring operation and retry. |
| [`DPONE_PIPELINE_LOCATOR_INVALID`](errors/DPONE_PIPELINE_LOCATOR_INVALID.md) | A source or sink locator is invalid. | Use `connection_ref:schema.table`. |
| [`DPONE_RECIPE_ANSWERS_INVALID`](errors/DPONE_RECIPE_ANSWERS_INVALID.md) | The answers file is invalid. | Keep it bounded, confined, non-secret, and recipe-declared. |
| [`DPONE_WORKLOAD_INDEX_INVALID`](errors/DPONE_WORKLOAD_INDEX_INVALID.md) | The CI baseline is malformed or non-canonical. | Regenerate it atomically from current authority. |
| [`DPONE_WORKLOAD_INDEX_APPROVAL_MISMATCH`](errors/DPONE_WORKLOAD_INDEX_APPROVAL_MISMATCH.md) | Candidate bytes no longer match approval. | Generate fresh impact and obtain protected approval again. |
| [`DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT`](errors/DPONE_WORKLOAD_INDEX_PROMOTION_CONFLICT.md) | Baseline or project identity changed after approval. | Preserve current bytes; re-fetch, compare, and reapprove. |
| [`DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED`](errors/DPONE_WORKLOAD_INDEX_PROMOTION_RECOVERY_REQUIRED.md) | Commit or cleanup state needs reconciliation. | Stop promotion and follow the digest-based recovery runbook. |
| [`DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID`](errors/DPONE_WORKLOAD_INDEX_PROMOTION_REQUEST_INVALID.md) | Baseline guards are incomplete or contradictory. | Use bootstrap absence or both existing-baseline identities. |

Scaffolding commands are idempotent. Existing user content is not overwritten.
A failed preflight or precommit CAS leaves no partial scaffold. Domain-first
`init pipeline` scans every `<system_root>/config/domains/*.yaml` catalog file
before writing membership: an existing registration with the same manifest is a
no-op even when the entry lives in a non-domain-named catalog bundle (for
example `wide_mart.yaml`). A committed
mutation with incomplete cleanup returns a recovery-required receipt and
preserves its project-relative recovery artifacts. Workload-index promotion is
a one-shot approval/CAS operation: replay after a successful promotion must use
a newly generated impact and the now-current baseline identity. Project
authoring uses a private, no-follow external lock keyed by the absolute project
root and waits at most a bounded interval; sibling project roots do not share a
lock.

## Flat compatibility

Omitting `layout` preserves the historical flat mode:

```text
pipelines/<pipeline-id>/pipeline.yaml
domains/<domain>.yaml
tests/<pipeline-id>.test.yaml
```

No migration is automatic. Existing flat projects remain valid. A project with
flat or domain-first authority cannot initialize the other mode implicitly.
`dpone migrate authoring` changes `classic`, `flow`, or `folder` syntax; it
does not change repository layout.

### Reviewed flat-to-domain-first migration

Until a dedicated layout migrator is approved, use one isolated platform
change:

1. require a clean branch and capture `dpone check`, `dpone test`, and
   `dpone airflow preview` JSON for every flat pipeline;
2. record each pipeline's owner, approver, schedule, authoring mode, source,
   sink, strategy, connection refs, tests, and canonical semantic fingerprint;
3. create a new project tree in a separate staging directory with
   `dpone init project --layout domain-first`, then initialize every domain;
4. move one primary source to
   `<layout.root>/<domain>/pipelines/<pipeline-id>/pipeline.yaml`, preserve its
   authoring mode and relative dependencies, and colocate its tests;
5. remove that pipeline's `domains/*.yaml` orchestration entry only after the
   domain-first source and ownership file are complete;
6. atomically replace the reviewed tree in one commit, with `dpone.yaml`
   selecting `domain_first`; never leave mixed authority in an intermediate
   commit;
7. rerun the captured checks and require identical canonical semantic
   fingerprints, DAG ids, schedules, connection refs, and hermetic outcomes;
8. generate the first workload-index candidate and bootstrap its accepted
   baseline through the protected CI flow.

Do not copy generated manifests, packs, cache entries, releases, or
deployments. If any semantic fingerprint or behavior differs, revert the
single migration commit and keep the flat project authoritative. Promotion to
an environment remains blocked until normal preview, release, and deployment
checks pass.

## Related operations

- [Domain-first discovery and CI](domain-first-discovery-ci.md)
- [Airflow cache sync and recovery](airflow-cache-sync.md)
- [Domain-first error catalog](errors/index.md)
