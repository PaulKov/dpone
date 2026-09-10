# Airflow self-service

For CPU, memory and temporary disk settings, see
[Configure Airflow workload resources](airflow-workload-resources.md). For
non-root hooks and OS startup failures, use
[runtime startup diagnostics](airflow-runtime-startup-diagnostics.md).

The canonical beginner journey is
[First Airflow DAG in 5 commands](getting-started/first-airflow-dag.md). It
creates a pipeline, validates it, and previews a provider-loadable DAG without
Airflow Python. Noun map:
[Airflow pipeline glossary](getting-started/airflow-pipeline-glossary.md).

This hub also points to the
[advanced Data Engineer CJM](airflow-self-service-advanced-cjm.md) for teams
that need explicit catalog, selectors, and GitOps control — not day one.

The default self-service plane is the indexed-v2 provider path: beginners start
with non-runnable preview v1 and a local safe-sample handoff, while platform
teams promote complete strict-v2 indexes. Use strict-v2 with
`trust_tier: non_production` for executable rehearsal. A production index is a
paused parse canary until the verifier and current live certification evidence
exist. The older runner-pack workflow is advanced compatibility, not the
default path.

## Audience

Start from your route (source → sink): pick a row in the
[source → sink matrix](source-sink-matrix.md), then scaffold it with a built-in
recipe on [First Airflow DAG](getting-started/first-airflow-dag.md).

| Persona | Goal | Primary commands |
| --- | --- | --- |
| New data engineer | Create the first DAG without Python | `dpone init pipeline`, optional `dpone init dag`, `dpone airflow preview` |
| Data engineer (advanced) | Governed multi-pipeline / GitOps ship | Follow [advanced CJM](airflow-self-service-advanced-cjm.md); then `dpone workload init` when the platform catalog requires it |
| dbt author | Publish a contracted MSSQL model to ClickHouse | `dpone dbt check`, `dpone dbt compile` |
| Junior data engineer | Wire dependencies and review graphs | `dpone gitops airflow deps`, `dpone dag explain-edge --dag-spec` |
| DataOps | Operate cache, loader, and recovery | `dpone gitops airflow pack`, `dpone airflow cache-sync` |

## Beginner authoring contract

`dpone init pipeline` creates one short `dpone.flow.v1` source by default.
`--authoring classic` creates the full existing `dpone.batch.v1` grammar. Both
compile through one canonical authoring compiler; they are not parallel runtime
engines and must not coexist as editable sources for one pipeline. The provider
continues to consume only generated deployment artifacts. See
[Airflow pipeline authoring](airflow-authoring.md).

Platform-owned reusable defaults and subgraphs use content-pinned declarative
[recipes, profiles, and components](airflow-recipes.md). This is an optional
authoring layer over the same compiler, pack, and provider contracts; it does
not add scheduler-side template execution or another runtime engine.

Every scaffolded pipeline also includes a credential-free `dpone.test.v1`
contract and bounded JSONL fixture. Run `dpone test <id>` as the fifth offline
golden-path command and before live
checks to prove canonical authoring and connector-neutral final-state semantics.
This does not claim route certification. See
[Hermetic pipeline tests](testing/hermetic-pipeline-tests.md).

Runtime credentials remain backend-neutral: pipeline authors use only logical
`connection_ref` aliases, while platform-owned deployment inputs select Vault,
Kubernetes projection, or an Airflow compatibility bridge. A credential is
resolved once per workload, and unsupported historical/DAG-wide policies fail
before backend I/O. See the
[credential resolver lifecycle and recovery runbook](airflow-credential-resolver-lifecycle.md).

## Advanced

> **Advanced path (not the beginner journey).** If you are creating your first
> Airflow pipeline, start at [First Airflow DAG](getting-started/first-airflow-dag.md)
> instead.

Entry points for Data Engineers after the beginner path:

| Topic | Page |
| --- | --- |
| End-to-end advanced journey | [Advanced Data Engineer CJM](airflow-self-service-advanced-cjm.md) |
| Multi-pipeline CLI selection | [Workload selectors](airflow-self-service-selectors.md) |
| Declarative multi-workload packs | [GitOps workload catalog](gitops-workload-catalog.md) |
| Migration reference tree | `examples/airflow-self-service-pilot/` |
| Evidence kits | [Airflow self-service evidence](airflow-self-service-evidence.md) |

The Airflow provider still loads only static artifacts and never evaluates
selector syntax on the scheduler.

For dirty feeds, set `sink.options.schema_contract.enforcement: quarantine`.
The runtime keeps valid rows moving and writes metadata-only `dpone.dlq.v1`
records for invalid rows. Airflow evidence distinguishes a successful task from
its data result with `execution_status: succeeded` and
`data_outcome: passed_with_quarantine`; rejected values are not serialized into
the DAG, XCom, evidence, or default DLQ artifacts. Pipeline authors do not need
to configure replay credentials or a Vault path. See
[Runtime data contracts and DLQ](data-contract-runtime.md).

## Process visibility in Airflow

New pipelines need no extra setting: every logical process appears as one
Airflow task by default. The generated DAG spec still pins the exact process
selector, so a multi-process workload cannot accidentally run in full for each
node.

Use `execution.visibility` only when you need a different operational view:

```yaml
processes:
  - name: orders
    execution:
      visibility: inline  # inline | task | group
```

| Mode | Airflow view | dpone execution semantics |
| --- | --- | --- |
| `inline` | One KPO per process | Hooks, runtime, and outcome validation run once inside the process task |
| `task` | Flat hook, runtime, and outcome tasks | Same selected process and evidence contract |
| `group` | The same expanded tasks inside a TaskGroup | TaskGroup is presentation only, not an execution boundary |

`dpone airflow preview <pipeline>` prints each node's resolved visibility,
selector, and estimated visible task count. Domain catalogs may lower the
default warning and hard budgets:

```yaml
wiring:
  visible_task_budget:
    warn: 100
    max: 250
```

The hard platform ceiling is 250 tasks per generated DAG. Tasks inside a
TaskGroup count toward the same budget. A warning does not block publication;
exceeding `max` does. The Airflow provider reads only the generated DAG spec and
compact pack. It does not parse manifests or rewrite selector shell commands.

### Optional bounded backfill mapping

The beginner path needs no mapping setting. A chunked backfill remains one
Airflow task by default, while dpone owns chunk planning, resume, retries and
commit state through its campaign ledger.

Platform teams may opt a workload into bounded Grid visibility:

```yaml
workloads:
  orders_history:
    manifest: ../../pipelines/orders_history.yaml
    airflow:
      mapping:
        mode: summary  # internal | visible | summary
        max_items: 40
        max_active: 8
        pool: dpone_backfill
```

`visible` creates one mapped task instance per planned chunk and fails when the
plan exceeds `max_items`. `summary` distributes all chunks over at most
`max_items` ordered, contiguous batches. Both modes require
`backfill.parallel_workers: 1` and certified PostgreSQL or MSSQL audit state
with distributed locks and owner-CAS completion. Local-file and ClickHouse
audit state fail closed for mapped execution.

The mapping plan is static, fingerprinted and stored in the generated pack.
Airflow parsing never discovers chunks from a database, XCom, Variable,
Connection, Vault or Kubernetes. At workload start, the runtime re-plans the
campaign, verifies the selected range before source I/O and uses the dpone
ledger to skip already committed chunks. Clearing an Airflow task therefore
does not reset or override dpone state. See
[Bounded Airflow backfill mapping](backfill.md#bounded-airflow-backfill-mapping).

## Advanced governed workload path

1. Scaffold the workload (plan-first, then apply):

```bash
dpone workload init marketing/sample_web_sync \
  --source clickhouse --sink mssql --strategy full_refresh \
  --layout batch
```

Review the plan, then:

```bash
dpone workload init marketing/sample_web_sync \
  --source clickhouse --sink mssql --strategy full_refresh \
  --apply
```

Or use the interactive wizard when you omit ``--source`` / ``--sink`` on a TTY
(or pass ``--wizard`` explicitly):

```bash
dpone workload init marketing/sample_web_sync --wizard
```

The wizard lists certified source/sink pairs and strategies, prints a plan, and
asks ``Apply? [y/N]`` before writing files.

2. Prove compile readiness before commit:

```bash
dpone gitops airflow pack \
  --workload-set dpone_workloads/gitops/gitops.yaml \
  --mode plan
```

3. Review lineage and DAG edges:

```bash
dpone gitops airflow deps \
  --workload-set dpone_workloads/gitops/gitops.yaml \
  --format md
```

4. Open an MR. CI builds compact packs and dag-spec artifacts; the deployment
   repo loads them through a single loader file (see
   [Lightweight Airflow pack provider](airflow-pack-provider.md)).

## Authoring surfaces

| Surface | Purpose |
| --- | --- |
| Domain catalog `workloads:` | Register manifests in GitOps |
| Domain catalog `dags:` | Declarative schedule, wiring, retries |
| Manifest `gitops.airflow.execution.outlets` | Producer asset URIs |
| Manifest `gitops.airflow.execution.inlets` | Explicit consumer asset URIs |
| `depends_on` in manifests | Declared edges (file, `#selector`, `group:`) |

## Dependency layers

Build plane merges three layers into each dag-spec:

1. **Inferred** — sink URI of producer matches source/inlet URI of consumer.
2. **Declared** — manifest `depends_on` and batch internal selectors.
3. **Curated** — `dags:` wiring (`waves`, `explicit`, or `assets`).

Use `dpone dag explain-edge --dag-spec --dag-id <id>` to inspect winning
layers and overrides.

The build plane also copies an inferable sink URI into the compact workload
pack as an outlet with `provenance: inferred:asset_graph.v1`. The provider reads
that static outlet and emits the matching Airflow Asset/Dataset update; it does
not infer lineage during DAG parsing. More than one writer for the same URI and
any inferred cross-DAG cycle are publish blockers that require an explicit
curator decision.

MSSQL outlets use deployment-owned `connection.asset_authority` and the
canonical AIP-60 form
`mssql://{host}:{port}/{database}/{schema}/{table}` — see
[MSSQL Airflow Asset URIs](airflow-mssql-asset-uri.md).

When lineage edges cross DAG boundaries, `dpone gitops airflow deps` lists
**Cross-DAG wiring recommendations** (missing outlets or unresolved
`schedule.assets` entries). Dag-spec build auto-adds `schedule.assets` for
consumers with null/asset schedules and emits `asset_graph_cross_dag_schedule_inferred`
info warnings. An explicitly authored asset schedule may reference the same
build-inferred producer outlet. Curated or other non-inferred edges still need a
declared/materialized producer outlet; otherwise the build emits an actionable
`asset_graph_cross_dag_wiring` warning. Cron-scheduled consumers still require
explicit authoring changes.

## Optional asset partitions

For a daily asset, declare one partition next to the producer outlet. The
consumer inherits the same contract through the build-time asset graph; no
Airflow Python or duplicate consumer declaration is required.

```yaml
airflow:
  execution:
    outlets:
      - uri: dpone://clickhouse/analytics/orders
        partition:
          dimensions:
            business_date:
              type: temporal
              granularity: day
              timezone: UTC
```

Partition v1 supports exactly one temporal dimension with `hour`, `day`, or
`month` granularity. `key_format` defaults to the matching stable calendar
format. `dpone check` blocks invalid timezones, multiple dimensions, unmatched
producer/consumer declarations, partitioned producers without a schedule, and
multiple partition contracts in one DAG.

Airflow 3.2.x and 3.3.x materialize native partition timetables and identity mapping.
Airflow 3.0/3.1 and 2.x keep the existing unpartitioned Asset/Dataset schedule
and expose `degraded_unpartitioned` in runtime evidence. The downgrade never
pretends that partition isolation occurred. Rollups, categorical/runtime
partitions, and multiple dimensions are intentionally unsupported in v1.

## Scheduler consumption

Add one loader file to the deployment repo:

```python
from airflow.providers.dpone import load_and_acknowledge_dpone_dags

loaded = load_and_acknowledge_dpone_dags(
    globals(),
    index_path="/opt/airflow/.dpone-cache/current/airflow-index.json",
    ack_path="/opt/airflow/.dpone-ack/loader-ack.json",
    ack_root="/opt/airflow/.dpone-ack",
)
if loaded.report.fatal:
    error_code = (
        loaded.report.errors[0].get("code", "DPONE_AIRFLOW_INDEX_INVALID")
        if loaded.report.errors
        else "DPONE_AIRFLOW_INDEX_INVALID"
    )
    raise RuntimeError(
        f"{error_code}: dpone Airflow deployment index could not be loaded"
    )
```

The loader reads fingerprinted dag-spec and pack JSON from the bounded,
read-only cache and writes only its bounded ACK to the separate writable
acknowledgement volume. It performs no network, Airflow metadata DB, Variable,
Connection, Vault, or Kubernetes access. A fatal index report raises with the
first stable error code so a missing deployment cannot look like an empty
successful parse. By
default, an invalid artifact inside a trusted index is skipped and reported
through `LoadReport`, structured logs, and metrics while valid DAGs continue
loading. A paused diagnostic DAG tagged `dpone_spec_error` is available only
through the explicit `create_diagnostic_dag` platform policy; publish/build
remains fail closed. Existing installations should use the
[provider and cache migration guide](airflow-provider-cache-migration.md)
before changing imports, generated loaders, or cache layout.

## Diagnostics and error catalog

Structured blockers include remediation hints and doc links under
`docs/errors/`. Canonical pipeline identity failures start with
[`DPONE_PIPELINE_ID_INVALID`](errors/DPONE_PIPELINE_ID_INVALID.md) and
[`DPONE_PIPELINE_ID_MISMATCH`](errors/DPONE_PIPELINE_ID_MISMATCH.md);
workload-scaffolding errors use `DPONE_WORKLOAD_INIT_*`.

Common next commands after a failed init:

- scaffold conflict → review JSON diff, rename workload, or merge manually;
- domain conflict → choose a new workload id;
- manifest invalid → fix generated manifest fields and rerun `--apply`.

## Plan a reproducible rerun

This is an operator recovery command, not a beginner-path step. Build a local,
non-mutating plan from the original attempt evidence and the current deployment
projection:

```bash
dpone airflow rerun-plan \
  --evidence .dpone/evidence/orders-load-attempt.json \
  --current-index .dpone-cache/current/airflow-index.json \
  --bundle original \
  --artifacts original \
  --critical \
  --format json \
  --output .dpone/reruns/orders-load.json
```

Airflow delivery and dpone artifacts are independent selectors:

| Bundle | Artifacts | Planned operation |
| --- | --- | --- |
| `original` | `original` | Clear the existing run against its retained identities |
| `latest` | `latest` | Create a new run pinned to the current deployment |
| `latest` | `original` | Create a new run with the retained dpone deployment |
| `original` | `latest` | Create a new run with the original Airflow delivery |

`--critical` blocks when the selected bundle is not versioned or snapshotted,
the original release/deployment expired, or an environment/runtime identity is
missing. The command reads local JSON only: it does not call Airflow, clear a
task, refresh the cache, read Connections, or resolve Vault credentials. A
separate platform adapter may consume a `ready` plan after approval.

The original attempt evidence carries `dpone.airflow-run-identity.v1`: exact
release, deployment, DAG spec, workload pack, runtime image, binding, registry,
credential-runtime, and DAG Bundle identities. Retention automation must protect
the plan's `release_ids` and `deployment_ids` before scheduling the rerun.

## Correlate Airflow, lineage, and metrics

This is an operator/CI workflow, not an extra beginner command. First collect a
final release-policy evidence bundle for the workload attempt. Then reuse that
single artifact in the existing exporters:

```bash
dpone ops lineage-export \
  --run-registry-entry .dpone/run-registry/orders.json \
  --airflow-evidence-bundle .dpone/evidence/orders-airflow.json \
  --output-dir .dpone/lineage/orders

dpone observability metrics-export \
  --run-report .dpone/runs/orders.json \
  --airflow-evidence-bundle .dpone/evidence/orders-airflow.json \
  --output-dir .dpone/observability/orders
```

The evidence bundle is authoritative. Its `dpone.airflow-correlation.v1`
section binds the Airflow attempt to the dpone run, release, deployment,
workload pack, runtime evidence digest, and observed pod. Export is local-only
and fails closed for an incomplete or contradictory correlation. No secret,
Vault path, signed URL, synthetic trace ID, or high-cardinality Prometheus label
is emitted. See [OpenLineage](lineage.md) and
[runtime observability](observability.md).

## Related runbooks

- [Credential resolver lifecycle and recovery](airflow-credential-resolver-lifecycle.md)
- [Advanced compatibility: GitOps Airflow runner pack](gitops-airflow-runner-pack.md)
- [Lightweight Airflow pack provider](airflow-pack-provider.md)
- [Provider and cache migration](airflow-provider-cache-migration.md)
- [Airflow cache sync and recovery](airflow-cache-sync.md)
- [Lightweight provider run identity](airflow-pack-provider.md#composite-run-identity)
- [OpenLineage and run history](lineage.md)
- [Runtime observability](observability.md)
- [Five-user and production reference evidence](airflow-self-service-evidence.md)
- Pilot layout: `examples/airflow-self-service-pilot/`
