# Feature design: Airflow self-service v0.72.3 hardening

- Status: IMPLEMENTED
- Owner: dpone maintainers
- Approval: maintainer request to remediate every confirmed `v0.72.2`
  self-service review finding as one goal
- Base commit: `bfa778b566bdf29fbe31b2c7efc7a61d528ed1c9`
- Target release: `0.72.3`
- Architecture: frozen by
  [Airflow self-service architecture](airflow-self-service-architecture.md)
- Validation evidence:
  `test_artifacts/airflow-self-service-v0723-hardening/validation-report.md`

## Problem and release decision

The `v0.72.2` release introduced the intended self-service surfaces, but a
post-release review reproduced eight defects across the First DAG journey,
deployment identity, production sampling, asset inference, provider parsing,
sample lifecycle, quality gates, and documentation. Several defects can create
false success or silently omit DAGs, so this is a correctness and trust hotfix,
not a new feature slice.

The release decision is fail closed: `v0.72.3` is not ready until all P1
findings below have direct regression tests and all P2 findings are either fixed
or explicitly proven outside the affected public contract. Live MSSQL,
ClickHouse, Vault, and Kubernetes checks remain `UNVERIFIED` unless the exact
environment runs.

## Personas and repaired journey

| Persona | Broken behavior | Required behavior |
|---|---|---|
| New data engineer | Preview reports success but provider loads no DAG | Golden path produces one parseable, non-runnable preview DAG and one workload pack |
| Platform engineer | Security delivery changes can collide on deployment identity | Nested security and delivery configuration changes the deployment digest |
| Production operator | Static route metadata can authorize an unsafe sample | Production sample requires current, verifiable cheap-read evidence |
| Airflow operator | Malformed index can silently load zero DAGs | Missing required index sections produce one structured parse error |
| Data architect | Asset inference can create ambiguous or cyclic schedules | Ambiguity and cross-DAG cycles block publication; inferred producers emit matching outlets |
| Data engineer | Interrupted sample can leak a table and overwrite evidence | Temporary target has server-side expiry and each run has a unique immutable evidence identity |

The public golden path remains unchanged:

```bash
dpone init project --airflow
dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental --airflow
dpone check pipelines/orders_daily
dpone airflow preview orders_daily
dpone run pipelines/orders_daily --sample 1000 --target temporary
```

## Scope and non-goals

### In scope

1. Generate a valid preview DAG node and matching compact workload pack; use
   the canonical DAG-spec fingerprint algorithm already owned by dpone.
2. Make deployment fingerprint exclusions top-level only, so nested
   `runtime_artifact_delivery.verify.attestations` remains identity-bearing.
3. Require live or content-addressed certification evidence before a production
   sample can claim pushdown/cheap-read safety.
4. Block multi-writer ambiguity and cross-DAG asset cycles; materialize inferred
   producer outlets with provenance.
5. Validate every required `dpone.airflow-deployment-index.v1` section in the
   parse-safe provider, reject duplicate logical artifact ids, and return a
   structured error instead of an empty success.
6. Enforce temporary ClickHouse target expiry and generate collision-resistant
   sample run IDs when the user does not supply one.
7. Bring every provider module below the global hard SLOC budget and include the
   provider in CI module-size and coverage gates.
8. Restore one canonical First DAG journey, generated CLI references, and docs
   that match provider policies.

### Non-goals

- New authoring modes, recipes, resolvers, or orchestration abstractions.
- Broad Airflow provider redesign beyond cohesive extraction required by the
  existing 400-SLOC quality budget.
- Claiming live route certification without current external evidence.
- Changing the five-command beginner journey or compatibility range.

## Public contracts and compatibility

This change is backward compatible for valid inputs and deliberately stricter
for invalid or unsafe inputs.

- Preview artifacts keep the same schemas and add the required workload node
  and workload pack that the schemas already describe.
- `deployment_id` changes only when an identity-bearing nested field was
  previously omitted by the buggy recursive exclusion. Existing affected local
  projections must be rematerialized; immutable artifacts are never edited.
- A deployment index missing `dag_specs`, `workload_packs`, or
  `runtime_artifact_delivery` is invalid and emits `dpone.error.v1` through
  `LoadReport.errors`.
- Production sampling that relied only on bundled status text now fails closed
  until certification evidence is supplied. Development policy remains usable.
- Ambiguous/cyclic asset graphs become publish blockers instead of warnings.
- Explicit user-provided sample `run_id` remains supported. Omitted `run_id`
  becomes unique per invocation.

## Detailed algorithms

### Preview compilation

1. Validate the single authoring source with static `dpone check`.
2. Compile one DAG node with `node_id`, `workload_id`, and
   `pack_ref=cached://workloads/<id>`.
3. Build the existing compact Airflow workload pack in advisory/non-runnable
   preview mode.
4. Compute `spec_fingerprint` with `compute_spec_fingerprint`, which excludes
   only advisory top-level fields.
5. Materialize both artifacts into one immutable release-set and list both in
   `airflow-index.json`.
6. Load the promoted preview through the public provider in a regression test;
   require `loaded=[<dag_id>]` and no errors.

### Deployment fingerprint

`release_id` and `deployment_id` exclude only their documented volatile
top-level fields. Recursive values with the same names remain semantic input.
Canonical normalization still sorts mapping keys, normalizes path separators,
and sorts artifact lists by logical ID.

### Production sample authorization

1. Read route capability metadata as a candidate, never as proof.
2. Require a certification evidence reference with digest, route identity,
   environment, measured bytes/rows, timestamp, and explicit pushdown result.
3. Verify evidence freshness, route match, environment, digest, and policy
   budgets before setting `supports_pushdown_sampling=true`.
4. If evidence is absent or unverifiable in production, return a safety
   violation before source execution. Do not infer cost from `TOP`/`LIMIT`.

### Asset graph

1. Normalize canonical asset URIs and collect every writer and reader.
2. More than one writer without an explicit curator decision is a blocker.
3. Every inferred producer edge adds a matching outlet to the producer spec,
   with `origin=inferred` provenance.
4. Build the DAG-to-DAG graph after declared, inferred, and curated edges merge.
5. Detect cycles deterministically and block every involved DAG before publish.
6. Accept explicit schedules that reference build-inferred producer outlets;
   require a declared/materialized outlet for non-inferred edges.
7. Airflow 2 downgrade preserves dpone evidence even when partition semantics
   are unavailable.

### Index validation and parse isolation

The provider validates the bounded JSON object without network or Airflow state.
Required arrays must exist and be lists; runtime delivery must be a mapping with
a supported mode. One invalid index returns a structured index error. One
invalid DAG inside a valid index remains isolated under `skip_and_report`.

### Sample lifecycle

1. Generate a sortable unique run ID when omitted.
2. Create a unique temporary ClickHouse table with server-side TTL/expiry
   metadata in addition to best-effort `DROP` cleanup.
3. Write evidence through create-only semantics for a run identity; never
   replace another run's evidence.
4. Preserve masking and avoid credentials or row values in names and errors.

## Test and evidence plan

Focused red-green tests must cover:

- the five-command scaffold/preview path followed by public provider loading;
- fingerprint equality for volatile top-level metadata and inequality for
  nested attestation policy;
- missing/wrong-typed required deployment-index sections;
- production sample with bundled metadata only (blocked), verified evidence
  (allowed), stale/mismatched evidence (blocked), and development behavior;
- multi-writer, inferred outlet, and two-DAG cycle cases;
- unique default run IDs, create-only evidence, TTL DDL, and cleanup failure;
- provider module-size gate and generated reference drift.

Broad validation follows `validate-dpone-change` and includes all non-live
tests, import/layer/module-size checks for both distributions, docs, strict
MkDocs, wheel builds, and Airflow 2.10/2.11/3.2/3.3 compatibility smoke.

## Documentation, rollout, and observability

- `getting-started/first-airflow-dag.md` is the canonical beginner journey;
  other pages link to it rather than publishing a competing command sequence.
- The provider page documents `skip_and_report` as default and diagnostic DAGs
  as explicit opt-in.
- Generated CLI references are regenerated and checked in CI.
- Structured errors and LoadReport remain the observability surface; no parse
  path network, metadata DB, Variables, Connections, or Vault calls are added.
- Rollout is a patch release. Existing local deployments affected by the
  fingerprint correction are rebuilt and promoted by digest.

## Market comparison

N/A. This specification repairs internal violations of already approved dpone
contracts and introduces no new competitive capability or product claim.
