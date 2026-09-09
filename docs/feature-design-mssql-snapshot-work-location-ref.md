# Feature design: environment-bound MSSQL snapshot work location

- Status: APPROVED
- Owner: dpone maintainers
- Target release: 0.74.32
Last verified: 2026-08-30

## Executive summary

MSSQL source snapshots currently accept physical work_database and work_schema
strings in a release-stable pipeline manifest. The same manifest is promoted
from DEV to PROD, so one physical location cannot correctly represent both
environments. Add work_connection_ref to resolve the complete work database and
schema from the loader-verified binding set and connection registry. The
measurable outcome is that one immutable pack resolves the reviewed work
location for each environment independently of its source-read location.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Data architect | Govern one pipeline across environments | Physical work coordinates leak into the shared manifest | One logical ref produces the reviewed location in each environment |
| Operator | Diagnose and clean failed snapshots safely | Snapshots can appear in a business database | Evidence contains logical ref and resolved coordinates |
| Pipeline author | Configure snapshots without deployment knowledge | Must choose a DEV or PROD database in YAML | Uses one registry-backed ref |

The author selects a logical work ref, CI validates both registries, the
deployment binds its environment-specific entry, runtime resolves it before
connector I/O, and cleanup uses the same immutable resolved coordinates.

## Scope

### In scope

- Optional work_connection_ref for MSSQL work-table materialization.
- Resolution through the existing verified RuntimeConnectionContext.
- Database and schema derived atomically from one registry entry.
- Same-server validation against the source connection.
- Evidence and docs for the selected logical and physical work location.

### Non-goals

- Cross-server snapshots.
- Dynamic environment variables in manifests.
- Changing cleanup_policy, TTL, or source query authority.
- Changing behavior of manifests that use explicit work_database/work_schema.

### Assumptions and constraints

- The work alias resolves to MSSQL on the same physical server as the source.
  It may reuse the source credential authority or select a dedicated principal
  with the required work-schema grants.
- Explicit physical coordinates and work_connection_ref are mutually exclusive.
- Runtime without verified connection context fails closed for a logical ref.

## Public contract

### CLI

Existing commands are unchanged. Validation accepts the new manifest field and
reports invalid mixed logical/physical configuration through the existing
non-zero validation path.

### Python API

SourceMaterializationPolicy adds optional work_connection_ref. LoadConfig carries
a composition-root-bound work location; it is not a credential lookup API.

### Manifest/schema

materialization.work_connection_ref is string or null, default null. Existing
work_database/work_schema remain backward compatible. When the ref is present,
both explicit physical fields are rejected.

### Artifacts and evidence

Source materialization decision and snapshot evidence contain
work_connection_ref plus resolved work_database/work_schema. No secret or raw
connection URI is emitted.

### Compatibility and migration

Old manifests behave unchanged. Pipelines can migrate atomically by adding the
logical ref and removing physical work coordinates. Rollback is to the prior
explicit coordinates and prior runtime.

## Detailed algorithm

1. Parse the optional logical ref and reject mixed physical coordinates.
2. During GitOps pack assembly, include the ref in the workload-scoped closed
   connection projection.
3. At runtime composition, add the ref to the bounded connection resolution set.
4. Require an MSSQL descriptor and complete database/schema coordinates.
5. Compare source and work host/port authority; reject cross-server routing.
6. Bind the immutable location to LoadConfig before source construction.
7. Materialization replaces unresolved policy coordinates with the bound
   coordinates.
8. Create, scan, export, eager cleanup, and TTL sweep all reuse one
   MssqlWorkTableLocation.
9. Publish logical ref and resolved coordinates in safe evidence.

### Pseudocode

    if policy.work_connection_ref:
        require verified runtime context
        work = resolve(policy.work_connection_ref)
        require work.type == mssql
        require same_endpoint(source, work)
        require work.database and work.schema
        bind(load_config, ref, work.database, work.schema)

    effective = policy.with(bound_location(load_config))
    prepare_and_cleanup_with_one(effective.location)

### State machine

    Parsed -> Bound -> Materialized -> Cleaned
    Parsed -> Blocked for mixed or missing authority
    Materialized -> Deferred on bounded lock timeout
    Deferred -> Cleaned by a later TTL sweep

### Edge cases

Missing registry entries, wrong connection type, absent database/schema,
cross-server aliases, unverified contexts, partial preparation, lock timeouts,
task failure, retries, and TTL sweep all fail or defer using existing cleanup
semantics. Empty snapshots follow the same lifecycle.

## Architecture

| Component | Existing/new | Responsibility | Dependencies |
|---|---|---|---|
| SourceMaterializationPolicy | existing | Parse public policy and validate exclusivity | pure helpers |
| Runtime connection authority | existing, extended | Resolve the work ref with source/sink refs | verified context |
| Snapshot work-location binder | new | Validate and bind safe coordinates | runtime contracts |
| MSSQL materialization provider | existing | Use one effective location for create/read/drop/sweep | connector port |

The hydrator remains the sole composition root. The MSSQL adapter receives
resolved coordinates through LoadConfig and never reads Airflow, environment
variables, Vault, or registry files.

### Alternatives and tradeoffs

| Alternative | Advantages | Disadvantages | Decision |
|---|---|---|---|
| Hardcode DEV/PROD names in YAML | Small runtime change | Shared release cannot represent both environments | Rejected |
| Read DPONE_ENVIRONMENT in provider | Simple | Ambient mutable authority, no registry binding | Rejected |
| Promotion-time text rewrite | No package change | Fragile path allowlists and drift on every promotion | Rejected |
| Registry-backed logical ref | Existing signed late binding, auditable | Requires patch release | Selected |

### ADR requirement

Not required. This extends the established binding-set/connection-registry
architecture without changing dependency direction.

### Quality-budget impact

One small cohesive binder module and focused extensions to existing models.
No new external dependencies and no import-time I/O.

## Market comparison

The listed systems are N/A for this narrow internal capability: dlt,
Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos, and
Apache Beam do not define dpone's signed per-deployment connection-registry
contract. No comparative product claim is made.

## Measurable differentiation

- Axis: environment isolation of ephemeral MSSQL source snapshots.
- Scenario: one immutable pipeline promoted from DEV to PROD.
- Baseline: one physical work location embedded in the manifest.
- Metric: snapshots outside the environment-approved database/schema.
- Target: zero.
- Procedure: run the same five workloads in DEV and PROD, inspect create and
  post-run cleanup.
- Artifact: CI contract results plus live database inventory.
- Limitation: same-server work locations only.

## Security, privacy, and operations

The ref exposes no secrets. Credentials remain in the existing operator bridge.
Cross-server routing fails closed. Logs contain only logical ref and physical
database/schema. Existing permissions, lock timeout, deferred cleanup, audit,
and 24-hour sweep policies remain active.

## Test and certification plan

| Layer | Scenario | Environment | Expected artifact |
|---|---|---|---|
| Unit | parse, exclusivity, resolved location | local | pytest |
| Contract | registry aliases and schemas | DEV/PROD fixtures | pytest |
| Integration | create/export/eager drop | test connector | pytest |
| Live certification | five impacted workloads | DEV then PROD | Airflow evidence and DB inventory |
| Compatibility | explicit physical coordinates | local | pytest |

## Documentation plan

Update source materialization reference, CHANGELOG, schema assertions, and the
Airflow interchange runbook.

## Rollout and rollback

Release dpone 0.74.32, synchronously update runtime/provider/toolbox pins, migrate
the five policies to the logical ref, run DEV live acceptance and verify cleanup,
then promote through the existing controller. Roll back by restoring 0.74.31
and explicit coordinates. Never cancel an active workload during rollback.

## Agent execution plan

One integrator owns all changed paths in one isolated worktree. No parallel
writers are used.

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm and failure semantics are implementable without guessing.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Relevant market systems are explicitly N/A; no market claim is made.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout, and rollback are complete.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer explicitly requested the environment-specific behavior; status is APPROVED.
