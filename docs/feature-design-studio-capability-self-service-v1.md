# Feature design: Self-service capability discovery and Studio v1

- Status: IMPLEMENTED
- Owner: dpone maintainers / Codex integrator
- Issue: implementation request approved by the maintainer on 2026-07-23
- Target release: next patches after 0.73.17; Studio UI 0.1.0
- Last verified: 2026-07-23

## Approval

The maintainer approved the complete implementation plan on 2026-07-23. This
specification is the implementation contract for the core capability read
model, correctness fixes, the local Studio API boundary, and the later
read-only UI. It does not approve remote production hosting, SSO/RBAC, runtime
execution from Studio, or a second authoring engine.

The core capability, CLI, Studio API, compatibility, and security scope is
implemented and validated in
`test_artifacts/studio-capability-self-service-v1/validation.md`. The separate
Studio UI release remains `NO-GO` until the commit-bound five-user usability
gate passes; implementation status does not manufacture that missing evidence.

## Executive summary

dpone already has a five-command Airflow self-service path and a `dpone studio`
HTTP bridge. The current bridge is not safe or internally consistent enough to
be a public product:

- non-executed quality checks can report success;
- connector and route status differs between CLI, marketplace, docs, and Studio;
- Studio can call unsupported route combinations valid;
- the HTTP adapter permits unauthenticated non-loopback access and wildcard CORS;
- OpenAPI and error responses are not sufficient for a typed external client;
- `airflow explain` can combine a missing pipeline with unrelated active-cache
  diagnostics.

The change introduces one read-only `CapabilityDiscoveryService`, fixes
false-success behavior, makes all public projections delegate to the same
snapshot, hardens the bundled HTTP adapter as a local development boundary, and
provides versioned DTOs for a separate read-only Nuxt/Vue UI.

Success means that a first-time engineer can select a scaffoldable route, create
a pipeline, check it, and preview an Airflow DAG in at most 15 minutes without
writing Airflow Python. Unsupported, unverified, skipped, or stale capabilities
must never appear successful or certified.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| New data engineer | Pick a supported route and create a DAG | Matrix, recipes, and connector statuses disagree | One route projection and one exact next command |
| Data engineer | Diagnose one pipeline | Explain can mix global and target state | Target-scoped summary and structured recovery |
| Platform engineer | Expose safe local Studio | HTTP boundary is fail-open | Loopback-first, bounded, authenticated remote opt-in |
| Airflow operator | Understand support versus certification | Maturity and evidence are collapsed | Separate support, maturity, certification, evidence |
| UI developer | Build without duplicating dpone policy | OpenAPI is incomplete | Typed OpenAPI 3.1 and stable DTOs |

### Beginner journey

```bash
dpone init project --airflow
dpone init pipeline orders_daily --route mssql:clickhouse:incremental_merge
dpone check orders_daily
dpone airflow preview orders_daily
dpone test orders_daily
```

`--recipe` remains compatible. When `--route` is omitted on an interactive
terminal, only scaffoldable routes are offered. A supported route without a
recipe remains visible but is labelled `beginner_recipe_available: false`.

The read-only Studio journey is:

```text
route/recipe discovery
  -> pipeline list
  -> target-scoped pipeline summary
  -> Airflow artifact state
  -> structured diagnosis and recovery
```

Studio authoring is a later milestone and uses the existing confined scaffold
plan/apply transaction rather than storing a second project model.

## Scope

### In scope

- Fail-closed managed quality outcomes.
- One connector/route/recipe capability snapshot and semantic digest.
- Additive recipe discovery filters and route metadata.
- Additive `init pipeline --route` facade.
- Target-scoped Airflow explanation.
- Strict Studio draft/reconciliation input and route validation.
- Versioned read-only Studio API and complete OpenAPI 3.1.
- Loopback-first HTTP security, resource limits, audit ordering, and redaction.
- Compatibility aliases for current `/api/*` routes and CLI JSON keys.
- Honest Studio metadata when the UI is not installed.
- A separate read-only Nuxt/Vue UI after core API certification.

### Non-goals

- A new manifest, recipe, route, certification, or runtime engine.
- A new top-level CLI command group.
- Drag-and-drop DAG authoring.
- Studio-triggered sample runs or credential resolution.
- Remote production hosting, SSO, or multi-role RBAC.
- Storing Vault paths, credentials, or project configuration in a Studio DB.
- Claiming every runtime-supported route has a beginner recipe.
- Treating missing live evidence as a pass.

### Assumptions and constraints

- PR #437 is the required base and is present in master as `77f2b530`.
- YAML, declarative catalogs, and verified evidence remain authoritative.
- The bundled server remains stdlib-only for the first core release.
- Existing compatibility routes remain available for at least two minor
  releases and 12 months.
- Existing valid `--recipe` commands and JSON keys remain accepted.
- `api` is an endpoint family and `rest` is a provider identity; they are not
  collapsed into an undocumented alias.

## Public contract

### Capability discovery

The public DTO has schema `dpone.capability-discovery.v1`:

```yaml
schema: dpone.capability-discovery.v1
snapshot_id: sha256:...
connectors:
  - id: mssql
    endpoint_types: [mssql]
    roles: [source, sink]
    install_extras: [mssql]
    maturity: experimental
    release_phase: beta
routes:
  - id: mssql:clickhouse:incremental_merge
    source: mssql
    sink: clickhouse
    strategy: incremental_merge
    support:
      status: supported
      limitations: []
    certification:
      level: experimental
      evidence_status: UNVERIFIED
      evidence_ref: null
    beginner:
      recipe_refs: [mssql-to-clickhouse-incremental]
      recipe_available: true
recipes:
  - ref: mssql-to-clickhouse-incremental
    source: mssql
    sink: clickhouse
    strategy: incremental_merge
```

Five axes remain independent:

| Axis | Vocabulary |
|---|---|
| Connector maturity | `certified`, `experimental`, `community` |
| Release phase | `stable`, `beta`, `alpha` |
| Route support | `supported`, `conditional`, `not_supported` |
| Route certification | `experimental`, `route-certified`, `production-certified`, `enterprise-certified` |
| Evidence | `PASS`, `FAIL`, `SKIP`, `UNVERIFIED` |

Missing, stale, malformed, mocked, skipped, foreign-commit, or unavailable
evidence yields `UNVERIFIED`. It cannot promote maturity or route
certification.

Credential-free `mock_contract` runs may report `passed: true` for their
behavioral assertions, but the report always carries
`evidence_status: UNVERIFIED`. Empty mock selections are `passed: false`.
Evidence bundles and go-live gates require both a passing report and
`evidence_status: PASS`; mock output can never satisfy that gate.
All certification-sensitive release and runtime readers use one typed
certification-trust predicate, including release summary, promotion,
change-request, post-deploy, production maturity, evidence packs, and native
transfer route activation. A certification payload without `evidence_status`,
including a legacy payload with `passed: true`, is `UNVERIFIED`; generic
evidence keeps its historical evaluation rules unless it declares an evidence
status. Static native-transfer planning can expose
`behavior_passed: true`, but it always returns `passed: false` and
`evidence_status: UNVERIFIED` until approved current live evidence exists.
Exact uppercase `PASS` is necessary but not sufficient: non-empty blockers,
violations, findings, errors, failed required stages, a mismatched release, or
a changed or missing stage artifact fail closed. Operational bundles are
non-vacuous, required names are unique, and consumers re-read each JSON file
and compare its recorded SHA-256 before a go-live decision. A stage or bundle
cannot become trusted by copying `passed: true` and `evidence_status: PASS`.
Fixture-driven CDC apply, performance threshold, route capability, and
live-state aggregation reports expose behavioral success separately and remain
`UNVERIFIED` until a current approved live proof supplies authority.

Public composition roots load optional evidence only from the explicit,
bounded `capability_discovery.certification_evidence` section in `dpone.yaml`.
The section pins one matrix path, one expected commit, a bounded list of proof
directories, and a freshness limit. No CLI, Studio, or marketplace surface
recursively discovers certification artifacts.
Certification candidates are derived from the validated canonical matrix rows
and injected into the proof reader; the generic discovery boundary does not
import a connector-specific safe-sample catalog.

`snapshot_id` is the SHA-256 of deterministic canonical JSON excluding volatile
timestamps. Connector, route, and recipe lists are sorted by logical identity.

### Quality

Managed quality outcomes use `passed | failed | skipped | unverified`.
Compatibility serializers may retain legacy `pass | fail | skip` values only
when the semantic result is unchanged.

```text
executed pass       -> passed
executed failure    -> failed
mode=skip           -> skipped
plan-only/live-miss -> unverified
empty run           -> unverified
```

Aggregate `passed` is true only when at least one check executed and every
configured check is `passed` or non-blocking warning. Empty, all-skipped, or any
unverified run has `passed: false`.

### CLI

```bash
dpone recipe list [--source TYPE] [--sink TYPE] [--strategy MODE]
dpone recipe show REF
dpone init pipeline ID (--recipe REF | --route SOURCE:SINK:STRATEGY)
dpone connectors certify [--report-only]
```

- Recipe list/show output adds route, support, evidence, install-extra,
  limitations, and next-command fields.
- Filtered list output also includes matched `routes[]`; a supported route with
  no recipe remains visible with `beginner.recipe_available: false`.
- Capability authority errors are returned as `passed: false` plus `issues[]`
  and a non-zero exit instead of being hidden behind `UNVERIFIED`.
- A malformed external recipe catalog is represented as a stable capability
  issue; built-in recipes remain discoverable, but every consuming CLI,
  Studio, marketplace, and evidence surface remains non-passing.
- Existing fields are not removed.
- `--route` and `--recipe` are mutually exclusive.
- Non-interactive invocation without either keeps the existing default recipe.
- Interactive invocation may present a bounded picker of scaffoldable routes.
- Unknown or non-scaffoldable routes fail before filesystem writes with
  `DPONE_ROUTE_NOT_SUPPORTED` or `DPONE_ROUTE_NOT_SCAFFOLDABLE`.
- Explicit `--recipe` selection resolves through the same capability authority
  before filesystem writes. A recipe whose declared route is absent or
  `not_supported` is projected as non-scaffoldable and fails closed.
- `connectors certify` exits `1` when `passed=false`; `--report-only` explicitly
  preserves report-only exit `0`.
- `--fail-on-missing` remains accepted with one deprecation warning per process.
- Self-service and discovery result payloads are written to stdout, including
  structured `passed: false` results, preserving the established automation
  contract. Parser/usage diagnostics and fatal Studio startup errors are
  written to stderr.

### Pipeline summary

`dpone.pipeline-summary.v1` is target-scoped:

```yaml
schema: dpone.pipeline-summary.v1
id: orders_daily
source_path: pipelines/orders_daily/pipeline.yaml
valid: true
operational_status: ready
authoring_mode: flow
recipe: mssql-to-clickhouse-incremental
processes:
  - name: orders_daily
    source: mssql
    sink: clickhouse
    strategy: incremental_merge
quality_gates: []
schedule: null
artifact_state: {}
support: {}
certification: {}
next_actions: []
errors: []
```

Every declared process must resolve to a supported canonical route. An unknown
or `not_supported` route makes the summary `valid: false`,
`operational_status: blocked`, adds `DPONE_ROUTE_NOT_SUPPORTED`, and replaces
Airflow preview guidance with `dpone check`.

Processes retain canonical source order. A missing pipeline cannot inherit
materialized, pinned, or runtime-delivery status from another pipeline.

### Studio API

Canonical routes:

```text
GET  /healthz
GET  /openapi.json
GET  /api/v1/meta
GET  /api/v1/capabilities
GET  /api/v1/recipes
GET  /api/v1/pipelines
GET  /api/v1/pipelines/{id}/explain
POST /api/v1/manifests/draft
POST /api/v1/plans
POST /api/v1/checks/static
```

The API is read-only with respect to durable project state. POST is used for
bounded query payloads, not mutation.

`GET /api/v1/recipes` returns the same `snapshot_id`, `passed`, and `issues[]`
authority state as CLI recipe discovery. Draft, canonical plan, deprecated plan
alias, inline static check, and pipeline-referenced static check all use one
injected route validator. Plan and static-check compile the authoring source
once, resolve classic table overrides, and validate the same resolved
`ProcessSpec` snapshot consumed by planning. A complete effective route absent
from the snapshot fails with `DPONE_ROUTE_NOT_SUPPORTED` before a plan can
report success. Effective certification-artifact references are read only from
those canonical objects, must resolve to bounded regular files inside the
project root, and cannot escape through `..` or symlinks.

All errors use `dpone.error.v1`. HTTP mapping is:

| Condition | Status |
|---|---:|
| malformed JSON | 400 |
| missing/invalid token | 401 |
| forbidden origin or policy | 403 |
| unknown route | 404 |
| wrong method | 405 |
| request timeout | 408 |
| body over limit | 413 |
| unsupported media type | 415 |
| schema/domain validation | 422 |
| rate/concurrency boundary | 429 |
| redacted internal failure | 500 |

Existing `/api/*` routes are deprecated aliases. Where legacy behavior was
unsafe, the alias returns a fail-closed result rather than preserving false
success. Aliases emit `Deprecation`, `Sunset`, and `Link` headers and remain
for at least two minor releases and 12 months. The earliest planned removal
date is 2027-07-23.

### HTTP boundary

Defaults:

```yaml
host: 127.0.0.1
body_limit_bytes: 1048576
request_timeout_seconds: 10
max_concurrent_requests: 32
audit_capacity: 1000
page_size_default: 100
page_size_max: 200
cors: same-origin
```

Non-loopback bind requires all of:

- `--allow-remote`;
- a non-empty token;
- one or more exact allowed origins.

Failure occurs before bind with CLI exit code `4`. Tokens are compared in
constant time. RBAC is not advertised; this milestone provides one shared-token
identity only.

Paths are resolved under the configured project root with bounded, no-follow
reads and symlink-escape checks. API output never contains shell command
strings assembled from user input; command suggestions are structured argv.

### Studio UI

After core API certification, the separate `PaulKov/dpone-studio` project will
use Nuxt 4, Vue, TypeScript, Node 22+, generated OpenAPI types, Vitest, and
Playwright. No public UI repository or release is claimed by the core patch.
UI 0.1.0 is read-only and contains route/recipe discovery, pipeline details,
Airflow artifact state, and structured diagnostics.

The UI owns presentation only. It does not maintain capability tables, parse
CLI output, compile manifests, resolve credentials, or decide certification.
The primary local topology serves versioned static assets from the same origin.
Without compatible assets, metadata reports `ui_status: not_installed`.
Asset installation is not a product-readiness decision: metadata independently
reports `usability_status: UNVERIFIED` and `release_verdict: NO-GO` until
commit-bound five-user evidence is approved.

## Detailed algorithms

### Capability snapshot

```text
at the start of each CLI command or Studio use case:
    load import-safe connector declarations
    load static route profiles and strategies
    load recipe catalog
    load optional certification evidence through strict validator
    evaluate freshness against the injected trusted clock
normalize endpoint family and provider identity explicitly
for each declared connector:
    project roles, extras, maturity, release phase
for each route profile:
    project support and limitations
    attach exact matching recipes
    attach certification only when evidence identity is current and valid
sort all collections by logical identity
canonicalize non-volatile payload
compute snapshot_id
return immutable DTO
```

Discovery performs no network, database, secret, runtime, or Airflow metadata
calls. A long-lived Studio captures exactly one snapshot per use case rather
than retaining a process-lifetime snapshot.

### Managed quality

```text
validate rows and checks as bounded mappings
if checks is empty:
    return aggregate unverified, passed=false
for each check:
    if mode=skip:
        outcome=skipped
    elif check requires live context and no exact live evidence:
        outcome=unverified
    else:
        execute check and return passed/failed
        preserve advisory behavior in mode=warn, not a fifth status
aggregate passed only when executed_count > 0
and failed_count == 0
and unverified_count == 0
and skipped_count < configured_count
```

### HTTP request

```text
reject non-loopback unsafe configuration before bind
acquire bounded concurrency slot or return 429
parse route and exact origin
authorize with constant-time token comparison
validate method, media type, content length, JSON, and request schema
invoke injected application service
map typed domain error to dpone.error.v1 and HTTP status
record exactly one redacted audit event
send exact-origin CORS only when allowed
release concurrency slot
```

### Studio authoring v0.2

```text
request route/recipe and logical connection refs
compile existing scaffold plan
return unified diff + source fingerprints + plan_digest
apply requires plan_digest + expected fingerprints + idempotency key
lock project transaction
re-read sources and recompute plan
reject fingerprint or digest mismatch
apply through existing atomic scaffold applier
persist rollback journal and idempotent receipt
never modify credentials, runtime, release, or deployment state
```

## Architecture

| Component | Responsibility | Dependencies |
|---|---|---|
| `CapabilityDiscoveryService` | Immutable connector/route/recipe projection | Explicit catalog/evidence ports |
| `PipelineSummaryService` | Target-scoped authoring/Airflow summary | Compiler, explain snapshot, capabilities |
| CLI facades | Parse/render only | Application services |
| `StudioApplicationService` | Strict-DI read-only use-case facade | Snapshot provider and injected application services |
| `StudioApiService` | Deprecated zero-argument Python compatibility facade | Studio composition root |
| HTTP adapter | Bounded transport, auth, CORS, errors, audit | Studio API protocols |
| Studio UI | Presentation and browser state | Generated API client |

Composition occurs in the `dpone.readiness` application composition modules or
thin command composition roots. Services do not instantiate vendor clients or
call private methods on other services.
Marketplace, readiness certification, and legacy Studio catalog functions
become delegating compatibility views.

### Alternatives and tradeoffs

| Alternative | Decision |
|---|---|
| Keep static lists in each facade | Reject: contradictory status and DRY violation |
| One generic `status` | Reject: hides support/evidence semantics |
| UI owns its own catalog | Reject: second policy engine |
| Add FastAPI immediately | Defer: unnecessary dependency for local adapter |
| Bundle a DAG canvas | Reject for v1: another authoring surface |
| Read-only UI before authoring | Adopt: validates API and user model safely |

### ADR requirement

ADR 0030 is required because authority, dependency direction, external API
versioning, evidence semantics, and the local/remote HTTP boundary are
cross-layer architectural decisions.

### Quality-budget impact

New domain/read-model modules stay under the repository `max_sloc: 400` hard
limit. HTTP routing, request parsing, security policy, audit storage, OpenAPI,
and service composition are separate responsibilities. No generic plugin
system is introduced. Existing clustering and layer budgets may not regress.

## Market comparison

Primary official sources verified 2026-07-23.

| System | Observed pattern | Decision | Source |
|---|---|---|---|
| Airbyte Connector Builder | UI and direct YAML represent the same declarative model with validation | Adopt one authoring authority; reject hidden UI state | https://docs.airbyte.com/platform/connector-development/connector-builder-ui/overview |
| dltHub 1.29 | Local dashboard exposes metadata, traces, history, schema, and state | Adopt local read-only operational view | https://dlthub.com/docs/hub/ingestion/dashboard |
| Fivetran | Connection list separates source, destination, status, and troubleshooting | Adopt concise filtering and status explanation | https://fivetran.com/docs/getting-started/fivetran-dashboard/connectors |
| Astronomer Cosmos | Static project artifacts materialize Airflow DAG/TaskGroup objects | Adopt static orchestration boundary only; N/A for Studio UI | https://astronomer.github.io/astronomer-cosmos/getting_started/core-concepts.html |
| Microsoft SSIS | Visual control/data-flow designer and connection managers | Reject drag/drop in v1; it creates opaque parallel authoring | https://learn.microsoft.com/en-us/sql/integration-services/ssis-designer |
| Informatica | N/A for this local open API boundary | No directly adopted pattern | N/A |
| Pentaho | N/A for parse-safe Airflow self-service | No directly adopted pattern | N/A |
| gusty | N/A for capability/Studio design | No directly adopted pattern | N/A |
| Apache Beam | N/A for authoring UI and connector certification | No directly adopted pattern | N/A |

## Measurable differentiation

```yaml
axis: time to a correctly diagnosed first Airflow DAG
scenario: first-time engineer selects a scaffoldable route and previews its DAG
baseline: capability surfaces disagree; human result is UNVERIFIED
metric: completion time and facilitator actions
target: ">=80% of at least 5 users finish within 15 minutes with 0 facilitator actions"
procedure: scripted fresh-project usability session plus telemetry-free observer scorecard
artifact: test_artifacts/studio-usability/<date>-<commit>/{usability-study.json,certification.json,certification.md,summary.md}
limitations: route sample begins with built-in scaffoldable routes
```

## Security, privacy, and operations

- No secrets or Vault paths in capability, summary, OpenAPI, logs, or audit.
- Shared tokens are read from environment/configuration and never echoed.
- Remote binding requires an explicit policy and is still development-only.
- Registry/evidence paths are redacted for non-platform views.
- Audit is process-local, bounded, synchronized, and explicitly non-durable.
- Read-only endpoints do not trigger live connector probes by default.
- Runs/artifact listing is confined, paginated, bounded, and race-safe.
- UI stores token in memory/session only, never durable browser storage.

## Test and certification plan

| Layer | Scenario | Environment | Expected artifact |
|---|---|---|---|
| Unit | quality truth table and validators | hermetic | pytest report |
| Contract | capability parity and schemas | hermetic | snapshot/parity JSON |
| Contract | HTTP/OpenAPI error and security matrix | loopback with fakes | JUnit + OpenAPI validation |
| Integration | 100 concurrent requests and bounded runs | local mocked | load-boundary report |
| Compatibility | legacy CLI/API aliases | hermetic | compatibility matrix |
| UI | route/pipeline/diagnosis workflows | Playwright desktop/mobile | screenshots + JUnit |
| Live | remote boundary and exact connector evidence | approved environment only | PASS/FAIL/SKIP evidence |
| Usability | first-time five-command path | at least 5 new users | scorecard |

Required negative cases include malformed JSON, oversized/slow body, wildcard or
`null` origin, missing token, path traversal, symlink escape, shell
metacharacters, stale/mock evidence, unknown route, duplicate identities,
source mutation between plan/apply, malformed recipe authority on every
projection, mock evidence presented to a go-live gate, and injected internal
failures. The matrix also covers status-less and `UNVERIFIED` certification at
every release/runtime consumer, static native-transfer evidence, effective
classic overrides, and explicit unsupported external recipes before writes.
Release-chain coverage additionally includes bundle replay across releases,
forged artifact indexes, missing or tampered stage files, empty bundles,
duplicate required evidence, contradictory `PASS` payloads, and unsafe retry
claims attached to failed certification evidence. Release summaries require
the latest chain entry and every certification suite to match the requested
release; the chain's immutable artifact index must exist, match its recorded
digest, and contain the exact suite digest. Every release-bound index item is
reread through bounded regular-file checks and its SHA-256 is recomputed,
including replay chains without a separately named suite. Duplicate logical
names or paths, traversal, symlinks, missing files, and digest drift fail
closed.

## Documentation plan

- Update the First DAG and recipe reference with route-aware discovery.
- Add a curated beginner command index; keep generated CLI reference generated.
- Correct Studio documentation to distinguish API bridge, optional assets, and
  not-yet-supported remote production use.
- Add capability DTO, OpenAPI, security, compatibility, and troubleshooting
  references.
- Keep matrix rows explicit about beginner recipe availability.
- Remove stale image/version examples and unimplemented RBAC claims.

## Rollout and rollback

1. Correct false-success and target-scope bugs.
2. Introduce capability service and migrate all projections.
3. Publish versioned schemas and hardened local API.
4. Certify CLI/API parity and security gates.
5. Run the human usability gate for the exact release candidate.
6. Release read-only UI 0.1.0 only after the gate passes.
7. Design and release authoring UI 0.2.0 under a separate approved milestone.

Rollback retains legacy aliases and can disable the versioned Studio server
without reverting capability truth fixes. A certification or quality
false-positive is an immediate rollback/release-blocking condition.

## Agent execution plan

| Role | Owned paths | Forbidden paths |
|---|---|---|
| Architect reviewer | read-only architecture and contracts | all writes |
| Correctness implementer | managed quality and focused tests | shared schemas/docs |
| Capability implementer | capability service and focused tests | CLI/docs/shared registries |
| HTTP implementer | Studio HTTP adapter and focused tests | capability authority/docs |
| Integrator | shared schemas, CLI, docs, changelog, composition | none |
| Fresh reviewer | read-only final diff and evidence | all writes |

The integrator owns shared schemas, registries, CLI composition, MkDocs,
`CHANGELOG.md`, and final evidence.

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm and failure semantics are implementable without guessing.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Market research uses current official primary sources.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout, and rollback are complete.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer approved implementation on 2026-07-23.
