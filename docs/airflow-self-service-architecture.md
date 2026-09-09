# dpone industrial self-service Airflow architecture

- Status: APPROVED
- Owner: dpone maintainers
- Last verified: 2026-07-26

This document is the frozen architecture for the dpone Airflow self-service
initiative. It defines the contracts that must stay stable while Phase 0, Phase
1A, and Phase 1B are implemented.

## Current implementation status

The current Phase 1A/1B foundation includes:

- `dpone init project --airflow`
- `dpone init project --airflow --layout domain-first`
- `dpone init domain <name>` with explicit owner and approver values
- `dpone init pipeline <name> --recipe <built-in>` with the project Airflow
  default and explicit `--airflow` / `--no-airflow` overrides
- route-first domain scaffolding with `--domain`, `--route`, `--from`, `--to`,
  and `--key`
- static `dpone check`
- credential-free `dpone test`
- credential-free `dpone check --connections --environment dev`
- `dpone airflow preview`
- platform `dpone airflow build`
- platform `dpone airflow publish`
- platform `dpone airflow cache-materialize`
- platform `dpone airflow cache-sync`
- platform `dpone airflow cache-retention-plan`
- diagnostic `dpone airflow explain`
- canonical provider facade under `airflow.providers.dpone`
- environment-neutral `release-set`, non-runnable preview `deployment-set`, and
  `airflow-index.json`
- one build-plane workload selector engine shared by project check, static
  preview, and sequential temporary sample execution

Phase 4 adds signed recipe/connection-registry bundle promotion and closed
extension conformance. Both stay in CI/materializer build planes. The Airflow
provider loader remains local, bounded, and signature-verifier-free; runtime
continues to receive only pinned release/deployment artifacts. See
[signed catalog bundles](signed-catalog-bundles.md) and
[extension conformance](extension-conformance.md).

The scaffolder is idempotent: repeated runs return `no_op`, user-owned
conflicts are not overwritten, JSON output includes unified diffs, and created
files are listed in a rollback journal. A failed multi-file apply reports what
actually happened: `rolled_back` for removed scaffold-owned bytes,
`preserved` for a concurrent pathname winner, `not_applied` for later planned
writes, and `recovery` with a project-relative path when displaced bytes need
manual recovery. A receipt never claims rollback for bytes it could not remove.

Pipeline scaffolding records the authority behind the effective Airflow
decision. `airflow_source` is `explicit` for `--airflow` / `--no-airflow`,
`project` for `dpone.yaml` policy, and `legacy_default` when no project policy
exists. The paired `airflow_enabled` boolean is returned in text and JSON so
users and CI can explain the result without inferring it from generated files.

### Workload selection boundary

Flat projects compile authoring sources explicitly named by bounded domain
catalogs. Domain-first projects use one bounded, exact-depth, no-follow
discovery snapshot over colocated ownership and pipeline sources. Both
authorities converge before selection in the same orchestrator-neutral graph.
One pure selector engine resolves exact metadata selectors,
upstream/downstream expansion, named selectors, and semantic-state comparison.
Commands consume the same immutable selection report; no command owns a
private graph or filesystem-discovery policy.

```mermaid
flowchart LR
    C["Flat domain catalogs"] --> A["Canonical compiler"]
    O["Domain-first ownership and pipeline sources"] --> D["Bounded discovery + canonical compiler"]
    D --> SNAP["Checked discovery snapshot"]
    A --> G["Selection graph"]
    SNAP --> G
    SNAP --> WI["Ephemeral candidate workload index"]
    BASE["Accepted workload-index baseline"] --> CI["Frozen candidate change impact"]
    WI --> CI
    CI --> APPROVE["Protected approval adapter"]
    APPROVE --> CAS["Project-locked digest CAS promotion"]
    CAS --> BASE
    N["Named selectors and optional state"] --> S["Pure selector engine"]
    G --> S
    S --> R["dpone.selection-report.v1"]
    R --> CHECK["Selected check"]
    R --> PREVIEW["Selected release/deployment preview"]
    R --> SAMPLE["Sequential temporary sample"]
    PREVIEW --> INDEX["Static airflow-index.json"]
```

The selected preview keeps an edge only when both endpoints are selected,
records pruned boundaries in provenance, writes `selection-state.json`, then
atomically promotes the complete immutable projection. The provider reads only
the resulting index; it never imports selector code, reads YAML/state, scans a
repository, or performs network/secret I/O.

Phase 1B live data movement remains intentionally separate: bounded live probes
and physical source-to-target copying still require explicit runtime/live-copy
configuration. The foundation now includes environment deployment projection,
binding/registry validation, credential resolver contracts, digest-only
`init_fetch` planning and local execution, local cache materialization,
beginner local safe-sample deployment promotion, and plan-first deployment
cache retention. `dpone check --connections --environment dev`
validates binding-set, connection-registry, and credential-runtime
configuration without reading secret values or calling Vault, Kubernetes,
databases, or object storage.
`dpone check --live` now performs the same offline validation first, then emits
catalog-registered `dpone.live-preflight.v1` and fails closed with
`DPONE_LIVE_CHECK_RUNNER_NOT_CONFIGURED` plus exit code 3 when no runtime-side
live preflight runner is configured. The payload shows the planned network,
secret, credential-resolution, and bounded source/sink probes without
performing them yet. Default text output uses the same self-service shape as
connection checks: `dpone check live: FAILED|OK`, target, runner status,
planned or executed network/secrets/source-query state, resolved connection
refs, planned probes, structured error codes, safe fixes, and the JSON rerun
hint. It does not fall back to the generic self-service failure block.
The readiness service also exposes an injected live-preflight runner contract:
platform/runtime code can provide a bounded runner that returns configured
probe results, while the CLI default stays fail-closed and credential-free.
Runner exceptions and runner-returned structured errors are redacted before
they enter `dpone.live-preflight.v1` or top-level `dpone.error.v1` output, so
`password=...`, `token=...`, `vault_token=...`, `api_key=...`, and secret-like
diagnostic keys cannot leak through live-check JSON, logs, or troubleshooting
copy.
The `dpone run ... --sample ... --target temporary` flags are accepted now.
Incomplete sample arguments fail fast with
`DPONE_SAFE_SAMPLE_ARGUMENTS_INVALID` and exit code 2 before reading the
pipeline source. Non-positive row budgets fail just as early with
`DPONE_RUNTIME_SAMPLE_SIZE_INVALID`. Complete development sample requests now
materialize a local runnable deployment projection in the dedicated
`.dpone-cache/safe-sample-deployments` cache when that cache does not represent
the current pipeline semantics. They never switch the scheduler-visible
`.dpone-cache/current`. Reuse requires a
matching source-derived `release_id` plus consistent pointer, audit, projection,
release-set, and artifact checksums. The command pins a new release/deployment
with a local workload pack, promotes it through the same cache materializer contract,
executes local `init_fetch`, and then selects a deployment-scoped live
authorization overlay. If no overlay exists, it preserves the existing
network-free handoff and stops at the most specific structured live-copy
boundary. For the built-in MSSQL -> ClickHouse route that boundary is
`DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED`. A complete overlay
reuses the existing signed live assembly automatically; a partial or invalid
overlay blocks without falling back. The command does not query sources or
write targets until the certified live path is verified. The payload includes local deployment
promotion metadata, available runtime contracts, remaining blockers, and the
evaluated safe-sample policy. It also includes a
`dpone.safe-sample-execution-plan.v1` object and a
`dpone.safe-sample-runtime-readiness.v1` report. Both receive only the locally
verified safe-sample deployment projection and pin its `release_id` and
`deployment_id` into the planned workload URI. Production policy fails closed
unless either explicit connector capabilities or a certified connector route
prove pushdown sampling, and production requests cannot reuse a development
current deployment: they block with `DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH`.
Content-addressed local release files are create-or-compare: an existing file
with different bytes fails the sample preparation and is never overwritten in
place.
Development policy allows only bounded full-scan rehearsal with an explicit
byte estimate. When the dedicated safe-sample deployment is runnable and uses
`runtime_artifact_delivery.mode=init_fetch`, the beginner runtime handoff now
fetches pinned `cache://` workload packs from that isolated local cache mirror
into the run's `runtime-artifacts` directory before stopping at the data-copy
boundary or entering the verified live assembly.

### Beginner automatic live selection

The beginner facade and the platform command share one runner. Their only
difference is port composition. After the execution plan pins one verified
deployment, the facade derives this platform-owned overlay without accepting a
path or enable flag from the user:

```text
.dpone-cache/route-authorizations/
  sha256-<deployment digest>/<pipeline_id>/
    route-attestation.json
    route-attestation.sigstore.json
    route-certification-bundle.json
    route-attestation-policy.json
```

```mermaid
flowchart LR
    PLAN["Pinned sample plan"] --> DISCOVER["Static overlay discovery"]
    DISCOVER -->|absent| LOCAL["Network-free local handoff"]
    DISCOVER -->|partial or unsafe| BLOCK["Structured blocker"]
    DISCOVER -->|complete| VERIFY["Existing live assembly"]
    VERIFY -->|invalid or unverified| BLOCK
    VERIFY --> PORTS["Credential-aware ports"]
    PORTS --> RUNNER["Shared runtime runner"]
    RUNNER --> EVIDENCE["Execution and cleanup evidence"]
```

Discovery checks only deterministic path shape and file completeness. It does
not parse registry content, resolve credentials, call cosign, fetch artifacts,
or contact a data system. The existing live assembly then verifies source pin,
signature, certification digest, route/deployment subject, policy,
environment fingerprints, schemas, and route binding in that order. Credential
resolver and SQL-client construction remain downstream of all trust checks.

The selection is asymmetric by design: absence keeps backward-compatible local
handoff; partial materialization indicates platform drift and fails closed.
Runtime evidence contains `execution_mode: local_handoff` or
`execution_mode: live_copy`. The overlay is an environment authorization layer,
not release/deployment identity and not a replacement for the signed
attestation.

The same runtime boundary is now exposed for KPO/runtime-pod rehearsal through
`dpone ops safe-sample-runtime-run --plan-json <plan.json>`. The command accepts
the prepared `dpone.safe-sample-execution-plan.v1`, never resolves `current`,
fetches only pinned local `cache://` artifacts, writes
`safe-sample-runtime-execution.json`, and remains fail-closed unless live copy
is explicitly enabled. With only `--pipeline-source`, it assembles the
certified MSSQL -> ClickHouse copier and emits the route-specific certified copy
request before stopping at the missing live executor boundary. With
`--enable-live-copy` and all pinned runtime inputs, it re-authorizes policy,
verifies the exact primary manifest against the immutable workload pack, checks
binding/registry/runtime fingerprints, and only then creates the TTL-protected
ClickHouse target, performs the bounded copy, and drops the target. Source,
sink, and target lifecycle share one workload-scoped credential resolver, so a
rotation cannot change the credential version halfway through a workload.

## Golden path

For the beginner tutorial, start with
[First Airflow DAG](getting-started/first-airflow-dag.md). The architecture
contract below explains why these commands stay separate from generated runtime
artifacts.

### Normative lane terminology

These names are normative: preview v1, local safe-sample handoff, strict-v2
non-production executable rehearsal, production parse canary, and certified
production execution.

| Lane | Contract and allowed execution | Claim boundary |
| --- | --- | --- |
| preview v1 | `dpone.airflow-deployment-index.v1` with `local_preview`; parse-safe and non-runnable | Proves local compilation and provider parsing only. |
| local safe-sample handoff | Isolated development compatibility projection plus local `InitFetchExecutor` prelude | May prepare a pinned plan; without a signed overlay it performs no source/sink I/O and is not strict-v2 evidence. |
| strict-v2 non-production executable rehearsal | Complete v2 `init_fetch` projection with `trust_tier: non_production` in dev/stage | May exercise KPO/init-fetch/runtime wiring, but does not establish production certification. |
| production parse canary | Complete production v2 projection loaded in a paused scheduler/DAG processor | Parse only; tasks must not be triggered while the production attestation verifier or current live evidence is unavailable. |
| certified production execution | Exact production projection plus configured verifier and current route, Kubernetes, identity, registry, runtime, and data-system evidence | The only lane that may make a production-certified execution claim. |

The first five commands are the successful offline preview v1 golden path:

```bash
dpone init project --airflow
dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental
dpone check orders_daily
dpone airflow preview orders_daily
dpone test orders_daily
```

The live sample is a separate platform-gated request:

```bash
dpone run orders_daily --sample 1000 --target temporary
```

`dpone airflow explain orders_daily` is diagnostic and should be suggested only
after an error or when a user asks why a graph/config was produced.

Default text output is a beginner-facing summary over the same parse-safe
payload. It names the pipeline, planned/materialized artifact states, operator
diagnostics status, operator pinning, runtime artifact delivery mode, parse
side-effect result, and the next safe command. Planned artifacts route the user
back to `dpone airflow preview`; invalid or operator-issue diagnostics render
`NEEDS_ATTENTION` and the first safe recovery action without changing the
parse-safe JSON contract. The summary deliberately omits binding topology,
Vault paths, Kubernetes Secret names, Secret keys, mount paths, and other
platform-only details; `--format json` is the supported path for fingerprints
and full operator checks.

When a local preview or deployment index exists, `explain` also includes
operator diagnostics derived only from `.dpone-cache/current/airflow-index.json`
and the workload packs explicitly listed in that index. The report includes the
pinned release/deployment ids, runtime artifact delivery mode, binding and
registry refs, runtime image digest, per-workload operator bridge summaries,
checksum failures, and a parse-side-effect contract showing zero network,
metadata DB, Airflow Variables/Connections, Vault, Kubernetes, or cache refresh
access. For Airflow Connection bridge packs, `explain` names the selected
`AirflowConnectionSecretVolumeKubernetesPodOperator`, logical connection refs,
mounted field names, digest-only Secret reference fingerprints, and deferrable
cleanup policy without serializing URI values, Kubernetes Secret names, Secret
keys, or mount paths. Failed nested operator checks are promoted to an operator
diagnostics summary and `next_actions`, so a first-time user sees the actionable
fix instead of hunting through pack internals. The same summary contract is
present for planned or invalid local indexes; invalid `airflow-index.json`
payloads preserve `status: invalid` and include a cache/preview recovery next
action.
The payload is registered as `dpone.airflow-operator-diagnostics.v1` in the
GitOps schema catalog so CI, IDE tooling, and platform checks can validate
`explain` output without importing Airflow or contacting runtime services.
The enclosing `dpone airflow explain --format json` response is also registered
as `dpone.airflow-explain.v1`, keeping the full user-visible troubleshooting
payload machine-checkable.

The five-command offline path succeeds after preview v1 is materialized and the
generated hermetic test passes. The separate live sample,
`dpone run ... --sample ... --target temporary`, materializes an isolated local
safe-sample compatibility projection, pins release/deployment identity, and
executes only the local `init_fetch` prelude. It is not the strict-v2
non-production rehearsal or a production-certified run. Source-to-sink copying
remains fail closed until the signed overlay and certified route executor are
configured; without them it may return exit code `3` without source/sink data
I/O. The CLI reports that exact stopping boundary instead of claiming a
successful data copy.

## Authority model

Each pipeline has exactly one editable authoring source:

```yaml
authoring:
  mode: classic  # classic | flow | folder
  source: pipelines/orders_daily/pipeline.yaml
```

The canonical manifest IR is a normalized execution contract consumed by
GitOps, runtime, and Airflow machinery. It is generated or in-memory and is
never an independently editable authoring source.

### Declarative recipe compilation

A `flow` primary source may pin one external `dpone.recipe.v1` plus its selected
profile and ordered component closure. Discovery consults the project-owned
catalog only while scaffolding. The committed source then carries exact
kind/path/SHA-256 identities, so check, preview, pack, and safe-sample never
depend on a mutable catalog lookup.

The bounded resolver produces data-only process mappings for the same canonical
authoring compiler. It cannot execute Python, Jinja, shell, network, or secret
lookups. Pack materialization embeds canonical `dpone.batch.v1` runtime IR;
Airflow parse does not import recipe modules and runtime does not re-expand the
recipe DSL. Complete closure pins are recorded in release provenance and
independently checked before sample execution. See
[Recipes, profiles, and components](airflow-recipes.md) and
[ADR-0016](adr/0016-content-pinned-declarative-recipes.md).

## Release and deployment

`dpone.release-set.v1` is environment-neutral. It contains DAG specs, workload
packs, and canonical schemas. Compact Airflow compatibility releases may also
carry a bounded, digest-pinned `runtime_payloads` inventory; that field is a
byte-transport bridge and never dbt selection, promotion, certification, or
evidence authority. Compact-v1 materialization accepts at most 64 distinct
payloads, 256 MiB per payload, and 512 MiB of actual bytes across the
release-wide inventory; aggregate overflow publishes no release. Authoritative
dbt releases use `dpone.release-set.v2`.
An authority-eligible v2 `release_id` can be promoted across dev, stage, and
prod without rebuilding; the compact v1 dbt byte bridge is non-production
transport and is rejected by production build and runtime receipt validation.
Each generated artifact entry contains an environment-neutral,
release-relative `path` plus its SHA-256. The v1 compatibility adapter can read
legacy `artifact_ref` entries only when the value uses the same release-relative
path grammar. An entry must contain exactly one of `path` and `artifact_ref`.
A `cache://` URI belongs to the deployment index
because it pins the already computed release identity; putting that URI back
into the release-set would make the release fingerprint self-referential.

`dpone.deployment-set.v1` is the non-runnable preview/legacy compatibility
wire. `dpone.deployment-set.v2` is environment-specific and executable. It
binds a release to a binding-set, connection registry, credential runtime,
exact runtime image, explicit trust tier, registry/trust snapshots, selected
workload inventory, Airflow bundle reference, and runtime artifact delivery
mode.

The `release-set`, `deployment-set`, `current-pointer`, and
`airflow-deployment-index` schemas are published in the
[GitOps schema catalog](reference/gitops-schema-catalog.md) and registered in
the CLI catalog, so CI, IDEs, and platform automation can discover the same
contracts used by the CLI.

Phase 1A uses a non-runnable preview deployment. Preview v1 shares the provider
loader entry point, current-pointer convention, and cache layout with later
platform deployments. It does not share the executable production schema
contract. The platform build regenerates new immutable executable v2 artifacts
from approved source inputs; users never edit or promote preview v1 in place.
The preview release contains one canonical DAG node and one compact workload
pack, allowing the public provider to validate the complete preview projection.
The `local_preview` delivery mode materializes harmless preview tasks instead
of wiring KPO/runtime execution.
Preview promotion also writes schema-compliant `current-pointer.json`, so
cache recovery, retention, sample-run planning, and operator diagnostics observe
the same current deployment identity model before Phase 1B runtime bindings are
available.

Deployment fingerprint exclusions apply only to documented volatile top-level
metadata. Nested runtime policy remains identity-bearing: changing
`runtime_artifact_delivery.verify.attestations` changes `deployment_id`, while
changing a top-level build attestation reference does not.

## Airflow deployment index

The provider reads exactly one `airflow-index.json`. V1 is accepted only for
non-runnable `local_preview`; strict executable `init_fetch` uses v2:

```yaml
# Structural excerpt; use the generated schema/reference for a complete index.
schema: dpone.airflow-deployment-index.v2
trust_tier: production
release_id: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
deployment_id: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
runtime_image_ref: registry.example/dpone-runtime@sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
runtime_image_digest: sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
dag_specs:
  - id: orders_daily
    artifact_ref: cache://releases/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/dags/orders_daily.dag-spec.json
    sha256: sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
    bytes: 4096
workload_packs:
  - id: load_orders
    artifact_ref: cache://releases/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/packs/load_orders.airflow-pack.json
    sha256: sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
    bytes: 8192
airflow_bundle_ref: git:7ac31f2
runtime_artifact_delivery:
  mode: init_fetch
  trust_tier: production
  artifact_registry_ref: dpone-prod-artifacts
  identity:
    method: kubernetes_workload_identity
    service_account: dpone-runtime
  registry_config_ref:
    kind: kubernetes_config_map
    name: dpone-artifact-registry
    key: registry.json
    sha256: sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
  trust_policy_ref:
    kind: kubernetes_config_map
    name: dpone-artifact-trust-policy
    key: policy.json
    sha256: sha256:9999999999999999999999999999999999999999999999999999999999999999
  source:
    artifact_registry_ref: dpone-prod-artifacts
  verify:
    checksums: required
    attestations: required_for_prod
```

`cache://` references are resolved relative to the configured cache root. The
resolver rejects path traversal, symlink escape, missing artifacts, checksum
mismatch, oversized artifacts, and any `current` path segment. Deployment
indexes must point at immutable release content; `current` is an operational
pointer owned by the cache materializer, not an artifact location.
The index reader consumes at most 8 MiB before UTF-8/JSON parsing, and listed
artifacts have a 64 MiB default ceiling. A declared `bytes` value above the
ceiling fails before a full read; otherwise the consumer reads once and checks
both declared size and SHA-256. Hybrid `DponeDag.from_spec()` resolves the
requested ref and materializes it from one pinned index object rather than
re-reading a mutable path.
`airflow_bundle_ref` is delivery identity for the Airflow DAG Bundle side of the
run. It is carried in the deployment index and evidence identity, but it is not
part of the environment-neutral release-set and is never treated as a dpone
workload pack fingerprint.
`dpone airflow explain` classifies that ref without Airflow imports or network
calls. `git:<sha>` is treated as versioned; `local:`, `s3://`, `gs://`, and
unknown refs produce a `DPONE_RERUN_NOT_REPRODUCIBLE` warning unless the
platform provides a separate content-addressed snapshot.
The public `CacheResolver` API exposes the same local-only resolution path for
pinned `cached://dags/<dag_id>` and `cached://workloads/<workload_id>`
references; it does not introduce repository scanning or cache synchronization
during Airflow parse. The URI grammar accepts only the optional `release` and
`deployment` query pins; unknown pin keys fail with
`DPONE_CACHE_REF_PIN_INVALID` so deployment pin typos cannot silently downgrade
to an unpinned or partially pinned reference. Duplicate query pins fail with
the same code because a pinned artifact identity must be unambiguous.
`DponeTaskGroup.from_pack(..., index_path=...)` uses that resolver for cached
workload refs before delegating to the pack task builder, so hybrid DAG escape
hatches also receive verified local pack paths rather than unresolved logical
URIs.

## Provider parse contract

The Airflow provider must not perform network calls, database calls, Kubernetes
calls, Vault calls, Airflow Variable/Connection reads, remote cache refresh, or
manifest/domain scanning during parse. It may only read the deployment index and
the listed local cache artifacts.

Parse SLO CI exercises a local `100 DAG / 500 workloads` pinned `init_fetch`
deployment-index fixture through the installed
`airflow.providers.dpone.load_dpone_dags(index_path=...)` wheel in every
documented Airflow/Python matrix cell. Each cell records 30 process-cold
samples and 30 immediate second-parse samples in a JSON artifact. Process-cold
means a fresh Python process and provider parse over the same local fixture;
the shared filesystem page cache is intentionally not claimed as cold.

| Check | Budget |
| --- | ---: |
| Cold parse | `p95 <= 5 seconds` |
| Warm parse | `p95 <= 3 seconds` |
| Additional process RSS | `<= 250 MiB` |
| Network, DB, Kubernetes, Vault, Variables, Connections | `0` calls |

The benchmark also requires exactly 100 materialized DAGs and 500 workload
runtime tasks with one stable topology fingerprint across cold/warm replay.
Missing side-effect probes, partial samples, absent evidence, invalid fixture
artifacts, or dependency inconsistencies fail the matrix job; they are never
reported as a timing pass.

Provider parse policies are explicit. The default is `skip_and_report` for both
duplicates and invalid dag-specs: valid DAGs continue loading, skipped duplicates
and invalid specs are returned in `LoadReport`, and no synthetic diagnostic DAG
is created. Platform-owned DAG files can opt into `fail_all` for fail-closed
parse behavior or `create_diagnostic_dag` for a paused diagnostic DAG when local
cache corruption must be visible in the Airflow UI. Cached workload `pack_ref`
resolution errors are isolated the same way: one DAG spec that references a
missing or mismatched workload pack is reported without preventing unrelated DAG
specs from loading unless `fail_all` is explicitly selected.

That isolation starts only after a trusted index exists. Missing, unreadable,
oversized, malformed, schema-invalid, or identity-invalid deployment indexes
return `LoadReport.fatal: true`, no release/deployment identity, and no loaded
DAGs under the default policy. The generated `dags/dpone.py` checks `fatal` and
raises the first stable index code, so an empty scheduler view cannot be
mistaken for successful parsing. Once the index is accepted, each DAG spec is
size/digest-verified at its own parse boundary and each workload pack is
reverified at final task construction. Damage remains attached to the affected
`dag_id`, and unrelated DAGs may continue with `fatal: false`.

## Structured errors

Self-service commands emit `dpone.error.v1` payloads in JSON mode. The legacy
`code`, `message`, and `path` fields remain present for existing automation, and
new fields add stable `stage`, `severity`, `entity`, `fixes`, and optional docs
or trace references. The schema is published in docs and registered in the
GitOps schema catalog as `error`, so CLI, CI, IDEs, and future Studio flows use
the same discoverable contract. Autofix metadata is advisory only: `safe`,
`manual`, and `destructive` fix classes are explicit, and self-service autofix
must not touch credentials, runtime infrastructure, releases, or deployments.
When a safe command exists, the error includes it in `fixes[].command`; for
example, legacy pipeline authoring errors point first to
`dpone fix <pipeline> --plan` and then to the manual
`dpone fix <pipeline> --apply` step.

## Binding and credentials

Users reference logical connection aliases through `dpone.binding-set.v1`.
Platform engineers own `dpone.connection-registry.v1`, which maps aliases to
backend-neutral credential resolvers.
Every `connection_ref` is a logical alias, not a path, URI, secret payload, or
backend-specific identifier. Valid aliases are 1-128 characters, start and end
with a letter or digit, and may contain only letters, digits, `.`, `_`, and `-`.
The same contract applies to pipeline `source.connection_ref` /
`sink.connection_ref`, binding-set keys, binding-set target values, and
connection-registry keys. `dpone check --connections` reports
`DPONE_CONNECTION_REF_INVALID` without echoing the raw value, so a mistyped ref
such as a pasted Vault path or `PASSWORD=...` assignment cannot leak into CLI
JSON, Airflow diagnostics, or migration plans.
`binding-set`, `connection-registry`, and `credential-runtime` schemas are
published in docs and registered in the GitOps schema catalog, so platform
automation can validate environment binding contracts without reading secrets.
The dependency-free `GitOpsSchemaValidator` accepts either legacy `kind` or
newer `schema` identity fields and enforces nested forbidden-schema rules, so
CI, IDE, and lightweight platform checks reject the same credential-runtime
secret-material shapes as the CLI schema validator. It also resolves local
`$defs` and validates map-like `additionalProperties`, `propertyNames`,
`minProperties`, resolver `oneOf` branches, and nullable/fingerprinted `anyOf`
branches, so connection-registry field mappings cannot silently contain empty
connector aliases or empty backend field names and deployment refs cannot hide
invalid fingerprints in lightweight checks. It also enforces
`additionalProperties: false` for
strict objects such as binding-set entries, so user-owned bindings cannot drift
into inline resolver, Vault path, or platform-owned registry configuration.
Array `items` are validated as well, so deployment-index `dag_specs[]` and
`workload_packs[]` must keep `cache://` artifact refs and `sha256:` checksums
even in dependency-free CI/IDE validation. Array `minItems` is enforced too, so
diagnostic payloads cannot claim validity with empty required collections.
Array `uniqueItems` is enforced with deterministic JSON item keys, so readiness
and runtime diagnostics cannot hide duplicate contract or blocker entries.
Nullable union types such as `string|null` are enforced too, so deployment refs
cannot be silently replaced by objects or numbers in lightweight validation.
Numeric `integer` and `number` types are enforced before numeric `minimum`
bounds are checked, so durations and safe-sample budgets like rows, bytes, and
timeouts cannot be strings or fall below the published safety contract. Common
`format` assertions are checked for `uri` and `date-time`, so Vault runtime
addresses and release/current timestamps fail early in local CI/IDE validation.

Resolver execution locations:

| Resolver | Location | Status |
| --- | --- | --- |
| `vault_kv` | runtime-side through `vault-kv-client` | primary production |
| `kubernetes_secret_volume` | pod projection, runtime reads files | production |
| `kubernetes_secret_api` | runtime calls Kubernetes API | restricted escape hatch |
| `airflow_connection` | operator-side bridge during task execution | compatibility |
| `env_var` | runtime-side | development/legacy only |

Vault paths are logical and never include `/v1/` or `/data/`. Registry bodies
are not serialized into DAG files or ordinary Airflow logs.

Legacy `connection_type` / `vault_path` authoring is not a second Airflow
self-service contract. Static checks report `DPONE_LEGACY_CONNECTION_CONFIG_FOUND`
for pipeline sources and `DPONE_LEGACY_CONNECTION_REGISTRY_ENTRY_FOUND` for
registry entries, including the affected entity and suggested migration target.
For pipeline sources, `dpone check --format json` also returns a read-only
`dpone.airflow-authoring-migration-plan.v1` with proposed `connection_ref`
replacements, redacted unified diffs, and executable safe/manual fix commands.
For registry entries,
`dpone check --connections --format json` returns a read-only
`dpone.connection-registry-migration-plan.v1` with proposed
`credentials.resolver` entries and unified diffs. These plans are intentionally
non-mutating: they never read secret values, never apply changes, and require
manual/platform review before authoring sources or registries are edited.
`dpone fix <pipeline> --plan` and `dpone fix <pipeline> --apply` are the
separate, explicit mutating facade for pipeline authoring sources. The command
reuses the same `dpone.airflow-authoring-migration-plan.v1`, emits a
schema-backed `dpone.airflow-authoring-fix.v1` report, and only replaces legacy
pipeline `source`/`sink` connection fields with `connection_ref`. It never edits
connection registries, binding-sets, credentials, runtime infrastructure,
release-sets, deployment-sets, or published cache artifacts. Secret-looking
legacy keys are removed from the source section and redacted from all plan/apply
output. Registry migration remains platform-owned and manual.
The user-visible migration is always toward `connection_ref` plus platform-owned
`connection-registry` entries with explicit `credentials.resolver`.
Default text output is self-service and action-oriented:

```text
dpone fix: PLAN
- target: pipelines/orders_daily/pipeline.yaml
- mode: plan
- legacy sections: 1
- applied sections: 0
- planned ref: processes[0].source -> mssql_dev
- plan_modify: processes[0].source
  --- before/pipelines/orders_daily/pipeline.yaml#processes[0].source
  +++ after/pipelines/orders_daily/pipeline.yaml#processes[0].source
  ...
- action: review the redacted diff, then run dpone fix pipelines/orders_daily --apply
- details: rerun with --format json for the full authoring migration plan
```

If the user invokes `dpone fix` with an absolute path inside the project, text
output still normalizes follow-up commands to the source-owned relative pipeline
target. This keeps support tickets and copy/paste commands free of workstation
or temporary-directory paths while JSON remains the precise automation contract.

After `--apply`, the same renderer reports `dpone fix: APPLIED`, the number of
applied sections, and the next `dpone check <pipeline>` command. JSON remains
the automation format for the full schema-backed migration plan.

Runtime-side foundation now includes:

- `BindingCredentialResolver`, which resolves `connection_ref` from a binding
  set and connection registry into the existing `CredentialsConfig` runtime
  object.
- A shared field-mapping contract for credential resolvers: every `fields`
  mapping must contain non-empty string keys and non-empty string values. This
  keeps connector-field aliases, Vault keys, environment variable names,
  projected file names, and Kubernetes Secret keys explicit and prevents silent
  partial credentials.
- Unsupported `credentials.resolver` names fail during `dpone check
  --connections` with `DPONE_CREDENTIAL_RESOLVER_UNSUPPORTED`, before any
  runtime pod can see the invalid registry.
- `binding-set`, `connection-registry`, and `credential-runtime` environment
  labels must match the requested `--environment`; mismatches fail during
  `dpone check --connections` before deployment projection or runtime
  execution.
- `credential-runtime` validation is secret-material aware and offline:
  `dpone check --connections` rejects Vault auth fields such as `password`,
  `token`, `jwt`, `secret_id`, `client_secret`, `private_key`, `access_key`,
  and `vault_token` with
  `DPONE_CREDENTIAL_RUNTIME_SECRET_MATERIAL_FORBIDDEN`.
  Runtime identity may reference workload identity settings like auth method and
  role, but static checks never accept inline Vault tokens, Kubernetes JWTs, or
  AppRole secret IDs and never print the forbidden values.
- `binding-set` performs late binding from pipeline logical refs to platform
  registry refs. For example, a pipeline can use `orders_source`, while the
  environment binding resolves it to `mssql_prod`; `dpone check --connections`
  validates the resolved registry refs and reports both logical and resolved
  refs. Default text output renders the configuration-only handshake, logical
  refs, resolved refs, bridge status, and next action as a short topology-free
  checklist. Failed connection checks use the same renderer: they keep the
  handshake, logical refs, resolved refs, bridge status, safe resolver detail,
  manual fixes, and a JSON hint visible without printing local registry paths,
  Vault paths, field mappings, or secret-looking values. JSON output preserves
  the full schema-backed `dpone.connection-check.v1` payload for CI and
  platform automation.
- Lazy Vault resolution through the existing `vault-kv-client` integration; the
  resolver module is safe to import without Vault installed.
- Explicit Vault rotation semantics: registry entries must declare `fields`,
  `version_policy`, and `resolution_scope`, so `latest` + `workload_start`
  behavior is visible in config, evidence, and operational reviews.
- `kubernetes_secret_volume` resolution, where Kubernetes/CSI/External Secrets
  projects secret keys as files and the runtime reads only relative field names
  inside `mount_path`. The resolver rejects absolute paths, `..` traversal, and
  symlink escape before mapping files into in-memory credentials. The public
  connection-registry JSON Schema and `dpone check --connections` also reject
  mount paths that leave `/run/secrets/dpone/` plus absolute or `..` field
  paths, so CI/IDE validation can catch unsafe projection mappings before
  runtime. The field mapping is required and must be non-empty, so a projected
  Secret that omits `username`, `password`, or another connector credential
  fails during `dpone check --connections` and again in the runtime plane instead
  of returning partial credentials.
- Restricted `kubernetes_secret_api` resolution through an injected runtime
  reader port. The resolver never imports a Kubernetes client by itself, maps
  declared non-empty fields into in-memory credentials, and fails closed unless
  the platform explicitly injects a reader with the required RBAC.
- `airflow_connection` compatibility validation. Registry entries must declare
  `execution_mode: operator_bridge`; `connection_id` must be a logical Airflow
  Connection id rather than an Airflow URI or inline credential payload. Runtime
  pods fail fast instead of importing Airflow or reading Airflow Connections
  directly. The bridge is an operator-side/deploy-time responsibility: Airflow
  resolves the Connection during task execution, writes the URI into a
  short-lived Kubernetes Secret, and mounts that Secret as files for the runtime
  pod. `dpone check --connections` emits a secret-free
  `airflow_connection_bridge` intent with required Airflow `connection_id`
  values, derived `AIRFLOW_CONN_*` key names, a
  `kubernetes_secret_volume` projection plan, and the platform bridge-plan
  command, so operators can prepare that bridge without parse-time Airflow,
  Vault, or secret access. Legacy GitOps `connection_bridge` artifacts use the
  same logical-id pattern for `required_connection_ids` and `env[].connection_id`;
  `env_name`, `secret_ref.key`, and compact-pack `secret_key` must be safe
  `AIRFLOW_CONN_*` keys. The bridge-plan command fails closed and redacts invalid
  URI-like ids or malformed Secret keys from its report instead of writing
  skeleton artifacts from them. The compact-pack provider repeats that validation
  at task execution and fails before reading Airflow Connections or publishing
  temporary Secrets when a corrupted projection reaches the operator. The
  default text renderer intentionally shows only whether the bridge is required,
  the logical pipeline refs it affects, and the bridge-plan command; it does not
  print Secret names, Secret keys, mount paths, `AIRFLOW_CONN_*` env names,
  Airflow connection URIs, Vault paths, or secret values. When binding aliases
  are used, the JSON report preserves both the pipeline
  `connection_ref` and the resolved `registry_connection_ref`.
  Runtime reads the mounted `uri` file through
  `payload_format: airflow_connection_uri` and the dependency-free Airflow URI
  parser; it never imports Airflow in the runtime image. The JSON report is
  schema-backed as `dpone.connection-check.v1`, so CLI users, CI, IDEs,
  platform automation, and future Studio flows can consume the same stable
  contract. The schema is also registered in the GitOps schema catalog as
  `connection-check`. Compact packs with
  `connection_projection.mode: kubernetes_secret_volume` and
  `payload_format: airflow_connection_uri` are loaded through
  `AirflowConnectionSecretVolumeKubernetesPodOperator` on both the legacy/local
  lane and the strict v2 `init_fetch` lane: the operator reads Airflow
  Connections only inside task execution, publishes an injected or
  Kubernetes-API-backed Secret through the `AirflowConnectionSecretProjector`
  port, patches only Secret references into the pod spec, and removes the
  Secret after synchronous execution by default. Strict init-fetch rejects
  `unsafe_airflow_env` and incomplete projections with
  `DPONE_INIT_FETCH_CONNECTION_BRIDGE_INVALID`. Published runtime registry
  snapshots rewrite source `airflow_connection` entries to the matching
  `kubernetes_secret_volume` mounts for in-pod `resolve()`. The configured
  `secret_name` is only a base prefix: task execution derives a bounded hash
  from Airflow's `dag_id/task_id/run_id/try_number/map_index` identity, creates
  one immutable Secret, and mounts and deletes that exact attempt-scoped
  object. Publication is create-only; an existing name fails closed instead of
  replacing another attempt's credentials. The pod keeps one stable logical
  volume slot and replaces only its physical `secretName`, preventing stale
  mounts when an operator object is reused. Identity validation precedes
  Connection reads, while vendor failures expose only stable codes, integer
  status, and digest-only references. Deferrable KPO execution must set
  `cleanup_policy: retain` and rely on platform-managed cleanup, because
  deleting the Secret in `execute()` would happen before the deferred pod has
  completed. No URI values are serialized in DAG files, KPO arguments, pod
  specs, XCom, evidence, or normal logs.
- `env_var` development/legacy resolution. Registry entries must declare
  `support: development_only` and explicit field-to-env mappings; each mapping
  value must be an environment variable name, not an inline assignment such as
  `PASSWORD=...`. Production environments reject this resolver. Runtime also
  enforces the support flag and env-name shape before reading environment
  variables, so unchecked registries cannot bypass the development-only
  contract or leak accidental assignment values in errors. Runtime fails with a
  named missing environment variable instead of returning partial credentials.
  New generated configs use the canonical
  `DPONE_CONN_<CONNECTION_REF>_<FIELD>` prefix;
  legacy `<CONNECTION_REF>_<FIELD>` and `DPONE_<CONNECTION_REF>_<FIELD>` names
  remain runtime fallbacks only for migration.
- Safe credential metadata for evidence: `connection_ref`, resolver type, and
  resolved version when the backend provides it. Kubernetes Secret resolvers add
  a `credential_ref_fingerprint` over the non-secret resolver reference instead
  of exposing `secret_name`, namespace, `mount_path`, or field filenames in
  evidence. Runtime callers can attach a safe deployment context: `release_id`,
  `deployment_id`, `pack_fingerprint`, `binding_set_fingerprint`,
  `connection_registry_fingerprint`, `credential_runtime_fingerprint`,
  `runtime_image_digest`, and `deployment_fingerprint`. Secret values, password
  hashes, Vault tokens, full Vault responses, Kubernetes Secret names,
  namespaces, and projected volume paths are never part of this metadata. The
  runtime validates `version_policy` and `resolution_scope` against the
  supported enum values before reading any credential backend. It also validates
  allowed evidence context values as well as keys: digest identity fields must
  be `sha256:` fingerprints, and text fields such as `airflow_bundle_ref` must
  be bounded single-line metadata without secret-like assignments.

## Attempt Secret retention GC

The GC composition root is activated only in a platform environment installed
with `dpone[kubernetes]`. The optional dependency is deliberately absent from
base and scheduler-side imports; missing support returns
`DPONE_AIRFLOW_SECRET_GC_SDK_UNAVAILABLE` with an installation fix before auth
or network I/O.

The Airflow Connection compatibility bridge creates one immutable Secret per
task attempt. Synchronous execution still deletes eagerly, but deferrable runs
use `cleanup_policy: retain` and a failed eager delete can leave an orphan.
v0.72.9 closes that lifecycle through a platform-only, plan-first GC. It adds no
pipeline authoring field and no beginner command.

```mermaid
flowchart LR
    OP["Provider task execution"] --> SEC["Attempt Secret: digest metadata"]
    OP --> POD["Consuming Pod: matching digest metadata"]
    CLI["GC plan/apply"] --> API["Metadata-only Kubernetes adapter"]
    API --> SEC
    API --> POD
    SEC --> POLICY["Pure classification policy"]
    POD --> POLICY
    POLICY --> PLAN["Protect / delete / quarantine plan"]
    PLAN --> DELETE["Confirmed bounded UID/RV delete"]
    DELETE --> REPORT["Schema-validated audit report"]
```

The adapter requests the fixed dpone label selectors and the exact
`PartialObjectMetadataList` media type. Full Secret/Pod fields, unsupported
metadata negotiation, expired or cyclic pagination, and malformed managed Pod
references fail closed. The application policy never sees Kubernetes SDK
objects: its port carries only name, UID, resourceVersion, creation timestamp,
labels, and annotations. Reports replace physical names with SHA-256 refs and
never contain Secret values, Pod specs, Airflow ids, raw API responses, or
vendor exception text.

Classification invariants are deliberately conservative:

1. every existing managed Pod protects the exact `(secret_ref, attempt_ref)`;
2. every Secret younger than the age floor is protected during create-before-Pod;
3. malformed Secret metadata quarantines only that Secret;
4. malformed managed Pod metadata blocks the whole plan;
5. expired unreferenced `retain` and `after_execute` objects become candidates;
6. apply validates actor/confirmation before client construction, re-plans,
   deletes a deterministic bounded prefix, and supplies UID plus
   resourceVersion preconditions;
7. `404` is an idempotent absent skip, while `409` is a truthful partial result
   requiring a fresh plan.

GC and provider imports perform no Airflow parse-time I/O. Kubernetes auth and
inventory happen only when a platform operator explicitly invokes the command.
Operational commands, RBAC, recovery, and rollout order are documented in the
[Airflow runner runbook](gitops-airflow-runner-pack.md#attempt-secret-retention-gc).

## Safe sample policy

`dpone run --sample ... --target temporary` evaluates policy before any source
or target access. The evaluator is deterministic and credential-free:

| Environment | Pushdown | Full scan | Read budget | Timeout |
| --- | --- | --- | ---: | ---: |
| `production` / `prod` | required | forbidden | 1 GiB | 60 s |
| `development` / `dev` | optional | allowed when bounded | 10 GiB | 300 s |

Unknown production source capability is treated as not proven and therefore
blocked. Policy evaluation itself remains credential-free and stops before
I/O. The enclosing command proceeds to source/target execution only when a
complete deployment-scoped authorization overlay passes every trust check and
the certified live ports are injected.

Source capability detection is credential-free. A recipe or platform-owned
profile can declare a pushdown candidate directly in the authoring source:

```yaml
source:
  type: mssql
  sampling:
    mode: pushdown
    proof: connector_capability
    estimated_read_bytes: 1048576
```

This declaration is sufficient for development planning, but never authorizes
a production read by itself. `mode: limit_only` is not considered production
proof, because `LIMIT 1000` does
not guarantee cheap reads. The same rule applies if a user writes
`mode: pushdown` but the `proof` is only a SQL `LIMIT` or `TOP` expression:
the detector keeps the metadata for diagnostics, but does not mark pushdown as
proven. `mode: full_scan` is allowed only where policy permits bounded full
scans. Bounded means `estimated_read_bytes` is present, positive, and fits the
environment budget; an unestimated or non-positive full scan is blocked with
`DPONE_SECURITY_SAMPLE_BUDGET_REQUIRED` even in development.

For the beginner golden path, the built-in
`mssql-to-clickhouse-incremental` recipe does not need inline sampling metadata:
the detector recognizes the built-in MSSQL -> ClickHouse `incremental_merge`
Airflow KPO planning candidate and emits:

```yaml
capabilities:
  mode: pushdown
  proof: route_certification:mssql_clickhouse_incremental_merge_airflow_kpo
  estimated_read_bytes: 1048576
```

This route certification reference is a credential-free static catalog entry.
It is a candidate for planning, not production authorization. The catalog is
deliberately narrow and route-level:

| Dimension | Value |
| --- | --- |
| Status | `experimental` |
| Source | `mssql` |
| Sink | `clickhouse` |
| Strategy | `incremental_merge` |
| Transport | `native_bcp_to_clickhouse` |
| Schema evolution | `widening` |
| Airflow/runtime mode | `kpo` |

Changing the route to `full_refresh`, another sink, or an unknown source
returns `unknown`. Even the matching static route remains blocked in
`production`: a trusted composition root must first verify content-addressed,
fresh, route- and environment-matched evidence, then inject the verified route
id into `SourceSamplingCapabilityDetector`. That internal trust decision is not
serialized by `SampleSourceCapabilities` and cannot be set in a manifest. The
default CLI therefore fails closed in production until the platform evidence
adapter is configured. User-authored `connector_capability` or invented
`route_certification_verified:*` strings cannot bypass this rule.

The same payload also includes a credential-free temporary target plan when the
pipeline source can be read. The plan records the original sink
`connection_ref`, a generated `dpone_tmp_<environment>` table name, TTL,
cleanup requirement, and `pii_policy: masked`. It does not create a table or
read any credential.

When both policy and temporary target planning are available, the beginner
`dpone run --sample ... --target temporary --format json` output also includes
`dpone.safe-sample-source-request.v1`. This lets a new user see the planned
read-only source mode, row budget, byte budget, timeout, temporary target, PII
policy, and pushdown proof before any runtime data movement exists.
Safe-sample CLI argument validation, source request, data-copier registry, and
certified copy request schemas are published in docs and registered in the
GitOps schema catalog, so CI, IDEs, and platform automation can validate the
planning/copy boundary before enabling live data movement.
Execution plan, runtime readiness, runtime execution, runtime evidence-write,
and runtime-run schemas are also catalog-registered. This keeps the safe-sample
handoff discoverable end-to-end: the beginner CLI, runtime pod, evidence writer,
and CI validators all resolve the same published contract names instead of
embedding private JSON Schema copies.
`dpone.safe-sample-airflow-deployment-context.v1` is a standalone catalog
schema as well as a nested execution-plan object. It records only local current
deployment projection metadata, pinned release/deployment ids, workload pack
references, Airflow Bundle delivery ref, artifact-delivery settings and
parse-side-effect guarantees.
The route-specific copy boundary is public as well:
`dpone.safe-sample-data-copy.v1`,
`dpone.mssql-clickhouse-safe-sample-copy-config.v1`,
`dpone.mssql-safe-sample-read-plan.v1`, and
`dpone.clickhouse-safe-sample-insert-plan.v1` are published and
catalog-registered so certified copiers can validate secret-free route payloads
without depending on service internals.

Safe sample planning now has an explicit execution-plan contract. A
preview-only current deployment still blocks safely:

```yaml
schema: dpone.safe-sample-execution-plan.v1
sample_rows: 1000
environment: development
runnable: false
policy_result:
  passed: true
  request: {}
  policy: {}
  capabilities: {}
  errors: []
artifact_pinning:
  release_id: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  deployment_id: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  pinned_workload_uri: cached://deployments/sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb/workloads/orders_daily
blockers:
  - schema: dpone.error.v1
    code: DPONE_DEPLOYMENT_NOT_RUNNABLE
    stage: safe_sample_execution_plan
    severity: error
    message: Current deployment is preview-only.
```

The execution-plan builder is parse-safe and local-only. It does not refresh
cache, read Airflow metadata, read Airflow Variables or Connections, import
Vault clients, call Kubernetes, query source systems, or create target tables.
When the dedicated safe-sample cache has only a preview deployment, the plan
intentionally blocks with `DPONE_DEPLOYMENT_NOT_RUNNABLE` while still preserving
pinned release/deployment identity for diagnostics. The beginner
`dpone run --sample` path upgrades only that isolated cache by materializing and
promoting a local runnable deployment with a workload pack before building the
execution plan. The scheduler preview/current projection remains byte-for-byte
unchanged. For runnable sample projections the plan carries the
`workload_packs` listed in the dedicated cache's `current/airflow-index.json`,
so runtime code can fetch exactly the pinned artifacts without resolving a
remote `current`. If the requested sample environment does not match that
deployment environment, the plan blocks with
`DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH`.

`SafeSampleRuntimeReadinessEvaluator` converts the execution plan into the
beginner-facing runtime readiness report:

```yaml
schema: dpone.safe-sample-runtime-readiness.v1
ready: true
available_contracts:
  - binding_resolver
  - cache_materializer
  - safe_sample_runtime_runner
  - safe_sample_runtime_evidence_writer
  - fail_closed_data_copier
  - mssql_clickhouse_bounded_copy_executor
  - credential_resolving_mssql_clickhouse_copy_executor
  - clickhouse_temporary_target_adapter
  - certified_source_data_copier
blockers: []
errors: []
```

The evaluator is also parse-safe and credential-free. It does not execute the
runtime runner; it only explains which contracts exist and which ports require
explicit live composition. A registered route-certified fail-closed copier
removes the `certified_source_data_copier` readiness blocker, but successful data
movement still requires the live executor/credential ports to be injected at the
runtime boundary.

Certified source copiers are resolved through
`SafeSampleDataCopierRegistry`, not by ad hoc runtime flags. The registry is
keyed by the existing route certification id such as
`mssql_clickhouse_incremental_merge_airflow_kpo`; unknown certification ids are
rejected before runtime. This keeps route certification, readiness reporting and
runtime DI aligned: a source copier can remove the
`certified_source_data_copier` blocker only when it is explicitly registered for
a route already proven by the sample policy. At runtime,
`RegistryBackedSafeSampleDataCopier` implements the existing
`SafeSampleDataCopier` protocol: it delegates to the registered certified copier
for the current plan or falls back to `FailClosedSafeSampleDataCopier` without
touching source systems.

The temporary target lifecycle foundation provides an adapter-based executor
with `prepare` and `cleanup` operations. It emits secret-free lifecycle evidence
and wraps backend failures in stable `DPONE_RUNTIME_TEMPORARY_TARGET_*` error
codes. Adapter metadata is recursively redacted at this boundary, including
tokenized keys such as `access_token`, `session-token`, `api_key`,
`aws_access_key_id`, `connection_string`, `lease_id` and nested
`password`/`vault_token` fields, before it can enter runtime reports or
evidence. The first concrete adapter is `ClickHouseTemporaryTargetAdapter`,
which creates `dpone_tmp_<environment>` and clones the original table structure
into a unique temporary sample table using injected SQL execution. Creation is
exclusive, adds a materialized expiry timestamp, and configures a table-level
ClickHouse TTL before the table can be used. A partial DDL failure triggers a
best-effort compensating `DROP`. Normal cleanup still drops the table after the
run; server-side TTL is the fallback for abandoned data and is asynchronous,
as documented by [ClickHouse TTL](https://clickhouse.com/docs/guides/developer/ttl).
`TemporaryTargetAdapterRegistry` selects backend adapters by `sink_type`, so
runtime orchestration depends on the generic lifecycle contract instead of
ClickHouse-specific classes. The default prelude remains no-I/O. The explicit
live composition root injects a credential-aware ClickHouse adapter only after
policy and immutable-input validation succeeds.

`SafeSampleRuntimeExecutor` is the runtime-side orchestration service. It
accepts an already validated `dpone.safe-sample-execution-plan.v1`, calls an
injected artifact fetcher, prepares the temporary target through the generic
lifecycle executor, always attempts cleanup after prepare, and then delegates to
an injected data-copy port. The generic fallback stops with
`DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED`; the built-in MSSQL -> ClickHouse
certified copier stops later, at
`DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED`, until live
reader/writer ports are explicitly injected. The live composition root now
injects those ports together with the matching physical target lifecycle;
non-live callers keep the same fail-closed defaults.
`SafeSampleInitFetchArtifactFetcher` is the
concrete artifact fetcher for this slice: it converts the deployment context
into a `build_init_fetch_plan` call and executes `InitFetchExecutor` against a
local/pod artifact registry mirror. It uses only pinned `cache://` workload
packs from the deployment index, never a `current` pointer. Runtime evidence is
`dpone.safe-sample-runtime-execution.v1` and preserves the two outcome axes from
the frozen plan. The same evidence now includes a secret-free
`deployment_identity` block with `release_id`, `deployment_id`,
`binding_set_ref`, `connection_registry_ref`, `credential_runtime_ref`,
`runtime_image_digest`, workload pack fingerprints, and runtime artifact
delivery mode. It records digest references and pack fingerprints only, never
registry bodies, binding values, Vault paths from platform registries, or secret
values. Source sampling/copy is now an explicit DI boundary:
`FailClosedSafeSampleDataCopier` is the default implementation and returns a
`dpone.safe-sample-data-copy.v1` block with a nested
`dpone.safe-sample-source-request.v1`. The nested request records the bounded
source intent: sample row budget, byte budget, timeout, read-only guarantee,
temporary target, PII policy, full-scan allowance, and pushdown proof. It never
contains credentials, Vault paths, tokens, or source query results. Future
certified copiers can replace only the live executor boundary through the
registry-backed wrapper without changing init-fetch, runner or temporary-target
lifecycle orchestration. Returned copier errors are sanitized by the runtime
executor before they are copied into top-level `dpone.error.v1` errors or
persisted as nested `data_copy.errors`, so password/token/Vault-like diagnostics
cannot leak through a certified copier implementation.

The first route-specific skeleton is
`MssqlClickHouseSafeSampleCopier` for
`mssql_clickhouse_incremental_merge_airflow_kpo`. It builds a
`dpone.safe-sample-certified-copy-request.v1` from non-secret source config,
the evaluated safe-sample policy and the temporary ClickHouse target. The class
does not know how to connect to MSSQL or ClickHouse by itself. Live work is
behind `MssqlClickHouseSafeSampleCopyExecutor`; the default executor is
fail-closed with `DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED`.
This lets platform engineers certify and inject a concrete copy executor later
without changing the runner, evidence writer or registry contracts.
`MssqlClickHouseBoundedSafeSampleCopyExecutor` is the first executable
implementation of that port. It still uses dependency injection for the actual
MSSQL reader and ClickHouse writer, but it now enforces the certified request's
read-only flag, row budget, byte budget and write-count invariants. It passes
sampled rows only in memory from the reader to the writer and returns
secret-free metrics (`rows_read`, `rows_written`, `bytes_read`, diagnostics and
structured errors) without serializing row values, PII, credentials, Vault
metadata or tokens into evidence.
`MssqlSafeSampleSqlReader` is the first concrete MSSQL reader adapter for this
port. It converts the certified copy request into
`dpone.mssql-safe-sample-read-plan.v1`, a secret-free SQL plan with a bounded
`SELECT TOP (@sample_rows)` statement, row/byte/time budgets, read-only intent
and non-secret source metadata. The reader receives an injected SQL client
instead of importing a database driver directly, so connector/runtime code can
decide how credentials become an in-memory connection. Client diagnostics are
redacted before evidence and the sampled rows remain only in the in-memory
batch handed to the ClickHouse writer.
`ClickHouseSafeSampleSqlWriter` is the matching concrete writer adapter. It
converts the certified copy request and in-memory batch into
`dpone.clickhouse-safe-sample-insert-plan.v1`, a secret-free
`INSERT INTO ... (<quoted columns>) VALUES` plan for the prepared temporary
table. The serializable plan contains row count and non-secret target metadata
only; row values are passed to
the injected ClickHouse client in memory and are never emitted in evidence.
The native client acknowledgement is fail-closed: it must be an integer equal
to the planned row count. A missing, ambiguous, boolean, or mismatched result
fails the copy and still enters target cleanup.
Client diagnostics are redacted before returning metrics to the bounded copy
executor.
`CredentialResolvingMssqlClickHouseSafeSampleCopyExecutor` is the runtime-side
handoff from logical `connection_ref` to those concrete ports. It accepts a
structural credential resolver such as `BindingCredentialResolver`, resolves the
source and sink refs at workload execution time, builds the injected reader and
writer with resolved in-memory credentials, and delegates to the bounded
executor. Its evidence stores only resolver-safe metadata (`connection_ref`,
resolver type, resolved version and deployment fingerprints) and redacts any
secret-like key before returning diagnostics.
`build_mssql_clickhouse_safe_sample_registry_from_pipeline_source` is the
assembly helper for platform/runtime code. Given the primary pipeline source,
runtime credential resolver, and concrete MSSQL reader / ClickHouse writer
factories, it returns a `SafeSampleDataCopierRegistry` entry for the certified
route. The helper stays service-layer clean by depending only on structural
protocols; the runtime pod decides which Vault-backed resolver and connector
ports to inject.
`build_mssql_clickhouse_safe_sample_sql_registry_from_pipeline_source` is the
convenience assembly for the SQL-port path. Runtime code supplies a
credential resolver plus MSSQL and ClickHouse SQL-client factories; the helper
wraps them in `MssqlSafeSampleSqlReaderPortFactory` and
`ClickHouseSafeSampleSqlWriterPortFactory`, then builds the same certified
registry entry. This keeps driver imports, connection pools and credential
objects outside the service layer while giving the runtime pod a single wiring
point for the concrete reader/writer ports.
The runtime plane provides the first concrete SQL-client adapters in
`dpone.runtime.safe_sample_sql_clients`. `MssqlDbApiSafeSampleSqlClient` executes
the secret-free MSSQL read plan through a DB-API cursor and converts rows to
dicts; `ClickHouseConnectionSafeSampleSqlClient` sends the in-memory batch to a
ClickHouse-driver-like `execute` call. The corresponding credentials factories
create connector-backed clients from already resolved `CredentialsConfig`
objects without opening the network connection during factory creation.
`MssqlClickHouseSafeSampleCopyConfigBuilder` derives the copier's non-secret
source binding from the pipeline source (`processes[].source`) and rejects
non-MSSQL -> ClickHouse incremental routes with structured `dpone.error.v1`
metadata. The same builder can later be fed by canonical manifest or pack
metadata without changing the copier protocol.
The beginner `dpone run --sample ... --target temporary --format json` path now
uses that builder to expose a `certified_copy_request` preview. This is still a
diagnostic, secret-free plan: it helps the user see the exact source table,
temporary sink, row budget, byte budget and proof before any source IO is
allowed.

```yaml
schema: dpone.safe-sample-runtime-execution.v1
execution_mode: local_handoff
execution_status: failed
data_outcome: unknown
release_id: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
deployment_id: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
deployment_identity:
  release_id: sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
  deployment_id: sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb
  binding_set_ref: sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc
  connection_registry_ref: sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd
  credential_runtime_ref: sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
  runtime_image_digest: sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff
  workload_packs:
    - id: load_orders
      artifact_ref: cache://releases/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/packs/load_orders.airflow-pack.json
      sha256: sha256:1111111111111111111111111111111111111111111111111111111111111111
      bytes: 8192
  runtime_artifact_delivery:
    mode: local_preview
data_copy: null
errors:
  - schema: dpone.error.v1
    code: DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED
    stage: safe_sample_data_copy
    severity: error
    message: Certified route executor is not configured.
```

The beginner CLI now invokes one fail-closed selection boundary. `dpone run
--sample ... --target temporary --format json` emits a
`dpone.safe-sample-runtime-run.v1` report, emits
`dpone.safe-sample-runtime-handoff.v1`, writes
`safe-sample-execution-plan.json`, and writes
`safe-sample-runtime-execution.json` under `.dpone-cache/safe-sample-runs/...`.
The handoff object contains a copy-paste `command` for rerunning the pinned
runtime prelude and a `live_copy_command` that adds the explicit runtime-only
`--enable-live-copy` inputs for platform diagnostics. It is credential-free: it
stores file references and command strings, not resolved secrets or source
query results. A complete deployment-scoped authorization overlay lets the
same beginner command select the existing live assembly automatically. Text and
Markdown renderers show the actual `live_copy`, `local_handoff`, or `blocked`
mode. Local mode shows one platform-preparation/retry action and never exposes
the platform command to the beginner; JSON retains the full diagnostic handoff.
The machine field `authorization_overlay_profile: deployment_scoped_v1`
describes only discovery of the platform-owned cache layout. It is not the
signed route-attestation `authorization_profile`; that value remains
`safe_sample_production` and is verified by the existing trust policy.
For the development beginner path, the CLI first promotes a local runnable
deployment inside `.dpone-cache/safe-sample-deployments`; it never replaces
the scheduler-visible preview/current projection. The handoff uses the
safe-sample compatibility `InitFetchExecutor` and writes fetched workload packs
plus `init-fetch-manifest.json` under the run's `runtime-artifacts` directory.
This is not the strict-v2 KPO `RuntimeInitFetchExecutor`, does not publish
`runtime-fetch-ready.json`, and is not evidence that production pod delivery or
attestation has passed.
It does not fetch remote `current` pointers. Without a complete overlay it does
not create/drop physical temporary tables, read sources, or copy rows. With a
complete overlay, the already-verified live ports perform those actions through
the shared runner and preserve the safe-sample pinned artifact/evidence
contracts.
If temporary-target cleanup fails after a successful copy, the runtime report
keeps `data_outcome: passed` for the copied sample but sets
`execution_status: failed` and promotes the cleanup error to top-level
`errors`, making leftover temporary objects visible to the operator.
Certified copiers may also return `status: copied_with_quarantine` when a
bounded sample was copied and some records were safely quarantined. The runtime
keeps `execution_status: succeeded` when no runtime errors are present and maps
that copy status to `data_outcome: passed_with_quarantine`, preserving the two
independent outcome axes from the frozen architecture.
When the certified copier completes safely but reads zero rows, the runtime
keeps `execution_status: succeeded` and reports `data_outcome: no_data` instead
of a clean `passed`, so first-time users can distinguish an empty source sample
from a successful non-empty smoke run.
When a certified copier returns a structured
`DPONE_SAFE_SAMPLE_QUALITY_GATE_FAILED` error, the runtime keeps the execution
failure visible and maps the data axis to `data_outcome: failed_quality_gate`.
This prevents user-facing diagnostics from collapsing data-quality failures into
generic runtime `unknown`.

The evidence-writer foundation is now available as
`SafeSampleRuntimeEvidenceWriter`. It writes
`dpone.safe-sample-runtime-execution.v1` to
`safe-sample-runtime-execution.json` atomically with create-only semantics,
drops secret-like mapping keys, calculates `sha256` and byte size, and returns
`dpone.safe-sample-runtime-evidence-write.v1`. The same stable artifact boundary
is used by fail-closed rehearsal and explicit live source reads. A repeated
explicit run identity fails instead of replacing earlier evidence. When
`--run-id` is omitted, the CLI generates one sortable timestamp-plus-random
identity and uses it consistently for the temporary table, handoff, init-fetch
artifacts, JSON output, and evidence directory.
The in-memory runtime report returned to the CLI uses the same recursive
secret-like key policy, so `access_token`, `vault_token`, and similar fields do
not leak through stdout before evidence persistence runs. It also redacts inline
`password=...`, `token=...`, `api_key=...`, `aws_access_key_id=...`, and
`connection_string=...` assignments in nested diagnostic strings and lists, so
certified copiers cannot accidentally leak secrets through free-form adapter
diagnostics.
Runtime adapter and copier exception messages are also bounded and redacted
before they become `dpone.error.v1`, so password/token/Vault-like text cannot
leak through runtime errors.
Evidence writer failures follow the same rule before they are surfaced in the
top-level `dpone.safe-sample-runtime-run.v1` report.

### Safe-sample redaction and recovery

Redaction is a producer and persistence-boundary contract:

- Mapping keys are normalized case-insensitively across `.`, `-`, spaces, and
  underscores. Keys containing `password`, `passwd`, `pwd`, `secret`, `token`,
  `jwt`, or `vault`, exact keys such as `api_key`, `access_key`,
  `client_secret`, `connection_string`, `private_key`, `secret_key`, and
  `lease_id`, plus keys ending in `_lease_id`, are removed recursively.
- Free-form strings redact complete URI userinfo
  (`scheme://user:password@host` becomes
  `scheme://[REDACTED]@host`), `Authorization: Bearer ...` and bare
  `Bearer ...` tokens, private-key blocks, secret-bearing CLI flags, and
  secret-key assignments. Safe context such as the URI scheme/host, stable
  error code, stage, and recovery action remains visible when possible.
- The in-memory runtime result is sanitized before CLI rendering. The evidence
  writer repeats recursive sanitization immediately before its atomic,
  create-only write, so a caller-provided mapping cannot bypass the runtime
  boundary. Sample rows and resolved credential values are never evidence.

`[REDACTED]` is not a request to print the value elsewhere. Diagnose with the
stable code, non-secret host/context, and protected runtime-adapter logs. If an
unredacted credential reaches stdout, logs, or
`safe-sample-runtime-execution.json`, stop sharing the artifact, restrict access,
rotate or revoke the credential, and preserve or quarantine the original under
the platform incident-retention policy. Fix the producer/redaction boundary and
rerun with a new run id. Never hand-edit evidence, overwrite the original run
id, or weaken redaction to recover diagnostic detail.

`SafeSampleRuntimeRunner` composes the runtime executor and evidence writer. It
returns `dpone.safe-sample-runtime-run.v1`, including the runtime execution
payload, evidence write metadata, release/deployment ids, outcome axes, and any
structured errors. It is also the handoff point used by the explicit certified
live-copy composition; object-store/init-container delivery remains injected
through the existing artifact-fetcher port.

The runtime-pod entrypoint is:

```bash
dpone ops safe-sample-runtime-run \
  --plan-json /run/dpone/safe-sample-plan.json \
  --pipeline-source /run/dpone/pipeline.yaml \
  --cache-root /run/dpone/cache \
  --output-dir /run/dpone/evidence \
  --format json
```

It rehydrates the secret-free execution plan, executes the same DI-based
runtime runner used by local readiness tests, and returns a machine-readable
`dpone.safe-sample-runtime-run.v1`. In Phase 1B the default data-copy port still
blocks with `DPONE_SAFE_SAMPLE_DATA_COPY_NOT_IMPLEMENTED`; certified copier
implementations can replace that port without changing the CLI contract. For the
built-in MSSQL -> ClickHouse route, `--pipeline-source` selects the certified
copier and records `dpone.safe-sample-certified-copy-request.v1`; without live
reader/writer ports it fails closed with
`DPONE_SAFE_SAMPLE_CERTIFIED_COPY_EXECUTOR_NOT_CONFIGURED`.

Platform runtime live-copy wiring remains an explicit operator opt-in:

```bash
dpone ops safe-sample-runtime-run \
  --plan-json /run/dpone/safe-sample-plan.json \
  --pipeline-source /run/dpone/pipeline.yaml \
  --enable-live-copy \
  --binding-set /run/dpone/binding-set.json \
  --connection-registry /run/dpone/connection-registry.json \
  --credential-runtime /run/dpone/credential-runtime.json \
  --route-attestation /run/dpone/route-attestation.json \
  --route-attestation-bundle /run/dpone/route-attestation.sigstore.json \
  --route-certification-bundle /run/dpone/route-certification-bundle.json \
  --route-attestation-policy /run/dpone/route-attestation-policy.json \
  --cache-root /run/dpone/cache \
  --output-dir /run/dpone/evidence \
  --format json
```

This path reads the selected pipeline once, verifies its exact byte digest
against the `kind: manifest` dependency in the indexed immutable workload pack,
and derives the expected certified route from that source. It verifies a
content-addressed external Sigstore/cosign route attestation against the exact
route-certification bundle, release, deployment, environment, runtime image,
authorization profile, signer identity, issuer, bounded validity, revocation
snapshot, and locally pinned trusted root. `invalid` and `unverified` are both
terminal and use security exit code `4`; malformed platform inputs use exit
code `2`. There is no fallback to a route ID in CLI context or authoring data.

After trust verification, the composition root recomputes the environment
policy and compares canonical fingerprints of the binding-set,
connection-registry, and credential-runtime files with the pinned deployment.
Any mismatch exits before `init_fetch`, credential resolution, or database I/O.
Only after all local validation does the readiness composition root assemble
one `WorkloadScopedCredentialResolver`, the concrete MSSQL/ClickHouse SQL-client
factories, and the credential-aware ClickHouse target adapter. Runtime then
executes pinned `init_fetch`, physical target create/TTL, bounded copy, and drop
in that order. For the explicit platform command, it is intentionally not
enabled by default: without `--enable-live-copy`, that command never resolves
credentials and never touches source or sink systems. The beginner facade does
not expose this flag; its complete signed overlay selects the same assembly.

Invalid runtime inputs, such as a missing binding-set or malformed connection
registry, fail before runtime execution with structured `dpone.error.v1`
(`DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID`) and exit code `2`. The CLI emits a
bounded, redacted message instead of a Python traceback.
Changed immutable inputs use dedicated
`DPONE_SAFE_SAMPLE_*_FINGERPRINT_MISMATCH` codes and instruct the operator to
rebuild/promote the deployment instead of bypassing identity checks. A
serialized production `passed: true` value and a route ID are not
authorization. Production execution requires a `verified`
`dpone.route-attestation-verification.v1` decision derived from exact signed
bytes and actual pinned runtime identities. Safe verification metadata is added
to `dpone.safe-sample-runtime-execution.v1`; bundles, certificates, trusted-root
bytes, infrastructure paths, and verifier subprocess output are never emitted.
Compact pack v3 records the primary authoring manifest in
`workload_dependencies`; old packs without that pin remain parse-compatible but
cannot enter explicit live sample execution until rebuilt.
The live composition root also requires an explicit project source root. It
never infers authoring identity from the runtime cache path; missing roots,
path traversal, symlink escape, or byte-digest drift block before credentials
or database I/O.

When live copy is enabled, runtime evidence records only safe credential
resolution metadata under `data_copy.diagnostics.credential_resolution`: the
source/sink `connection_ref`, resolver type, resolved safe version metadata
(`resolved_version` and `resolved_at` when the backend returns a version),
`version_policy`, `resolution_scope`, Kubernetes resolver
`credential_ref_fingerprint` when a workload uses a projected/API Secret,
safe credential-runtime context (`credential_runtime_environment`,
`credential_runtime_auth_method`), and digest-only deployment identity
(`release_id`, `deployment_id`,
`binding_set_fingerprint`, `connection_registry_fingerprint`,
`credential_runtime_fingerprint`, `runtime_image_digest`, and
`airflow_bundle_ref`). It does not serialize secret values, Vault paths, Vault
addresses, Vault roles, Kubernetes Secret names, namespaces, projected volume
paths, registry bodies, sampled rows, or driver connection objects.
The same sink resolution is reused for target prepare, row write, and cleanup
for the duration of one workload. A later workload resolves again and may
observe a rotated secret version.

## Runtime artifact delivery

`init_fetch` planning is represented by a digest-only runtime contract. It
requires pinned `release_id`, pinned `deployment_id`, a non-`current` artifact
registry reference, checksum verification, and Kubernetes workload identity.
Workload artifact references are validated at plan time: they must use
`cache://`, stay pinned to release content, and must not contain any `current`
path segment before the handoff is serialized.
The runtime handoff preserves and validates the published verification policy:
`verify.checksums` must be `required`, and `verify.attestations` must be either
`optional` or `required_for_prod`. Invalid verification values fail before any
artifact fetch starts, matching the public deployment and safe-sample JSON
Schemas instead of silently rewriting a malformed projection.

The runtime foundation includes a local artifact registry mirror,
`InitFetchExecutor`, and the connector-neutral `ArtifactRegistry` port. The
S3, GCS, Azure Blob, and local adapters implement exact stat/download and
conditional create-if-absent operations without entering Airflow parse code.
`dpone airflow publish` validates a complete local projection, publishes the
release marker last, then publishes the deployment marker last. An identical
retry is a no-op; different bytes at an existing content-addressed key are an
immutability conflict and are never overwritten. Before the first registry
operation, every validated local file is copied through a no-follow descriptor
into a private snapshot and rechecked for size and SHA-256. A concurrent edit
to the build cache therefore cannot change the bytes uploaded under an already
validated identity.

`dpone airflow cache-materialize` accepts exact release and deployment IDs,
checks both remote markers, and downloads a deterministic inventory into a
private staging cache. It never lists a registry or resolves remote `current`
or `latest`. Metadata size is a preflight only: downloaded bytes are checked
again and SHA-256 is computed locally before the existing projection validator
runs. Negative or non-integer backend sizes fail with
`DPONE_ARTIFACT_REGISTRY_METADATA_INVALID` before download. Verified release
and deployment trees are installed with local
create-or-compare semantics, but activation remains a separate `cache-sync`
operation.

The materializer composition root accepts only platform workload identity or a
local proof registry. It does not expose Airflow/Vault logical connection
options. Publish-time CI may use a bounded logical connection name when
workload identity is unavailable, but secret-shaped values and physical paths
are rejected. Azure workload identity requires the account-scoped
`azure://<account>/<container>/<root>` form and constructs
`WorkloadIdentityCredential` only in this composition root. The local proof
backend pins one no-follow root device/inode for the client lifetime and
opens every lexical root component from the filesystem anchor without
following symlinks. It anchors every descendant operation to descriptors, so a
concurrent root-path replacement during or between calls cannot redirect or
split reads, writes, inventory, deletion, and completion markers.
The immutable cache installer receives that same pinned descriptor; it does not
reopen the cache pathname after a guard check. Publisher snapshots are created
in a private operating-system temporary directory outside the mutable cache
pathname.

```mermaid
flowchart LR
    Build["Verified local release/deployment"] --> Publish["AirflowArtifactPublisher"]
    Publish --> Port["ArtifactRegistry port"]
    Port --> Store["S3 / GCS / Azure / local"]
    Store --> Authority{"Exactly one trust authority"}
    Authority --> ReleaseProof["GitHub/SLSA release proof"]
    Authority --> DeploymentProof["Cosign deployment overlay"]
    ReleaseProof --> Fetch["AirflowArtifactMaterializer"]
    DeploymentProof --> Fetch
    Fetch --> Stage["Private bounded staging cache"]
    Stage --> Validate["DeploymentCacheProjectionValidator"]
    Validate --> Local["Immutable local release/deployment"]
    Local --> Sync["Reviewed cache-sync"]
    Sync --> Current["current"]
    Chart["Official Airflow Helm chart"] --> PostRender["Fail-closed ACK post-renderer"]
    PostRender --> Parser["One parser: ACK read-write"]
    PostRender --> Readers["Watcher/projector: ACK read-only"]
    Current --> Parser
    Parser --> Ack["Separate loader ACK volume"]
    Ack --> Readers
    Ack --> Retention["Plan-bound retention authority"]
```

The provider remains local and parse-safe. Its lower-level
`load_dpone_dags()` API is parse-only; the production
`load_and_acknowledge_dpone_dags()` API binds that local parse to the separate
loader ACK used by retention. Neither path constructs the registry adapter nor
imports cloud SDKs, credentials providers, Vault clients, or the materializer.
Runtime strict-v2 init fetch reads only pinned artifacts,
verifies `deployment-set.v2` as the runtime deployment receipt, independently
verifies the release wire allowed for the lane (`release-set.v1` only for
legacy/non-dbt or non-production compatibility transport; `release-set.v2` for
authoritative production dbt), verifies the selected pack, and publishes
`runtime-fetch-ready.json` last. The base launcher validates that ready state and
never resolves `current` or reads init-only registry/trust ConfigMaps.

Cache materialization is a separate filesystem component. It validates
`deployment.json`, `airflow-index.json`, and `_SUCCESS`, copies the accepted
projection to a sealed
`activations/<environment>/<deployment_id>` snapshot, revalidates the copied
bytes and pinned release tree, compares the complete source/staging tree, and
only then selects that snapshot as
`.dpone-cache/current`. Incomplete
deployments fail with `DPONE_DEPLOYMENT_INCOMPLETE` and leave the previous
current pointer untouched. Corrupt deployment projections fail with
`DPONE_DEPLOYMENT_INVALID` or `DPONE_AIRFLOW_INDEX_INVALID` and are returned as
structured `dpone.error.v1` payloads by `cache-sync`, not Python tracebacks.
The same projection-specific errors are used when those files contain valid
JSON that is not an object.
Schema validation failures also carry the offending projection file path.
Deployment directories outside the configured cache
root fail with `DPONE_DEPLOYMENT_PATH_OUTSIDE_CACHE_ROOT`, so `current` cannot
become a link to arbitrary local filesystem content. Directories inside the
cache root must still use the canonical projection layout
`deployments/<environment>/<deployment_id>`; non-canonical paths fail with
`DPONE_DEPLOYMENT_PATH_INVALID`. Deployment environment mismatches fail with
`DPONE_DEPLOYMENT_ENVIRONMENT_MISMATCH`, so a prod projection cannot be promoted
as dev or the other way around. The materializer also verifies that
`deployment.release_ref` matches `airflow-index.release_id`; mismatches fail
with `DPONE_RELEASE_ID_MISMATCH` before any pointer switch. Identity mismatch
errors include the projection file that must be fixed, such as
`airflow-index.json` for `deployment_id` mismatches. Missing release identity
is reported as `DPONE_RELEASE_NOT_FOUND` against `deployment.json`.
Promotion also loads the pinned
`releases/<release_id>/release-set.json`, confines that control file to the
pinned release directory, and recomputes its canonical content fingerprint.
The declared identity must be a SHA-256 digest and must equal both the requested
release and the recomputed fingerprint. The `dag_specs` and `workload_packs` sets in
`airflow-index.json` must match the immutable release-set exactly by logical id,
path, and SHA-256. Every listed artifact is resolved inside that release
directory, checked against optional index byte metadata, bounded by the
configured per-artifact size limit (64 MiB by default), and checksum-verified
before `current`, `current-pointer.json`, or the promotion audit log can change.
Artifact and projection files are opened by walking no-follow directory
descriptors from the configured cache root; an intermediate symlink swap cannot
redirect a verified path. An existing activation is immutable and is reused
only when its complete regular-file tree exactly matches the new candidate.
Missing, unreadable, oversized, unpinned, traversal, release/index mismatch,
fingerprint mismatch, and checksum failures are returned through stable
`DPONE_CACHE_*` or `DPONE_RELEASE_*` errors. A failed
text-mode promotion shows the issue and a topology-free restore/rebuild action;
JSON keeps the exact artifact or projection path for platform automation.

```text
complete deployment projection
  -> validate layout, environment, and identities
  -> resolve the pinned release-set inside cache root
  -> recompute the canonical release fingerprint
  -> match DAG/pack entries and verify size + SHA-256
       | failure: structured error; no validation-time mutation
       ` success
  -> copy candidate to a content-addressed activation snapshot
  -> seal activation and release trees; compare and revalidate copied bytes
       | unreadable subtree/non-regular entry: fail closed
  -> prepare a unique relative current symlink to the activation
  -> durably replace current-pointer.json
  -> append and durably authorize the promotion audit event
  -> atomically replace current as the final activation point
```

Write failures are deliberately distinguishable:

| Failed step | Possible local state | Required action |
| --- | --- | --- |
| Prepare `current` symlink | No pointer, audit, or active state changed, but the sealed activation or release permissions may already exist. | Repair local symlink capability/permissions and retry; `state_may_have_changed` is true for cache artifacts, while control-state recovery is not required. |
| Prepare pointer JSON | Active `current` remains unchanged, but the pointer may already name the candidate when directory durability fails after rename. | Run recovery plan; do not infer no mutation from the failed command. |
| Append audit event | Active `current` remains unchanged while pointer may be ahead and audit missing or older. | Run recovery plan; confirmed apply reconciles only a freshly revalidated candidate. |
| Activate `current` | Pointer/audit may name the candidate while active `current` still names the previous deployment. | Run recovery plan and explicitly select the reviewed repair candidate. |

The three write steps are ordered but are not one filesystem transaction.
`current` must be a canonical relative symlink and is activated only through an
atomic local rename. Copying occurs before activation into an isolated sealed
snapshot; there is no non-atomic copied-`current` fallback. Any pointer, audit, or final
activation durability failure returns
`DPONE_CACHE_PROMOTION_WRITE_FAILED` and requires `cache-recovery-plan` before
retry. The stronger no-mutation guarantee applies to validation and integrity
errors, all of which finish before the first write step. Unreadable tree
entries and non-regular files fail closed; FIFO opens are nonblocking so a
malformed candidate cannot hold the global transaction lock indefinitely.
Recovery planning also compares the latest valid environment audit event with
`current-pointer.json`. A missing or mismatched event is reported as
`DPONE_CURRENT_POINTER_AUDIT_MISSING` or
`DPONE_CURRENT_POINTER_AUDIT_MISMATCH`; an explicit recovery apply re-promotes
the selected complete projection and appends a consistent event. Historical
malformed lines remain visible as `DPONE_CURRENT_POINTER_AUDIT_INVALID_LINE`
warnings rather than being deleted.
Release, deployment, pointer, and retention protection identities are canonical
lowercase SHA-256 strings. Every current consumer also requires publisher and
offset-aware promotion time, so an unauthorized pointer cannot become verified
merely because matching fields are absent from both pointer and audit.
The pointer release must also equal the fully validated active projection.
Otherwise recovery performs a full reviewed promotion instead of legitimizing
the stale pointer through audit-only repair. If `current` exists but its target
has no canonical physical deployment identity, the plan is `blocked`; a null
CAS guard is reserved for a physically absent `current` path.
Successful promotion records `promoted_by`, `promoted_at`,
`previous_deployment_id`, optional
provenance, and a JSONL audit event. The
`expected_current_deployment_id` guard protects updates, while
`expect_current_absent` protects first promotion. CAS
mismatches are reported against `current-pointer.json`, not the candidate
deployment directory. Platform mutation commands require an exact-match
promoter allowlist; an unknown `promoted_by` identity fails with
`DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED` before the `current` link or
`current-pointer.json` is changed. The CLI value is a policy assertion, not an
authentication mechanism: filesystem permissions, workload identity, and the
CI/deployment boundary authenticate the caller and control who can update the
cache. A caller able to choose both CLI values without that external boundary
must not have cache write access.

Local self-service promotion uses the same mutation facade without adding
platform options to the beginner commands. Preview and safe-sample each own one
fixed `local://dpone-*` actor and pass exactly that actor as the one-entry
allowlist. Before delegation they read the active physical deployment identity:
an absent current selects expected-absent CAS, while an existing current selects
expected-current CAS. The materializer verifies that observation again under
the cache transaction lock. It never retries a stale CAS automatically, so a
concurrent winner cannot be overwritten by a local facade that did not observe
it. Empty policy configuration fails before activation snapshot, pointer,
audit, or current mutation.

Platform cache sync promotes one already materialized complete deployment. It
does not fetch remote artifacts, parse authoring YAML, resolve secrets, or call
Airflow metadata APIs. The pinned release-set and all indexed DAG specs and
workload packs must already exist below the same cache root. Promotion,
recovery apply, and retention apply use one inter-process cache transaction
lock; deployment/release fingerprints,
deployment-index mirror fields, artifact checksums, and CAS are verified under
that lock. Pointer and audit authorization are durable before the relative
`current` symlink is switched as the final activation point. See the
[cache sync and recovery runbook](airflow-cache-sync.md) for preparation,
migration, and failure recovery:

```bash
dpone airflow cache-sync \
  --environment prod \
  --deployment-dir .dpone-cache/deployments/prod/sha256-... \
  --promoted-by ci://github-actions/dpone-airflow \
  --allowed-promoter ci://github-actions/dpone-airflow \
  --expected-current-deployment-id sha256:... \
  --confirm-promote
```

Default cache-sync text output confirms the current-pointer promotion without
dumping local cache paths. JSON remains the automation format for
`current_path`, `pointer_path`, and full audit metadata:

```text
dpone airflow cache sync: OK
- environment: prod
- promoted deployment: sha256:...
- previous deployment: sha256:...
- release: sha256:...
- promoted by: ci://github-actions/dpone-airflow
- source commit: 7ac31f2
- attestation: attestation://dpone/cache-sync/1
- current pointer: updated
- action: run dpone airflow cache-recovery-plan to verify cache health
- details: rerun with --format json for current/pointer paths and audit metadata
```

The basic retention foundation is intentionally plan-first. It scans local
deployment projections for an environment, protects the current deployment,
protects deployment ids supplied from active-run or reproducible-evidence
policy, marks complete unreferenced deployments as delete candidates, and
quarantines incomplete or invalid projections. Deletion is a later platform
operation, not an implicit side effect of planning. Platform engineers can
inspect this report with:

Malformed cache identities are not copied into digest-typed report fields.
Recovery emits `null` plus an issue; retention emits `null` on a quarantine
item. This keeps damaged-cache diagnostics schema-valid and non-actionable by
delete/apply code.

```bash
dpone airflow cache-retention-plan --environment dev --format json
```

When another control-plane component has already identified protected evidence,
it can pass those deployments explicitly:

```bash
dpone airflow cache-retention-plan \
  --environment prod \
  --protect-deployment-id sha256:...
```

The same protection can be derived from local evidence JSON files without
querying Airflow metadata DBs or remote evidence stores:

```bash
dpone airflow cache-retention-plan \
  --environment prod \
  --evidence-file evidence/run-evidence.json
```

An explicitly supplied evidence file must contain at least one valid
`deployment_id`. If it does not, retention fails closed with
`DPONE_DEPLOYMENT_EVIDENCE_DEPLOYMENT_ID_NOT_FOUND` instead of treating the file
as an empty protection set. If any `deployment_id` occurrence is malformed, the
entire evidence file is rejected even when it also contains a valid ID.

Production GC is a separate confirmed action. It builds the same fresh plan,
requires `--confirm-delete`, an explicit `--promoted-by`, and at least one
exact-match `--allowed-promoter`. It deletes only complete `unreferenced` candidates,
and skips current, protected, incomplete, or invalid deployments. Immediately
before each delete, the applier verifies both pointer and physical `current`;
split state fails with `DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED`. If the candidate
became current after planning, GC fails closed with
`DPONE_DEPLOYMENT_CACHE_GC_CURRENT_CHANGED` and leaves the deployment intact:

All delete candidates are fully revalidated before the first removal. If a
later filesystem operation fails, the structured result names deployments
already deleted, the failed deployment, and the failed step. This is truthful
partial-mutation evidence, not an atomic multi-directory claim.

```bash
dpone airflow cache-retention-apply \
  --environment prod \
  --promoted-by ci://github-actions/dpone-airflow-retention \
  --allowed-promoter ci://github-actions/dpone-airflow-retention \
  --expected-plan-sha256 "${DPONE_REVIEWED_RETENTION_PLAN_SHA256:?copy the reviewed plan_sha256}" \
  --review-id "${DPONE_CACHE_RETENTION_REVIEW_ID:?set the approved UUIDv4 attempt id}" \
  --loader-ack-file "${DPONE_LOADER_ACK_FILE:?set the strict loader ACK path}" \
  --evidence-file evidence/run-evidence.json \
  --confirm-delete
```

Default retention text output is designed for copy/paste operator work. The
plan view shows the current deployment, delete candidates, protected entries,
and a safe confirmed apply command. Evidence-file references are normalized to
their effective deployment ids, and every id is carried into the action as a
`--protect-deployment-id` flag, so apply recomputes the same protected set
without exposing evidence-file paths. Deployment projection paths stay
in JSON only. When the command was invoked with a non-default absolute
`--cache-root`, text output uses a shell-safe
`${DPONE_CACHE_ROOT:?set DPONE_CACHE_ROOT}` guard instead of printing
workstation or pod-local paths; rerun with `--format json` for the exact
automation value:

```text
dpone airflow cache retention: NEEDS_CLEANUP
- environment: prod
- current deployment: sha256:...
- delete candidates: 1
- delete candidate: sha256:... (unreferenced)
- protected: sha256:... (current)
- action: dpone airflow cache-retention-apply --environment prod --promoted-by "${DPONE_RETENTION_ACTOR:?set DPONE_RETENTION_ACTOR}" --allowed-promoter "${DPONE_RETENTION_ACTOR:?set DPONE_RETENTION_ACTOR}" --expected-plan-sha256 sha256:... --review-id "${DPONE_CACHE_RETENTION_REVIEW_ID:?set one approved UUIDv4 attempt id}" --loader-ack-file "${DPONE_LOADER_ACK_FILE:?set DPONE_LOADER_ACK_FILE}" --confirm-delete
- details: rerun with --format json for the full cache-retention plan
```

Confirmed apply text output summarizes the effect without exposing local cache
paths:

```text
dpone airflow cache retention apply: OK
- environment: prod
- current deployment: sha256:...
- deleted deployments: 1
- deleted: sha256:... (unreferenced)
- skipped deployments: 1
- skipped: sha256:... (current)
- action: run dpone airflow cache-retention-plan to verify cache health
- details: rerun with --format json for deleted/skipped deployment paths
```

Cache recovery is also explicit and local-only. The plan command diagnoses
`current-pointer.json`, `.dpone-cache/current`, sealed activations, and complete
deployment projections. `current` must be a canonical relative symlink; an
absolute, aliased, traversal, or out-of-root target is rejected. Because such
an existing path has no trustworthy CAS identity, automatic apply is blocked
until platform change control restores or removes it and a new plan is reviewed.
When recovery is needed,
incomplete deployments are reported as `DPONE_DEPLOYMENT_INCOMPLETE` blockers,
invalid deployment projections are reported as `DPONE_DEPLOYMENT_INVALID`, and
neither is offered as a repair candidate. The plan separates pointer state
(`current_deployment_id`) from active state (`current_path_deployment_id`). The
apply command requires a selected complete deployment, authorized recovery
actor, active-state CAS guard, and `--confirm-repair`, then reuses the same
verified, locked materializer used by normal promotion:

```bash
dpone airflow cache-recovery-plan --environment prod --format json

dpone airflow cache-recovery-apply \
  --environment prod \
  --deployment-id sha256:... \
  --promoted-by ci://github-actions/dpone-airflow-recovery \
  --allowed-promoter ci://github-actions/dpone-airflow-recovery \
  --expected-current-deployment-id sha256:... \
  --confirm-repair
```

Default text output is intentionally actionable for platform operators. It does
not dump projection file paths by default, but it names the cache status,
pointer/active deployment, preferred repair, issue codes, repair candidates,
and a safe confirmed apply command. Confirmed apply text output is similarly
operator-focused: it names the recovered deployment and release, confirms that
the current pointer was repaired, and keeps physical cache paths in JSON output
only. As with retention, non-default absolute cache roots are rendered through
the shell-safe `${DPONE_CACHE_ROOT:?set DPONE_CACHE_ROOT}` guard in text output
and kept exact in JSON.

```text
dpone airflow cache recovery: REPAIRABLE
- environment: prod
- pointer deployment: sha256:...
- active deployment: absent
- preferred repair: sha256:...
- issue: DPONE_CURRENT_PATH_MISSING: current deployment path is missing
- repair candidate: sha256:... (current_state)
- action: dpone airflow cache-recovery-apply --environment prod --deployment-id sha256:... --promoted-by "${DPONE_RECOVERY_ACTOR:?set DPONE_RECOVERY_ACTOR}" --allowed-promoter "${DPONE_RECOVERY_ACTOR:?set DPONE_RECOVERY_ACTOR}" --expect-current-absent --confirm-repair
- details: rerun with --format json for the full cache-recovery plan
```

```text
dpone airflow cache recovery apply: OK
- environment: prod
- recovered deployment: sha256:...
- release: sha256:...
- current pointer: repaired
- action: run dpone airflow cache-recovery-plan to verify cache health
- details: rerun with --format json for current/pointer paths
```

Retention and recovery JSON outputs are published as
`dpone.deployment-cache-*.v1` schemas and registered in the GitOps schema
catalog, so platform automation can validate cache-plane plans before applying
destructive operations.

Environment deployment projection materializes a `dpone.deployment-set.v2` and
matching `dpone.airflow-deployment-index.v2` from an existing release-set plus
local environment files. It writes only digest references for connection
registry and credential runtime, stores the non-secret binding-set alongside the
deployment, emits `_SUCCESS` last, and never serializes registry bodies or
secret values into Airflow parse artifacts. The projection directory is
content-addressed and immutable: rebuilding the exact same payload is a no-op,
while an existing directory with different content fails closed with
`DPONE_DEPLOYMENT_ALREADY_EXISTS` instead of being overwritten.
Before fingerprinting or writing anything, the build-plane validates that the
binding-set, connection registry, and credential-runtime files all declare the
requested deployment environment; mismatches fail closed with the same
environment-specific error codes used by `dpone check --connections`.

Platform usage:

```bash
dpone airflow build \
  --release-id sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --environment prod \
  --trust-tier production \
  --runtime-image-ref registry.example/dpone-runtime@sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd \
  --runtime-image-digest sha256:dddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddddd \
  --artifact-registry-ref dpone-prod-artifacts \
  --registry-config-map-name dpone-artifact-registry \
  --registry-config-map-key registry.json \
  --registry-config-sha256 sha256:eeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee \
  --trust-policy-config-map-name dpone-artifact-trust-policy \
  --trust-policy-config-map-key policy.json \
  --trust-policy-sha256 sha256:ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff \
  --airflow-bundle-ref git:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa
```

This command is intentionally outside the beginner golden path. It is meant for
CI or platform engineers after a release-set already exists.
The release id, runtime image digest, release artifact checksums, and deployment
refs must be real `sha256:<64 hex>` digests; symbolic values fail before any
deployment projection is read or written.
After build validation, CI or a cache materializer step promotes the complete
deployment with `dpone airflow cache-sync`; the Airflow provider still only
reads `.dpone-cache/current/airflow-index.json` during parse.
Default text output is intended for platform operators and uses repo-relative
paths instead of absolute workstation paths:

```text
dpone airflow build: OK
- environment: prod
- release: sha256:...
- deployment: sha256:...
- runnable: yes
- dag specs: 1
- workload packs: 1
- runtime delivery: init_fetch
- artifact registry: dpone-prod-artifacts
- airflow bundle: git:7ac31f2
- output: .dpone-cache/deployments/prod/sha256-...
- airflow index: .dpone-cache/deployments/prod/sha256-.../airflow-index.json
- action: dpone airflow cache-sync --deployment-dir .dpone-cache/deployments/prod/sha256-... --environment prod --promoted-by "${DPONE_CI_IDENTITY:?set DPONE_CI_IDENTITY}" --allowed-promoter "${DPONE_CI_IDENTITY:?set DPONE_CI_IDENTITY}" --expect-current-absent --confirm-promote
- details: rerun with --format json for fingerprints and full deployment projection
```

Missing or invalid build inputs use the same command-specific renderer and omit
absolute projection paths from text output:

```text
dpone airflow build: FAILED
- environment: prod
- release: sha256:...
- issue: DPONE_RELEASE_NOT_FOUND: required JSON file is missing
- action: materialize release-set, then rerun dpone airflow build
- details: rerun with --format json for structured errors and projection diagnostics
```

## Primary production path

```text
dpone authoring
  -> publisher builds environment-neutral release-set
  -> deployment builder creates prod deployment-set + airflow-index.json
  -> exact remote read-back publication
  -> exactly one signed release-set or deployment attestation authority
  -> cache materializer verifies fingerprints/checksums under promotion lock
  -> full local attestation subject verification
  -> pointer/audit authorization
  -> atomic current activation
  -> parse-safe Airflow provider reads current/airflow-index.json
  -> Airflow KPO with pinned release_id/deployment_id
  -> init_fetch by digest into runtime artifact directory
  -> bounded runtime attestation observation
  -> runtime-fetch-ready.v2
  -> dpone runtime pod
  -> verify signed route attestation for exact runtime subject
  -> configured credential provider (env, Airflow, or Vault when enabled)
  -> in-memory ResolvedConnection
  -> MSSQL/ClickHouse connector
  -> evidence with release/deployment/credential version
```

## Companion contracts

- ADRs: `docs/adr/0006-*` through `docs/adr/0014-*`
- JSON Schemas: `docs/schemas/gitops/*release-set*`, `*deployment-set*`,
  `*airflow-deployment-index*`, `*binding-set*`, `*connection-registry*`,
  `*credential-runtime*`, `*current-pointer*`, `*deployment-cache-retention-*`,
  `*deployment-cache-recovery-*`, `*safe-sample-execution-plan*`,
  `*live-preflight*`,
  `*safe-sample-source-request*`, `*safe-sample-data-copier-registry*`,
  `*safe-sample-certified-copy-request*`, `*safe-sample-runtime-readiness*`,
  `*safe-sample-runtime-execution*`, `*safe-sample-runtime-evidence-write*`,
  `*safe-sample-runtime-run*`, `*safe-sample-airflow-deployment-context*`,
  `*safe-sample-data-copy*`,
  `*mssql-clickhouse-safe-sample-copy-config*`,
  `*mssql-safe-sample-read-plan*`, `*clickhouse-safe-sample-insert-plan*`,
  `*route-attestation*`, `*route-attestation-policy*`,
  `*route-attestation-verification*`, and
  `*error*`; each listed safe-sample runtime and route-execution schema is
  available through the GitOps schema catalog.
- Catalog CLI:
  `dpone gitops schema list --prefix dpone.safe-sample`,
  `dpone gitops schema show dpone.safe-sample-runtime-run.v1`, and
  `dpone gitops schema validate --kind dpone.safe-sample-runtime-run.v1 --payload <file>`.
  These commands are parse-safe, local-only, and intended for CI, IDEs and
  platform automation that need to discover or validate public GitOps payloads
  without importing runtime or Airflow code.
- Provider API: `docs/airflow-provider-api.md`
- External acceptance evidence: `docs/airflow-self-service-evidence.md`
- Phase backlog: `docs/airflow-self-service-backlog.md`
