# Feature design: independent Airflow delivery routes

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: standalone public OSS route-isolation request; no private issue dependency
- Target release: next unused compatible release, selected after implementation gates
- Last verified: 2026-09-23
- Source baseline: `eb4cba09d4f0849eaf2442492e9f64f8b9d920e0` (`0.83.9`)

## Executive summary

Independent ordinary dpone and native dbt+dpone workloads need independent
publication, deployment selection, authority, recovery and parsing boundaries.
Today a composed release is intentionally one immutable unit. Its native
development authority becomes a deployment-wide requirement and is copied into
every runtime plan. Splitting authority checks alone cannot isolate publication
or rollback, and interpreting a missing grant subject as permission is forbidden.

Introduce a protected delivery-route binding around the existing immutable
release/deployment services. One route owns one complete release generation,
desired-state object, registry scope, cache root, loader and ACK namespace.
Existing release-set schemas and identity algorithms remain unchanged. Existing
composition remains an explicit choice for workloads deployed together.

This document is a proposed contract, not a declaration that the capability is
implemented or certified. Maintainer review must resolve the approval checklist
before production implementation.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Workload author | Release ordinary pipelines independently | Unrelated native authority or build failure stops delivery | Ordinary route publishes and runs while native route is rejected |
| Platform engineer | Operate two routes in one Airflow environment | One current pointer replaces the whole deployment | Separate CAS revisions, activation IDs and loader acknowledgements |
| Integrator | Upgrade CLI/API consumers safely | Filtering a composition looks like isolation but is not | Explicit opt-in route contract; legacy artifacts remain readable |

The platform registers disjoint route ownership, validates protected configuration,
then deploys route-aware consumers before producers. Authors use the existing
ordinary or complete native producers in separate build roots. Each CI job selects
only its authorized route, publishes immutable content, and performs that route's
conditional desired-state mutation. A separate watcher restores and activates each
cache. A separate loader module acknowledges each route. Operators diagnose,
retry, revoke or roll back one route using its own evidence.

## Scope and constraints

In scope: protected route configuration and resolution, CLI/API opt-in,
ownership/dependency validation, namespace separation, admission consistency,
warm-cache revalidation, synthetic lifecycle tests, migration documentation and
an additive amendment to ADR 0033.

Non-goals: changing row movement, inventing grants, weakening physical-target
fences, automatic splitting of immutable releases, cross-route atomic commits,
new connector capabilities, or changing historical release tags.

Only public code, public documentation and synthetic examples are admissible.
No corporate configurations, credentials, data, issue identifiers or other tasks
are inputs. A shared Airflow outage, filesystem failure, database failure or
deliberate shared physical resource remains a shared failure domain.

## Findings on the source baseline

1. `InitFetchDeliveryContext.encode_plan` copies
   `development_authority_required` and immutable runtime authority from the
   common context. Pod construction also selects authority from that context.
2. Development authority permits exact workloads/hooks. An absent subject must
   continue to deny execution. Workload names or `dbt__` prefixes are not grants.
3. A cache root owns `current`, `current-pointer.json`, locks, recovery,
   checkpoints, activation history and retention. Different environment
   subdirectories beneath one cache root are insufficient isolation.
4. Strict executable index preflight validates a complete index. Historical
   per-DAG error-isolation tests do not prove independent strict-route loading.
5. `depends_on` resolution outside `DagMembership` currently emits a warning;
   it does not implement an edge across deployment indexes.
6. Runtime receipt mirroring does not independently compare the development
   requirement and authority source against fetched release/deployment content.
   Native dbt execution has an additional authority guard, so this finding must
   not be described as a proven universal execution bypass.
7. Early protected warm-cache reuse does not receive the freshly bound release
   validator used by cold fetch. It can fail closed after successful admission.

## Public contract

### Protected configuration

Add the closed schema `dpone.airflow-delivery-routes.v1`, loaded only from
`DPONE_AIRFLOW_DELIVERY_ROUTES_FILE`. This is platform-owned read-only
configuration, not a workload manifest. Its top-level fields are `schema` and
`routes`. Each route has exactly these fields:

| Field | Contract |
|---|---|
| `route_id` | Unique lowercase `[a-z][a-z0-9_-]{0,62}` identifier |
| `environment` | Existing environment-segment validation |
| `source_root` | Absolute build-plane project root; no implicit current-directory fallback |
| `release_profile` | `ordinary`, `native_workspace`, or `composed` |
| `authority_file` | Absolute path to existing desired-state authority v1/v2 |
| `authority_sha256` | SHA-256 of exact protected authority file bytes |
| `certified_s3_endpoint_url` | Canonical HTTPS endpoint, equal to selected authority |
| `artifact_registry_uri` | Canonical immutable registry root, equal to selected authority |
| `artifact_registry_ref` | Logical registry reference, equal to selected authority |
| `desired_state_uri` | Exact mutable object URI, equal to selected authority |
| `build_cache_root` | Exactly `<source_root>/.dpone-cache`, preserving the current build facade's layout |
| `cache_root` | Absolute route-owned runtime parser cache root, separate from build cache |
| `ack_root` | Absolute route-owned loader acknowledgement directory |
| `loader_id` | Unique stable owner of one loader module |
| `dag_ids` | Non-empty exact, unique allowed DAG ID set |
| `workload_ids` | Non-empty exact, unique allowed workload ID set |

The file is bounded to 256 KiB and 64 routes. Reject duplicate JSON keys,
unknown fields, invalid types, noncanonical paths and traversal. Compare local
roots after resolving existing ancestors; reject aliases, ancestor/descendant
writable roots across routes, and symlink traversal. The declared build cache
inside its own source root is intentional. Registry roots must be disjoint on the
same endpoint/bucket; mutable object URIs must be unique and outside every
immutable root. Registry refs, loader IDs and Airflow DAG ownership must be
unambiguous across the installation. Workload IDs are checked within their
route; their immutable release identities remain the ultimate content binding.

The roster contains enough credential-free namespace metadata for collision
validation without opening every route's authority file. A selected operation
opens only its own authority and source artifacts. A missing/revoked authority
for another route must not prevent it. Whole-roster structural invalidity is an
installation configuration error; prevent it with the read-only validator before
deploying the roster. Dynamic publication never edits this protected roster.

Exact ownership lists deliberately avoid implicit prefix-based security. Adding
or moving a DAG requires a reviewed ownership update. A future ownership-prefix
extension needs a separate compatible contract and collision tests.

### CLI

Add the read-only command:

```text
dpone airflow routes validate --manifest FILE [--format json]
```

JSON is the only format and default. Emit one
`dpone.airflow-delivery-routes-report.v1` object containing `passed`, `routes`
and sanitized `errors`. Each successful route entry contains `route_id`,
`environment`, `authority_sha256`, `registry_scope_id`, `cache_root`, `ack_root`,
`loader_id`, and owned IDs. No clients are constructed and no artifacts or
configuration are written. Exit `0` means valid; `2` means input/ownership
rejection; unexpected internal failures return `5`. Parser errors use stderr.

The roster validator performs structural and namespace checks only. It derives
registry scope from declared canonical endpoint/root metadata and does not open
authority files or certify their current availability. Selected operations verify
the exact selected authority bytes and matching scope before constructing clients.

Add optional `--route-id ID` to these existing operations:

```text
dpone gitops airflow release-materialize ... [--route-id ID]
dpone airflow build ... [--route-id ID]
dpone airflow publish ... [--route-id ID]
dpone airflow cache-materialize ... [--route-id ID]
dpone airflow cache-recovery-plan ... [--route-id ID]
dpone airflow cache-recovery-apply ... [--route-id ID]
dpone airflow cache-retention-plan ... [--route-id ID]
dpone airflow cache-retention-apply ... [--route-id ID]
dpone airflow desired-state prepare ... [--route-id ID]
dpone airflow desired-state publish ... [--route-id ID]
dpone airflow desired-state fetch ... [--route-id ID]
dpone airflow desired-state reconcile ... [--route-id ID]
```

In route mode, resolve the selected protected binding before any remote client
construction or mutation. Existing explicit root/environment/registry arguments
must equal the binding; contradictory values fail with
`DPONE_AIRFLOW_ROUTE_SCOPE_MISMATCH`. Route-aware materialization accepts only
the declared complete release profile and owned inventory. `airflow build`
uses `source_root` instead of the process working directory. `--cache-root`,
where already present, remains accepted and must identify the selected route.
Do not add a mutable untrusted URI/root override.

Release materialization, deployment build and immutable publication use
`build_cache_root`; watcher reconcile and cache materialize/recovery/
retention use runtime `cache_root`. The two roots are never implicit aliases.
Reject the legacy `cache-sync` compatibility command in route mode before
mutation; the supported route watcher uses desired-state reconcile. Route-aware
commands must distinguish omitted default path options from explicitly
conflicting values and choose the declared root only for omitted options.
Read-only cache status commands also accept the selector for consistent
diagnosis. This includes every cache mutation entrypoint; direct lower-level API
callers remain responsible for supplying the resolved route context.

With no route option and no route file, legacy behavior is unchanged. If the
route file is configured, a missing selector fails with
`DPONE_AIRFLOW_ROUTE_REQUIRED`; there is no first-route or legacy fallback.
When both authority environment variables are configured, the legacy authority
path must equal the selected route's authority path or selection fails.

Existing success report schemas and exit conventions remain unchanged. Route
scope errors use each command's existing invalid-input exit `2`. Existing
desired-state CAS failure and uncertain-outcome conventions remain intact.
External consumers retain the route selector alongside existing report IDs;
they must never infer route identity from release IDs or task names.

### Python API and dependency injection

Add immutable contracts in `dpone.contracts.airflow_delivery_routes` and a
filesystem adapter plus application composition root:

```python
from pathlib import Path
from dpone.app.airflow_delivery_routes import resolve_airflow_delivery_route

route = resolve_airflow_delivery_route(
    path=Path("/etc/dpone/routes.json"), route_id="ordinary"
)
```

The result exposes the selected validated binding and existing
`AirflowDesiredStateAuthority`; its resolution does not construct a network
client. Route-aware application services accept this resolved value explicitly.
Existing lower-level public services continue accepting their current arguments.
The route composition root supplies and validates those arguments; it never
modifies process-global environment to select a route.

The provider adds a thin API exported from `dpone_airflow_pack`:

```text
load_and_acknowledge_dpone_route(
    globals_dict, *, route_id, index_path, ack_path, ack_root,
    expected_dag_ids, expected_artifact_registry_ref,
    operator_overrides=None, semantic_refresh_callables=None
) -> AcknowledgedDagLoad
```

It delegates immutable parsing and ACK writing under the existing lease,
requires the index's DAG inventory to be within exact owned IDs, verifies the
registry reference, and forces duplicate rejection. It must stage output before
updating caller globals, so rejected route state cannot remove or overwrite
another module's DAGs. Protected loader modules supply arguments from reviewed
route configuration; the provider does not import core dpone or scan authors'
repositories during parse. One route uses one module and ACK namespace.

### Identities and evidence

The logical key is `(environment, route_id)`. Its selected generation is
`(release_id, deployment_id)` and occurrence is `activation_id`. Existing
content-addressed IDs are preserved; identical content may share a release ID.
Registry references and route-owned authorities distinguish deployments and
delivery scopes. Do not add fields to strict legacy release/deployment,
desired-state, checkpoint or ACK schemas.

Route selection must validate actual promotion inventory and registry scope,
not merely the producer's requested route ID. Existing publication preparation
binds the full authority fingerprint, including desired URI and registry scope,
and remains create-once. Copying one route's preparation into another must fail.

## Detailed algorithm and failure semantics

1. Parse and statically validate protected route namespaces without peer I/O.
2. Select one exact route. Read and hash only its authority file; compare its
   environment, URI/ref and source authority with the selected binding.
3. Build the complete selected ordinary/native/composed release through existing
   public producers. Validate declared workload/DAG ownership and dependency
   closure before immutable publication; never repair an immutable source.
4. Materialize and project within the selected root. Retain normal release,
   workspace, attestation, target and development admission.
5. Publish immutable artifacts, then use the existing create/replace CAS and
   preparation protocol for this route's sole desired-state object.
6. Reconcile only this route. Retain the existing recovery record, pointer,
   receipt and checkpoint ordering under this root's lock. No sibling pointer,
   authority, repair, receipt or ACK participates in the decision.
7. Parse/acknowledge the exact local index in its own loader module. Registry and
   owned DAG checks precede installation into module globals. No network I/O.
8. Runtime reopens existing development runtime authority for protected native
   releases and the configured runtime attestation policy on cold fetch, warm
   reuse and base launch. It does not load desired-state publication authority.
   Independently validate release/deployment/plan authority consistency.
9. Retry or rollback only this route using existing CAS and fresh activation
   occurrence semantics. An uncertain result is never converted to success.

For each route the state machine remains:

```text
built -> verified -> immutable-published -> desired-selected -> staged
      -> admitted -> activated -> acknowledged
```

Clean failures preserve that route's last-known-good pointer. Post-switch
uncertainty is recorded and repaired using only that route's recovery protocol.
Concurrent writers contend only for the same route's revision/lock. Retention
operates on one disjoint root; shared mutable caches are prohibited. A protected
native route whose current development grant is revoked may remain audit-visible
in the parser but must fail fresh execution admission. A sibling's runtime and
active selection stay untouched.

Publication authority and execution authority are distinct. Removing or changing
the desired-state authority file prevents new publication/reconciliation; it does
not revoke already-published execution. Protected native execution revocation
uses the existing development grant/revocation-epoch mechanism. Ordinary
production execution uses its configured attestation/key revocation mechanism.
Ordinary non-production releases without either authority have no new generic
execution-revocation API in this proposal. Separate runtime registry
configurations, trust policies and grant scopes are required for independent
revocation; deliberately sharing a key or resource preserves that shared scope.

### Explicit dependencies

Before splitting, compute dependency closure from validated source/workload/DAG
graphs. A `depends_on` crossing a proposed independent boundary is rejected with
`DPONE_AIRFLOW_ROUTE_DEPENDENCY_CROSSES_BOUNDARY`; never retain only the existing
outside-membership warning. Keep the complete connected group in one executable
DAG within one route. Merely placing two DAGs in the same route or composing
their releases does not implement an execution edge. Reject cross-DAG
`depends_on` with `DPONE_AIRFLOW_ROUTE_DEPENDENCY_UNREPRESENTABLE` unless the
source already expresses the dependency through a supported explicit mechanism.
Native workspace closure is also indivisible. Dependencies are preserved, not
silently weakened.

Existing explicit Airflow asset URI schedules retain their event semantics.
They do not become exact generation-pinned cross-route dependencies. Supporting
separately mutable routes with exact cross-route compatibility/pins is outside
this minimal version and must not be claimed in migration documentation.

### Admission hardening

Unconditionally compare the development requirement derived from verified
release content with the fetched deployment and plan. Reject omitted/false
protected flags, spurious ordinary-route authority, incompatible trust tiers,
and mismatched immutable authority source references. Do this on cold and warm
receipt verification before ready publication, extraction or execution. Preserve
legacy composed-release protection; missing workload grants continue to deny.

Pass the freshly authorized release validator into the early warm-cache path.
A prior ready marker is never a reusable authority decision. Test ordinary,
native runtime and pre-hook paths, including stripped/downgraded plans.

## Architecture, alternatives and quality

| Component | Responsibility |
|---|---|
| New route contracts/policy | Pure namespace, ownership and selection validation |
| New file adapter/application root | Bounded config acquisition and explicit authority injection |
| Existing producers/materializer/projection | Complete immutable releases and deployments |
| Existing desired-state publisher/reconciler | Per-route CAS, crash recovery and activation |
| Thin provider route loader | Owned-index guard plus existing parse/ACK machinery |
| Generic runtime receipts | Independent authority consistency for every execution kind |

Recommended: additive protected route binding, preserving existing wires.
Alternative: route IDs in new versions of every release, deployment, desired,
checkpoint and ACK schema. This makes portable artifacts self-describing but
greatly expands migration and is unnecessary when trusted registry scope already
binds delivery. Rejected: filter authority inside one composed deployment;
publication/rollback coupling remains and security semantics become ambiguous.

Amend ADR 0033 from one desired-state object per environment to one per
independently deployable route. Preserve its prohibition on multiple mutable
selectors purporting to select one atomic generation. Follow canonical package
directions, injected capabilities and `docs/benchmarks/quality_budgets.yml`;
do not grow existing oversized modules or create a generic plugin framework.

## Market comparison and measured outcome

Checked official documentation on 2026-09-23:

- [Cosmos execution modes](https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/index.html)
  document `DbtDag`/`DbtTaskGroup` and execution granularity. Adopt explicit DAG
  ownership; this source does not establish dpone-style desired-state or grant
  isolation. No comparative superiority claim is made.
- [Airflow 3.3.2 DAG bundles](https://airflow.apache.org/docs/apache-airflow/3.3.2/administration-and-deployment/dag-bundles.html)
  describe versioned code per run. Adopt exact immutable generation binding;
  bundle versioning alone is not evidence of independent dpone admission.
- dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS and Apache Beam: N/A for this
  narrow extension of dpone's existing Airflow desired-state/authority protocol;
  this design does not compare connector or data-movement capabilities.
- gusty: N/A for the authority/CAS claim; DAG generation alone is not the
  lifecycle under assessment.

The baseline is current dpone, not another product. The measurable target is
zero changes to sibling release/deployment/activation IDs, pointer/checkpoint/
receipt bytes and usable DAGs when one independent synthetic route is updated,
rejected, revoked or rolled back. Producer-backed offline tests record these
assertions. They do not establish live database or production readiness.

## Test and certification plan

Use `ordinary_root`, `prepare_projects`/`workspace_service`, filesystem registry
fixtures, desired-state CAS fixtures and actual cache/ACK services from existing
tests. Add failing regressions before product code.

| Layer | Required scenarios |
|---|---|
| Contract | Closed/bounded config, unknown route, duplicate keys, namespace overlap, symlink aliases, inventory substitution, stale authority hash |
| CLI/API | Legacy parity, selector requirement, conflicting explicit options, JSON and exit codes, no peer clients or mutation on rejection |
| Producer integration | Independent ordinary and complete synthetic dbt workspace releases; no composed-parent dependency |
| Lifecycle | A and B publish/activate; update A; reject A at compile/admission/parse; revoke A; roll back A; B bytes and execution unchanged; reverse roles |
| Recovery | Same-route CAS contention, uncertain write, crash after switch, replay, warm cache, independent retention and ACK leases |
| Authority | Missing subject denies; stripped/downgraded plan denied for runtime/hooks; current authority checked on reuse; ordinary sibling never calls native authority |
| Dependencies | Split crossing edge rejected; same-route cross-DAG edge rejected; connected single-DAG cohort retains edge; native closure preserved; asset schedules unchanged; shared physical fences remain enforced |
| Compatibility | Existing release v1/v2/v3, executable index versions, legacy CLI, full-composition atomicity, lazy imports |
| Provider | Supported Airflow matrix and separate loader modules; duplicate DAG ownership rejected; per-route failure does not abort sibling parse |

Run change-aware selection, focused regressions, required lint/type/import/layer/
module gates, non-live suite, docs checks and strict MkDocs build. R1/R5/R6/R7/R8/
R9 apply; R2 is limited to changed selection parity; R3 and unrelated R4 live
connector campaigns are N/A absent data-movement changes. No live environment
is authorized by this synthetic request. Report unavailable live checks as
UNVERIFIED/SKIP, never as PASS.

## Migration, operations and rollback

1. Upgrade core/provider readers together and retain old immutable artifacts.
2. Inventory dependencies and DAG ownership; keep connected groups together.
3. Register protected routes and validate all namespaces before installation.
4. Build ordinary and native sources independently into separate roots and
   publish into distinct registry scopes with separate CI concurrency groups.
5. Install one watcher and loader/ACK owner per route. For reused existing DAG
   IDs, pause and retire the old loader owner before exposing new owners; verify
   convergence before resuming. This migration is not promised to be zero-downtime.
6. Verify both identities and ACKs, then exercise synthetic independent failure
   and rollback checks. Retire the old combined pointer only after the cutover.

Do not extract one constituent from a published v3 parent and call it the same
release. Rebuild through existing public producers. Do not hand-edit digests,
ready markers, authority receipts or state for downgrade. Returning to the old
combined deployment requires its exact still-authorized immutable generation,
the old loader owner and deliberate removal of new owners to avoid duplicates.

Route-level CI jobs must not have unrelated build success as a prerequisite.
Each job keeps its own reports, preparation, status and rollback occurrence.
Global physical-target admission remains shared where the declared resource is
shared; logical aliases do not create independent physical targets.

## Documentation, execution ownership and release

Add a first-success ordinary/native example, CLI/API reference, migration guide,
failure/rollback runbook, schema registrations and an ADR amendment. Update
release-composition operations to distinguish deliberate composition from
independent delivery. Update compatibility, changelog and navigation.

The primary integrator owns production changes, shared schemas, CLI registration,
version files, changelog and documentation. Read-only explorer, architecture and
test analyses are complete. Any parallel writer requires a separate worktree and
disjoint path contract. An independent fresh-context reviewer must review the
final implementation commit and fixes before merge.

Create/update a dedicated PR; preserve required checks and the merge receipt.
Select an unused version only after readiness checks and synchronize all four
package versions/pins. Follow `docs/release.md`: the external release controller
is the ordinary PyPI publisher. Do not replace tags, republish an existing
version, invent public-byte evidence, or claim a published version before the
controller run and verification prove it.

## Approval checklist

- [x] Public-only problem, baseline and customer journey are explicit.
- [x] Whole-route identity and failure boundaries are distinguished from grants.
- [x] Additive CLI/API contract and legacy behavior are specified.
- [x] Exact-dependency limitation and migration interruption are explicit.
- [x] Tests, evidence limits, documentation, rollback and release workflow are specified.
- [ ] Maintainer accepts ownership-list updates and connected-cohort limitation.
- [ ] Maintainer reviews this written specification and marks it APPROVED.
- [ ] Written implementation plan is reviewed and execution method selected.
