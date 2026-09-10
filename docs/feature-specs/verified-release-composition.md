# Feature design: verified release composition

- Status: APPROVED
- Owner: dpone maintainers
- Target release: 0.75.0
- Last verified: 2026-09-10

## Executive summary and approval

The maintainer authorized implementation of the explicit composition-envelope
approach in the task request, including completion of the consumer audit and
selection of concrete API/schema names. This specification records that existing
approval; it does not grant deployment activation, live SQL, or publisher changes.

One complete native multi-project dbt workspace and independently authored
ordinary declarative transfer packs must share one verified immutable release.
Existing native v2 continues to mean exactly one complete workspace, including
only its generated transfers. Composition adds an outer authority boundary.

## Personas and customer journey

Data engineers author the workspace and ordinary workload set independently.
Platform engineers compile both with public producers, materialize the native
workspace with the existing compact command, capture the ordinary inventory,
then compose their pinned identities. Operators inspect the publication report,
build an ordinary immutable deployment/index, deliver through the provider and
init-fetch, and retain previous immutable IDs for recovery. Activation remains a
separate physical-target admission decision. Offline preparation is not SQL
execution or live certification.

## Scope and public contract

The new outer schema is `dpone.release-set.v3`; its producer is
`dpone.release-composition.v1`. The dbt child remains release-set.v2 with dbt
wire-v2 and execution-pack.v2. These are separate version axes.

Public commands:

- `dpone gitops airflow release-inventory --pack-root ROOT
  --xcom-sidecar-image IMAGE --format json` captures
  and verifies an ordinary producer root and reports `inventory_sha256`.
- `dpone gitops airflow release-compose --manifest FILE --output-dir DIR
  --format json` verifies pinned inputs and publishes one immutable tree at DIR.

The strict bounded authoring manifest is `dpone.release-composition.v1` with
`native_workspace: {root, expected_release_id}`, `standalone: {root,
expected_inventory_sha256}`, and `transport: {profile, xcom_sidecar_image}`.
Roots are local and relative to the manifest directory unless absolute. Profile
is the existing `compact_v2_runtime_connection_context`; the sidecar is digest
pinned. Unknown/null/missing fields, source/output overlap and symlink traversal
fail. Native input must already have this compact promotion profile: users run
the supported `release-materialize` first. Composition performs no further native
rewrite, preserving its exact ID, descriptor, source snapshot and payload bytes.
This deliberate first-scope constraint avoids a second transformation identity.

`dpone.app.release_composition.build_release_composition_service()` constructs
`ReleaseCompositionService`. Its `compose(request) -> ReleaseCompositionReport`
uses typed request/report contracts in `dpone.contracts.release_composition`.
CLI and Python share that service. Inventory is a verified producer observation,
not a caller-supplied assertion. No caller-provided producer or artifact mapping
is accepted. A report uses `dpone.release-composition-report.v1`, `passed`,
`status`, `release_id`, `output_dir`, constituent identities and stable blockers.
Exit 0 means durably published/reverified; 2 means rejected or failed before
publication; 3 means publication is visible but durability is uncertain. JSON
stdout is one object. Diagnostics are sanitized. Optional report-file support is
out of scope; shell redirection preserves the same JSON contract without a
second publication failure boundary.

## Detailed algorithm and artifact authority

1. Parse and bound the manifest, validate IDs/transport and disjoint roots.
2. Capture the complete compact native child using the existing confined
   `VerifiedWorkspaceReleaseCapture`; preserve exact authorized descriptor bytes.
3. Capture the explicit ordinary root `_dags/*.dag-spec.json` and exactly its
   referenced workload `airflow-pack.json` files. Verify original framework pack
   fingerprints before any rewrite. Decode every embedded archive with bounded
   no-follow extraction, validate declarative manifests and complete dependency
   closure. Reject hidden dbt execution, external dbt trios, undeclared files,
   unsupported commands/dependencies and nonempty connection projection.
   Semantically empty `{query_overrides: {}}` can canonicalize to `{}`; secret
   volume authority cannot be relabeled. Unsupported ordinary kinds fail closed.
4. The inventory producer hashes captured source descriptors. Compose compares
   this computed digest with the explicit expected digest. Original ordinary
   DAG/pack bytes remain bound source sidecars; only existing strict compact
   transport rewriting is allowed, with source fields unchanged.
5. Parent top-level executable artifacts are the exact flat union. Every DAG and
   workload has one owner; IDs/paths/memberships must not collide. Native ordered
   runtime trios and CAS paths stay unchanged. Cross-constituent DAG splicing and
   dependencies are unsupported. Check combined logical writes using the existing
   dbt relation policy; aliases never prove physical disjointness.
6. Embed the native release object in its typed constituent for selected runtime
   authority without extra source fetches. Bind its exact original descriptor,
   snapshot and integrity subject under `_composition/native/`. Bind original
   ordinary inventory/source bytes under `_composition/standalone/`. Register
   these in an explicit `composition_sources` artifact section so remote delivery
   cannot omit them. Parent source↔final descriptor equality/transformation is
   checked by the verifier, never supplied as trusted input.
7. Canonically sort unordered inventories; preserve ordered trios. Hash all
   constituents, ownership, descriptors, profile and composer version in ordinary
   identity-bearing fields, never top-level provenance. Do not hash local roots,
   timestamps or output directories. Apply aggregate count/byte and payload limits
   once across the complete parent, including source sidecars.
8. Write a private stage, produce the integrity subject, independently reread all
   parent bytes and reconstruct only the native child view in a private directory.
   Invoke the unchanged native source and integrity verifier on that view. Recheck
   ordinary closure, transport mapping, exact union and combined writes.
9. Publish through the existing immutable writer only after verification. Identical
   retries reverify/create-or-compare; conflicting bytes remain untouched. Owned
   stages clean up before publication. After-rename durability uncertainty retains
   visible identity and never reports success or removes the visible tree.

States: parsed → captured → validated → staged → independently verified → durably
published. Any prepublication failure leaves no published output. Visible but
uncertain publication is a separate recoverable outcome. There is no checkpoint,
SQL transaction, deployment-pointer mutation or implicit execution order here.

## Architecture, compatibility and consumer audit

Contracts own identity/ownership. Manifest modules own bounded source acquisition
and detached verification. A service coordinates capture/staging/publication via
injected capabilities constructed in `dpone.app`; CLI is thin. Reuse existing
framework verifiers, compact transport policy and immutable writer. New modules
follow `docs/benchmarks/quality_budgets.yml`; existing debt must not increase.

Audit of source 3977ca2d04ca5dbcf3338d7c31faff8a1199549e identified:

| Consumer | Required behavior |
|---|---|
| Shared schema validation / release identity | Explicit closed v3 dispatch; v1/v2 unchanged |
| Deployment artifact index and cache integrity | Verify exact flat union and composition source descriptors |
| Remote publication/materialization | Transport every registered source artifact with parent budgets |
| Provider / init-fetch plan and READY receipts | Admit v3 explicitly; retain selected runtime payload checks |
| VerifiedPackLauncher source projection | Resolve selected native owner, compare parent/child descriptors and ordered trio; then run unchanged v2 wire policy |
| Attestation and trust policies | Bind exact parent; no child-only signature becomes parent authority |
| Activation / recovery / retention | Preserve mandatory admission and reject composition activation until combined physical admission is implemented; do not invoke native-only coordinator for union |
| dbt evidence / mirror / promotion | Explicitly reject composition with actionable native-child guidance; never infer legacy authority from absent dbt producer |
| Legacy materializer | Reject actual v2 execution/payload authority before publication; no producer insertion |

Old readers reject v3. Existing legacy v1 and native v2 remain supported unchanged.
Migration upgrades core/provider/runtime readers first, builds complete native and
ordinary sources, pins producer inventory, composes and verifies delivery. Retain
old release/deployment IDs; rollback never undoes committed data.

## Alternatives and ADR

ADR 0058 extends ADR 0052 with constituent authority. Relaxing v2 closure or
manually inserting producer is rejected. Copying whole native payload trees
would duplicate large artifacts; only small source sidecars and ordinary
pre-rewrite sources are retained. Separate releases remain an interim option
without one atomic publication boundary. Generic plugin registries are unneeded.

## Market comparison and measurable differentiation

Official sources checked 2026-09-10:

- Astronomer Cosmos rolling docs: `DbtTaskGroup` places a project in a regular DAG,
  and its multi-project guide supports project relationships. Adopt explicit
  project boundaries; no inference that task grouping supplies immutable source
  closure. https://astronomer.github.io/astronomer-cosmos/guides/multi_project/multi-project.html
- gusty public project documentation: file-authored tasks and explicit intra/inter
  DAG dependencies. Adopt declarative inventories; arbitrary cross-DAG composition
  is outside this first release. https://github.com/pipeline-tools/gusty
- Airflow 3.3.1 serialization documentation separates serialized DAG state and
  execution communication. Preserve parse-safe provider/runtime boundaries.
  https://airflow.apache.org/docs/apache-airflow/stable/dag-serialization.html

For this narrow immutable dpone release envelope, dlt, Informatica, Airbyte,
Fivetran, Pentaho, SSIS and Apache Beam are N/A: their data movement or pipeline
execution layers are not the dpone artifact consumer contract under change. No
feature-count or performance superiority claim is made. The measured improvement
against dpone 0.74.36 is binary: two native projects plus one independent transfer
pass the real offline delivery chain as one immutable release, while omissions,
forged ownership and reordered trios fail. Test artifacts establish offline
correctness only, not production authority.

## Test, documentation and rollout plan

Red-first regression: real native v2 execution/trio in descriptor-less legacy
root rejects before any publication. Add producer-generated mixed positive CLI
and Python paths through deployment/index, provider, init-fetch and launcher;
negative closure, fingerprints, hidden dbt, unsupported connection modes,
collisions, aggregate bounds, determinism, mutation-after-READY, identical retry,
concurrent writers, immutable conflict and durability faults. Run pinned real dbt
parse/ls offline if available, reporting missing toolchain as SKIP. Live SQL,
reconciliation and physical activation remain UNVERIFIED without authorization.

Run focused suites followed by change-aware checks, Ruff, mypy, architecture
budgets, full non-live pytest, docs/language/strict MkDocs and affected packaging.
A fresh reviewer inspects exact integrated changes. Docs include first-success
how-to, API/schema reference, architecture diagram, retry runbook and reader-first
migration; repair stale native-only availability and legacy wire guidance.

## Agent execution plan and completion

The parent is integrator and owns shared contracts/dispatch, schemas, registries,
CLI wiring, changelog/navigation and final validation. Delegated writers use
separate worktrees and validated path-scoped contracts; review agents are read
only. No publication before exact reviewed-head gates and ordinary controller
policy. Mark IMPLEMENTED only after linked evidence demonstrates all supported
behavior. This specification records approved intended behavior, not completion.

## Approved review corrections

The maintainer authorized correction of the independent review findings. The
inventory reader must apply the existing provider OCI validator and preserve its
ordinary API error, CLI rejection, original source bytes and source-only digest.

The internal dependency correction preserves the public release and execution
contracts. Extract five pure policies into canonical contracts: composition
transport byte binding, semantic-refresh template proof binding, generic release
authority after schema admission, native producer installation admission, and
release artifact metadata. Existing adapters retain schema orchestration, provider
fingerprints/topology parsing, confined I/O, error translation and publication.
Keep the existing order of rejected conditions and public imports/class identity.
Each policy owns its actual validation algorithm, not aliases to unrelated helpers.

Dependencies used only by postponed annotations may be guarded by `TYPE_CHECKING`.
Keep runtime bases, constructors, type checks, dataclass field types and advertised
re-exports available. Explicit dependency injection and raw string annotations
remain unchanged. Incidental `typing.get_type_hints` calls may require explicit
type namespaces; automatic runtime annotation resolution is not an injection
mechanism for these adapters. Verify reduced module loading in fresh interpreters.

Independent transport, proof and artifact-metadata writers use separate worktrees
and path-scoped task contracts. The integrator owns generic/native admission,
remaining annotation imports, shared docs and final validation. Acceptance includes
first-error characterization, v1 legacy checksum spelling, native v2 authority,
v3 byte closure, actual registry/cache/provider preparation and unchanged activation
refusal. Compare the canonical graph from the real final files against unchanged
budgets; a simulated graph does not establish acceptance or release readiness.
