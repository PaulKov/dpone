# Feature design: native compact delivery for dbt workspace wire v2

- Status: APPROVED
- Owner: repository maintainer; implementation integrator: Codex
- Issue: none opened; implementation PR pending validation
- Target release: TBD, through the established release controller
- Last verified: 2026-09-09
- Inspected upstream: `f8c6a4a5e75d167829c05f65d5d3033acb193878`

## Executive summary

Workspace compilation already creates a complete dbt wire-v2 release. Compact
materialization currently reads a different directory layout, requires singleton
payload IDs, and creates release-set v1 without the original dbt authority.
Supporting the native workspace output requires a coordinated artifact-contract
extension, not a schema-number substitution or a relaxed runtime validator.

This specification supplements the [multi-project design](feature-design-dbt-multi-project-release.md)
and the [compact promotion design](feature-design-airflow-compact-pack-v2-context-promotion-v1.md).
It preserves [ADR 0052](adr/0052-dbt-workspace-release-source-authority.md)'s activation
restrictions. Local preflight success will not certify SQL execution, physical
target admission, production activation, or a release.

Classification: compatible public artifact and Airflow/dbt integration change,
with identity validation across layers. Maintainer approval for this additional compact input and derived-release
contract was recorded on 2026-09-09 before production edits.

## Personas and customer journey

| Persona | Goal | Current problem | Success signal |
| --- | --- | --- | --- |
| Data engineer | Deliver project_alpha and project_beta together | Compile output is not native compact input | One materialized release retains both source identities |
| Platform engineer | Build a pinned Airflow deployment/index | Legacy transport loses release authority | Each provider plan contains exactly its workload's trio |
| Operator | Diagnose and recover safely | Selection drift can originate in transport | Safe error code identifies the failed boundary and regeneration step |
| Maintainer | Assess release readiness | Separate component tests miss composition failures | Full-path preflight and adversarial tests on the PR commit |

Discover the workspace tutorial, prepare the exact pinned toolchain and synthetic
project-local publishing configuration, compile the complete workspace, then pass
that output directory to compact materialization. Inspect the derived release ID,
source snapshot identity, and workload inventory before deployment projection.
Load the projected index through the provider and inspect its init-fetch plan.
Offline validation runs real parse/selection preflight against synthetic sources;
it never claims a database build. On drift, retain the failed artifacts, regenerate
from validated source using producers, and retry with the new pinned identities.
Production operation continues to require the existing admission/evidence gates.

## Scope

### In scope

- Native canonical workspace release input to the existing compact API/CLI.
- Explicit wire/schema/producer validation and complete source preservation.
- Two or more independent dbt projects, multiple workflows per project, and mixed
  dbt/non-dbt transfer workload packs already owned by the canonical workspace
  workflow inventory. Unrelated DAG union is not introduced.
- Descriptor acquisition, strict transport rewrite, immutable publication,
  deployment/index projection, provider selection and verified runtime preflight.
- Existing legacy compact and dbt wire-v1 compatibility; safe boundary diagnostics.

### Non-goals

Cross-project dbt references, new warehouse support, SQL execution certification,
activation-gate removal, production attestation generation, automatic promotion,
runtime image changes, arbitrary merging of unrelated release roots or DAGs, downstream
changes, new schema versions, new runtime flags, or publication authority changes.

### Assumptions and constraints

Use only this repository, public sources and synthetic fixtures named
project_alpha/project_beta. Do not access other repositories, tasks, credentials
or deployment artifacts. No local user paths in published evidence. No validator
monkeypatches, inferred wire versions, or generated evidence repair.

## Public contract

### CLI and Python API

Keep `dpone gitops airflow release-materialize --pack-root ... --cache-root ...`
and `materialize_compact_pack_release(...)` signatures and report shape. Existing
options, text/JSON routing, success/failure exit codes and immutable conflict
behavior remain unchanged. Extend pack-root to accept a canonical compile tree
containing `release-set.json`. A present but invalid release descriptor fails;
it must never fall back to legacy directory scanning.

| Input root | Materialization decision |
| --- | --- |
| Legacy compact layout without `release-set.json` | Existing legacy path, including supported v1 payloads |
| Canonical release-set v2 with valid dbt wire v2 | New complete workspace transformation |
| Canonical release-set v1 or release-set v2 with dbt wire v1 | Explicit unsupported-native-input blocker; use the existing documented singleton delivery path |
| Descriptor present but malformed, unknown or missing required authority | Validation failure; no fallback |

The unsupported-native-input rule does not remove existing singleton release
readers or runtime wire-v1 support. This change adds native workspace compact
input only; it does not reinterpret canonical singleton roots as legacy packs.

For native workspace input, omitted DAG filtering means the complete inventory.
An explicit filter is accepted only if it equals the complete DAG set. Reject a
proper subset instead of silently dropping projects or manufacturing a partial
source snapshot. Legacy input retains its documented DAG filtering behavior.

### Manifest/schema

Keep release-set v2, source snapshot v2 and execution-pack v2 for workspace wire
`dpone.dbt-airflow-self-service.v2`. Release schema and dbt wire are distinct axes:
wire v1 can occur in an authoritative release-set v2. Missing producer may select
only the historical legacy runtime path and cannot authorize v2 IDs/packs.
Present null, malformed, unknown or mismatched producer metadata always fails.
Release v1 plus explicit dbt wire v2 fails, including a self-consistently rehashed
artifact. Never infer the wire from payload IDs, filenames or environment values.

### Artifacts and evidence

Retain the complete producer, dbt release metadata, source snapshot and canonical
schemas. Preserve exact source-object bytes and descriptors, including media type,
size, path and digest. The strict rewrite changes DAG/pack transport bytes, so
regenerate only their descriptors/fingerprints and the derived release identity
using canonical producers. Retain validated existing provenance; add the existing
typed compact promotion marker as transport provenance. Do not overwrite caller
authority with an unchecked provenance mapping.

The source snapshot remains unchanged because sources/selections/workflow owners
remain unchanged. Rebuild the release integrity subject through its producer and
verify the resulting full tree. Old signatures/attestations on the input release
do not authorize the derived release; protected signing remains a separate gate.
Do not copy a stale integrity subject or claim the new release has the old ID.

### Compatibility and migration

Legacy roots without release-set metadata retain singleton wire-v1 transport and
its historical order handling and bounds. Invalid v2 metadata is never repaired
through this branch. Runtime continues to reject v1 dbt releases for production
trust where documented. Non-dbt packs acquire no fabricated dbt payload trio.
No old artifact is relabelled. Upgrade readers and producer together under exact
version checks, regenerate the full compile/materialize/deployment chain, and keep
the former deployment and runtime image for rollback. Rollback does not undo SQL.

## Detailed algorithm

1. Acquire input metadata through a bounded no-follow confined reader. Distinguish
   descriptor-present native input from descriptor-absent legacy input explicitly.
   Reject malformed native input, unsafe paths, unexpected files and ambiguous IDs.
2. Validate release identity, schema, producer and explicit wire. For workspace v2,
   verify the complete source tree using existing source/integrity readers before
   any output publication. Acquire a frozen byte map; later copies must match it.
3. Build detached descriptor indexes using canonical contracts. Validate complete
   DAG/workload membership and source/project/workflow ownership. Each dbt workload
   has exactly three unique references in project, manifest, selection order.
   Verify both descriptor and pack references, execution-pack identity, and the
   project's source-snapshot membership. A correct set in the wrong order fails.
4. Validate all runtime objects against canonical ID/kind/path/media/digest/size
   rules. Count unique objects once across the release. Share identical same-kind
   content safely; reject duplicate descriptor rows, conflicts and orphan payloads.
   A non-dbt workload may have no dbt references; it may not borrow another
   workload's trio. Existing dbt transfer workload ownership remains intact.
5. Apply the existing closed strict init-fetch rewrite to every admitted DAG and
   pack. Do not change execution-pack bytes, source bindings, selection IDs or
   workload ordering. Compute rewritten fingerprints through the current pack
   producer. Verify identity-bearing dbt fields before and after the rewrite.
6. Assemble a derived release retaining authoritative metadata and exact unchanged
   artifacts. Add the compact marker; derive changed descriptors and release ID.
   Regenerate its integrity subject. Independently run schema, integrity, complete
   source and workload-binding validation on the staged result.
7. Atomically publish with the immutable local publisher. Equal retry is a no-op;
   same identity with different bytes fails. No deployment pointer is changed here.
8. Project the new release into deployment/index with existing identity checks.
   The provider selects only the workload's ordered descriptors. The init-fetch
   plan retains the verified release artifact as runtime wire authority.
9. Init-fetch verifies pinned release/deployment/pack and selected bytes, produces
   the existing receipt, and passes it to the actual verified launcher. Runtime
   validates schema/wire and semantic project/manifest/selection agreement, safely
   extracts the project, and completes `VerifiedPackLauncher.prepare`. This is
   artifact identity preflight and command preparation, not dbt subprocess execution.
   A separate offline test invokes `DbtRuntimePreflight.verify` with the exact
   toolchain for real parse/ls, stopping before build/SQL. Do not fetch other
   projects' bodies for task preflight.

```text
read bounded metadata -> select explicit input mode -> verify complete source
 -> capture exact bytes -> validate ownership and ordered closure
 -> rewrite transport -> derive identity and integrity subject
 -> verify staged tree -> immutable publish
 -> deployment/index -> per-workload provider plan -> verified fetch receipt
 -> verified launcher -> real offline parse/ls preflight
any validation failure -> safe blocker, no publication/SQL authority
```

### State machine and failure semantics

Acquired -> Validated -> Staged -> Verified -> Published. Failure before Published
leaves no active release; a crash may leave only publisher-owned staging files.
An uncertain durable result is reported as uncertain, never as a validation pass;
retry the identical input through the existing publisher. Concurrent writers use
its established conflict behavior. This introduces no retry loop, checkpoint,
network backoff policy or cross-project transaction.

At runtime, Fetched -> ReceiptVerified -> SourceVerified -> PreflightPassed.
PreflightPassed is neither BuildSucceeded nor certification. Keep the current
attempt identity and retry rules; transport success cannot authorize SQL replay.

Empty native inventories, missing/null mandatory metadata, partial writes,
unsupported schema/wire and duplicate deliveries with different bytes fail.
Resource limits remain 64 unique runtime objects, 256 MiB per object and 512 MiB
release-wide, with the stricter 16 MiB manifest and 1 MiB selection readers and
existing archive expansion limits. Test exact limits and limit-plus-one; do not
multiply budgets per project. Nested source files use existing archive confinement.

## Architecture

| Component | Change | Responsibility |
| --- | --- | --- |
| `contracts.dbt_runtime_payloads` | Reuse | Explicit-wire ID, descriptor, order and bounds |
| `contracts.dbt_runtime_release_binding` | Reuse | Detached inventory and per-workload binding |
| `contracts.dbt_release` | Focused extension if needed | Release-schema/producer/wire compatibility decision |
| New canonical contract module for compact source planning | New | Pure input classification, retained metadata and rewrite invariants |
| Confined file reader port/adapter | Reuse | Bounded acquisition without symlink traversal |
| Canonical manifest compact planner | New if orchestration cannot be reused | Capture, validate and construct a derived native release |
| Existing compact readiness entrypoint | Thin integration | Dispatch native vs legacy without new domain policy |
| Source/integrity readers and immutable publisher | Reuse | Full-tree verification and publication |
| Projection/provider/init-fetch/launcher | Focused integration | Preserve and independently verify selected identity |

Keep policy in `contracts` and canonical planning in `manifest`; inject file I/O
at the composition root using the existing port. Adapters never import services;
runtime never imports services/readiness to decide identity. Compatibility entry
points delegate. Do not add an optional callback that bypasses source validation.
One integrator owns all writes; no parallel writers are needed initially.

Reject: changing only release schema (loses authority), hardcoded v2 filenames
(breaks multiple projects), sorting received v2 trios (repairs corrupt input), and
an input-release pass-through (skips the required strict rewrite). Prefer a
validated derived release despite the extra integrity pass and new release ID.

Extend the producer-owned release-set v2 schema construction to apply the existing
typed compact promotion property/guards currently applied only to v1 in
`gitops.schema_release_deployment_contracts`. Preserve v1 behavior, regenerate
schema/reference artifacts through their generators, and test missing optional
marker versus malformed or mismatched explicitly signalled marker. A schema
check without this v2 guard is insufficient. The marker changes transport identity,
not dbt selection or activation authority; no new release schema version is needed.

Amend ADR 0052 and the compact design to document this transport boundary without
changing activation authority; no unrelated architecture ADR is proposed. Enforce
`docs/benchmarks/quality_budgets.yml` without new allowlists. Existing large modules
must not grow debt; extract cohesive pure policy instead of mechanical splitting.

## Market comparison

Checked 2026-09-09 against current official documentation; unversioned web guides
are observations, not a claim about every shipped release.

| System/version | Capability and observed design | Strength / limitation for this task | Adopt / reject |
| --- | --- | --- | --- |
| Astronomer Cosmos, current OSS docs | Per-project configuration and local-node task generation; multiple projects can use separate DAGs or task groups | Clear project boundaries; the guide does not establish dpone release attestation semantics | Adopt explicit per-project ownership; reject adding dbt-loom/cross-project refs to this scope |
| gusty, current official repository | Builds Airflow DAGs from directory-based task definitions | Declarative construction; not evidence of dpone runtime artifact identity | Adopt simple declarative input; reject directory inference as release authority |
| dlt | N/A | Extraction/loading is outside the selected compact Airflow artifact boundary | No comparative claim |
| Informatica | N/A | Enterprise integration capability breadth is outside this narrow OSS wire change | No comparative claim |
| Airbyte | N/A | Connector replication is outside this transport boundary | No comparative claim |
| Fivetran | N/A | Managed delivery is outside this source-controlled compact wire | No comparative claim |
| Pentaho | N/A | General ETL orchestration is not the artifact contract being changed | No comparative claim |
| Microsoft SSIS | N/A | Package deployment is not this Airflow/dbt wire | No comparative claim |
| Apache Beam | N/A | Runner data processing is outside this artifact boundary | No comparative claim |

Sources: [Cosmos multi-project guide](https://astronomer.github.io/astronomer-cosmos/guides/multi_project/multi-project.html),
[Cosmos execution guide](https://astronomer.github.io/astronomer-cosmos/guides/run_dbt/index.html),
[gusty official repository](https://github.com/pipeline-tools/gusty).
Adoption decisions are design inferences, not documented competitor guarantees.

## Measurable differentiation

```yaml
axis: preservation of per-workload dbt identity through compact delivery
scenario: project_alpha and project_beta plus non-dbt workload
baseline: upstream f8c6a4a5e75d167829c05f65d5d3033acb193878
metric: exact selected bytes and preflight result for every workload
target: all valid cases pass; every corrupted case fails before SQL
procedure: public compile through compact, projection, provider, fetch and launcher
artifact: test_artifacts/dbt-compact-wire-v2/validation.md
limitations: no live SQL, performance superiority or production certification claim
```

## Security, privacy and operations

Diagnostics expose stable error codes, safe workload IDs and remediation; they
must not echo raw JSON, exception payloads, SQL, credentials or untrusted paths.
Test secret-shaped sentinel strings for absence from stdout, stderr and reports.
Missing/malformed metadata instructs recompilation with compatible producers;
hash or ownership drift instructs regeneration of the full pinned chain. Never
suggest hand-editing generated JSON, weakening checks or reordering v2 payloads.

## Test and certification plan

The first implementation step is a red full-path test on unchanged production
code, using two wholly synthetic projects and the real public compile service.
Do not use the existing check-overriding/monkeypatch fixtures as proof of this
path. Inject synthetic route evidence only through existing declared ports and
label it structural test input; invoke the exact installed dbt toolchain for
parse/selection. If a port is insufficient, revise the approved plan instead of
patching validators. No real credentials or SQL connections are used.

| Layer | Required cases | Expected evidence |
| --- | --- | --- |
| End-to-end red/green | Both projects through compile, compact, projection/index, provider plan, actual init-fetch and verified launcher preflight | Initial failure at compact boundary; later both launcher prepares pass, plus separately labelled real parse/ls checks |
| Composition | Multiple workflows, shared same-kind objects, repeated node IDs across projects, mixed dbt/non-dbt, complete DAG filter | Exact closure and workload isolation |
| Contract | Missing/null/malformed/mismatched producer, release schema, wire and execution schema | Closed failure, including consistently rehashed adversarial envelopes |
| Ownership | Missing/extra/duplicate/reordered/swapped/cross-project trios and descriptor/pack disagreement | Fail before runtime build and no leaked project bodies |
| Content | Bundle/manifest/selection disagreement, wrong semantic selection digest, wrong byte hash, valid hash under wrong kind | Semantic and physical identity independently checked |
| Filesystem | Traversal, absolute paths, symlinks, archive escape, mutation between acquisition and copy | Confined failure with no partial active release |
| Bounds | Zero/bool/negative/oversize sizes, actual bytes versus declared length, unique count and aggregate limits | Exact boundary and limit-plus-one assertions |
| Replay | Identical retry, concurrent conflict, partial stage, changed input on retry | Immutable publication semantics |
| Compatibility | Legacy unversioned compact/v1 wire order handling; non-dbt release; production v1 rejection | No v2 guessing, old supported behavior retained |
| UX | CLI/API parity and secret sentinel failures at actual runtime boundary | Safe diagnostics and existing exit/report contracts |
| Live certification | No approved live environment | SKIP, never PASS |
| Performance | Bounds and only selected object fetches | Resource correctness; throughput benchmark N/A |

Run focused tests first, then the change-aware selector and all required Python,
architecture, docs and offline pytest gates from AGENTS.md. Build affected
distribution packages and run twine checks before release readiness. Record exact
commit, commands, outcomes, skips and environment dependencies. A fresh-context
reviewer must inspect code, tests, compatibility, docs and evidence before the PR
is described as merge-ready. Existing separate component tests are baseline
evidence only; the required new red/preflight test remains pending approval.

## Documentation plan

Add a native compact delivery how-to linked from the workspace authoring and
Airflow provider guides. Show a synthetic two-project example and mixed workload
membership. Explain derived release identity and unchanged source identity in the
developer guide; update the compact design, ADR, compatibility/migration table,
error troubleshooting and changelog. Keep production activation restrictions
visible. Update exact toolchain prerequisites where the tutorial references them.
Do not turn source verification or preflight output into a live certification badge.

## Rollout and rollback

No feature flag or implicit upgrade. Validate on the PR's exact commit, publish
only through the established controller after separate release approval, and
regenerate artifacts with compatible pinned components. Retain previous deployment
and image. Any cross-project mismatch, false success, schema downgrade or lost
metadata blocks rollout. Live SQL and production admission remain UNVERIFIED
until their independently authorized certification completes.

## Agent execution plan

| Role | Owned paths | Read-only paths | Forbidden paths | Dependency |
| --- | --- | --- | --- | --- |
| Integrator | Scoped canonical modules, compact facade, focused tests, relevant docs, changelog/navigation | Remaining repository | Other repositories, tasks, credentials, deployment artifacts | Maintainer approval; explicit task contract |
| Explorer/architect/test/docs reviewers | None | Relevant repository source/docs/tests | All writes and external private material | Independent analysis |
| Fresh-context reviewer | None | Complete scoped diff and generated evidence | All writes | Integrated implementation and gates |

Before implementation, instantiate the repository task-contract template with
exact files and this baseline, validate it, and keep one shared-file owner. Expand
ownership explicitly if tracing identifies another necessary file. Parallel
writers, if later needed, require separate worktrees and disjoint contracts.

## Approval checklist

- [x] User problem and complete journey are defined.
- [x] Algorithm, identity, failure semantics and compatibility are explicit.
- [x] Architecture, alternatives and bounded-resource behavior are described.
- [x] Current official comparisons and measurable acceptance are scoped.
- [x] Test, documentation, rollout and ownership plans are present.
- [x] Maintainer approved implementation in this task on 2026-09-09.
- [ ] Red full-path test, implementation and green full-path evidence.
- [ ] Required gates, fresh-context implementation review and upstream PR.
- [ ] Separately authorized release readiness/publication decision.

## Implementation clarifications

Tracing established that native compile packs already use empty connection
projection and deployment-owned RuntimeConnectionContext. Reuse their existing
strict native pack rewrite; do not infer the legacy Airflow Connection bridge.
The report identifies this as `runtime_connection_context`. The provider's exact
sidecar validator is mandatory before publication. Protect CLI report output from
input/cache overlap before materialization; a report write is not part of the
immutable publication transaction. These refinements preserve existing authority.

The full-path red test also exposed sorting in both deployment artifact indexing
and provider inventory parsing. Both now retain input reference order without
inferring the wire; legacy runtime compatibility order handling remains intact.
