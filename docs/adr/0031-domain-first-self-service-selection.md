# ADR 0031: Domain-first self-service uses canonical discovery and selection

## Status

Accepted.

## Context

The original self-service layout stores pipeline sources under `pipelines/`
and orchestration declarations under shared `domains/*.yaml` catalogs. This is
compatible and expressive, but a beginner adding one pipeline must update
multiple ownership surfaces. A domain-oriented repository needs colocated
ownership and pipeline sources without introducing another runtime manifest,
another selection implementation, or a generated catalog as an editable
authority.

Filesystem discovery can also become unsafe or nondeterministic if each CLI
command recursively scans the project, follows symlinks, or computes identity
from source bytes while ignoring folder dependencies.

## Decision

`dpone.project.v1` selects exactly one project layout:

- missing `layout` or `mode: flat` preserves the existing catalog-backed
  layout;
- `mode: domain_first` uses a confined root, default `workloads`;
- pipeline ids are unique across the project in v1.

A domain-first project has these editable authorities:

```text
<root>/<domain>/ownership.yaml
<root>/<domain>/pipelines/<pipeline-id>/pipeline.yaml
```

The primary pipeline source remains the only editable pipeline authority.
`classic`, `flow`, and `folder` remain mutually exclusive authoring modes and
compile through the canonical `AuthoringCompiler`.
The scaffold persists the effective Airflow participation decision as the
boolean `metadata.airflow`. A disabled workload remains valid for static
authoring and runtime use, but it is excluded from generated DAG declarations.
An explicit first preview fails with `DPONE_AIRFLOW_DISABLED` without creating
cache artifacts. If a verified current local preview advertises only that
pipeline, dpone atomically promotes an empty immutable retirement projection
and then returns the same error. A multi-workload current is
preserved and the error provides a bounded project-preview exclusion command;
automatic retirement never removes unrelated DAGs. Eligibility captures the
exact inspected deployment id and promotion uses that id as its CAS baseline.
The empty release is universal and byte-identical across workloads because
retirement context is operational evidence, not environment-neutral release
identity.

Preview promotion injects an authoring or selection precondition into the
cache materializer. It runs while the promotion lock is held, immediately
before current-pointer commit, and is the linearization authority for the
immutable snapshot. Editable source changes after that point belong to the
next build and never trigger an observable post-commit rollback. Project
preview reruns the same bounded
selection and compares graph, report, state, and consumed-file identities, so a
new or removed workload cannot hide outside the original read set. This closes
the final check-to-promotion race without introducing a runtime-to-readiness
dependency.

`ProjectDiscoveryService` performs one bounded, exact-depth, no-follow scan. It
validates path ownership, domain and pipeline ids, ownership documents,
project-wide duplicate ids, source metadata, dependency confinement, file and
byte limits, and canonical compilation. Every observed domain-root and
pipelines-directory membership has a no-follow identity token. SQL dependencies
are recorded by lexical project-relative path and symlinks are rejected rather
than followed to a mutable target. Discovery re-observes those bounded
namespaces and all consumed-file digests before
emitting a successful snapshot. An added, removed, replaced, or retyped entry
during discovery fails with `DPONE_SELECTION_STATE_CHANGED`; it cannot produce
an incomplete successful snapshot. Unsafe paths and malformed entries are
reported as structured issues; they are never silently skipped.

The service returns an immutable snapshot containing checked compiled sources,
consumed-file digests, semantic fingerprints, ownership, logical connection
references, and issues. `ProjectSelectionLoader` converts that snapshot into
the existing canonical selection graph and DAG declarations. Individual
lookup, static check, Airflow preview, hermetic test discovery, change impact,
release materialization, and publish selection consume these same authorities.

`dpone.workload-index.v1` is an ephemeral deterministic projection of the
snapshot. It is generated for CI/change-impact use and is neither committed nor
used as an editable source of truth. The closed projection includes layout root
and scope plus the non-secret ownership fingerprint. Validation recomputes each
workload fingerprint and the project fingerprint from canonical public
material before a baseline can drive change impact. Every closed workload item
field except the digest itself participates in that identity. A copied or
partially edited fingerprint is rejected rather than trusted. Workloads,
connection references, and dependencies have canonical ordering, while source
and dependency paths plus `layout_root` must remain relative, POSIX, and
traversal-free. The CLI requires an explicit baseline for impact comparison;
omission is a usage error rather than an all-added success report.
Candidate-file comparison validates frozen bytes and truthfully reports
`current_source: candidate`, `validation_status: passed`, and
`discovery_status: not_run`. It records raw content digests separately from
semantic fingerprints. A protected approval adapter promotes only the captured
approved candidate through a project-locked operation: bootstrap uses
create-only semantics and later changes use baseline digest compare-and-swap.
The candidate path is never renamed over accepted evidence.
The promotion receipt proves the candidate and baseline identities used by the
CAS mutation; it is not an approval attestation. Approval policy, actor
identity, and protected evidence remain owned by the platform CI adapter. A
retry with the original pre-promotion guard fails closed after a successful
commit. Recovery captures the now-current baseline identity before replay.

Scaffolding validates layout, ownership, capability/recipe, locators, answer
files, and project-wide duplicate ids before writing. It then uses the existing
confined atomic scaffold applier. One project-scoped advisory lock linearizes
project, domain, and pipeline initialization; the applier's CAS still detects
external editors. The adapter uses a private no-follow external lock file
content-addressed by the absolute project root, never locks the shared parent,
and fails after a bounded wait. This identity is stable before and after a new
project directory exists. A self-service operation captures the real project
directory device and inode. Every confined writer opens the root with
`O_NOFOLLOW`, verifies that descriptor against the captured identity, and only
then traverses child descriptors. Replacing the project pathname with another
ordinary directory cannot redirect scaffold or promotion writes.
`AuthoringAuthorityGuard` binds layout classification and
`dpone.yaml` identity for project/domain changes. `DomainFirstScaffoldGuard`
binds the complete consumed-file read set, exact-depth namespace observations,
every pre-existing workload fingerprint, and the preflight project identity
for pipeline changes. Exact consumed inputs include built-in answers and the
external recipe catalog, closure, and answers. The postcondition accepts only
the exact old workload set plus the planned target, verifies all unaffected
identities, and verifies the planned bytes. A failed precondition writes
nothing; any write, final-read, or postcondition failure rolls back
scaffold-owned bytes while preserving concurrent external bytes. Repeating an
identical scaffold at the same authoritative path returns `no_op`, while the
same pipeline id in another domain fails.
Domain-first scaffolds never update
`domains/*.yaml`; flat scaffolds keep the existing behavior.

An external recipe may pin a domain as part of its declarative provenance. A
conflicting CLI `--domain` fails before writes with
`DPONE_PIPELINE_DOMAIN_MISMATCH`; the scaffold never rewrites recipe authority.

Project initialization classifies existing authority as `empty`, `flat`,
`domain_first`, `mixed`, or `unsafe` without following links. Any non-empty
layout mismatch, including a missing `dpone.yaml` beside an existing
domain-first tree, requires an explicit migration. This prevents recovery from
silently changing which files are authoritative. The same classification runs
before project selection and direct pipeline lookup, so check and preview
cannot silently hide one side of mixed authority.

The complete normalized ownership authority, including owner contact and
approver team, contributes a digest to every workload in that domain. The
index exposes only the owner team while governance-only values remain inside
the content fingerprint. Selection state uses the composite workload
fingerprint rather than the canonical pipeline semantic fingerprint alone.
Consequently ownership, folder dependencies, SQL, and durable Airflow
participation all affect `state:modified`. Single-pipeline preview binds this
same workload fingerprint into release identity and provenance.

This ADR supersedes ADR 0029 only where its explicit Airflow override skipped
all project configuration reads. An override still selects the Airflow boolean,
but malformed project layout/configuration now fails closed because layout is
an independent authoring authority.

The Airflow scheduler does not perform repository discovery. It continues to
read immutable release/deployment indexes through the parse-safe provider.

The public `dpone.workload-index.v1` field vocabulary belongs to the neutral
`dpone.contracts.workload_index` module. Canonical discovery identity fields
and resource budgets belong to `dpone.contracts.project_discovery`; the
ephemeral projection re-exports compatible names but does not own those
semantics. Generic authority classification, discovery snapshots, and
fingerprint algorithms belong to `dpone.manifest`. Closed serialization validation and change-impact
result contracts belong to `dpone.services.workload_index_contract`; projection
orchestration belongs to `dpone.services.workload_discovery_projection`.
Filesystem serialization uses ports for the project lock and workload-index
baseline store, POSIX implementations in `dpone.adapters`, and explicit
composition roots. `dpone.ports.workload_index_baseline_store` owns the minimal
promotion capability and commit-state result. Bounded parsing remains in
`dpone.manifest.workload_index_io`; the POSIX hard-link, fsync, and atomic
replacement implementation lives in
`dpone.adapters.workload_index_baseline_store`; and
`dpone.readiness.workload_index_promotion_composition` captures one project-root
identity and injects the store and project lock into the application service.
This keeps domain decisions independent of operating-system locking and command
adapters.

Confined scaffold rollback records every directory created by the transaction
with its device and inode. It removes only the same still-empty directories,
deepest first, after owned files are removed; replaced or externally populated
directories are preserved. Cache promotion evaluates the final
authoring/selection precondition under the promotion lock before pointer commit.
Failure leaves the current activation unchanged. A successful pointer commit is
final for that immutable snapshot; later authoring changes belong to the next
build and never trigger post-commit compensation.
If a write fails before a file ownership receipt exists, directory rollback
outcomes are attached to the scaffold failure receipt. Atomic loader and
workload-index replacement never unlink a displaced recovery leaf while
cleanup remains unverified; the structured error exposes every surviving
project-relative recovery artifact.

## Consequences

- A beginner can add a domain and pipeline without editing a shared catalog.
- Flat projects and their generated artifacts remain backward compatible.
- Project-wide check, preview, build, and publish cannot disagree about which
  domain-first pipelines exist.
- Folder dependency changes affect semantic identity even when the root source
  file is unchanged.
- Symlinks, unexpected depth, duplicate ids, and partial directories fail
  closed rather than disappearing from selection.
- Domain-first v1 generates one default Airflow DAG per workload. Rich
  multi-workload DAG composition remains an explicit orchestration concern and
  is not inferred from directory structure.
- Moving between layouts is an explicit migration; dpone does not rewrite
  authoring paths automatically.
- Existing flat or domain-first authoring blocks implicit creation of a
  conflicting project configuration; mixed and unsafe trees fail closed.
- Workload-index baselines are self-verifying closed projections. They are not
  signed attestations and remain project-confined CI inputs.
- Approved workload-index candidates are promoted only by a supported
  raw-digest and semantic-fingerprint-bound CAS operation. Bootstrap is
  create-only; a shell rename is not a promotion boundary.
- Disabling Airflow retires a matching single-workload local preview through an
  immutable byte-identical retirement bound to the exact inspected current
  deployment. Multi-workload current state is never implicitly cleared and
  requires an explicit bounded refresh.

## References

- [Domain-first Airflow project](../getting-started/domain-first-airflow.md)
- [Domain-first discovery and CI](../domain-first-discovery-ci.md)
- [Domain-first operations and recovery](../domain-first-operations.md)
- [ADR 0006: Authority and canonical IR](0006-authority-and-canonical-ir.md)
- [ADR 0015: Single authoring compiler](0015-single-authoring-compiler.md)
- [ADR 0017: Canonical workload selection](0017-canonical-workload-selection.md)
- [ADR 0023: Confined authoring transactions](0023-confined-authoring-transaction-semantics.md)
- [ADR 0029: Canonical pipeline identity](0029-canonical-pipeline-identity-and-reference.md)
