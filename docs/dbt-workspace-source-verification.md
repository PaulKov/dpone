# Workspace source verification

This page describes **unreleased reader support**, not an available end-to-end
workspace publishing feature. The single-project compiler still emits dbt wire
v1. Workspace discovery and aggregate compilation are implemented locally;
release publication, production integration, consumer rollout and live
certification remain pending under the
[approved multi-project design](feature-design-dbt-multi-project-release.md).
Do not hand-edit a compiled singleton release to enable workspace delivery.

## Purpose and boundaries

A successful workflow in project A must not certify missing sources or a failed
workflow in project B. The workspace source reader checks the complete desired
inventory before constructing DEV evidence expectations. It does not select
projects by Git diff: unchanged publishing projects remain in the release.

SQL, resolved packages, manifests and selection locks belong to immutable release
artifacts. The runtime image supplies the certified execution toolchain. A
source-only change must not require a toolchain image rebuild.

| Boundary | Required check |
| --- | --- |
| Release metadata | Exact v2 producer wire, release identity, selection authority and source fingerprint |
| Source inventory | Canonical project paths, unique global workflow/DAG/workload IDs and exact membership |
| Runtime descriptors | Content-addressed ID, kind, path, media type, digest and length; no missing, orphan or duplicate descriptors |
| Complete release tree | Canonical descriptor paths only, one source snapshot and release descriptor; no extra files, directories, aliases or links; every descriptor including schemas matches actual bytes |
| Project source | Confined byte reads, safe archive extraction, verified extracted tree and matching project/manifest names |
| Selection | Manifest graph fingerprint, complete expected results, admitted SQL Server policy and logical target |
| Workflows | Execution-pack identity, exact ordered source trio, DAG ownership, transfer coverage and project-local graph ownership |
| DEV request | Every verified workflow receives its own campaign run identity; an incomplete source tree prevents request creation |

The reader does **not** verify a cryptographic signature, certify a database
route, execute dbt, or resolve physical production targets. Release schema,
whole-tree integrity, attestation and environment-bound collision checks remain
mandatory independent gates. Source verification is not permission to promote.

## Platform integration

The composition root supplies the same release reader to campaign request
creation and evidence verification. Existing v1 expectations and report shapes
are preserved. V2 requires the complete `_dbt/dbt-source-snapshot.json`; there is
no fallback to the current checkout or to one project's successful evidence.

For application code that already has a compiled candidate and its expected
release identity:

```python
from pathlib import Path

from dpone.app.dbt_promotion_composition import build_dbt_expected_release_loader


def inspect_candidate(compiled_root: Path, expected_release_id: str) -> tuple[str, ...]:
    expected = build_dbt_expected_release_loader()(compiled_root, expected_release_id)
    return tuple(sorted(expected.dbt_workflows))
```

The existing `dbt prepare-dev-evidence-request` command uses this composition.
Its flags and output schema are unchanged. This is reader readiness; it does not
mean the locally implemented `dbt workspace compile` command has been released.

Lower-level callers can inject bundle operations and a bounded file reader into
`DbtReleaseSourceReader`. All release/DAG/workload/source file reads use that
capability; bundle extraction is performed by the injected bundle operations in
an owned temporary directory. Neither path runs a subprocess or reads database
credentials.

Internally, one immutable metadata index supplies canonical paths, descriptor
limits and workload membership to both the file-tree verifier and the pure
source plan. Nested references are detached from mutable input mappings. The
plan owns graph, target and complete-project policy; it never reads files or
imports the Airflow/dbt SDK. The service verifies actual bytes and framework
fingerprints before interpreting them. Every reread is hash-checked even if an
earlier tree pass succeeded. Projects and transfer manifests are processed one
at a time, retaining compact observations rather than all source bytes.

## Runtime behavior and compatibility

### Cache installation is not activation

The application factory `build_dbt_release_materializer()` in
`dpone.app.dbt_publish_composition` installs a complete workspace candidate under
its release content address. It explicitly dispatches the validated producer
wire, preserves the source snapshot and checksum subject, and requires the exact
installed dpone version. Unknown producers, incomplete sources and changed
descriptor bytes fail before cache publication. Repeating the same installation
returns `no_op: true` without replacing existing files.
Generic release-v1 cannot carry a workspace-v2 producer: changing the outer schema
and rehashing the metadata is rejected, not treated as legacy activation.

Before retaining artifact bytes, the installer validates the canonical metadata
index, per-kind acquisition limits and the existing complete-tree integrity
budget (50,000 files / 2 GiB, excluding the checksum subject). It verifies source
closure again against a private stage made from exactly the captured bytes.
Direct low-level callers must inject a complete-source reader for v2; omitting it
does not fall back to singleton validation. The v1 constructor and layout remain
compatible.

Local projection validation can read the installed candidate, but **workspace
activation is unavailable** until the protected physical-target admission,
runtime reservation and finalizer are implemented and certified. Ordinary
promotion, recovery and audit restoration reject v2 with
`DPONE_DBT_WORKSPACE_ADMISSION_UNAVAILABLE`. The decision uses the producer wire
from the same integrity-checked release, not a caller flag or deployment name.
The prior current symlink, pointer and audit remain unchanged. A sealed inactive
snapshot may remain for inspection; do not mistake it for an active deployment.

On this error, keep the existing deployment and let the platform finish physical
certification. Repeated retries or a renamed domain cannot supply admission.
Do not edit generated pointers, change the wire to v1 or disable checks. This
cache guard does not certify direct runtime entrypoints or live database writes.

### Per-task source compatibility

Per-task runtime reads the wire from its already verified release artifact. It
fetches only its selected project, manifest and selection lock, not other
projects' SQL or the workspace sidecar. V2 checks the release workload's exact
ordered references and canonical descriptors before executing the workload.
Consistently rehashing an invalid path, media type or foreign workload reference
does not make it valid.

Historical unversioned compact runtime data remains v1-only. Legacy v1
order-only repair is preserved; v2 never repairs reordered references. An
unknown or malformed explicit producer is rejected. Exact producer/provider
version gates are independent and are not relaxed by wire compatibility.
The pure runtime contract owns that legacy plan-order compatibility; the launcher
still performs strict execution-pack and source-byte identity validation after
applying it. The public identity validator does not repair order by default.

Bounds remain release-wide: 64 unique runtime payloads, 256 MiB per payload,
512 MiB aggregate. Manifest JSON retains the 16 MiB acquisition bound;
selection JSON retains the narrower 1 MiB evidence-reader
bound. Source inventory is at most 1 MiB. Archive expansion and confinement use
the existing bundle policy.

## Aggregate producer boundary (unreleased)

The reusable project projector captures a bundle and resolves each workflow once,
then constructs its packs using the final versioned payload IDs. The workspace
assembler requires projections for **every** publishing project from the complete
successful check. It rejects missing projects, global identity collisions,
conflicting route receipts and excess resources before publication. Reversing
project order does not change the release.

The pure producer contracts own captured source/projection snapshots and release
metadata assembly. They do not import Airflow, dbt, or application services.
The service verifies every actual Airflow pack with the framework verifier and
supplies an exact workload-to-fingerprint map; missing or extra entries fail
closed. Schema acquisition and schema validation remain service responsibilities.
Full checked-report identity is rechecked after external builders and verifiers,
so changed inputs cannot reuse previously captured certification receipts.
The assembled canonical file map must match the source plan's index exactly,
and every body is checked against its descriptor before staging.

Legacy selection fingerprinting retains repeated selection entries; workspace
fingerprinting normalizes them. Both sort selection entries for hashing.
Other descriptor/provenance arrays retain their established order. Reordering
route receipts changes provenance bytes but not the selection fingerprint or
release ID; provenance is deliberately excluded from the latter.

Workspace output has only `release-set.json`, `_dbt/dbt-source-snapshot.json`,
canonical descriptor paths under `packs/`, `dags/`, `runtime/dbt/objects/`,
`schemas/dbt/`, and the fixed `release-subjects.sha256` integrity subject.
Transfer packs contain their own workload/runtime YAML; loose intermediate YAML,
singleton aliases and singleton compile-evidence reports are deliberately absent.
The v1 command retains its original layout and bytes.

`DbtWorkspaceArtifactWriter` rechecks confined manifest snapshots, projects every
checked project, validates an owned temporary tree with the complete source reader,
and only then generates/verifies the checksum subject and publishes once. A failed
project or source check leaves the destination untouched. Identical output is an
idempotent retry; different existing output is a conflict, not an overwrite.
The subject is allowed as a fixed transport artifact, never as authority to add
other files. This is not a database transaction, signature or live certification.

The workspace CLI compile command is implemented locally but unreleased;
environment-bound physical target preflight and consumer activation remain
pending. Non-executable semantic-refresh templates
remain on their existing singleton path; workspace projection rejects them before
capturing source rather than silently dropping their missing DAGs.

## Failure and recovery

If verification fails, do not drop the affected project, recreate a sidecar from
the checkout, or mark an unavailable observation successful. Restore the exact
immutable candidate from its artifact source, or compile a new complete release
once workspace compilation is available. Changed executable or identity-bound
source bytes require a new release identity and fresh acceptance. Provenance-only
changes may preserve that identity, but still change transport bytes: regenerate
their integrity/signature evidence rather than reusing a checksum for old bytes.

Cross-project repeated dbt node IDs are allowed: node ownership is project-local.
Within one project, two workflows cannot share published models or conflicting
materialized closures. Merge the workflows or make their ownership disjoint;
do not rename an existing Airflow DAG to bypass the check.

Every selected model's final target, the pinned SQL Server adapter's
temporary/backup/helper-view slots and unit-test temporary relations participate
in literal collision checking across projects. Generated transfer targets are read from the exact
embedded workload manifest, not from untrusted loose YAML or the current Git
checkout. The archive must contain precisely its canonical regular manifest
member (at most 1 MiB), with matching archive bytes/hash, canonical base64 and no
extra member, link or PAX metadata. The expanded archive, including metadata, is
bounded before TAR parsing. A candidate with consistently recomputed hashes still
fails if two owners claim the same logical write coordinates.

The reader returns these declared writes alongside verified source membership.
Different aliases or identifier spelling remain unresolved until platform
binding proves actual physical separation under the database's comparison
rules. Passing source verification is not that physical proof.
The adapter's dynamic dependent-view deletion also requires environment-bound
verification; the fixed-name inventory cannot prove that cascade safe.

The reader makes no production writes. Runtime retries and deployment rollback
must still retain the original release and deployment identities; restoring code
does not undo committed data. See [promotion and rollback](dbt-self-service-promotion.md).

## Audit-mirror transaction groundwork

Unreleased mirror internals can stage the complete workspace as one subtree,
alongside its source snapshot and promotion descriptor. They reuse the existing
rollback journal with at most three replacements, regardless of project count.
Project B must finish staging and verification before any active output changes.
An exception rolls all outputs back; a process crash leaves the journal for the
next installing transaction to recover. This is serialized, recoverable
installation, not atomic visibility for arbitrary filesystem readers.
Recovery restores output content, not an exact directory-topology snapshot:
empty metadata parent directories may remain after interrupted bootstrap. It
does not remove existing empty directories based on a guess about their owner.

Before staging, `DbtSourceInventory.bind_project_bundles()` requires exactly the
inventory's projects, immutable nonempty archive bytes, matching content hashes,
and the existing per-object and aggregate resource limits. Shared archive bytes
are charged once. It returns a detached, read-only map in inventory order;
`project_directories` includes the structural parents needed for that inventory.
This pure binding does not inspect archive contents. The mirror service still
performs actual extraction, bundle verification and no-follow tree checks.

The same exclusive repository lock fences installers and verifiers. A read-only
verifier refuses a pending journal and tells the operator to recover it; acquiring
that read lock never restores or deletes repository files. The lock file itself
is held in the operating-system temporary directory. Do not manually delete a
journal or its backups to make verification pass.

Workspace comparisons reject unlisted projects, stray files, ignored/generated
directories, missing files, symlinks and executable audit files. A byte-identical
Git checkout with ordinary non-executable file permissions remains valid.
Runtime extraction retains its stricter private-file permissions; verification
does not change either tree's permissions. Destination paths are pairwise
disjoint, including ancestor and case collisions, with no symlink parents.

The [v3 descriptor schema](schemas/dbt/dpone.dbt-prod-promotion.v3.schema.json)
requires the release, complete source snapshot, mirror layout, DEV deployment,
evidence subject and producer identities, plus both campaign identity fields.
Its fingerprint binds every field. Parsing a valid descriptor does **not**
authenticate its evidence: signatures and independently verified expected DEV
identities remain mandatory promotion gates. V1/v2 singleton descriptors cannot
be interpreted as v3.

The [workspace promotion commands](dbt-workspace-promotion.md) now compose these
primitives with ownership/bootstrap validation and complete per-project reports.
They remain unreleased and do not authorize production activation or certify a
live route. Do not bypass the service by calling the low-level transaction to
adopt or overwrite an author-owned directory.

## Validation and rollout

Focused tests cover real project bundle extraction and embedded execution packs,
two-project source isolation, missing/tampered/symlinked project B artifacts,
rehashed semantic drift, graph ownership, real CLI request creation, and v1
compatibility. They use fixture manifests and do not certify a live SQL Server
or Airflow environment.

Additional offline tests finalize both projects' provider evidence with transfers
and terminal outcomes, and check specific semantic rejection reasons for missing
or substituted project B evidence. Mirror tests inject failure after every backup
and install rename, exercise crash recovery, project removal, idempotency,
read/write contention, exact tree membership and ordinary Git file permissions.

Reader support must be followed by aggregate producer/schema support, complete
promotion/mirror verification, exact runtime rollout and scoped two-project live
acceptance. Only then remove the consumer's single-project guard. The broader
CI latency objective still requires its own comparable production measurements.
