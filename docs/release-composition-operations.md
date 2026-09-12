# Operate and recover composed releases

This runbook is for operators and platform engineers who have a verified composed
release. Begin with the [composition guide](release-composition.md) and retain
its producer reports. The [contract reference](release-composition-reference.md)
explains source authority, limits, and CLI/API status semantics.

## Observe the result

Retain the composition report, exact producer version, parent release ID, native
child release ID, ordinary inventory digest, and integrity subject together.
Keep the original approved composition request for reproducible rebuilds. Store
reports outside the immutable artifact tree. The parent contains the source
artifacts needed for independent source revalidation; it does not depend on
mutable original authoring directories during later cache installation.

A successful `release-compose` report establishes verified durable artifact
publication. It does not establish that a registry upload succeeded, a deployment
was activated, an Airflow task ran, or a table received data. Check the relevant
producer's report at each later boundary. Keep offline test status and live
certification status separate.

## Install the complete parent into a cache

Use the same sidecar used to compose the parent:

```bash
dpone gitops airflow release-materialize \
  --pack-root composed-release --cache-root .dpone-cache \
  --xcom-sidecar-image "$XCOM_SIDECAR_IMAGE" --format json \
  > reports/composed-cache-install.json
```

For a v3 input, materialization recaptures the complete composition, reconstructs
both source views, and verifies them before immutable cache installation. It
preserves the parent identity and transport; it does not rewrite the sidecar.
Omit `--dag-id` to install the full release. An explicitly selected subset cannot
remove a constituent's DAGs. The cache destination is
`.dpone-cache/releases/sha256-<parent digest>`, as reported by `release_dir`.

Check exit status and `passed`. The existing `release-materialize` command uses
its own materialization report and blocker exit convention; the composition
command's exit `3` contract does not replace that command's interface. On an
uncertain materialization result, inspect its blockers and the visible cache
destination before retrying. See [compact delivery](dbt-compact-delivery.md)
for the existing materializer interface.

## Prepare deployment and runtime delivery

Bind logical connections from **all** constituents in the platform-owned
binding set and runtime connection configuration. Source aliases do not create
credentials or physical target admission. Use the parent `release_id` when
building the deployment/index with `dpone airflow build`, following the
[compact deployment projection recipe](dbt-compact-delivery.md#project-and-verify-the-runtime-path).
Use approved digest-pinned runtime images, registry configuration, and Airflow
bundle identity. The synthetic image in the first-success tutorial must not be
used as a production runtime image.

Publish and deliver every registered parent artifact, including
`artifacts.composition_sources`. Supply the generated index through the normal
[Airflow provider](airflow-pack-provider.md). Its init-fetch plan, deployment
identity, trust policy, and READY receipt retain their existing checks. A child
signature or child-only artifact upload does not authenticate the parent.

For a selected native workload, composition ownership resolves its native child
and checks parent/child descriptor equality and the ordered project/manifest/
selection trio before the unchanged native runtime verification. The
`VerifiedPackLauncher` rechecks selected artifacts after READY. For ordinary
workloads, the verified strict transfer bootstrap remains the execution entrypoint.
Do not write custom init-fetch plans, reorder trios, or weaken readers to accept
missing source artifacts.

Public composition activation is the shipped
[activation contract](composition-activation-contract.md) factory and the
operator how-to
[Operate Kubernetes composition supervision](guides/composition-supervisor-kubernetes.md).
Cache-sync and desired-state require `--workspace-authority-connection-ref`.
Authenticated v3 pack-exec reaches the native dbt parent worker when parent
context exists, the ordinary transfer worker when that context and
`DPONE_CACHE_ROOT` reopen the sealed plan, and the ClickHouse worker when those
plus the sealed snapshot sidecar and enrolled supervisor/HTTP collaborators
compose. Missing ClickHouse originals fail-close with
`composition_ordinary_worker_unavailable`. Supplying a native-only workspace
activation coordinator does not grant v3 authority. Local projection, cache
installation, parse/selection checks, and successful artifact verification are
not SQL execution and are not route certification. Live three-cell execution
remains `UNVERIFIED`, never `PASS`.

## Diagnose a failed build or installation

| Observation | Check | Recovery |
|---|---|---|
| `DPONE_COMPOSITION_MANIFEST_INVALID` | Closed manifest shape, local roots, required pins, and transport profile | Generate the request from successful producer reports and correct the authoring manifest |
| `DPONE_COMPOSITION_INVENTORY_INVALID` | Complete ordinary root and supported plain transfer capability | Rebuild with public pack/reconcile producers; inspect producer blockers and the supported closure reference |
| Ordinary dependency, archive, or producer mismatch | Source pins, referenced SQL files, runtime manifest, and all bootstrap projections | Regenerate the entire ordinary pack; do not edit generated hashes or producer fields |
| Expected inventory or native identity differs | Whether authoring or producer version changed after pins were recorded | Review the change, produce new reports, and explicitly pin the new verified inputs |
| Ownership or logical target collision | DAG IDs, workload IDs, paths, memberships, and declared write coordinates across both inputs | Correct authoring/publishing policy; rebuild both affected inventories; do not splice generated DAGs |
| Source/output overlap or orphan files | Root layout and report placement | Use separate roots and a fresh complete reconcile destination; keep reports outside artifact roots |
| Count or byte limit exceeded | Original source files, expanded archives, parent artifacts, and source sidecars | Reduce the complete workload scope through authoring or keep independent releases; do not omit required closure |
| Existing immutable destination differs | Existing release descriptor and requested input pins | Retain the old release and choose a new destination for changed inputs |
| Composition cache installation rejected | Full source sidecars, integrity subject, unchanged sidecar, and complete DAG selection | Restore the exact complete parent or regenerate it; repeat source admission through `release-materialize` |
| Unknown release schema in a consumer | Installed core/provider/runtime versions | Upgrade compatible readers first; never relabel v3 as v1/v2 |
| `composition_native_worker_unavailable` | Parent context or native factory missing at pack-exec | Restore activation identity and workspace authority; see the [supervisor how-to](guides/composition-supervisor-kubernetes.md) |
| `composition_ordinary_worker_unavailable` | ClickHouse pack-exec, or ordinary pack-exec missing cache/plan/parent | Restore `DPONE_CACHE_ROOT` and parent identity; do not treat as a three-cell campaign pass |
| SQL `control_schema_reference` or `login_gate_schema_reference` | Generated CHECK reference and the exact DDL producer | Follow the [controlled catalog capture procedure](composition-shared-sql-storage.md#catalog-reference-and-controlled-installation); retain failed capture evidence and rerun the new committed source after generation |
| SQL `shared_transaction_identity` or trust `trust_ledger_lock` | Whether a callback closed, replaced or invalidated the protected transaction | Roll back the caller-owned transaction, retain uncertainty and recover from original records; reacquiring the same lock does not validate earlier observations |

Diagnostics intentionally avoid source contents and credentials. Inspect your
local producer reports and reviewed source declarations instead of adding raw
connection data to error reports.

## Retry and durability uncertainty

For a normal rejection, no successful publication has been established. Correct
the input or storage problem and repeat with reviewed pins. Existing immutable
bytes are never an overwrite target.

For `release-compose` exit `3`, `status: durability_uncertain`, the complete
release is already visible but its directory durability is unconfirmed. Retain
the report and visible release ID. Do not delete that tree, infer rollback, mark
it passed, or launch work because the files happen to exist. Recover the storage
condition, then retry the same request, exact output directory, producer version,
and input bytes. Only a successful verification/publication report closes this
incident. If the authoring sources have changed, first restore the pinned source
inputs or prepare a separately reviewed new release.

Repeated identical requests are idempotent at the artifact boundary. Competing
writers with different bytes cannot replace the existing immutable destination.
The producer uses private temporary stages and cleans them up; source trees are
not repaired or modified during verification. Registry transport and cache
installation are separate operations with their own reports, not one transaction
with the initial output publication.

## Rollback and reader-first migration

1. Record the currently active release/deployment IDs and retain their complete
   artifact trees and environment bindings.
2. Upgrade core, provider, init-fetch runtime, registry/cache consumers, and
   validation tooling to compatible composition-aware versions. Verify that old
   v1/v2 releases remain readable before changing producer inputs.
3. Compile the entire native workspace and materialize its supported compact
   transport. Reconcile the independent plain transfer catalog into a fresh root.
4. Capture the ordinary inventory, review both pins, compose, and install the
   complete parent. Build the deployment/index against the parent identity.
5. Exercise offline delivery and negative integrity tests. Keep activation
   blocked until the required combined physical-target admission is available.

A legacy root containing dbt wire-v2 payloads is rejected before publication.
Migrate it by regenerating the canonical native workspace and composing it
explicitly. Do not insert producer metadata, disable verification, or change
schema labels. Native-only evidence, mirror, and promotion tools operate on their
native child contract; a composed parent is not a replacement native input.

For a rollback, use the platform's existing authorized deployment recovery
procedure with the retained old release and bindings. Do not overwrite a composed
directory or use a native child as if it represented the whole parent. Artifact
rollback does not undo SQL already committed by a previously active deployment.

## Verify changes without claiming live certification

From a dpone source checkout, the focused offline suites are:

```bash
uv run pytest \
  tests/test_release_composition_ordinary.py \
  tests/test_release_composition_policy.py \
  tests/test_release_composition_legacy_boundary.py \
  tests/test_release_composition_cli.py \
  tests/test_release_composition_delivery.py -q
```

They cover supported source admission, ownership and closure, legacy rejection,
CLI/service behavior, and delivery contracts. Read the actual pytest report;
this command listing is not a recorded passing result. Missing optional dbt
parse/selection tooling is `SKIP`, not a pass. Use the repository's
[testing guide](testing/overview.md) and exact release gates for integration and
release decisions. Database execution, reconciliation, physical target admission,
and live route certification require a separately approved environment and
current evidence. Return to the [composition guide](release-composition.md) for
an artifact-only first run.
