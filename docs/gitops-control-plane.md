# GitOps control plane

`dpone gitops affected`, `dpone gitops bundle`, `dpone gitops plan`, and
`dpone gitops verify` turn manifest dependency metadata into reviewable runner
contracts for sparse checkout systems.

Use this layer when Airflow, `KubernetesPodOperator`, GitHub Actions, Dagster,
Argo, or a shell runner should fetch only the files required for one workload
and then run a deterministic dpone command.

The GitOps control plane does not execute manifests, call Git, import Airflow,
open database connections, or mutate runtime state. It builds and verifies
plain JSON or Markdown artifacts.

## Quickstart

Create a plan:

```bash
dpone gitops plan dpone_workloads/manifests/mssql/orders.yaml \
  --include-env-overrides dev \
  --include-registry \
  --support-path dpone_workloads/sql/orders/ \
  --runner kubernetes_pod_operator \
  --run-command "dpone run dpone_workloads/manifests/mssql/orders.yaml" \
  --output .dpone/gitops/orders/gitops_plan.json
```

Verify a sparse worktree before the runner executes:

```bash
dpone gitops verify .dpone/gitops/orders/gitops_plan.json \
  --worktree . \
  --verify-lock \
  --output .dpone/gitops/orders/gitops_verify.json
```

Both commands print to stdout and can also write the same artifact to
`--output`. Use `--format markdown` for human review comments and `--format
json` for CI gates.

Find impacted manifests from a Git diff:

```bash
dpone gitops affected \
  --from-ref origin/master \
  --to-ref HEAD \
  --include-env-overrides dev \
  --include-registry \
  --support-path dpone_workloads/sql/orders/ \
  --runner kubernetes_pod_operator \
  --emit-plans \
  --output .dpone/gitops/affected.json
```

`dpone gitops affected` scans manifest entrypoints under the workload root,
builds their sparse-path reports, and returns only the manifests whose
allowlists intersect the changed repo-relative files. With `--emit-plans`, it
writes a `gitops_plan.json` artifact for every impacted manifest under
`.dpone/gitops/affected/`.

Changed files can come from `--changed-files`, a newline-delimited
`--changed-files-file`, or `git diff --name-only` via `--from-ref` and
`--to-ref`. The inputs are deduplicated in first-seen order before impact
analysis.

Build a complete scheduler handoff bundle:

```bash
dpone gitops bundle \
  --from-ref origin/master \
  --to-ref HEAD \
  --include-env-overrides dev \
  --include-registry \
  --support-path dpone_workloads/sql/orders/ \
  --runner kubernetes_pod_operator \
  --policy-profile release \
  --attest \
  --output-dir .dpone/gitops/bundle \
  --output .dpone/gitops/bundle.json
```

`dpone gitops bundle` is the highest-level GitOps control-plane command. It
uses the same changed-file inputs as `dpone gitops affected`, emits a
`gitops.plan` for every impacted manifest, verifies each emitted plan, and
writes one deterministic handoff directory:

```text
.dpone/gitops/bundle/
  affected.json
  bundle.json
  summary.md
  manifests/<manifest-slug>/gitops_plan.json
  manifests/<manifest-slug>/gitops_verify.json
```

Use the policy flags when CI should fail closed before a scheduler starts:

- `--policy-profile advisory` keeps the bundle informational.
- `--policy-profile pr` enables lock verification and empty-impact blocking
  for pull-request gates while keeping warnings reviewable.
- `--policy-profile release` enables lock verification, empty-impact blocking,
  warning blocking, and required lock verification for release gates.
- `--fail-on-empty-impact` returns an `empty_impact` blocker when the changed
  files do not map to any manifest.
- `--fail-on-warnings` returns a `warnings_present` blocker when dependency
  discovery, changed-file resolution, or verification produced warnings.
- `--require-lock` requires `--verify-lock` and returns
  `lock_verification_required` if lock verification is not enabled.
- `--attest` embeds a SHA-256 attestation in `bundle.json`.

The bundle JSON is safe to store as a CI artifact or pass to Airflow,
`KubernetesPodOperator`, GitHub Actions, Dagster, Argo, or a shell runner. It
contains repo-relative paths only: `output_dir`, `affected_path`,
`summary_path`, per-manifest `plan_path`, per-manifest `verify_path`,
`warnings`, `blockers`, the active policy profile, and optional attestation
evidence.

When `--attest` is enabled, `bundle.json.attestation` contains:

- `schema_version`, `producer`, and `hash_algorithm`;
- sanitized provenance such as `from_ref`, `to_ref`, changed files, runner,
  policy profile, and output directory;
- artifact digests for `affected.json`, each `gitops_plan.json`, each
  `gitops_verify.json`, and `summary.md`;
- `bundle_digest`, a stable SHA-256 digest over the artifact digest records.

The attestation intentionally excludes `bundle.json` from the digest set to
avoid circular self-hashing. Store `bundle.json` with runner logs and release
evidence so downstream systems can prove which files were planned and verified.

Verify a stored bundle before a scheduler or release finalizer consumes it:

```bash
dpone gitops bundle verify .dpone/gitops/bundle/bundle.json \
  --require-attestation \
  --format json
```

`dpone gitops bundle verify` is an offline integrity check. It reads the
repo-relative `bundle.json`, validates the public bundle contract, recomputes
attested artifact SHA-256 digests and byte sizes, recomputes `bundle_digest`,
and returns blockers when any artifact is missing or drifted. Use
`--require-attestation` for release gates where a bundle without digest evidence
must fail closed. Without `--require-attestation`, missing attestation is a
warning so older advisory bundles can still be inspected.

Public JSON Schema files are shipped in `docs/schemas/gitops/` and listed in
the generated [GitOps schema catalog](reference/gitops-schema-catalog.md).
Validate catalog entries and payloads without Airflow or runtime imports:

```bash
dpone gitops schema list
dpone gitops schema show dpone.release-set.v1
dpone gitops schema validate --kind dpone.safe-sample-runtime-run.v1 --payload runtime-run.json
```

`dpone.release-set.v1` keeps `artifacts.runtime_payloads` optional for backward
compatibility. When present, it is a bounded inventory of 1–64 external dbt
runtime files. The compact-v1 producer additionally limits the distinct
release-wide inventory to 512 MiB of actual bytes; duplicate references count
once, and equality with the limit passes. Every entry is closed and contains
`id`, `kind`, confined
release-relative `path`, hexadecimal `sha256:...`, positive `bytes` (at most
256 MiB), and `media_type`:

```json
{
  "id": "dbt_manifest",
  "kind": "dbt_manifest",
  "path": "runtime/dbt/manifest.json",
  "sha256": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
  "bytes": 1024,
  "media_type": "application/vnd.dbt.manifest+json"
}
```

Legacy v1 releases without this optional section remain valid. If a compact
pack declares runtime payload IDs, the producer must materialize and inventory
the matching bytes. This is compatibility transport evidence only: protected
dbt compile, promotion, selection/certification authority, and production
evidence still require `dpone.release-set.v2`. The strict production projection
rejects v1 releases that carry dbt runtime
payloads. Rebuild them through the canonical
[`dpone dbt compile`](dbt-self-service-reference.md#command-contract)
journey; do not promote a compact v1 inventory by hand. IDs and derived paths use only
ASCII letters, digits, underscore, dot, and hyphen; workflow IDs containing
colon or path-control characters are rejected before filesystem access. A dbt
compact pack supports exactly this ordered producer input:

| ID | Derived release path | Media type |
| --- | --- | --- |
| `dbt_project` | `runtime/dbt/project.tar.gz` | `application/vnd.dpone.dbt-project-bundle+gzip` |
| `dbt_manifest` | `runtime/dbt/manifest.json` | `application/vnd.dbt.manifest+json` |
| `dbt_selection_<workflow>` | `runtime/dbt/<workflow>.selection-lock.json` | `application/vnd.dpone.dbt-selection-lock+json` |

The three IDs must appear in that order. `<workflow>` is 1–235 ASCII
letters/digits/`_`/`.`/`-`, keeping the derived leaf within the portable
255-byte filename limit. The public release-set schema intentionally accepts a
broader bounded consumer inventory; `release-materialize` is the narrower
producer above and rejects unsupported IDs rather than inventing locators. More
than 64 unique payloads, any payload larger than 256 MiB, or more than 512 MiB
of actual bytes across the distinct compact-v1 inventory fails before the
release is accepted, as do missing, duplicate, unsafe, or digest-mismatched
descriptors. The total is computed from the same confined bytes used for each
descriptor; it cannot be raised by a CLI flag. Repair the producer inputs and
build a new immutable release-set rather than hand-editing its generated JSON.

Strict projection reports `DPONE_RELEASE_ARTIFACT_BYTES_MISMATCH` when the
descriptor's declared `bytes` differs from the confined file actually read.
Treat it as immutable producer drift: rebuild the release-set and bytes from the
same producer; never normalize or hand-edit the claim. A non-positive or
non-integer claim is rejected defensively as
`DPONE_RELEASE_ARTIFACT_BYTES_INVALID`.

`release-materialize --format json` writes one `GitOpsView` report to stdout
(and to `--output`, when selected), exits `0` only when `passed=true`, and exits
`2` for blockers. Diagnose compact payload failures by stable code:

| Code suffix | Recovery |
| --- | --- |
| `RUNTIME_PAYLOAD_REFS_INVALID` | Restore the exact trio, unique IDs, count, and workflow-length grammar in the source pack. |
| `RUNTIME_PAYLOAD_ORDER_INVALID` | Preserve `project → manifest → selection`; do not sort the pack list. |
| `RUNTIME_PAYLOAD_MISSING` / `EMPTY` | Rebuild the missing non-empty producer output before retrying. |
| `RUNTIME_PAYLOAD_OVERSIZED` | Reduce the producer artifact below the 256 MiB per-file bound; do not truncate generated bytes. |
| `RUNTIME_PAYLOAD_TOTAL_LIMIT_EXCEEDED` | Reduce the distinct compact-v1 inventory to 512 MiB or less, or publish through the canonical release-set-v2 path; do not truncate generated bytes. |
| `RUNTIME_PAYLOAD_UNSAFE` | Replace symlinks, root/path races, or non-regular inputs with stable confined regular files. |
| `RUNTIME_PAYLOAD_UNREADABLE` | Repair filesystem access or filename portability, then rebuild from the same source head. |
| `RUNTIME_PAYLOAD_UNSUPPORTED` | Use only the documented trio; a new producer kind requires a reviewed contract change. |

All full codes are prefixed
`DPONE_COMPACT_PACK_RELEASE_`. A blocker writes no accepted immutable release;
repair producer inputs and rerun rather than editing release JSON.

For a copyable compact-build and reconciliation journey, including the
non-production v1 boundary, see [GitOps Airflow reconcile](gitops-airflow-reconcile.md#verify-before-promotion).

Legacy human-maintained contract groups include:

- `affected.schema.json`
- `plan.schema.json`
- `verify.schema.json`
- `bundle.schema.json`
- `attestation.schema.json`
- `airflow-render.schema.json`
- `airflow-doctor.schema.json`
- `airflow-image-contract.schema.json`
- `airflow-run-spec.schema.json`
- `airflow-runtime-evidence.schema.json`
- `airflow-runtime-profile.schema.json`
- `airflow-xcom-summary.schema.json`
- `airflow-run-identity.schema.json`
- `airflow-rerun-plan.schema.json`
- `airflow-cluster-doctor.schema.json`
- `airflow-k8s-manifests.schema.json`
- `airflow-admission-check.schema.json`
- `airflow-pack.schema.json`
- `airflow-outcome-gate.schema.json`
- `airflow-pod-contract.schema.json`
- `airflow-pod-doctor.schema.json`
- `airflow-connection-bridge-plan.schema.json`
- `airflow-artifact-index.schema.json`
- `airflow-preflight.schema.json`
- `airflow-k8s-smoke.schema.json`
- `airflow-pod-launch-evidence.schema.json`
- `airflow-evidence-bundle.schema.json`

These JSON Schema contracts describe stable fields for external CI, Airflow,
`KubernetesPodOperator`, Dagster, and release tooling. They are intentionally
additive: consumers should accept unknown fields and require only the documented
stable contract fields.

## Airflow Kubernetes runner pack

Use [GitOps Airflow runner pack](gitops-airflow-runner-pack.md) when Airflow
should run a custom dpone image through `KubernetesExecutor`,
`KubernetesPodOperator`, or KubernetesPodExecutor-style wrappers. The command
family adds self-service helpers on top of `gitops.bundle`:

```bash
dpone gitops airflow image-contract --image ghcr.io/acme/dpone:2026.06.16 --tool dpone
dpone gitops airflow render .dpone/gitops/bundle/bundle.json --image ghcr.io/acme/dpone:2026.06.16 --require-attestation
dpone gitops airflow run-spec .dpone/gitops/bundle/bundle.json --image ghcr.io/acme/dpone:2026.06.16 --require-attestation
dpone gitops airflow runtime-profile .dpone/gitops/bundle/bundle.json --image ghcr.io/acme/dpone:2026.06.16 --run-spec-path .dpone/gitops/airflow/run-spec.json --runner-policy release
dpone gitops airflow run-spec-exec .dpone/gitops/airflow/run-spec.json --evidence-output .dpone/gitops/airflow/runtime-evidence.json
dpone gitops airflow k8s-manifests --artifact-dir .dpone/gitops/airflow --gitops-controller argocd --manifest-output .dpone/gitops/airflow/airflow-k8s-manifests.yaml
dpone gitops airflow admission-check --artifact-dir .dpone/gitops/airflow --mode plan --runner-policy release
dpone gitops airflow pack --artifact-dir .dpone/gitops/airflow --mode verify --runner-policy release
dpone gitops airflow outcome-gate .dpone/gitops/airflow/xcom-summary.json --required-status passed
dpone gitops airflow evidence-verify .dpone/gitops/airflow/runtime-evidence.json --run-spec-path .dpone/gitops/airflow/run-spec.json --require-all-steps
dpone gitops airflow evidence-bundle --dag-id dpone_gitops --task-id dpone_orders --run-id manual__2026-06-16T10:00:00+00:00 --try-number 1 --require-k8s-smoke --require-pod-launch-evidence
dpone gitops airflow doctor .dpone/gitops/bundle/bundle.json --pod-template .dpone/gitops/airflow/pod_template.yaml --image-contract .dpone/gitops/airflow/image-contract.json --require-attestation
```

The renderer writes `pod_template.yaml`, `executor_config.json`,
`airflow_task.py`, `entrypoint.sh`, `run-spec.json`, `runtime-profile.json`,
`xcom-summary.json`, `airflow_dag_factory.py`, `outcome_gate.py`, and
`image-contract.json`. The runtime adapter writes `runtime-evidence.json` and
the final XCom outcome. The outcome gate checks that XCom `status` is
`passed` before a downstream Airflow task or release collector marks the run
successful. The doctor checks bundle
attestation, `pod_template_file` requirements, the Airflow `base` container
contract, image contract consistency, `runner_policy`, and
`--runner-policy release` hardening checks before the scheduler starts a pod.
The evidence verifier checks the pod result after the custom dpone image exits.
The evidence-bundle collector then correlates `dag_id`, `task_id`, `run_id`,
`try_number`, `map_index`, pod name, optional pod UID, and SHA-256 digests for
`bundle.json`, `run-spec.json`, `runtime-profile.json`, `pod-contract.json`,
`runtime-evidence.json`, `xcom-summary.json`, `airflow-k8s-smoke.json`, and
`airflow-pod-launch-evidence.json` into `airflow-evidence-bundle.json`.
`artifact-index.json` records the deploy-time artifact inventory,
`airflow-runtime-pack.json` records the top-level golden-path command and
artifact checklist, and `dpone gitops airflow preflight` checks schema
validity, stale artifact digests, pod-doctor output, and release-policy
`next_actions` before Airflow consumes the pack.

## What the plan contains

The plan embeds:

- the root manifest and inferred workload root;
- the sparse checkout allowlist from `dpone manifest sparse-paths`;
- a runner family such as `generic`, `airflow`, or `kubernetes_pod_operator`;
- the command the runner should execute after checkout;
- warnings and blockers from sparse-path discovery;
- a provenance block with the GitOps plan schema version;
- a lock block with repo-relative paths and SHA-256 digests for file entries.

The sparse paths remain repo-relative. Public output does not expose local
absolute filesystem paths.

The lock is intentionally lightweight: directories and missing optional paths
are recorded with `sha256: null`, while present files get a deterministic
digest. Runners can persist the lock next to task logs to prove which manifest
and support files were planned at handoff time.

Use `dpone gitops verify --verify-lock` when a runner must prove that file
contents still match the plan. Digest mismatches and missing locked files are
blockers; directory entries and lock entries without a digest are warnings
because there is no stable file hash to compare.

## Runner handoff

A `KubernetesPodOperator` or similar runner should treat the JSON plan as the
contract:

1. checkout the repository;
2. apply the plan's `sparse_paths` allowlist;
3. run `dpone gitops verify <plan> --worktree <checkout>`;
4. execute the plan's `command.text` only when verification exits `0`;
5. store the plan and verify artifacts with task logs.

Runner code should not know dpone manifest internals. If a new manifest field
starts referencing files, add that field to dpone's dependency rule catalog
instead of duplicating schema logic in scheduler code.

For CI systems that need one artifact instead of per-manifest orchestration,
use `dpone gitops bundle` first and hand `entries[*].plan_path` to downstream
runner tasks. The runner should still run `dpone gitops verify` inside the
sparse checkout before `dpone run`.

## Impact planning runbook

Use `dpone gitops affected` in CI when a scheduler wants to start only the
workloads touched by a pull request:

1. collect changed repo-relative paths from Git, a CI file, or refs;
2. run `dpone gitops affected --from-ref origin/master --to-ref HEAD`;
3. review `impacted_manifests[*].reasons` to see why each manifest matched;
4. optionally enable `--emit-plans` and publish the emitted plan artifacts;
5. hand the emitted plan path to Airflow, `KubernetesPodOperator`, Dagster, or
   another runner;
6. run `dpone gitops verify` inside the sparse checkout before `dpone run`.

Invalid changed paths, such as absolute paths or `..`, fail closed with
`blockers`. Missing dependency files remain warnings so GitOps output stays
deterministic during branch creation.

## Bundle policy runbook

Use `dpone gitops bundle` when CI should publish a complete handoff pack:

1. collect changed paths from `--changed-files-file` or Git refs with
   `--from-ref` and `--to-ref`;
2. include the same overrides, registries, and support paths that the runner
   will need;
3. use `--policy-profile pr` for pull-request gates or
   `--policy-profile release` for release gates;
4. add `--attest` when the bundle should carry artifact digest evidence;
5. run `dpone gitops bundle verify <bundle.json> --require-attestation` before
   scheduler handoff or release promotion;
6. enable `--verify-lock` directly only when using `--policy-profile custom`;
7. add `--fail-on-empty-impact` for pull-request gates that must prove the
   change either affects a workload or is intentionally ignored;
8. add `--fail-on-warnings` for release gates where unresolved group
   dependencies or missing optional paths must be reviewed before merge;
9. add `--require-lock` when digest verification is mandatory;
10. publish `bundle.json`, `affected.json`, per-manifest plan/verify JSON, and
   `summary.md` as CI artifacts.

The command writes artifacts even when policy blockers are present. This keeps
debugging self-service: CI can fail the job while still exposing the exact
changed files, impacted manifests, warnings, blockers, and plan paths.

## Runbook

1. Generate a plan locally and review `warnings`.
2. Commit the manifest, support assets, and plan wiring in the same pull
   request.
3. In CI, run `dpone gitops verify` after sparse checkout and before runtime
   execution.
4. Treat `blockers` as fail-closed. Treat `warnings` as review items unless
   your deployment policy escalates them.
5. Keep `--support-path` explicit for SQL, templates, or scripts until those
   files have first-class manifest dependency fields.

## Common fixes

| Symptom | Fix |
|---|---|
| `missing_required_path` blocker | Add the file to the repository or include the correct support path. |
| `missing_optional_path` warning | Create the optional override or leave the deterministic future path. |
| `invalid_plan_path` blocker | Remove absolute paths, `..`, or empty path segments from the plan. |
| `invalid_changed_path` blocker | Pass repo-relative Git paths only to `dpone gitops affected`. |
| `changed_files_missing` blocker | Provide `--changed-files`, `--changed-files-file`, or `--from-ref` with `--to-ref`. |
| `lock_digest_mismatch` blocker | Regenerate the plan or investigate why the file changed after planning. |
| `empty_impact` blocker | Remove `--fail-on-empty-impact` for advisory runs or confirm the change should not launch workloads. |
| `warnings_present` blocker | Resolve warnings or move the run to an advisory policy without `--fail-on-warnings`. |
| `lock_verification_required` blocker | Add `--verify-lock` when `--require-lock` is enabled. |
| Missing attestation | Add `--attest` and publish the resulting `bundle.json` with CI artifacts. |
| `artifact_digest_mismatch` blocker | Regenerate the bundle or investigate artifact drift after planning. |
| `bundle_digest_mismatch` blocker | Regenerate the bundle; at least one attested artifact digest no longer matches the bundle digest. |
| Runner checks out too much | Prefer plan sparse paths over hand-maintained scheduler allowlists. |
