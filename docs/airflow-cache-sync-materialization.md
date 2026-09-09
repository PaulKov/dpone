# Materialize an exact Airflow cache

**Purpose.** Prepare, verify, publish, and materialize one immutable release/deployment pair before activation.

**Audience.** Platform and CI engineers responsible for artifact publication and scheduler-cache materialization.

[Back to cache sync and recovery overview](airflow-cache-sync.md) · **Next likely task:** [certify strict-v2 runtime fetch](airflow-cache-sync-strict-v2.md).

## Prerequisites

The service identity running the command needs read access to the candidate
deployment and pinned release, plus write access to the cache pointer and audit
files. The cache must contain all of the following:

Use two explicit roots in automation:

```bash
DPONE_BUILD_CACHE_ROOT="${DPONE_BUILD_CACHE_ROOT:-${PWD}/.dpone-cache}"
DPONE_SCHEDULER_CACHE_ROOT="${DPONE_SCHEDULER_CACHE_ROOT:-/opt/airflow/.dpone-cache}"
```

`DPONE_BUILD_CACHE_ROOT` is the CI build/publish input. The materializer writes
the exact pinned projection to `DPONE_SCHEDULER_CACHE_ROOT`, and every
promotion, recovery, retention, and provider path below uses that scheduler
root. Do not let a relative build cache accidentally become the scheduler
cache.

```text
.dpone-cache/
  .dpone-cache-layout.json  # exact_deployment_v1
  releases/sha256-<release>/
    release-set.json
    dags/...
    packs/...
  deployments/<environment>/sha256-<deployment>/
    deployment.json
    airflow-index.json
    _SUCCESS
```

The layout marker is a durable filesystem identity, not an advisory label.
The exact materializer refuses a historical legacy root containing
`generations/` or a text-file `current`; the compatibility pack watcher refuses
an exact root containing an atomic `current` symlink. Historical unmarked roots
are classified once from their existing structure while holding the writer
lease. An ambiguous root returns `DPONE_CACHE_LAYOUT_AMBIGUOUS`. Preserve it for
diagnosis, create a clean root of the intended type, and follow the migration
runbook instead of deleting individual control files.

`release-set.json` uses release-relative `path` values. `airflow-index.json`
uses pinned `cache://releases/<release>/...` references. Both must name the same
DAG specs and workload packs with the same SHA-256 digests.

Before upgrading an older cache that retained only deployment projections,
rematerialize the corresponding immutable release-set and artifacts through the
platform publisher/materializer that originally created the release. There is
no `cache-sync` restore mode: stop if that trusted input is unavailable. For a
local preview, `dpone airflow preview <pipeline>` materializes the release and
preview deployment. Production promotion remains a platform/CI operation.
Generated Phase 1A/1B release-sets use the preferred `path` field. The v1
compatibility adapter also accepts the legacy field name `artifact_ref` when
its value is the same release-relative path. Exactly one locator is required.
`cache://` references are invalid in a release-set because they would make its
content identity self-referential; new producers must emit `path`.

`--promoted-by` and every `--allowed-promoter` value are policy inputs, not
proof of caller identity. The platform must authenticate the process through
CI/workload identity and restrict cache writes with filesystem permissions.
The mutation CLI requires an allowlist so an accidental or misconfigured actor
cannot bypass the policy check, but an untrusted process must never be allowed
to choose both values and write the cache.

The readiness mutation facade applies the same rule: an empty or mismatched
allowlist returns `DPONE_CURRENT_POINTER_PROMOTER_UNAUTHORIZED` before
projection validation or cache mutation. Beginner commands do not expose actor
flags. `dpone airflow preview` and the local safe-sample deployment path each
use one fixed local actor as both the declared actor and the sole allowed actor.
They observe the current physical deployment identity and submit either an
expected-current or expected-absent CAS guard. A concurrent change therefore
returns `DPONE_CURRENT_POINTER_CAS_MISMATCH`; rerun the original beginner
command to evaluate the newly active deployment instead of retrying a stale
promotion automatically.

Python callers of the readiness facade must now provide the platform-owned
allowlist explicitly:

```python
result = cache_sync_result(
    cache_root=cache_root,
    deployment_dir=deployment_dir,
    environment="prod",
    promoted_by=authenticated_actor,
    allowed_promoters=platform_allowed_actors,
    confirm_promote=True,
)
```

Calls that omit the policy, or whose actor is not an exact allowlist member,
return exit code `4` and do not mutate the cache. Build the allowlist from
trusted deployment configuration, never from the same untrusted request that
supplies `promoted_by`. The CLI and five-command beginner journey require no
migration.

## Recognize provider parse failures

The scheduler-side `LoadReport` separates a shared index failure from damage to
one listed artifact:

| Signal | Meaning | Operator response |
| --- | --- | --- |
| `fatal: true` | The provider could not establish a trusted deployment index; no DAG from it is safe to load. | Keep the generated loader guard enabled, preserve the first error code, and inspect cache state before mutation. |
| `fatal: false` with `errors[].dag_id` | The index is trusted, but one DAG spec or workload pack failed its isolated size/digest/read/materialization check. | Leave unrelated valid DAGs available, but rematerialize the affected immutable release/projection. |

For a local authoring preview, regenerate the complete local projection:

```bash
dpone airflow preview orders_daily
dpone airflow explain orders_daily
```

For a shared environment, do not retry `cache-sync` blindly. Start with the
read-only planner and inspect `status`, `issues`,
`current_path_deployment_id`, and `preferred_repair_deployment_id`:

```bash
dpone airflow cache-recovery-plan \
  --cache-root /opt/airflow/.dpone-cache \
  --environment prod \
  --format json
```

If the plan identifies missing or corrupt immutable content, rematerialize the
exact release/deployment from trusted storage, rerun the plan, and apply only
its reviewed recovery or promotion action. Never hand-edit the active
`airflow-index.json`, DAG spec, or workload pack.

## Materialize the prerequisite

Verify the environment-neutral release attestation before building an
environment projection. Set real canonical digests first; the fail-fast shell
checks prevent a placeholder from becoming a deployment. This is a production
promotion procedure, not certification evidence for a live installation.

```bash
: "${DPONE_RELEASE_ID:?set the canonical release sha256 digest}"
: "${DPONE_RUNTIME_IMAGE_DIGEST:?set the canonical runtime image sha256 digest}"
: "${DPONE_REGISTRY_CONFIG_SHA256:?set the registry ConfigMap file sha256 digest}"
: "${DPONE_TRUST_POLICY_SHA256:?set the trust-policy ConfigMap file sha256 digest}"
: "${DPONE_TRUST_POLICY_PATH:?set the reviewed local trust-policy v2 JSON path}"
: "${DPONE_ATTESTATION_BUNDLE:?set the detached release-set attestation bundle path}"
: "${DPONE_RELEASE_SET_PATH:?set the exact local release-set.json path}"
: "${DPONE_SOURCE_COMMIT:?set the Airflow bundle Git commit}"

dpone airflow verify-attestation \
  --subject "${DPONE_RELEASE_SET_PATH}" \
  --bundle "${DPONE_ATTESTATION_BUNDLE}" \
  --trust-policy "${DPONE_TRUST_POLICY_PATH}" \
  --expected-trust-policy-sha256 "${DPONE_TRUST_POLICY_SHA256}" \
  --expected-trust-tier production \
  --format json
```

The verifier reads only the exact local subject, detached bundle, and policy.
This preflight is required before registry writes and is not a substitute for
the independent verification performed by the runtime init container.

With `--format json`, success writes one
`dpone.runtime-artifact-attestation-verification.v1` receipt to stdout and
exits `0`. The receipt records only the subject digest, verifier backend and
version, and verified-attestation count. A handled trust failure writes one
`dpone.error.v1` result to stdout, leaves stderr empty, and exits `4`; parser
usage errors use stderr and exit `2`. The reusable prod workflow persists the
read-only receipt as
`.dpone-ci/runtime-attestation-verification.json`. The command never mutates
release, deployment, registry, cache, or policy bytes. See the
[attestation verification schema](schemas/gitops/runtime-artifact-attestation-verification.schema.json)
and [stable recovery catalog](dbt-self-service-errors.md#runtime-artifact-attestation-errors).

Build the exact deployment projection and parse-smoke its generated
`airflow-index.json` before publication:

```bash
dpone airflow build \
  --release-id "${DPONE_RELEASE_ID}" \
  --environment prod \
  --trust-tier production \
  --runtime-image-ref "registry.example/dpone-runtime@${DPONE_RUNTIME_IMAGE_DIGEST}" \
  --runtime-image-digest "${DPONE_RUNTIME_IMAGE_DIGEST}" \
  --artifact-registry-ref dpone-prod-artifacts \
  --registry-config-map-name dpone-artifact-registry \
  --registry-config-map-key registry.json \
  --registry-config-sha256 "${DPONE_REGISTRY_CONFIG_SHA256}" \
  --trust-policy-config-map-name dpone-artifact-trust-policy \
  --trust-policy-config-map-key policy.json \
  --trust-policy-sha256 "${DPONE_TRUST_POLICY_SHA256}" \
  --airflow-bundle-ref "git:${DPONE_SOURCE_COMMIT}"
```

The command emits a structurally executable
`dpone.airflow-deployment-index.v2`. It binds the exact digest-pinned runtime
image, explicit trust tier, registry ConfigMap snapshot, workload identity, and
production trust-policy snapshot. The stock runtime composes its concrete
offline verifier from the pinned v2 policy. If the installed producer does not
expose these options, stop and upgrade it; never patch a v1 index into v2.

Use the provider's fail-all policies for the parse smoke so no malformed or
duplicate DAG is silently admitted:

```bash
DPONE_INDEX_PATH="<deployment-dir>/airflow-index.json" \
python3 -c "import os; from airflow.providers.dpone import load_dpone_dags; namespace={}; report=load_dpone_dags(namespace,index_path=os.environ['DPONE_INDEX_PATH'],invalid_dag_policy='fail_all',duplicate_policy='fail_all'); assert not report.fatal and not report.errors and report.loaded"
```

With `--format json`, the result also includes `airflow_index_artifact`.
Its `sha256` and `bytes` fields are calculated from the exact immutable
`airflow-index.json` file after materialization. A promotion controller must
verify that descriptor instead of comparing the unredacted file with the
public, credential-redacted `airflow_index` projection.

Publish that exact release/deployment pair. Do so only after the checks above.
This command validates
every local
fingerprint and checksum before constructing the optional storage client. It
uses conditional create-or-compare and publishes each `_SUCCESS` marker last;
it never overwrites or deletes a content-addressed object:

```bash
: "${DPONE_DEPLOYMENT_ID:?set the deployment sha256 digest emitted by build}"
: "${DPONE_ARTIFACT_REGISTRY_SCOPE_ID:?set the protected endpoint-bound registry scope digest}"

dpone airflow publish \
  --cache-root "${DPONE_BUILD_CACHE_ROOT}" \
  --release-id "${DPONE_RELEASE_ID}" \
  --deployment-id "${DPONE_DEPLOYMENT_ID}" \
  --environment prod \
  --artifact-registry-ref dpone-prod-artifacts \
  --registry-uri s3://platform-artifacts/dpone/airflow \
  --identity-mode workload_identity \
  --artifact-attestation-bundle "${DPONE_ATTESTATION_BUNDLE}" \
  --expected-registry-scope-id "${DPONE_ARTIFACT_REGISTRY_SCOPE_ID}" \
  --publication-mode exact \
  --format json
```

Run materialization as a deployment step, init container, or sidecar. Supply
both exact identities; `current`, `latest`, remote listing, tags, and branch
names are not accepted:

For the official Apache Airflow Helm chart placement, fail-open startup,
continuous refresh, `emptyDir` budget, `fsGroup`, and read-only parser mount,
see [Deploy the exact cache on Kubernetes](airflow-cache-kubernetes-deployment.md).

```bash
dpone airflow cache-materialize \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --release-id "${DPONE_RELEASE_ID}" \
  --deployment-id "${DPONE_DEPLOYMENT_ID}" \
  --environment prod \
  --artifact-registry-ref dpone-prod-artifacts \
  --registry-uri s3://platform-artifacts/dpone/airflow \
  --identity-mode workload_identity \
  --max-object-bytes 67108864 \
  --max-total-bytes 536870912 \
  --format json
```

For Azure Workload Identity, install `dpone[azure]` or
`dpone[object_storage]` and use the account-scoped form:

```bash
dpone airflow cache-materialize \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --release-id "${DPONE_RELEASE_ID}" \
  --deployment-id "${DPONE_DEPLOYMENT_ID}" \
  --environment prod \
  --artifact-registry-ref dpone-prod-artifacts \
  --registry-uri azure://platformaccount/dpone-artifacts/dpone/airflow \
  --identity-mode workload_identity
```

This mode constructs Azure `WorkloadIdentityCredential` only during command
execution. The accountless `az://` form remains available to injected clients,
but is rejected for workload identity because no safe account URL can be
derived from it. S3, GCS, and Azure registry roots must all include a non-empty
canonical path after the bucket/container.

The materializer first pins the local cache-root identity, checks both remote
completion markers, and streams only fixed deployment files and
release-declared artifacts into a private mode-`0700` operating-system staging
directory through write-time byte limits. It verifies downloaded sizes and
SHA-256 values, runs the full projection validator, and uses atomic no-replace
installation for immutable local trees. It does not create or change `current`.
A separate reviewed `cache-sync` remains the only activation step.
The publisher similarly freezes every validated source file into a private,
mode-`0700` operating-system temporary snapshot before its first registry
operation, so changing a build file during upload cannot change the bytes
associated with a pinned identity and replacing the cache pathname cannot
redirect snapshot bytes.

After promotion, the provider reads `<cache-root>/current/airflow-index.json`.
It infers `<cache-root>` from that lexical pointer before resolving the
activation symlink, so custom cache directory names retain correct
`cache://releases/...` resolution.

The accepted provider pointer is deliberately narrower than a generic local
symlink:

```text
current -> activations/<environment>/sha256-<64 lowercase hex>
```

The deployment ID and, when present in the index, environment must match that
activation identity. A direct directory or a symlink to `deployments/**` is not
a compatibility path; run the reviewed materialize/promote workflow to create
the sealed activation instead.

For a deterministic local proof, replace `--identity-mode workload_identity`
with `--local-registry-root .dpone-artifact-registry`. `publish` and, starting
with `0.73.21`, `cache-materialize` may instead use a bounded logical
`--connection-id <logical-id>` plus `--connection-type` when the deployment
step receives a read-only credential reference instead of workload identity.
The ID grammar is
`[A-Za-z0-9][A-Za-z0-9_.-]{0,127}`; it is a name, never a URI, JSON payload,
Vault path, token, or secret value. Resolution is lazy and remains in the CLI
composition root; release/deployment artifacts and materialization evidence
never contain the credential source or value.

For an Airflow environment bridge, install `dpone[s3]`, inject the read-only
connection as `AIRFLOW_CONN_S3_DPONE_ARTIFACTS_READER`, and run:

```bash
dpone airflow cache-materialize \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --release-id "${DPONE_RELEASE_ID}" \
  --deployment-id "${DPONE_DEPLOYMENT_ID}" \
  --environment dev \
  --artifact-registry-ref dpone-dev-artifacts \
  --registry-uri s3://example-data-bucket/dpone-artifacts/prod/example-workloads \
  --connection-type airflow \
  --connection-id s3_dpone_artifacts_reader \
  --max-object-bytes 67108864 \
  --max-total-bytes 402653184 \
  --format json
```

The expected JSON status is `materialized` on the first successful download
and `no_op` on a byte-identical retry. Both results have `activated: false`;
`<cache-root>/current` remains unchanged until a separately authorized
`cache-sync`. Logical S3 references require an explicit access-key/secret-key
pair. Use `--identity-mode workload_identity` when an ambient SDK identity is
the intended contract; an empty logical connection never falls back to it.
See the `0.73.21` migration note in
[Compatibility](compatibility.md#airflow-releasedeployment-artifact-delivery)
before upgrading an existing ambient-credential deployment.

The local emulator records one root device/inode for the client lifetime and
opens the root from the filesystem anchor and every descendant directory with
descriptor-relative no-follow operations. A symlink in any root ancestor is
rejected before a directory is created. Conditional create, stat, download,
listing, and deletion remain anchored to the first root identity. Replacing
the pathname during or between object operations fails closed, so artifacts
and `_SUCCESS` cannot be split across different trees. Noncanonical root paths
also fail closed.

Production deployment projections generated with
`verify.attestations: required_for_prod` require exactly one configured
authority:

- `dpone.runtime-artifact-trust-policy.v2` plus its detached GitHub/SLSA
  release-set bundle; or
- `dpone.airflow-deployment-trust-policy.v1` plus the immutable
  deployment-scoped Cosign package.

Supplying both is rejected before registry I/O. The selected publisher writes
its proof marker last; stock cache/runtime consumers verify the exact pinned
package without mutable lookup. There is no bypass flag. S3/GCS/Azure
cloud-live and Kubernetes end-to-end certification remain `UNVERIFIED` unless
the exact environment and credentials are available; local object-store tests
are not a substitute.

Publication/materialization failure recovery is retry-by-revalidation:

| Code | Meaning | Safe action |
| --- | --- | --- |
| `DPONE_ARTIFACT_REGISTRY_INCOMPLETE` | Marker or declared object is absent. | Rerun the same pinned publisher; do not activate. |
| `DPONE_ARTIFACT_REGISTRY_IMMUTABILITY_CONFLICT` | Existing bytes differ at a content-addressed key. | Quarantine the prefix and investigate; never overwrite it. |
| `DPONE_ARTIFACT_REGISTRY_CHECKSUM_MISMATCH` | Downloaded bytes do not match the release contract. | Stop, audit storage, and create a new valid release. |
| `DPONE_ARTIFACT_REGISTRY_METADATA_INVALID` | Storage returned a negative or malformed object size. | Stop, inspect the backend/adapter, and rerun only after metadata is trustworthy. |
| `DPONE_ARTIFACT_REGISTRY_OBJECT_TOO_LARGE` | One object exceeds the local policy. | Review the artifact and explicit platform budget. |
| `DPONE_ARTIFACT_REGISTRY_TOTAL_LIMIT_EXCEEDED` | The bounded projection exceeds the total budget. | Review release size and platform limits. |
| `DPONE_ARTIFACT_REGISTRY_UNAVAILABLE` | Storage, IAM, or credential dependency is unavailable. | Repair the dependency and rerun the exact pins. |

If a legacy production cache has no trusted release source, stop the upgrade
and escalate to the platform artifact owner. Do not reconstruct an immutable
release by editing deployment JSON or copying the currently visible pack.
