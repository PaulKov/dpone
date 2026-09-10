# Operate strict-v2 init-fetch Pods

**Purpose.** Prepare, verify, diagnose, and certify the runtime artifact handoff used by strict-v2 task Pods.

**Audience.** Airflow platform engineers validating the task-Pod runtime contract.

[Back to cache sync and recovery overview](airflow-cache-sync.md) · **Next likely task:** [promote the reviewed deployment](airflow-cache-sync-promotion.md).

## Operate strict-v2 init-fetch pods

Cache promotion and pod execution are separate trust boundaries. `cache-sync`
makes one verified index visible to the parse-safe provider; it does not contact
Kubernetes, mount ConfigMaps, exercise workload identity, or certify runtime
fetch.

Run an executable migration rehearsal first with `trust_tier: non_production`
in an approved dev/stage environment. A production deployment may run only
after offline attestation preflight, immutable publication, runtime verifier
composition, and current live certification evidence are all present.

### Prepare the runtime inputs

Before promotion, verify that the executable deployment has:

- `schema: dpone.airflow-deployment-index.v2`;
- `runtime_artifact_delivery.mode: init_fetch`;
- matching explicit top-level and delivery-level `trust_tier`;
- an OCI `runtime_image_ref` pinned by the same digest as
  `runtime_image_digest`;
- exact release, deployment, and selected workload descriptors with positive
  `bytes`;
- `kubernetes_workload_identity` with the reviewed namespace and service
  account;
- digest-pinned ConfigMap references for registry configuration and, for
  `production`, trust policy.

The registry configuration is runtime-only platform configuration. It may
contain a physical registry URI but no static credential:

```json
{
  "registries": {
    "dpone-prod-artifacts": {
      "access": {
        "mode": "workload_identity"
      },
      "registry_uri": "s3://platform-artifacts/dpone/airflow"
    }
  },
  "schema": "dpone.artifact-registry-runtime-config.v1"
}
```

A production trust policy has this closed shape:

```json
{
  "schema": "dpone.runtime-artifact-trust-policy.v2",
  "trust_tier": "production",
  "attestations": "required_for_prod",
  "verifier": {
    "backend": "github_artifact_attestation_v1",
    "repository": "example/airflow-dev",
    "signer_workflow": "example/airflow-dev/.github/workflows/dbt-release.yml",
    "signer_digest": "0123456789abcdef0123456789abcdef01234567",
    "predicate_type": "https://slsa.dev/provenance/v1",
    "cert_oidc_issuer": "https://token.actions.githubusercontent.com",
    "deny_self_hosted_runners": true,
    "trusted_root": {
      "encoding": "base64",
      "content": "eyJtZWRpYVR5cGUiOiJhcHBsaWNhdGlvbi92bmQuZGV2LnNpZ3N0b3JlLnRydXN0ZWRyb290K2pzb24iLCJub3RlIjoicmVwbGFjZSB3aXRoIHJldmlld2VkIGdoIGF0dGVzdGF0aW9uIHRydXN0ZWQtcm9vdCBvdXRwdXQifQo=",
      "sha256": "sha256:ea0f13f1404b7b4263a52983df4cca66e39cf9f99d6ef960e296c43bc66a7877",
      "generated_at": "2026-07-01T00:00:00Z",
      "refresh_after": "2035-01-01T00:00:00Z"
    },
    "gh": {
      "minimum_version": "2.93.0",
      "maximum_version_exclusive": "3.0.0",
      "timeout_seconds": 30
    }
  }
}
```

The embedded root is only a parser/schema specimen. Generate reviewed offline
root bytes with `gh attestation trusted-root`, base64-encode those exact bytes,
and calculate `trusted_root.sha256` over the decoded bytes. Calculate the
deployment-pinned policy SHA-256 over the complete JSON file bytes. Rotate all
root fields together before `refresh_after`, publish the policy as a new
immutable ConfigMap snapshot, and build a new deployment identity. The
`signer_digest` value is the raw lowercase 40-hex workflow commit; do not add a
`sha256:` prefix. Validate the file against
[runtime artifact trust policy v2](schemas/gitops/runtime-artifact-trust-policy-v2.schema.json)
and use the [attestation error catalog](dbt-self-service-errors.md#runtime-artifact-attestation-errors)
for recovery.

For a deployment-scoped Cosign authority, the same digest-pinned ConfigMap
reference instead contains
`dpone.airflow-deployment-trust-policy.v1`. That policy signs the complete
Airflow deployment subject and is documented in
[Airflow artifact trust and attestation](airflow-artifact-trust.md). The
GitHub/SLSA and Cosign policies are alternative authorities: configuring both
is rejected before registry access, and production never falls back to
checksums when the selected verifier is unavailable.

Publish each exact file as the selected ConfigMap key, preferably under a new
immutable, content-addressed ConfigMap name. The two authorities have different
byte producers:

- `dpone.runtime-artifact-trust-policy.v2` hashes its exact reviewed file bytes,
  including a final newline when that producer emits one;
- `dpone.airflow-deployment-trust-policy.v1` must be produced by
  `dpone airflow artifact-attestation policy-render`; its canonical bytes never
  contain a trailing newline.

Put the resulting exact-byte SHA-256 in the matching reference. Do not put the
URI, ConfigMap body, token, signed URL, secret, or Vault path in
`airflow-index.json`.

At pod start, the init process:

1. resolves each selected `registry.json` / `policy.json` path under its mount
   directory (following kubelet ConfigMap projection symlinks such as
   `key -> ..data/key`), then opens the resolved regular file once with
   no-follow semantics;
2. reads at most 64 KiB from `registry.json` and 1 MiB from `policy.json`;
3. verifies the digest over those exact bytes;
4. parses those same bytes;
5. builds the registry reader only after verification.

A symlink that escapes the mount directory is rejected before read.

The ConfigMap name is therefore a selector, not integrity evidence. A digest
mismatch is not retryable as a transient registry outage. Roll out changed
bytes as a new reviewed ConfigMap snapshot and immutable deployment identity;
do not rely on an in-place ConfigMap update.

### Verify the fixed pod contract

The provider must produce exactly these commands with no arguments:

| Container | Image | Command |
| --- | --- | --- |
| `dpone-runtime-init-fetch` | Exact `runtime_image_ref` | `dpone airflow runtime-init-fetch` |
| base | The same exact image | `dpone airflow runtime-pack-exec` |

Reserved mounts are:

| Volume | Init | Base |
| --- | --- | --- |
| `dpone-fetched-artifacts` | RW `/var/lib/dpone/artifacts` | RO `/var/lib/dpone/artifacts` |
| `dpone-worktree` | RW `/workspace/repo` | RO `/workspace/repo` |
| `dpone-run-output` | absent | RW `/var/lib/dpone/run` |
| `dpone-artifact-registry-config` | RO `/etc/dpone/artifact-registry` | absent |
| `dpone-artifact-trust-policy` | RO `/etc/dpone/artifact-trust` when selected | absent |

Image, namespace, service account, security context, init containers, and these
volumes/mounts are provider-owned. A pack collision must fail
`DPONE_INIT_FETCH_RESERVED_COLLISION`; it must not be merged or ignored.
Strict tasks default to `retries=0`. A positive retry count is admitted only
when the authenticated workload pack contains the compiler-issued closed retry
authority for the PostgreSQL XMin initial to MSSQL target-atomic backfill route.
The count must be an integer from one through three. Authority is part of the
pack fingerprint and deployment-index digest; DAGs and operator overrides can
select a count but cannot self-declare safety. An absent, malformed, forged, or
route-inapplicable authority fails closed before any DAG is installed.

On a certified retry, Airflow starts a fresh KPO. Runtime target receipts, not
Airflow task state, decide whether a chunk committed: committed chunks are
skipped, an ambiguous commit is reconciled, and only non-committed chunks rerun.
All other routes retain the zero-retry contract.

Strict release materialization preserves only the validated scheduler-owned
`operator_overrides.pool` and `operator_overrides.retries` fields. Resource-capable
Pod overrides fail with field-specific migration guidance to
[workload resources](airflow-workload-resources.md). It removes other legacy `in_cluster` and every executable, image, namespace, secret, and pod
security override before fingerprinting the immutable DAG spec. The promoted
compiler, `dpone-airflow-pack`, and `apache-airflow-providers-dpone` must be a
coordinated set with both reader packages at `0.74.20` or newer. After cache
convergence, use the Airflow serialized-task API to verify the effective pool;
the repository DAG YAML is not proof that release rewriting retained it.

The init container publishes
`/var/lib/dpone/artifacts/runtime-fetch-ready.json` only after the plan,
configuration, artifact bytes, deployment receipt, inventory, pack
fingerprint, and effective attestation policy pass. The base container does not
start when init fails. On start, `runtime-pack-exec` revalidates the same plan,
ready manifest, `deployment-set.v2` runtime receipt, the independently verified
release wire allowed for the lane (`release-set.v1` only for legacy/non-dbt or
non-production compatibility transport; `release-set.v2` for authoritative
production dbt), fetched pack, pinned trust-policy fingerprint, and effective
attestation decision, then selects a structured argv from that verified pack.
The base container does not mount or reread
`registry.json` or `policy.json`; those are init-only inputs whose verified
decision is committed by `runtime-fetch-ready.json`. Runtime workloads run the
verified argv without `shell=True`, `/bin/sh -c`, scheduler-copied command, or
inline bootstrap fallback; stdout/stderr are captured under
`/var/lib/dpone/run`, including a local `runtime-summary.json`. Runtime commands
publish a JSON object to `/airflow/xcom/return.json`; separate hooks disable
XCom and require no access to that path. Normal runtime commands exit `0` so the KPO xcom
sidecar can publish the outcome summary; pre-hook and dbt commands propagate
the real child exit code after writing evidence.

### Diagnose and recover

Interpret runtime command exits with the stable code in the container log:

| Exit | Error family | Safe response |
| --- | --- | --- |
| `0` | Init published a ready manifest. | Confirm the base container started; this is not route or data-success evidence. |
| `2` | `DPONE_INIT_FETCH_PLAN_INVALID`, `DPONE_INIT_FETCH_PLAN_TOO_LARGE`, `DPONE_INIT_FETCH_PLAN_HASH_MISMATCH`, or `DPONE_INIT_FETCH_PLAN_NON_CANONICAL`. | Rebuild the v2 index/KPO with compatible producer and provider bytes. Do not retry the same malformed pod. |
| `3` | `DPONE_ARTIFACT_REGISTRY_UNAVAILABLE`. | Repair registry/IAM availability, then retry the same pinned task. Each retry gets a fresh `emptyDir`. |
| `4` | Configuration/trust mismatch, missing or corrupt artifact, required attestation, invalid ready state, or other integrity/contract failure. | Stop execution. Restore exact immutable bytes or publish and promote a new deployment; do not bypass verification. |
| `5` | `DPONE_RUNTIME_PACK_EXEC_FAILED` or verified child process could not start. | Use stage, exception type, errno and service-path role to distinguish permissions, missing executable/path, full disk and publication failures; follow [startup diagnostics](airflow-runtime-startup-diagnostics.md). |
| Child exit | Verified workload command started and returned non-zero. | Use normal dpone run/evidence/state recovery for that exact attempt; the launcher preserves the child exit code. |

Useful stable runtime codes include
`DPONE_ARTIFACT_REGISTRY_CONFIG_INVALID`,
`DPONE_ARTIFACT_REGISTRY_CONFIG_MISMATCH`,
`DPONE_ARTIFACT_TRUST_POLICY_INVALID`,
`DPONE_ARTIFACT_TRUST_POLICY_MISMATCH`,
`DPONE_ARTIFACT_TRUST_POLICY_EXPIRED`,
`DPONE_ARTIFACT_ATTESTATION_REQUIRED`,
`DPONE_ARTIFACT_ATTESTATION_BUNDLE_NOT_FOUND`,
`DPONE_ARTIFACT_ATTESTATION_BUNDLE_INVALID`,
`DPONE_ARTIFACT_ATTESTATION_BUNDLE_TOO_LARGE`,
`DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE`,
`DPONE_ARTIFACT_ATTESTATION_VERIFIER_VERSION_UNSUPPORTED`,
`DPONE_ARTIFACT_ATTESTATION_VERIFICATION_FAILED`,
`DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH`,
`DPONE_CACHE_ARTIFACT_NOT_FOUND`,
`DPONE_CACHE_ARTIFACT_TOO_LARGE`,
`DPONE_RUNTIME_FETCH_READY_INVALID`, and
`DPONE_RUNTIME_ARTIFACT_INTEGRITY_FAILED`. Preserve the code and logical
artifact reference. Do not copy physical URIs, mounted config bodies, tokens,
or absolute pod paths into tickets or XCom.

### Trust and certification status

The current TCB includes the producer, promoted cache/index, Airflow
scheduler/provider, Kubernetes control plane and kubelet, digest-pinned runtime
image, workload-identity/IAM enforcement, registry adapter, and configured
attestation verifier. Fixed KPO fields constrain pack-owned overrides; they do
not provide compromised-scheduler, compromised-provider, or compromised-cache
resistance.

Unit and contract evidence is not live certification. Kubernetes projected
ConfigMaps, workload identity, object storage, production attestation, Vault,
MSSQL, and ClickHouse remain `UNVERIFIED` unless the exact commit, image digest,
cluster, configuration, and evidence artifacts were exercised and retained.
