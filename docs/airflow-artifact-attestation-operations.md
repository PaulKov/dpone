# Airflow artifact attestation operations

> **Fail-closed preview.** Use this runbook for rehearsal and incident design.
> Signed dev restart and staged production cutover/rollback remain
> `UNVERIFIED`; the current release does not certify production activation.

This runbook is for CI, platform, security, and on-call engineers operating a
deployment-scoped Cosign authority. Start with
[the trust model and first setup](airflow-artifact-trust.md).

## Healthy sequence

1. Exact publication produces `dpone.airflow-artifact-publish.v2`.
2. `artifact-attestation prepare` writes a canonical statement.
3. Protected CI signs that exact file and writes a Cosign v3 bundle.
4. `artifact-attestation publish` verifies locally, creates the statement and
   bundle, reads both back, then creates `_SUCCESS`.
5. Desired-state promotion selects the exact release/deployment IDs.
6. Cache materialization verifies the full subject before activation.
7. KPO init-fetch verifies the release/deployment roots it independently
   observes and records `airflow_index_sha256` under `unobserved_claims`.

An intentionally unobserved KPO index claim is not a partial registry package.
It is explicit evidence about the worker isolation boundary.

## Exit classes

| Exit | Meaning | Retry rule |
| --- | --- | --- |
| `2` | input, canonical policy, or local contract is invalid | correct reviewed input; do not publish |
| `3` | registry, SDK, or verifier is unavailable | restore dependency and retry exact IDs |
| `4` | trust, signature, subject, revocation, or immutability failed | stop promotion and investigate |
| `5` | unexpected internal failure | retain safe evidence and escalate |

## Failure matrix

| Code | Detection | Safe action |
| --- | --- | --- |
| `DPONE_ARTIFACT_ATTESTATION_PREPARE_FAILED` | exact publication evidence, timestamp, cache, or output contract is invalid | fix the reported input; use the stable CI pipeline creation timestamp for `issued_at` |
| `DPONE_ARTIFACT_ATTESTATION_PUBLISH_FAILED` | local exact subject or publication input cannot be rebuilt | inspect the exact publish v2 receipt and rebuild the immutable release |
| `DPONE_ARTIFACT_DELIVERY_INPUT_INVALID` | registry credential provider is ambiguous or an input is invalid | pass `--connection-type env`, `airflow`, or `vault` explicitly with `--connection-id` |
| `DPONE_ARTIFACT_REGISTRY_SDK_UNAVAILABLE` | selected object-storage SDK is missing | use the certified publisher image with the matching registry extra |
| `DPONE_ARTIFACT_REGISTRY_UNAVAILABLE` | selected registry client cannot complete a bounded operation | restore the configured credential/endpoint path and retry |
| `DPONE_ARTIFACT_ATTESTATION_DEPENDENCY_UNAVAILABLE` | a required attestation dependency is missing | use the certified signing/publisher image |
| `DPONE_INTERNAL_ARTIFACT_ATTESTATION_PREPARE_FAILED` | an unexpected internal prepare error was safely classified | retain the JSON receipt and open a framework incident; do not retry with changed evidence |
| `DPONE_INTERNAL_ARTIFACT_ATTESTATION_PUBLISH_FAILED` | an unexpected internal publish error was safely classified | retain the JSON receipt and open a framework incident; do not bypass verification |
| `DPONE_ARTIFACT_ATTESTATION_AUTHORITY_CONFLICT` | both GitHub/SLSA and Cosign authorities configured | select exactly one authority before retry |
| `DPONE_ARTIFACT_ATTESTATION_REQUIRED` | a production consumer has no complete selected authority package | publish/restore the exact package selected by desired state |
| `DPONE_ARTIFACT_REGISTRY_AUTHORITY_REQUIRED` | the registry adapter cannot prove its endpoint-bound authority | use an authority-aware certified registry adapter |
| `DPONE_ARTIFACT_ATTESTATION_POLICY_INVALID` | `policy-render` or consumer rejects policy | compare with the public schema, render canonical bytes again |
| `DPONE_ARTIFACT_TRUST_POLICY_MISMATCH` | mounted policy hash differs from desired state | roll out the exact reviewed ConfigMap and deployment pin |
| `DPONE_ARTIFACT_ATTESTATION_KEY_MISMATCH` | key bytes do not match the allowlisted digest | stop; restore the reviewed public-key file |
| `DPONE_ARTIFACT_ATTESTATION_SIGNATURE_INVALID` | no trusted key verifies the exact statement | sign the prepared bytes again in protected CI |
| `DPONE_ARTIFACT_ATTESTATION_SUBJECT_MISMATCH` | signed claims differ from locally observed roots | rebuild from exact publish v2 evidence |
| `DPONE_ARTIFACT_ATTESTATION_POLICY_DENIED` | environment or registry authority is outside policy | review policy; never broaden it during an incident without approval |
| `DPONE_ARTIFACT_ATTESTATION_SOURCE_NOT_ALLOWED` | source project or protected ref is outside policy | publish from an allowlisted project/ref or review the policy through normal change control |
| `DPONE_ARTIFACT_ATTESTATION_REVOKED` | statement or key is revoked | select a non-revoked exact deployment |
| `DPONE_ARTIFACT_ATTESTATION_VERIFIER_UNAVAILABLE` | pinned Cosign cannot execute | repair the exact image/toolchain; do not downgrade to checksums |
| `DPONE_ARTIFACT_ATTESTATION_NOT_FOUND` | `_SUCCESS` does not exist | inspect partial-package rules below |
| `DPONE_ARTIFACT_ATTESTATION_INVALID` | marker/object size, digest, identity, or shape differs | quarantine and investigate; do not activate |
| `DPONE_ARTIFACT_ATTESTATION_IMMUTABILITY_CONFLICT` | a local output or remote immutable object has different bytes | preserve existing bytes; for local render choose a new `--output` or restore the exact expected bytes, and for remote publication quarantine the prefix and create a new valid deployment; never overwrite automatically |
| `DPONE_ARTIFACT_ATTESTATION_REGISTRY_UNAVAILABLE` | bounded registry read/write failed | restore access and retry exact immutable inputs |

## Partial package retry

Publication orders objects as statement, bundle, then `_SUCCESS`. A job may
stop after either of the first two writes.

- If `_SUCCESS` is absent and every existing object is byte-equal to the local
  intended object, rerunning the same prepare/sign/publish identity is safe.
- If any existing object differs, publication returns an immutability conflict.
  Never overwrite or delete it as an automated retry.
- Consumers treat a missing marker as not found and never infer completeness
  from the statement or bundle alone.
- A marker with a missing or mismatched object is invalid, not retryable as an
  ordinary outage.

## Rotation and revocation

1. Add the new public key and its digest to a new canonical policy.
2. Render, pin, and deploy the new policy before signing new deployments.
3. Keep the old key for the approved rollback window.
4. Revoke an attestation ID for one compromised deployment, or a key ID for all
   deployments signed by that key.
5. Reconcile desired/current and run a KPO smoke after the policy rollout.

Immutable packages remain audit evidence. Retention must not be used as a
revocation mechanism.

## Rollback verification

Rollback changes desired exact IDs; it does not bypass current trust policy.
After rollback confirm:

- desired and current release/deployment IDs match;
- loader ACK reports the same source commit;
- cache verification is green;
- KPO `runtime-fetch-ready.v2` is green;
- the selected attestation and key are not revoked.

## No-Vault interim

For the current rollout, registry credentials use an explicit
`--connection-type env`. The Cosign private key is a protected GitLab file
variable and its password is a protected hidden variable. dpone does not load,
print, persist, or receive either secret. Moving custody to Vault or KMS later
changes the signing adapter, not the signed subject or activation protocol.
See the canonical
[S3 environment-variable contract](airflow-artifact-trust.md#first-setup-without-vault)
for access key, secret key, endpoint, region, and optional session token names.
