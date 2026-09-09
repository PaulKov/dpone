# Airflow cache sync and recovery

**Purpose.** Choose the safe task for materializing, activating, observing, or recovering one exact Airflow cache deployment.

**Audience.** Platform and CI engineers, Airflow operators, incident responders, and Python integrators.

Use this runbook when a platform deployment step must make one already built
dpone deployment visible to the Airflow loader. `cache-sync` performs local,
fail-closed verification and promotion. It never downloads artifacts, rebuilds
a release, reads credentials, or calls Airflow, Vault, Kubernetes, or a remote
registry.

For production, remote materialization verifies the signed deployment package
before installing candidate bytes. Configure and operate that boundary through
[Airflow artifact trust and attestation](airflow-artifact-trust.md). A
checksum-valid but unsigned production deployment is intentionally rejected.


## Choose the task

| Task | Canonical guide | Outcome |
| --- | --- | --- |
| Prepare and install immutable bytes | [Materialize an exact cache](airflow-cache-sync-materialization.md) | Verified release/deployment projection, not yet active |
| Prove the task-Pod handoff | [Operate strict-v2 init-fetch Pods](airflow-cache-sync-strict-v2.md) | Runtime contract and certification status |
| Select the parser deployment | [Promote the reviewed deployment](airflow-cache-sync-promotion.md) | CAS-protected desired state and atomic activation |
| Diagnose or repair cache state | [Verify and recover the cache](airflow-cache-sync-recovery.md) | Read-only plan, guarded repair, and stable error action |
| Downgrade after WAL/ACK v2 | [Restore older retention state](airflow-cache-retention-state-downgrade.md) | Certified fresh `emptyDir` recovery; persistent-volume downgrade stops as `UNVERIFIED` |
| Remove inactive generations | [Retain bounded cache generations](airflow-cache-retention.md) | Reviewed, receipt-backed deletion and replay evidence |
| Approve or withdraw deletion | [Approve one exact retention plan](airflow-cache-retention-approval.md) | One-plan GitOps approval and post-removal proof |
| Embed the local services | [Use the Python API](airflow-cache-sync-python-api.md) | Explicit materializer and promoter composition |

For a first rollout, follow the guides in table order. The stock runtime composes its offline verifier from the exact pinned trust policy; cloud-live, Kubernetes, and production attestation remain `UNVERIFIED` without evidence from the exact environment.

## Prerequisites

Start with the [cache identity, permissions, and immutable input prerequisites](airflow-cache-sync-materialization.md#prerequisites).

## Recognize provider parse failures

Use the [provider parse-failure guide](airflow-cache-sync-materialization.md#recognize-provider-parse-failures) before changing cache state.

## Materialize the prerequisite

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


Continue with the complete [publication and materialization procedure](airflow-cache-sync-materialization.md#materialize-the-prerequisite).

## Operate strict-v2 init-fetch pods

Follow the [strict-v2 runtime preparation, verification, diagnosis, and certification guide](airflow-cache-sync-strict-v2.md#operate-strict-v2-init-fetch-pods).

## Promote

The protected CI writer publishes only from passed promotion evidence:

```bash
export DPONE_AIRFLOW_DESIRED_STATE_AUTHORITY_FILE=/etc/dpone/airflow-authority.json

dpone airflow desired-state prepare \
  --promotion-evidence .ci/out/dpone_deployment_promotion.json \
  --expected-revision "${OBSERVED_REVISION:-absent}" \
  --output .ci/out/dpone_desired_state_publish_preparation.json \
  --status-output .ci/out/dpone_desired_state_prepare_error.json

dpone airflow desired-state publish \
  --connection-id s3_dpone_artifacts_writer \
  --connection-type airflow \
  --promotion-evidence .ci/out/dpone_deployment_promotion.json \
  --preparation .ci/out/dpone_desired_state_publish_preparation.json \
  --output .ci/out/dpone_desired_state_publish.json \
  --status-output .ci/out/dpone_desired_state_publish_error.json
```

`--expected-revision` is either the exact opaque revision returned by the
preceding authoritative read or the literal `absent` for the first deployment.
It is never a SHA-256 assumption. Exit `4` means the remote state was not
safely selected; CI must stop instead of retrying with an unconditional write.
The protected authority file supplies the fixed environment key, certified S3
endpoint, registry root/ref, watcher identity, source project, and protected
ref. The preparation binds a hash of that entire authority and is create-once
for one candidate. GitLab head authorization happens immediately before CAS.
Publish evidence v2 records both the preparation job and the mutating job. The
status outputs are distinct failure-only artifacts and never replace the
success preparation or publish evidence. The legacy
`--intent + --expected-revision` form remains available only for
single-job/local compatibility.

Continue with the complete [promotion procedure](airflow-cache-sync-promotion.md#promote).

## Verification order

Run the [cache verification order](airflow-cache-sync-recovery.md#verification-order) before recovery or retry.

## Recover partial writes

Use the [read-only recovery plan and guarded apply procedure](airflow-cache-sync-recovery.md#recover-partial-writes).

## Recovery by error family

Map stable failures with the [recovery table](airflow-cache-sync-recovery.md#recovery-by-error-family).

For an older runtime that cannot read retention WAL/ACK v2, use the
[state downgrade runbook](airflow-cache-retention-state-downgrade.md). Never
hand-edit or reverse-convert v2 control documents.

## Python integration

Use the [Python integration guide](airflow-cache-sync-python-api.md#python-integration).

For the next likely deployment task, continue with [Kubernetes cache deployment](airflow-cache-kubernetes-deployment.md).
