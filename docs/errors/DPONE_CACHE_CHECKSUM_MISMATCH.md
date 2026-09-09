# DPONE_CACHE_CHECKSUM_MISMATCH

The actual SHA-256 of a pinned DAG spec or workload pack differs from its
declared digest. The same stable code can be raised at three boundaries:

- `cache_sync`: local deployment promotion;
- Airflow parse: provider validation of the local deployment index;
- runtime `init_fetch`: verification of a pinned artifact before workload start.

Use the structured `stage` when present, otherwise use the caller/log context,
before choosing the recovery path below.

## Cache sync

The promotion is rejected before `.dpone-cache/current`,
`current-pointer.json`, or the promotion audit log changes. Airflow continues
reading the previously promoted deployment when one exists. On the first
promotion, `current` remains absent.

### Fix

Do not edit the published release in place. A platform identity with read access
to trusted artifact storage must restore the exact artifact by release digest,
or the CI build/reconcile job must build a new release and deployment when the
source content intentionally changed. Then materialize the complete pinned
release locally and rerun the original `dpone airflow cache-sync` command.
The core CLI does not fetch a missing production release from remote storage;
follow the exact external handoff and escalation contract in the
[cache sync and recovery runbook](../airflow-cache-sync.md#materialize-the-prerequisite).

JSON output identifies the local artifact in `errors[0].path` without exposing
cache topology in ordinary terminal output. `cache-sync` is mutating rather
than a read-only doctor: it exits `4` and changes no state while the mismatch is
present, but it can promote after the artifact has been correctly restored.
Expected stderr is empty.

Text-mode retry for an update (replace both digests with values from the
reviewed recovery plan and rebuilt deployment):

```bash
dpone airflow cache-sync \
  --cache-root .dpone-cache \
  --deployment-dir .dpone-cache/deployments/prod/sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa \
  --environment prod \
  --promoted-by ci://your-platform/dpone-airflow \
  --allowed-promoter ci://your-platform/dpone-airflow \
  --expected-current-deployment-id sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \
  --confirm-promote \
  --format text
```

Typical text output:

```text
dpone airflow cache sync: FAILED
- environment: prod
- issue: DPONE_CACHE_CHECKSUM_MISMATCH: indexed cache artifact checksum does not match the release-set
- help: https://paulkov.github.io/dpone/errors/DPONE_CACHE_CHECKSUM_MISMATCH/
- current pointer: not changed
- action: restore the pinned release artifacts, then rerun dpone airflow cache-sync
- details: rerun with --format json for the structured error and failing path
```

For machine-readable diagnostics, rerun the same complete command with
`--format json`. On the first promotion, replace
`--expected-current-deployment-id ...` with `--expect-current-absent`; exactly
one guard is mandatory.

The corresponding JSON error includes:

```json
{
  "schema": "dpone.error.v1",
  "code": "DPONE_CACHE_CHECKSUM_MISMATCH",
  "stage": "cache_sync",
  "severity": "error",
  "path": ".dpone-cache/releases/sha256-cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc/dags/orders_daily.dag-spec.json"
}
```

After recovery, verify the active pointer:

```bash
dpone airflow cache-recovery-plan --environment prod
```

A successful retry exits `0` and prints `current pointer: updated`. See the
[cache sync and recovery runbook](../airflow-cache-sync.md) for prerequisites,
all integrity error families, and migration guidance. The public payloads are
listed in the [GitOps schema catalog](../reference/gitops-schema-catalog.md),
and command options are generated in the
[`cache-sync` CLI reference](../cli-reference.md#dpone-airflow-cache-sync).

## Airflow parse

The provider must not refresh or repair cache during DAG parsing. Restore the
exact local pinned release through the cache materializer, then let the DAG
processor parse the corrected local index. Valid DAGs remain isolated from a
single invalid DAG according to the configured invalid-DAG policy.
The failure is observable in the provider's structured `LoadReport.errors` and
the local DAG processor log; parse never sends it to a metadata database or
remote repair service. See the
[provider API parse-side-effect contract](../airflow-provider-api.md#parse-side-effect-contract).

## Runtime init fetch

The runtime must not resolve mutable `current` or accept the mismatched bytes.
Restore the object identified by the already pinned release/deployment and
digest, or publish a new immutable release and deployment when the content
intentionally changed. Then retry the workload through the orchestrator; do not
rewrite the running pod's artifact or checksum.
The failure is observable in init-fetch/runtime task output. A failed fetch
does not publish a successful `init-fetch-manifest.json` or runtime evidence
receipt. See
[runtime artifact delivery](../airflow-self-service-architecture.md#runtime-artifact-delivery).
