# Verify and recover an Airflow cache

**Purpose.** Verify cache state in the required order, recover partial writes, and map stable error families to safe actions.

**Audience.** On-call operators and incident responders repairing exact scheduler-cache state.

[Back to cache sync and recovery overview](airflow-cache-sync.md) · **Next likely task:** [embed cache sync with the Python API](airflow-cache-sync-python-api.md).

## Verification order

```text
CI/service identity
  -> acquire the cache-root inter-process promotion lock
  -> validate canonical deployment layout
  -> recompute deployment_id and compare deployment/index runtime fields
  -> read the confined pinned release-set.json
  -> recompute release_id
  -> match release/index artifacts and verify size + SHA-256
       | failure: dpone.error.v1, exit 4, no validation-time mutation
       ` success
  -> copy candidate to activations/<environment>/<deployment_id>
  -> seal activation and pinned release trees read-only
  -> compare the complete candidate/staging tree and revalidate sealed bytes
       | unreadable subtree/non-regular entry: fail closed
  -> exact-create-or-compare any existing activation
  -> prepare a unique relative current symlink to the activation
  -> revalidate optional bounded precommit guard under the promotion lock
  -> durably replace current-pointer.json
  -> append the promotion authorization to current-pointer-audit.jsonl
  -> atomically replace current as the final activation commit point
  -> current-pointer result, exit 0
```

Promotion and recovery serialize through `.promotion.lock`; snapshot creation,
the CAS check, and activation happen while that lock is held. `current` is
always a canonical relative symlink to a sealed activation, never to the
mutable build candidate, and is switched last with an atomic local rename.
The writer creates the metadata-only lock with mode `0664`, while parse/status
readers open the existing inode read-only. Deploy or run one writer
materialization before rolling out read-only consumers; a pre-existing cache
root without the lock is an explicit migration error rather than an unlocked
read. Recovery checks its reviewed-current CAS guard before publishing an
activation snapshot, so a stale recovery plan has no durable side effect.
Late edits to the candidate therefore cannot alter what Airflow parses. The
pinned release tree is sealed before the snapshot is revalidated. Pointer JSON
uses a unique, exclusive, fsynced temporary file. Lock, pointer, and audit files
reject symlinks; audit reads and appends use no-follow descriptors. Release and
projection files are opened component by component from an anchored cache-root
descriptor, so replacing an intermediate directory with a symlink cannot
redirect verification outside the cache.

The write steps remain ordered rather than one multi-file transaction. A
`prepare_current` failure leaves pointer, audit, and active `current`
unchanged, but the content-addressed activation may already exist and the
pinned release may already be sealed read-only. JSON therefore reports
`state_may_have_changed: true` and `recovery_required: false`. A pointer
precommit rejection caused by a superseded remote desired-state revision uses
the same conservative mutation flag: the pointer and `current` stay unchanged,
but snapshot preparation may already have published immutable local bytes. A
local CAS mismatch or activation snapshot conflict/failure uses the same flag
for that reason. The materializer attaches this flag when the mutation stage is
reached; CLI and desired-state adapters project that evidence and do not infer
it from a fixed list of error codes. A pointer preparation failure leaves active `current`
unchanged, but a failure
after pointer rename and before directory durability confirmation means the
pointer may already be visible. An audit or final activation failure can leave
prepared pointer/audit metadata ahead of active `current`; JSON therefore
reports `failed_step`, `state_may_have_changed`, and `recovery_required`
conservatively. Do not retry blindly when recovery is required. Run
`cache-recovery-plan`, repair the reported state, and only then retry promotion.
Snapshot cleanup is best-effort after the original promotion failure. If a
staging or newly published activation cannot be removed, the original domain
error remains authoritative and JSON adds `cleanup_failed_paths`, forces
`state_may_have_changed: true`, and forces `recovery_required: true`. A cleanup
exception never replaces the structured promotion error.
The exception is recovery-plan CAS rejection: it is evaluated before snapshot
publication and therefore reports no mutation flag. Callers may refresh the
plan and retry only after re-reading current state.
The planner independently validates active `current` and every candidate,
including deployment/release fingerprints, the Airflow index, confinement,
sizes, and checksums. Invalid candidates are never offered. It reports pointer
state as `current_deployment_id` and active state as
`current_path_deployment_id`. A malformed identity is reported as `null` plus a
structured issue, so recovery JSON remains schema-valid. Pointer `release_id` must equal the fully
validated active projection before audit-only repair can append the existing
pointer without changing its original publisher provenance. Malformed
historical lines remain warnings; recovery never silently deletes audit
history.

All cache identities are lowercase `sha256:<64 hex>`. The current pointer is
trusted only when it also has a non-empty `promoted_by` and an offset-aware
`promoted_at`; recovery, retention, and safe-sample reuse the same pure pointer
contract. Uppercase identities and incomplete authorization fail before
control-state mutation.

## Recover partial writes

First build a read-only plan:

```bash
: "${DPONE_SCHEDULER_CACHE_ROOT:?set the absolute scheduler cache root}"

dpone airflow cache-recovery-plan \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --environment prod \
  --format json
```

The plan command exits `0` when it can produce diagnostics, including when
`status` is `blocked`; automation must inspect `status`, `issues`, and
`preferred_repair_deployment_id`, not only `passed` or the process exit code.
Apply only the reviewed complete candidate:

```bash
: "${DPONE_REVIEWED_REPAIR_DEPLOYMENT_ID:?set the repair candidate sha256 digest}"
: "${DPONE_REVIEWED_CURRENT_PATH_DEPLOYMENT_ID:?set the reviewed current-path sha256 digest}"
: "${DPONE_SCHEDULER_CACHE_ROOT:?set the absolute scheduler cache root}"

dpone airflow cache-recovery-apply \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --environment prod \
  --deployment-id "${DPONE_REVIEWED_REPAIR_DEPLOYMENT_ID}" \
  --promoted-by ci://your-platform/dpone-airflow-recovery \
  --allowed-promoter ci://your-platform/dpone-airflow-recovery \
  --expected-current-deployment-id "${DPONE_REVIEWED_CURRENT_PATH_DEPLOYMENT_ID}" \
  --confirm-repair
```

Use `--expect-current-absent` instead of
`--expected-current-deployment-id` only when the reviewed plan confirms that
the `current` path is physically absent and the status is `repairable`. If
`current` exists but has no canonical identity, the plan reports
`DPONE_CURRENT_PATH_ID_UNAVAILABLE` with `status: blocked`; repair that path
under platform change control and rerun the plan. Apply re-plans immediately,
rejects a stale active-state guard, rejects candidates absent from the fresh
validated plan, and refuses to mutate a healthy cache. Recovery apply and retention apply hold
the same cache transaction lock as promotion from fresh plan through mutation,
so GC cannot delete a deployment that becomes current concurrently.

Retention apply is also a cache mutation and requires the same explicit actor
policy as promotion and recovery. Always run the plan with all evidence and
explicit protection inputs first, then review the JSON plan and its
`plan_sha256`. JSON output is evidence; it intentionally does not contain a
shell command.
The lifecycle, durable state, replay, and migration model is defined in
[bounded cache retention](airflow-cache-retention.md); the commands below are
the single contract-tested executable procedure.
The plan resolves evidence files to effective deployment ids; every occurrence
of `deployment_id` must be canonical. One malformed value invalidates the
whole evidence file even when another value is valid. Its action
preserves every one as `--protect-deployment-id`, so apply cannot silently lose
the reviewed protection scope:

```bash
set -uo pipefail

: "${DPONE_SCHEDULER_CACHE_ROOT:?set the absolute scheduler cache root}"

evidence_dir="$(mktemp -d cache-retention-plan.XXXXXX)"
tmp="${evidence_dir}/output.json"
rc=0
dpone airflow cache-retention-plan \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --environment prod \
  --evidence-file evidence/run-evidence.json \
  --format json >"${tmp}" || rc=$?
if [ ! -s "${tmp}" ]; then
  [ "${rc}" -ne 0 ] || rc=1
  printf 'retention plan produced no JSON; temporary output is %s\n' \
    "${tmp}" >&2
  exit "${rc}"
fi
schema_rc=0
dpone gitops schema validate \
  --kind dpone.deployment-cache-retention-plan.v1 \
  --payload "${tmp}" \
  --format json >/dev/null || schema_rc=$?
if [ "${schema_rc}" -ne 0 ]; then
  [ "${rc}" -ne 0 ] || rc="${schema_rc}"
  printf 'retention plan is not valid JSON evidence; temporary output is %s\n' \
    "${tmp}" >&2
  exit "${rc}"
fi
destination=cache-retention-plan.json
if [ "${rc}" -ne 0 ]; then
  destination="${evidence_dir}/failed.json"
fi
publish_rc=0
if [ "${rc}" -eq 0 ]; then
  mv -f "${tmp}" "${destination}" || publish_rc=$?
  [ "${publish_rc}" -ne 0 ] || rmdir "${evidence_dir}" || publish_rc=$?
else
  mv "${tmp}" "${destination}" || publish_rc=$?
  if [ "${publish_rc}" -eq 0 ]; then
    (cd "${evidence_dir}" && sha256sum failed.json > SHA256SUMS && sha256sum -c SHA256SUMS) \
      || publish_rc=$?
    [ "${publish_rc}" -ne 0 ] || chmod 0400 "${destination}" "${evidence_dir}/SHA256SUMS" \
      || publish_rc=$?
  fi
  printf 'retention failure evidence: %s\n' "${destination}" >&2
fi
if [ "${rc}" -eq 0 ] && [ "${publish_rc}" -ne 0 ]; then
  rc="${publish_rc}"
fi
exit "${rc}"
```

Only exit `0` replaces `cache-retention-plan.json`. A non-zero command keeps
the previous reviewed plan intact and publishes schema-valid failure evidence
under a unique `cache-retention-plan.*/failed.json` directory with a verified
`SHA256SUMS` binding and read-only local files. Copy that directory to the
platform incident evidence store before leaving the host. Empty, partial, or
schema-invalid output remains only at the printed temporary path and is not
called evidence; a publication failure is itself non-zero.

Apply the complete reviewed protection set without assuming zero, one, or many
deployment IDs:

```bash
set -euo pipefail

: "${DPONE_CACHE_RETENTION_REVIEW_ID:?set the approved UUIDv4 attempt id}"
: "${DPONE_LOADER_ACK_FILE:?set the strict loader ACK path}"
: "${DPONE_SCHEDULER_CACHE_ROOT:?set the absolute scheduler cache root}"
: "${DPONE_REVIEWED_RETENTION_PLAN_FILE:=cache-retention-plan.json}"

DPONE_REVIEWED_RETENTION_PLAN_SHA256="$(jq -er '.plan_sha256' \
  "${DPONE_REVIEWED_RETENTION_PLAN_FILE}")"
protected_ids=()
while IFS= read -r deployment_id; do
  [ -n "${deployment_id}" ] && protected_ids+=("${deployment_id}")
done < <(jq -er '.protected_deployment_ids[]?' \
  "${DPONE_REVIEWED_RETENTION_PLAN_FILE}")
protect_args=()
for deployment_id in "${protected_ids[@]}"; do
  protect_args+=(--protect-deployment-id "${deployment_id}")
done

dpone airflow cache-retention-apply \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --environment prod \
  --promoted-by ci://your-platform/dpone-airflow-retention \
  --allowed-promoter ci://your-platform/dpone-airflow-retention \
  "${protect_args[@]}" \
  --expected-plan-sha256 "${DPONE_REVIEWED_RETENTION_PLAN_SHA256}" \
  --review-id "${DPONE_CACHE_RETENTION_REVIEW_ID}" \
  --loader-ack-file "${DPONE_LOADER_ACK_FILE}" \
  --confirm-delete \
  --evidence-version v3 \
  --format json
```

`--expected-plan-sha256`, `--review-id`, and `--loader-ack-file` are
conditionally mandatory:
the parser keeps them optional so a no-candidate maintenance cycle remains a
safe no-op, but a fresh plan containing any delete candidate rejects apply
without both values. `required_options` in the frozen CLI baseline means that
the option must remain available as a public surface; it does not mean that
every invocation must provide it.

Retention plans remain schema v1. Apply preserves the published
`dpone.deployment-cache-retention-apply.v1` projection by default. Strict
controllers explicitly pass `--evidence-version v2`; the closed v2 contract
adds the reviewed `plan_sha256` and durable `activation_history_revision`.
Receipt-aware controllers pass `--evidence-version v3`; destructive v3 output
also binds the approved `review_id`, stable `operation_id`, closed `receipt_revision`, and
`transaction_status: committed`. The v1 and v2 projections remain unchanged.
For a process death or retryable filesystem failure, repeat the exact v3 apply
command with the same reviewed plan digest and review id. dpone validates the entire bounded
`<cache-root>/.retention-apply-receipts` inventory, current authority, ACK,
activation history without mutation, transaction WAL, and operation identity before it resumes
an `applying` receipt or replays a `committed` receipt. It never deletes a
committed candidate twice.

Generate a new plan and UUIDv4 review id only when evidence reports plan/authority drift, recovery
changed `recovery_revision`, or current activation changed. On replayable
authority drift dpone first closes the old `applying` receipt as `aborted` under
the retention locks: WAL-confirmed deletions remain `deleted`, while pending
items become forensic skips. A corrupt or unsafe receipt remains fail-closed
and must not be treated as ordinary drift. Do not edit or delete a receipt to force progress. Receipt roots must
be owner-only `0700`; canonical receipt files must be owner-only `0600`.
Corruption, unsafe ownership/permissions, more than 10,000 receipt files, or
more than the bounded aggregate inventory capacity returns
`DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID` and blocks mutation. Inspect
`operation_id`, `receipt_revision`, `transaction_status`, and
`deleted_deployment_ids` in the v3 report before closing an incident.
The `items` collection is authoritative; deleted/skipped ID arrays are derived
projections. Always validate reports with `dpone gitops schema validate`, which
also enforces projection equality that portable JSON Schema cannot express.
For every non-empty deletion, regenerate the plan with the current binary,
obtain a current `dpone.airflow_loader_ack.v2`, and apply without copying fields
into an old evidence file. Selecting an output version never relaxes the plan,
ACK or desired-state authority checks.

After validating ACK v2 and while the same exclusive promotion lock is still
held, apply first validates the complete deletion set without mutation. It then
upserts the exact occurrence into
`<cache-root>/.retention-activation-history.v2.json`. The key is the canonical
UUIDv4 `activation_id`; the entry binds release, deployment, Airflow index and
loaded DAG inventory. The file is atomically replaced and fsynced before any
candidate deletion. A write, fsync, conflict, capacity or schema
failure returns `DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_FAILED` and leaves
all candidates untouched. Successful destructive v2 and v3 JSON includes the required
`activation_history_revision` digest whenever a deployment is deleted. If the
historical local v1 file exists,
its bounded entries are migrated only as diagnostic SHA-256 hashes; legacy
payload fields never authorize deletion and are not copied into v2. The
history and recovery-journal control files are validated against closed public
schemas; unknown fields cannot silently become deletion authority.

Destructive apply holds `reconcile_lock` and then `promotion_lock`. Under that
fixed hierarchy it requires the local desired-state checkpoint, physical
`current`, pointer, promotion audit and loader ACK to name the same release,
deployment, activation, Airflow index and complete DAG inventory. Remote S3 is
never polled inside the destructive transaction; the checkpoint is the local
linearized desired-state authority, and a later remote update must first pass
the normal reconcile cycle.

If pointer and physical `current` state disagree, retention fails with
`DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED` before deleting any candidate.
The active projection itself must also pass full
deployment/index/release/artifact validation under that lock; fingerprint or
artifact corruption requires recovery and cannot turn another valid deployment
into a delete candidate. Each delete candidate passes the same validation.
The entire delete set is validated before activation-history mutation and the
first removal. Candidate validation failure reports
`state_may_have_changed: false`, an empty deleted set and the underlying stable
`cause_code`. Before each validated candidate is detached, retention creates or
validates a sealed activation snapshot for those exact bytes, including a
generation that was never active. If snapshot preparation fails, the original
candidate remains in place and apply reports
`DPONE_DEPLOYMENT_CACHE_ACTIVATION_SNAPSHOT_FAILED`. If a later filesystem
deletion fails, JSON
returns `DPONE_DEPLOYMENT_CACHE_GC_DELETE_FAILED` with `failed_step`,
`state_may_have_changed`, `deleted_deployment_ids`, `deleted_count`,
`failed_deployment_id`, `failed_paths`, `restored_deployment_ids`,
`quarantined_paths`, and `retention_incomplete`; rerun the plan before any
retry only when recovery or authority changed. Otherwise a v3 controller
repeats the same reviewed digest so the durable receipt can resume. Absolute
paths are redacted at the CLI boundary.

Interrupted trash recovery writes `.retention-recovery.json` before the first
restore and after every restored deployment. `status=recovering|blocked`
prevents a new plan; `status=recovered` advances `recovery_revision` and forces
a fresh review. A blocked record names restored/pending identities and the
quarantined paths so a crash after restoring only part of the inventory cannot
be mistaken for an untouched cache. `blocked` is retried after its underlying
filesystem or permission fault is repaired. Recovery restores an intact
detached inode first. A partially deleted detached tree is never published; a
validated sealed activation is copied through private staging, sealed,
fingerprinted, atomically published and fsynced instead. Replay repairs a crash
between that rename and the final root seal, while the activation remains
available as last-known-good evidence.

New journals use `dpone.deployment-cache-retention-recovery.v2`, key each
transaction by immutable `transaction_id`, and bind it to the exact retention
`operation_id`. Historical v1 records are read and projected as
operation-unbound (`operation_id: null`) for diagnostics.
They never advance a receipt merely because a deployment id matches. An
incomplete receipt encountering unbound legacy WAL is durably aborted and
requires a fresh plan and approval; already committed filesystem evidence is
not discarded or relabelled.
Recovery acknowledgement v2 records exact transaction IDs cumulatively;
unrelated journal writes do not invalidate an acknowledged restoration. Before
downgrading to a runtime that cannot read v2, stop mutation and follow the
executable [retention state downgrade runbook](airflow-cache-retention-state-downgrade.md),
which distinguishes fresh `emptyDir` reconstruction from persistent-volume
snapshot restoration. Never hand-edit v2 WAL, ACK, receipts, or activation
history to force a downgrade.
An invalid directory name has no trustworthy deployment identity, so its
quarantine item carries `deployment_id: null` with the exact error code and
path; it is never included in delete or skipped-ID lists. Human-readable plan
and apply output reports `NEEDS_ATTENTION`, labels the item as an
`unidentified cache entry`, and shows its stable error code without exposing
the cache path. JSON remains the topology-bearing diagnostic format.

The retention schemas permit a `null` deployment identity only for an
incomplete/invalid quarantine item and, in apply output, only with
`action: skipped`. `protect`, `delete`, and `deleted` actions always require a
canonical deployment digest. This prevents a damaged cache entry from being
misrepresented as a destructive action.

Apply exits `0` after a complete repair, `4` when confirmation is missing, and
`1` for other recovery failures. If apply itself reports
`DPONE_CACHE_PROMOTION_WRITE_FAILED`, local state may have changed; run the plan
again before any retry.

## Recovery by error family

| Error family | Meaning | Recovery |
|---|---|---|
| `DPONE_AIRFLOW_INDEX_NOT_FOUND`, `DPONE_AIRFLOW_INDEX_READ_FAILED`, `DPONE_AIRFLOW_INDEX_TOO_LARGE`, `DPONE_AIRFLOW_INDEX_JSON_INVALID`, index schema/field/identity errors | The provider cannot establish a trusted deployment snapshot and returns `fatal: true`. | Run `cache-recovery-plan`. Rematerialize or rebuild the exact projection as directed, then use a fresh reviewed CAS guard; never suppress the generated loader failure. |
| `DPONE_AIRFLOW_DAG_SPEC_LOAD_FAILED`, `DPONE_CACHE_ARTIFACT_MISSING`, `DPONE_CACHE_ARTIFACT_SIZE_MISMATCH`, `DPONE_CACHE_CHECKSUM_MISMATCH` with `fatal: false` | One listed DAG/pack is corrupt or unreadable; unrelated DAGs may remain available. | Restore the immutable release bytes from trusted storage or build a new release/projection. Do not patch the affected file in place. |
| `DPONE_RELEASE_NOT_FOUND` | The release is absent or the projection has no usable release reference. | Materialize the pinned release, or rebuild the deployment projection with a valid `release_ref`. |
| `DPONE_RELEASE_*INVALID`, `DPONE_RELEASE_FINGERPRINT_MISMATCH` | The immutable release contract or content address is wrong. | Build a new immutable release-set, then rebuild its deployment projection. |
| `DPONE_RELEASE_INDEX_ARTIFACT_MISMATCH`, index/reference errors | The deployment projection does not represent the pinned release. | Rebuild the deployment projection from the unchanged release. |
| Missing, unreadable, size-mismatched, or checksum-mismatched artifact | Local release content is incomplete or corrupt. | Restore the exact content-addressed release from trusted storage; never edit it in place. |
| `DPONE_CACHE_ARTIFACT_TOO_LARGE` | The artifact exceeds the configured local verification budget. | Review the platform size policy or build a smaller artifact; restoring the same oversized file is not a fix. |
| Confirmation required | The explicit mutation acknowledgement is absent. | Review the candidate and add `--confirm-promote`. |
| Promoter missing/unauthorized | The caller is not an allowed CI/service identity. | Use an allowed identity or update the platform allowlist through review. |
| `DPONE_CURRENT_POINTER_CAS_MISMATCH` | Another promotion or recovery changed active current. | Refresh the plan and retry with its active deployment ID as the CAS guard. |
| `DPONE_CURRENT_RELEASE_ID_MISMATCH` | Pointer release does not match the validated active projection. | Run confirmed recovery for the reviewed deployment; audit-only repair is forbidden. |
| `DPONE_CURRENT_PATH_ID_UNAVAILABLE`, `DPONE_DEPLOYMENT_CACHE_RECOVERY_BLOCKED` | `current` exists but has no canonical identity for stale-plan CAS. | Repair or remove the unsafe path through platform change control, rerun the plan, then apply only a fresh repairable plan. |
| `DPONE_DEPLOYMENT_FINGERPRINT_MISMATCH`, mirror mismatch | Deployment content or its Airflow projection no longer matches `deployment_id`. | Rebuild the immutable deployment projection; never rebless the old ID. |
| `DPONE_DEPLOYMENT_CACHE_RECOVERY_REQUIRED` | Existing pointer and active current are inconsistent. | Stop normal promotion and run the recovery plan/apply workflow. |
| `DPONE_CACHE_PROMOTION_LOCK_FAILED` | The exclusive control-plane lock is unsafe or unavailable. | Repair cache-root ownership/type; never replace it with a symlink. |
| `DPONE_DEPLOYMENT_PATH_OUTSIDE_CACHE_ROOT` | The selected projection is outside the configured cache. | Select or materialize the deployment below the configured cache root. |
| `DPONE_DEPLOYMENT_PATH_INVALID` | The selected path does not use the canonical deployment layout. | Use `deployments/<environment>/<deployment_id>` below the cache root. |
| `DPONE_DEPLOYMENT_ACTIVATION_FAILED` | A sealed activation snapshot could not be copied, verified, or published. | Keep the existing current deployment, repair local cache permissions or storage, then retry promotion. |
| `DPONE_DEPLOYMENT_ACTIVATION_CONFLICT` | Existing immutable activation bytes differ from the newly validated candidate for the same deployment ID. | Quarantine the cache and rematerialize from trusted content; never overwrite the activation in place. |
| `DPONE_CURRENT_POINTER_ACTIVATION_ID_INVALID` | The supplied desired-state occurrence or local activation factory did not provide a canonical UUIDv4. | Republish the desired state through the trusted promotion command. The materializer rejects this before creating an activation snapshot or changing cache permissions. |
| `DPONE_DEPLOYMENT_CACHE_GC_VALIDATION_FAILED` | At least one reviewed delete candidate could not be revalidated before activation history or directory mutation. JSON includes the underlying stable `cause_code` and `state_may_have_changed=false`. | Repair or rematerialize the candidate, then generate and review a fresh retention plan. No deployment or retention control state was changed by candidate processing. |
| `DPONE_DEPLOYMENT_CACHE_GC_PLAN_REQUIRED` | A fresh plan contains delete candidates but apply has no canonical reviewed digest. | Run `cache-retention-plan --format json`, review the complete candidate set, then pass its exact `plan_sha256`. |
| `DPONE_DEPLOYMENT_CACHE_GC_REVIEW_ID_REQUIRED` | A destructive plan has no approved-attempt UUIDv4. | Generate one UUIDv4 in the approval job, persist it beside the reviewed plan digest, and reuse it only for retries of that attempt. |
| `DPONE_DEPLOYMENT_CACHE_GC_REVIEW_ID_INVALID` | The supplied review id is not a canonical UUIDv4. | Replace it through the reviewed GitOps change; do not normalize arbitrary text inside the watcher. |
| `DPONE_DEPLOYMENT_CACHE_GC_EVIDENCE_VERSION_INVALID` | An embedded caller requested an unknown apply evidence projection. Validation fails before retention locks or mutation. | Use `v1` for the original projection, `v2` for reviewed-plan/history evidence, or `v3` for replayable receipt evidence. CLI callers should use the enumerated `--evidence-version` choices. |
| `DPONE_DEPLOYMENT_CACHE_GC_ACK_REQUIRED` | A fresh plan contains delete candidates but no loader ACK was supplied. | Obtain the external `dpone.airflow_loader_ack.v2` written by the exact active DAG parse and pass it with `--loader-ack-file`. |
| `DPONE_DEPLOYMENT_CACHE_GC_PLAN_CHANGED` | Current, activation authority, protection evidence, recovery revision, candidate inventory, or filesystem occurrence changed after review. An interrupted receipt is durably aborted before this result is returned. | Confirm `transaction_status=aborted`, discard the old approval, create and review a new plan plus UUIDv4 review id, then retry. |
| `DPONE_DEPLOYMENT_CACHE_GC_ACK_INVALID` | The ACK is stale, fatal, partial, has errors/skips, or does not match the exact active release/deployment/index/activation and DAG inventory. | Diagnose the DAG parse, produce a clean ACK for current, and rerun plan/apply. Do not edit ACK JSON. |
| `DPONE_DEPLOYMENT_CACHE_ACTIVATION_HISTORY_FAILED` | The fsynced exact-occurrence history could not be committed before retention. | Preserve all deployments, repair the history path/capacity or conflicting occurrence, then generate a fresh plan. Do not bypass history. |
| `DPONE_DEPLOYMENT_CACHE_ACTIVATION_SNAPSHOT_FAILED` | A reviewed candidate could not obtain a sealed recovery snapshot before detach. | Keep the original candidate, repair local storage or permissions, run recovery if requested by evidence, then generate a fresh plan. Never bypass the snapshot gate. |
| `DPONE_DEPLOYMENT_CACHE_GC_PATH_CHANGED` | A candidate path or inode changed between validation and atomic detach. | Stop deletion, inspect the original and quarantined paths, restore trusted immutable bytes, then build a fresh plan. |
| `DPONE_DEPLOYMENT_CACHE_GC_DETACH_FAILED` | The private detach root is unavailable or unsafe. | Repair cache-root filesystem type/permissions; do not replace the detach root with a symlink or bypass atomic detach. |
| `DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_REQUIRED` | A previous destructive cycle left a verified detached deployment. The applier restored it and invalidated the old plan with a new recovery revision. | Confirm the restored canonical path, generate a fresh plan, review it, and retry. Never replay the pre-recovery digest. |
| `DPONE_DEPLOYMENT_CACHE_GC_RECOVERY_JOURNAL_FAILED` | A recovery transition changed or may have changed local state but its fsynced journal could not be updated. | Stop retention, inspect trash plus restored/pending identities, repair storage, and run recovery before a new plan. |
| `DPONE_DEPLOYMENT_CACHE_GC_DELETE_FAILED` | Filesystem deletion stopped after zero or more reviewed candidates. | Inspect `deleted_deployment_ids` and `failed_deployment_id`. For v3, repair the filesystem and retry the same digest so the durable receipt resumes; generate a fresh plan only when recovery or authority changed. v1/v2 lack receipt replay evidence and require a fresh reviewed plan. |
| `DPONE_DEPLOYMENT_CACHE_GC_RECEIPT_INVALID` | The replay receipt root, inventory, bytes, ownership, permissions, revision, or capacity is unsafe. | Stop retention and preserve the receipt directory. Repair the exact control-state problem; do not delete or edit receipts. Retry the same v3 digest only after the complete inventory validates. |
| Incomplete/environment/schema deployment errors | The projection is not promotable as requested. | Rebuild/select the complete projection for the requested environment. |
| `DPONE_CACHE_PROMOTION_WRITE_FAILED` | A local current, pointer, or audit write failed after validation. | Run `cache-recovery-plan`; reconcile split state before retry. |

For checksum-specific fields and output examples, see
[`DPONE_CACHE_CHECKSUM_MISMATCH`](errors/DPONE_CACHE_CHECKSUM_MISMATCH.md).
After a successful retry, verify local pointer health:

```bash
: "${DPONE_SCHEDULER_CACHE_ROOT:?set the absolute scheduler cache root}"

dpone airflow cache-recovery-plan \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --environment prod
```
