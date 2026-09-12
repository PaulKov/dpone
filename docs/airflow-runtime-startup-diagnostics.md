# Diagnose Airflow runtime startup failures

This runbook helps operators diagnose strict runtime and separate-hook startup
without exposing container credentials. Start from the task's base-container
log and collect run-volume diagnostics while the containers are still running.
See [strict delivery](airflow-cache-sync-strict-v2.md) for init-fetch failures.

## Service files and publication

Both runtime and separate hooks use `/var/lib/dpone/run` on the provider-owned
writable `emptyDir`. The verified workload and fetched artifacts stay mounted
read-only. A non-root image can write the run volume without changing its
workload files. The child still uses the verified workload directory as its
working directory; a custom hook must place its own writable outputs elsewhere.

| File | Meaning |
|---|---|
| `runtime-evidence.json` | Captured child stdout; hooks may emit text rather than JSON |
| `runtime-stderr.log` | Captured child stderr, also streamed to the Airflow log |
| `runtime-summary.json` | Local outcome summary, including hooks without XCom |
| `runtime-startup-error.json` | Safe OS-failure diagnostic when the run volume can be written |

Runtime, including dbt, enables KPO XCom and publishes the summary to
`/airflow/xcom/return.json`. KPO supplies the writable XCom volume and sidecar.
Separate pre-hooks disable publication and perform no required access to
`/airflow/xcom`. `VerifiedPackCommand.publish_xcom` defaults to true for existing
Python callers; the verified launcher sets it from the execution kind.

SQL hooks with canonical `connection_ref` use the same pinned binding set,
connection registry and credential-runtime context as the main workload. The
verified launcher supplies that context automatically. If a separate hook reports
`DPONE_RUNTIME_CONNECTION_CONTEXT_REQUIRED` after successful init-fetch, check
that the pinned runtime image includes the hook context-handoff fix, then rebuild
and activate the deployment with the matching packages.

Ordinary runtime retains the XCom outcome-gate exit policy. dbt and hooks retain
the real child exit code; failed hooks block downstream `all_success` runtime
tasks and do not gain automatic retries. An OS failure in preparation, capture
or publication returns nonzero. Kubernetes/Airflow task status and local
artifacts remain necessary for a failed task; Airflow only collects XCom from
successful KPO tasks.

## Read the failure

The `DPONE_RUNTIME_PACK_EXEC_FAILED` diagnostic includes a stable stage,
exception class, numeric `errno` and a logical service-path role. It omits raw
arguments, environment, exception messages and exception filenames. The log is
the primary fallback when permissions or a full disk prevent artifact writes.
A secondary artifact failure must not hide the original failure.

| errno | Typical cause | Recovery |
|---|---|---|
| `13` (`EACCES`) | Run/XCom path or executable is inaccessible | Inspect admitted mounts, image UID and filesystem permissions for the reported role |
| `2` (`ENOENT`) | Working directory or executable is missing | Verify the pinned image entry point and verified workload contents |
| `28` (`ENOSPC`) | Run volume or underlying node storage is full | Inspect actual filesystem capacity and node ephemeral-storage pressure; free or increase capacity |
| `30` (`EROFS`) | A required output path is mounted read-only | Restore the provider-owned writable service mount |

## Composition dispatch rejections

A workload from an authenticated `dpone.release-set.v3` release carries
`DPONE_RUNTIME_RELEASE_ADMISSION=dpone.release-composition-admission.v1` plus the
pinned `DPONE_COMPOSITION_SUPERVISOR_B64` capability. The verified launcher
derives the marker from the authenticated release bytes and the supervisor
transport from the sealed deployment bytes, and it pins both into the verified
command itself. The runtime classifies admission from that command environment
only, so the ambient Pod environment can neither admit, erase nor substitute
composition. Such a workload runs exclusively through its supervised composition
worker. It never falls back to native-v2 admission, generic `dpone run` or shell
execution, so a missing capability is a rejection instead of a degraded run.

A rejection exits `5`, logs `DPONE_RUNTIME_COMPOSITION_DISPATCH_REJECTED` with
`stage=composition_dispatch` and a fixed `reason` token, writes that payload to
`runtime-startup-error.json`, and publishes a failed summary. An admission
rejection also replaces `runtime-evidence.json`, because no worker ran, no child
process started and no source connection opened. A rejection after the worker was
already dispatched preserves whatever evidence that attempt produced. Arguments,
environment values and credentials never appear in the diagnostic.

| `reason` | Meaning | Recovery |
|---|---|---|
| `unknown_release_admission` | The verified command admission value is neither empty nor the exact composition marker. | Rebuild the deployment with matching provider and runtime-image versions |
| `composition_dispatcher_unavailable` | The pinned runtime image provides no supervised composition worker. | Activate a deployment whose runtime image includes the composition workers; do not retry the same image |
| `composition_supervisor_authority_missing` | The verified command carries no pinned `DPONE_COMPOSITION_SUPERVISOR_B64` capability. | Rebuild the deployment with an approved supervisor capability (`dpone airflow build --composition-supervisor-pvc ...`) |
| `composition_supervisor_authority_invalid`, `composition_supervisor_authority_noncanonical`, `composition_supervisor_authority_oversize`, `composition_authority_invalid` | The supervisor transport is not the exact canonical projection sealed into the verified deployment. | Rebuild and promote the deployment; never hand-edit pod environment values |
| `composition_command_shape`, `composition_command_unknown` | The verified command is not an admitted composition command. | Rebuild the release with a supported workload command; composition admits only `dpone dbt execute-pack <pack> --format json` and `dpone run <manifest> --format json [--selector <id>]` |
| `composition_command_path` | The admitted command input is not a safe worktree-relative path. | Rebuild the release; report the projection defect, because a verified command must never carry an absolute or traversing input |
| `composition_evidence_missing`, `composition_evidence_invalid`, `composition_evidence_stale` | The worker reported success without writing valid evidence for this attempt to the run volume. | Treat the attempt as failed; inspect the worker attempt ledger, then retry with a new task Pod |
| `composition_dispatch_failed` | The supervised worker itself refused or failed before returning a status. | Inspect the parent attempt ledger and worker evidence for that exact attempt; treat the outcome as unknown until proven |

Use the stage to distinguish directory preparation, output-file opening, process
start, stderr capture, local summary and XCom publication. A start marker records
an attempted launch, not proof that the child started. If a child ran before the
error, inspect its evidence and hook side effects before manual retry. Resource
declarations and actual disk availability are separate checks; see
[workload resources](airflow-workload-resources.md).

Retry uses a new task Pod in normal strict delivery. Direct Python callers that
reuse a run directory must not run two commands concurrently in it; the wrapper
invalidates previous attempt summaries before execution. Run-volume files are
ephemeral: kubelet can remove them after all containers stop, even when the Pod
object is retained. Configure platform collection during execution; retaining a
completed Pod alone does not guarantee that its service files remain available.

`dpone airflow runtime-pack-exec` is the provider's internal entry point. Running
it without the injected verified plan fails with exit `2` and
`DPONE_INIT_FETCH_PLAN_INVALID`; it is not a command to repair a failed Pod.
Restore the configuration/image and rebuild through the documented deployment
path. Return to the [provider overview](airflow-pack-provider.md).
