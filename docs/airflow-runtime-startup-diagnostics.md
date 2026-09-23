# Diagnose Airflow runtime startup failures

This runbook helps operators diagnose strict runtime and separate-hook startup
without exposing container credentials. Start from the task's base-container
log and collect run-volume diagnostics while the containers are still running.
See [strict delivery](airflow-cache-sync-strict-v2.md) for init-fetch failures.

## Live logs are unavailable before runtime starts

`DPONE_KPO_LOG_STREAM_DEGRADED` means Kubernetes rejected the base-container
log stream with a retryable response, including the structured HTTP 400
`PodInitializing` startup response. The task switches to pod-status polling at
its configured base-container interval. It reads fresh status immediately and
on every poll; a running workload continues without live logs.

If a one-shot init container terminates unsuccessfully and the pod cannot retry
it, the task reports `DPONE_KPO_INIT_CONTAINER_FAILED exit_code=7` (synthetic
example). If the pod is terminal while the base has no termination status, it
reports `DPONE_KPO_POD_TERMINATED`. Both stop polling immediately and refresh
the pod status used by cleanup. These dpone diagnostic messages omit pod/container names,
termination messages, environment values and API response bodies.

Inspect the retained pod's init status and platform diagnostics using your
normal restricted access. Correct the init configuration or image, then retry
through Airflow. Increasing execution timeout does not repair a failed init.
Normal Kubernetes init retries and restartable init sidecars remain supported;
base exit-code handling, XCom, task retries and pod retention policy still
belong to the Kubernetes provider.

Upgrade requires the matching updated worker packages; no DAG ID, manifest,
retry, or timeout changes are required. Maintainers can run the synthetic
regressions with `uv run pytest tests/test_kpo_fallback_completion.py
tests/test_kpo_live_base_container_logs.py -q`. Installed-provider lifecycle
tests require the corresponding Airflow/Kubernetes provider environment; these
unit tests do not certify live cluster behavior.

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

For external ClickHouse publication, also verify that the Pod receives an
absolute `DPONE_EXTERNAL_ARTIFACT_STORE` path backed by durable storage shared
across eligible retry workers. Typed external-publication failures are expected
in `runtime-stderr.log`; an empty stdout is intentional and is not missing
success evidence.

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

## Unknown dbt target outcome

When `dpone dbt execute-pack` reports `COMMIT_UNKNOWN`, the target mutation may
have committed. The CLI reports an unknown outcome, not a safe rejection before
execution. Preserve the attempt evidence and retained resources; do not retry
automatically. Ask the platform owner to reconcile the target against the
original execution evidence before admitting another attempt. Exit code `1`,
the `COMMIT_UNKNOWN` code and the existing recovery guidance remain unchanged.
