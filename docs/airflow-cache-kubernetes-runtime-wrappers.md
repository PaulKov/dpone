# Configure Airflow cache runtime wrappers

**Purpose.** Configure the bounded fail-open init wrapper and continuous watcher, including reviewed cache-retention approval.

**Audience.** Platform engineers responsible for parser-cache startup, refresh, and retention policy.

[Back to Kubernetes cache deployment overview](airflow-cache-kubernetes-deployment.md) · **Next likely task:** [deliver the reviewed wrappers](airflow-cache-kubernetes-wrapper-delivery.md).

## Fail-open init wrapper

The init container may delay startup by at most the configured timeout and must
always exit zero. This preserves ordinary Airflow DAG availability. dpone DAGs
remain fail-visible when no verified `current` exists.

```sh
set +e
timeout "${DPONE_PACK_SYNC_TIMEOUT_SECONDS:-20}" \
  dpone airflow desired-state reconcile \
  --connection-type airflow \
  --connection-id "${DPONE_AIRFLOW_PACK_READER_CONNECTION_ID}" \
  --artifact-connection-type airflow \
  --artifact-connection-id "${DPONE_AIRFLOW_PACK_READER_CONNECTION_ID}" \
  --cache-root "${DPONE_AIRFLOW_PACK_CACHE_DIR}" \
  --max-total-bytes "${DPONE_PACK_CACHE_MAX_TOTAL_BYTES:-536870912}"
rc=$?
printf 'dpone exact-cache init exit=%s; Airflow startup remains fail-open\n' "${rc}"
exit 0
```

The image must provide a POSIX-compatible `timeout` command. Verify that in the
image build; do not discover it during a production restart.

## Watch wrapper

The sidecar repeats the same atomic reconcile command. It does not split fetch,
materialize and activation into shell-managed steps.

```sh
set +e
trap 'exit 0' TERM INT

publish_retention_status() {
  publication_tmp="$(mktemp "$(dirname "$2")/.retention-publication.XXXXXX" 2>/dev/null)" || return 4
  dpone airflow cache-status-publish \
    --status-root "$(dirname "$2")" \
    --source "$(basename "$1")" \
    --target "$(basename "$2")" \
    --failure-marker "$4" \
    --expected-schema "$3" \
    --format json >"${publication_tmp}"
  publication_rc=$?
  if [ -s "${publication_tmp}" ]; then
    chmod 0600 "${publication_tmp}" && mv -f "${publication_tmp}" "$5" || publication_rc=4
  else
    rm -f "${publication_tmp}"
  fi
  return "${publication_rc}"
}

retention_error_code() {
  python3 -c 'import json,sys; p=json.load(open(sys.argv[1])); errors=p.get("errors") or []; print((errors[0].get("code") if errors else None) or p.get("error_code") or ("ok" if p.get("passed") is True or p.get("status") in {"ok", "needs_cleanup", "committed"} else "unavailable"))' "$1" 2>/dev/null || printf 'unavailable\n'
}

cycle=0
while :; do
  timeout "${DPONE_PACK_SYNC_TIMEOUT_SECONDS:-20}" \
    dpone airflow desired-state reconcile \
    --connection-type airflow \
    --connection-id "${DPONE_AIRFLOW_PACK_READER_CONNECTION_ID}" \
    --artifact-connection-type airflow \
    --artifact-connection-id "${DPONE_AIRFLOW_PACK_READER_CONNECTION_ID}" \
    --cache-root "${DPONE_AIRFLOW_PACK_CACHE_DIR}" \
    --max-total-bytes "${DPONE_PACK_CACHE_MAX_TOTAL_BYTES:-536870912}"
  rc=$?
  printf 'dpone exact-cache watch exit=%s\n' "${rc}"
  cycle=$((cycle + 1))
  if [ "${rc}" -eq 0 ] \
    && [ $((cycle % ${DPONE_PACK_RETENTION_INTERVAL_CYCLES:-60})) -eq 0 ] \
    && [ -f /opt/airflow/.dpone-ack/loader-ack.json ]; then
    retention_status_dir="${DPONE_AIRFLOW_PACK_CACHE_DIR}/status"
    umask 077
    mkdir -p "${retention_status_dir}"
    chmod 0700 "${retention_status_dir}"
    status_dir_rc=$?
    if [ "${status_dir_rc}" -ne 0 ]; then
      printf 'dpone cache retention status directory unavailable exit=%s\n' "${status_dir_rc}"
      sleep "${DPONE_PACK_SYNC_INTERVAL_SECONDS:-60}" & wait $!
      continue
    fi
    plan_tmp="$(mktemp "${retention_status_dir}/.retention-plan.XXXXXX" 2>/dev/null)"
    if [ -z "${plan_tmp}" ]; then
      printf 'dpone cache retention plan temp unavailable\n'
      sleep "${DPONE_PACK_SYNC_INTERVAL_SECONDS:-60}" & wait $!
      continue
    fi
    plan_status="${retention_status_dir}/last-retention-plan.json"
    dpone airflow cache-retention-plan \
      --cache-root "${DPONE_AIRFLOW_PACK_CACHE_DIR}" \
      --environment "${DPONE_ENVIRONMENT:-dev}" \
      --evidence-file /opt/airflow/.dpone-ack/loader-ack.json \
      --format json >"${plan_tmp}"
    plan_rc=$?
    plan_code="$(retention_error_code "${plan_tmp}")"
    plan_publication_status="${retention_status_dir}/last-retention-plan-publication.json"
    publish_retention_status \
      "${plan_tmp}" "${plan_status}" \
      dpone.deployment-cache-retention-plan.v1 \
      last-retention-plan-publication-failure.json \
      "${plan_publication_status}"
    publish_rc=$?
    publish_code="$(retention_error_code "${plan_publication_status}")"
    rm -f "${plan_tmp}"
    printf 'dpone cache retention plan exit=%s code=%s publish=%s publish_code=%s evidence=%s publication=%s\n' \
      "${plan_rc}" "${plan_code}" "${publish_rc}" "${publish_code}" \
      "${plan_status}" "${plan_publication_status}"
    if [ "${plan_rc}" -eq 0 ] && [ "${publish_rc}" -eq 0 ]; then
      plan_sha256="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["plan_sha256"])' "${plan_status}" 2>/dev/null)"
      approved_sha256="${DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256:-}"
      approved_review_id="${DPONE_CACHE_RETENTION_REVIEW_ID:-}"
      if [ -z "${approved_sha256}" ]; then
        printf 'dpone cache retention plan-only reason=approval_missing plan_sha256=%s\n' "${plan_sha256}"
      elif [ -z "${approved_review_id}" ]; then
        printf 'dpone cache retention plan-only reason=review_id_missing plan_sha256=%s\n' "${plan_sha256}"
      elif [ "${approved_sha256}" != "${plan_sha256}" ]; then
        printf 'dpone cache retention plan-only reason=approval_mismatch approved=%s actual=%s\n' \
          "${approved_sha256}" "${plan_sha256}"
      else
        apply_tmp="$(mktemp "${retention_status_dir}/.retention-apply.XXXXXX" 2>/dev/null)"
        apply_status="${retention_status_dir}/last-retention-apply.json"
        if [ -z "${apply_tmp}" ]; then
          printf 'dpone cache retention apply temp unavailable\n'
        else
          dpone airflow cache-retention-apply \
          --cache-root "${DPONE_AIRFLOW_PACK_CACHE_DIR}" \
          --environment "${DPONE_ENVIRONMENT:-dev}" \
          --evidence-file /opt/airflow/.dpone-ack/loader-ack.json \
          --expected-plan-sha256 "${approved_sha256}" \
          --review-id "${approved_review_id}" \
          --loader-ack-file /opt/airflow/.dpone-ack/loader-ack.json \
          --promoted-by "${DPONE_CACHE_RETENTION_IDENTITY}" \
          --allowed-promoter "${DPONE_CACHE_RETENTION_IDENTITY}" \
          --confirm-delete \
          --evidence-version v3 \
          --format json >"${apply_tmp}"
          apply_rc=$?
          apply_code="$(retention_error_code "${apply_tmp}")"
          apply_publication_status="${retention_status_dir}/last-retention-apply-publication.json"
          publish_retention_status \
            "${apply_tmp}" "${apply_status}" \
            dpone.deployment-cache-retention-apply.v3 \
            last-retention-apply-publication-failure.json \
            "${apply_publication_status}"
          apply_publish_rc=$?
          apply_publish_code="$(retention_error_code "${apply_publication_status}")"
          rm -f "${apply_tmp}"
          printf 'dpone cache retention apply exit=%s code=%s publish=%s publish_code=%s evidence=%s publication=%s\n' \
            "${apply_rc}" "${apply_code}" "${apply_publish_rc}" "${apply_publish_code}" \
            "${apply_status}" "${apply_publication_status}"
        fi
      fi
    else
      printf 'dpone cache retention skipped apply code=%s\n' \
        "${plan_code}"
    fi
  fi
  sleep "${DPONE_PACK_SYNC_INTERVAL_SECONDS:-60}" & wait $!
done
```

Initialize `cycle=0` before the loop. Retention is plan-only by default. It can
apply only when a regular external ACK exists **and** the reviewed Helm/config
rollout supplies `DPONE_CACHE_RETENTION_APPROVED_PLAN_SHA256` equal to the exact
published plan and a persisted UUIDv4 `DPONE_CACHE_RETENTION_REVIEW_ID` for
that approved attempt. The next changed plan no longer matches, and an aborted
attempt requires a new review id, so one approval cannot authorize a later
deletion cycle. Current is always protected by cache authority;
the ACK protects the last parsed activation as the rollback window.
Any plan/apply blocker keeps bytes. The loop publishes only a bounded payload
that passes its registered command schema. A malformed, oversized, unsafe or
wrong-schema temporary file leaves the last-known-good status untouched and
writes `dpone.airflow-cache-status-publication-failure.v1` to the corresponding
`*-publication-failure.json` marker. Container logs contain exit codes, stable
error code and evidence path rather than unbounded command output.
Infrastructure must alert on non-zero plan/apply or evidence-publication return
codes and on a present publication-failure marker. The controller mounts ACK
read-only and never publishes it.

After authority loading and cache-root initialization, reconcile writes bounded
`running`, `success` or `failure` evidence under
`<cache-root>/status/last-reconcile-status.json`. The shell must not
manufacture a second status schema. A killed in-progress cycle leaves its
non-green `running` record and preserves the last-known-good activation.

Failures before that boundary, such as a missing authority mount or an
unreadable cache root, can exit before a status file exists. Treat an absent or
stale file as a diagnostic blocker, not success. The wrapper still exits zero
so ordinary Airflow DAGs start; the original reconcile return code remains in
the container log:

| Reconcile exit | Meaning | Status expectation |
| --- | --- | --- |
| `0` | cycle completed or already converged | exact success evidence exists |
| `2` | local input, authority or filesystem preflight invalid | status may be absent; alert immediately |
| `3` | desired state temporarily unavailable/not found | failure evidence exists after cycle entry; otherwise status is absent |
| `4` | policy/identity/activation failure or recovery required | failure evidence, possibly `state_may_have_changed=true` |
| `5` | unexpected dependency failure at the public boundary | status may be absent; alert immediately |
| `124` | wrapper timeout terminated reconcile | keep last-known-good; a non-terminal `running` status may remain; alert and retry next cycle |
| `128+N` | wrapper or process terminated from signal `N` | keep last-known-good; inspect pod termination reason before retrying |

The command writes no success payload to stdout. Diagnostic details are in the
bounded status file; stderr/logs expose only error class/category. Alert when
the status file is absent after startup, older than two watch intervals, or
contains `passed=false`. Add bounded jitter in infrastructure when many Airflow
deployments share one object store.

## Retention approval and withdrawal

Use the task-focused [cache-retention approval runbook](airflow-cache-retention-approval.md#executable-approval-and-withdrawal-procedure). It owns plan review, one-attempt approval identity, apply evidence, withdrawal polling and sealed retry identity.
