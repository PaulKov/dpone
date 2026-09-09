# Withdraw runtime Pod retention safely

**Purpose.** Define the evidence and ordering required to remove the runtime Pod retention control without deleting a replacement Kubernetes object or losing incident forensics.

**Audience.** SREs and Kubernetes platform operators performing rollback or complete control withdrawal.

[Back to runtime Pod retention overview](airflow-runtime-pod-retention.md) · **Next likely task:** execute the procedure below and retain its sealed evidence.

## Safe ordering

```text
preflight every read and mutation permission
  -> capture exact CronJob UID/resourceVersion
  -> capture label-scoped Jobs/Pods and retain only exact owner-chain matches
  -> require every owned Job to be terminal and inactive
  -> capture bounded Pod JSON, current/previous logs and object-UID events
  -> recapture Job/Pod UID sets and revalidate every Job terminal/inactive
  -> seal the immutable pre-mutation forensic manifest
  -> validate an external durable acknowledgement bound to that manifest
  -> suspend that occurrence with JSON Patch tests as the first mutation
  -> seal the acknowledged pre-delete evidence and checksums
  -> delete exact CronJob and captured Jobs with preconditions
  -> reject late owned Jobs and Pods on ordered recaptures
  -> require a quiet Job/Pod watch from the recapture resourceVersions
  -> recapture Jobs/Pods after that watch before deleting access
  -> delete exact access objects and prove their absence
  -> delete the exact monitoring object last
  -> prove absence and seal post-delete evidence
```

This page owns both the acceptance model and the single contract-tested executable procedure below.

## Forensic completeness

For every Pod owned by a captured Job, evidence includes:

- selector-scoped, byte-bounded Job/Pod inventory and complete captured Pod JSON;
- every init, regular, and ephemeral container name;
- tail/byte-bounded current and previous log attempts for every container;
- bounded capture status with exit code, stdout file, and stderr file;
- CronJob, Job, Pod, RBAC, optional alert, and per-object UID event snapshots;
- verified pre-delete and post-delete `SHA256SUMS`.

Unavailable previous logs are recorded as failed capture attempts; they are not
silently omitted. The evidence is `restricted` because Pod objects and logs may
contain operationally sensitive values. Before any withdrawal mutation, an
infrastructure-owned external sink must acknowledge the exact forensic manifest
with encryption both in transit and at rest. A local checksum, filesystem write,
stdout flush, or operator-authored receipt is not that acknowledgement.

The operator chooses one absolute operation directory, one reviewed 40-hex
removal commit, and one infrastructure-owned acknowledgement verifier whose
digest and issuer are pinned by the reviewed change. A first attempt seals
`pre/FORENSIC-SHA256SUMS`; the verifier publishes those exact files, performs
credential-bound remote readback, and returns an authenticated receipt. Only
then does the helper suspend the CronJob. The final
`pre/SHA256SUMS` includes the acknowledged receipt and post-suspend identity. A
retry validates and reuses those exact bytes only when the sealed Kubernetes
context, namespace, alert topology, CronJob UID and removal commit match its
inputs, so it can finish after the CronJob or access objects are already absent
without crossing an environment boundary. It never invents new authority from
remaining cluster state. Each retry writes a separate `post/attempt.*`
directory; final success writes sealed `withdrawal.json` bound to the removal
commit and acknowledgement digest.

## Mutation safety

The withdrawal helper uses the official Kubernetes Python client with explicit
context and namespace. Every shell read/log call uses a configurable kubectl
request timeout; every Python delete and follow-up read has a deadline-aware
`_request_timeout`. UID and resourceVersion preconditions bind mutation to the
captured occurrence. A replacement object, oversized response, API ambiguity,
timeout, active second Job snapshot, late Job, or late Pod blocks completion
instead of being treated as absence. Monitoring remains active until both final
late-work recaptures pass and access-object removal is verified.

## Acceptance

Withdrawal is complete only when:

- the exact CronJob and all exact captured Jobs are absent;
- no late Job owned by the captured CronJob UID and no Pod owned by a captured
  Job UID exists;
- the exact RoleBinding, Role, ServiceAccount, and optional PrometheusRule are
  absent;
- pre/post evidence directories are created only on verifier-attested encrypted
  storage, sealed, and retained outside the cluster;
- `pre/durable-ack.json` is external-sink output bound to the operation and
  `FORENSIC-SHA256SUMS` digests, classified `restricted`, encrypted in transit
  and at rest, authenticated by the pinned issuer/verifier, and marked
  `external_durable_acknowledged`;
- verification records `withdrawal_verified=true`, `late_owned_jobs=0`, and
  `late_owned_pods=0`;
- `withdrawal.json` and root `SHA256SUMS` bind success to the reviewed removal
  commit, context, namespace, alert topology, operation digest,
  acknowledgement digest and exact captured CronJob UID.

If a condition fails before access mutation, preserve access and monitoring
resources plus evidence for incident handling. A later failure may have already
removed access; monitoring remains until its explicitly last mutation. Do not
rerun committed data tasks because cleanup control withdrawal failed.

## External acknowledgement handoff

The procedure is deliberately two-phase inside one invocation. The pinned
`DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND` first verifies that the local operation
directory is on approved encrypted-at-rest storage. After capture, the helper
seals `pre/FORENSIC-SHA256SUMS` and invokes that command in
`publish-and-ack` mode. The command owns credentials, uploads every named file
to the approved restricted forensic sink over an encrypted channel, performs
remote readback, authenticates the sink response, and returns this bounded
receipt only after durable acknowledgement:

```json
{
  "schema": "dpone.airflow-runtime-pod-retention-withdrawal-durable-ack.v1",
  "status": "acknowledged",
  "durability": "external_durable_acknowledged",
  "evidence_classification": "restricted",
  "encryption": "at_rest_and_in_transit",
  "verification_method": "credentialed_remote_readback",
  "issuer": "urn:example:approved-forensic-sink",
  "verifier_sha256": "sha256:<reviewed verifier executable digest>",
  "operation_sha256": "sha256:<digest of pre/operation.json>",
  "evidence_manifest_sha256": "sha256:<digest of pre/FORENSIC-SHA256SUMS>",
  "sink_uri": "s3://approved-forensic-vault/runtime-pod-withdrawal/<operation>",
  "acknowledgement_id": "<external sink receipt identity>",
  "acknowledged_at": "<RFC 3339 timestamp>"
}
```

Do not hand-author or repair this receipt. The helper accepts it only from the
pinned verifier process and asks that same verifier to authenticate and remotely
read it back on retry. A verifier digest or issuer mismatch, local or insecure
sink URI, different evidence digest, weaker classification, partial encryption,
or unauthenticated response fails before the exact CronJob suspension patch.

## Executable withdrawal procedure


This page is the single contract-tested withdrawal procedure. Read the
[architecture and acceptance guide](airflow-runtime-pod-retention-architecture.md)
before executing it.

- When selectors, RBAC, evidence, or alerting are uncertain, stop before
  mutation. Use the full procedure below: its acknowledged forensic phase
  precedes the exact suspension patch. An emergency GitOps suspension performed
  outside this procedure is rollback mitigation, not certified full-withdrawal
  evidence, and must not be reported as such.

- Replace apply resources with the reviewed plan-mode manifest. Resume only
  after two green plan cycles. For full withdrawal, keep the reviewed context
  explicit:

  ```bash
  set -euo pipefail

  : "${KUBE_CONTEXT:?set the reviewed kubeconfig context}"
  : "${AIRFLOW_NAMESPACE:?set the namespace used by the reviewed Airflow Helm release}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_EVIDENCE_DIR:?set one externally retained operation directory}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT:?set the reviewed 40-hex removal commit}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_ACK:?set to preserve-forensics-and-withdraw}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND:?set the approved acknowledgement verifier executable}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256:?set its reviewed sha256 digest}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_ACK_ISSUER:?set the approved external sink issuer}"
  : "${DPONE_RUNTIME_POD_RETENTION_ALERTS:?set prometheus or off to match the rendered control}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_REQUEST_TIMEOUT_SECONDS:=30}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_MAX_JSON_BYTES:=8388608}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_LOG_TAIL_LINES:=1000}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_LOG_LIMIT_BYTES:=1048576}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT:=100}"
  : "${DPONE_RUNTIME_POD_WITHDRAW_QUIESCENCE_SECONDS:=30}"
  [ "${DPONE_RUNTIME_POD_WITHDRAW_ACK}" = preserve-forensics-and-withdraw ] || exit 2
  case "${DPONE_RUNTIME_POD_RETENTION_ALERTS}" in prometheus|off) ;; *) exit 2 ;; esac
  case "${DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT}" in *[!0-9a-f]*|'') exit 2 ;; esac
  [ "${#DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT}" -eq 40 ] || exit 2
  for value in "${DPONE_RUNTIME_POD_WITHDRAW_REQUEST_TIMEOUT_SECONDS}" \
    "${DPONE_RUNTIME_POD_WITHDRAW_MAX_JSON_BYTES}" \
    "${DPONE_RUNTIME_POD_WITHDRAW_LOG_TAIL_LINES}" \
    "${DPONE_RUNTIME_POD_WITHDRAW_LOG_LIMIT_BYTES}" \
    "${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" \
    "${DPONE_RUNTIME_POD_WITHDRAW_QUIESCENCE_SECONDS}"; do
    case "${value}" in ''|*[!0-9]*) exit 2 ;; esac
    [ "${value}" -gt 0 ] || exit 2
  done
  evidence_dir="${DPONE_RUNTIME_POD_WITHDRAW_EVIDENCE_DIR%/}"
  case "${evidence_dir}" in /*) ;; *) printf 'evidence directory must be absolute\n' >&2; exit 2 ;; esac
  [ ! -L "${evidence_dir}" ] || exit 2
  ack_command="${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND}"
  case "${ack_command}" in /*) ;; *) printf 'acknowledgement verifier path must be absolute\n' >&2; exit 2 ;; esac
  case "${ack_command}" in
    "${evidence_dir}"|"${evidence_dir}"/*)
      printf 'acknowledgement verifier must be installed outside the operation directory\n' >&2
      exit 2
      ;;
  esac
  [ -x "${ack_command}" ] && [ ! -L "${ack_command}" ] || exit 2
  case "${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256}" in
    sha256:*) ;;
    *) printf 'acknowledgement verifier digest must use sha256\n' >&2; exit 2 ;;
  esac
  [ "${#DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256}" -eq 71 ] || exit 2
  case "${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256#sha256:}" in
    *[!0-9a-f]*|'') exit 2 ;;
  esac
  observed_ack_command_sha256="sha256:$(sha256sum "${ack_command}" | awk '{print $1}')"
  [ "${observed_ack_command_sha256}" = "${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256}" ] || {
    printf 'acknowledgement verifier digest does not match the reviewed executable\n' >&2
    exit 10
  }
  case "${DPONE_RUNTIME_POD_WITHDRAW_ACK_ISSUER}" in
    urn:*|https://*) ;;
    *) printf 'acknowledgement issuer must be an approved URN or HTTPS identity\n' >&2; exit 2 ;;
  esac
  mkdir -p "${evidence_dir}/pre" "${evidence_dir}/post"
  chmod 0700 "${evidence_dir}" "${evidence_dir}/pre" "${evidence_dir}/post"
  withdraw_selector='app.kubernetes.io/name=dpone-runtime-pod-retention,app.kubernetes.io/managed-by=dpone'

  kube() {
    kubectl --context "${KUBE_CONTEXT}" --namespace "${AIRFLOW_NAMESPACE}" \
      --request-timeout="${DPONE_RUNTIME_POD_WITHDRAW_REQUEST_TIMEOUT_SECONDS}s" "$@"
  }
  capture_json_bounded() {
    output="$1"
    shift
    tmp="${output}.tmp.$$"
    trap 'rm -f "${tmp}"' RETURN
    kube "$@" | python3 -c '
import sys
limit = int(sys.argv[1])
payload = sys.stdin.buffer.read(limit + 1)
if len(payload) > limit:
    raise SystemExit("Kubernetes JSON capture exceeded configured byte limit")
sys.stdout.buffer.write(payload)
' "${DPONE_RUNTIME_POD_WITHDRAW_MAX_JSON_BYTES}" >"${tmp}"
    [ -s "${tmp}" ] || { printf 'sealed pre-delete evidence is required when the CronJob is absent\n' >&2; exit 8; }
    mv "${tmp}" "${output}"
    trap - RETURN
  }
  capture_relevant_events() {
    kind="$1"
    name="$2"
    uid="$3"
    output="${evidence_dir}/pre/event-${kind}-${name}.json"
    capture_json_bounded "${output}" get events \
      --field-selector "involvedObject.uid=${uid}" -o json
    jq -cn --arg kind "${kind}" --arg name "${name}" --arg uid "${uid}" \
      --arg file "$(basename "${output}")" \
      '{kind:$kind,name:$name,uid:$uid,file:$file}' \
      >>"${evidence_dir}/pre/event-captures.jsonl"
  }
  require_complete_inventory() {
    jq -e '(.metadata.continue // "") == ""' "$1" >/dev/null || {
      printf 'Kubernetes inventory exceeded configured item limit\n' >&2
      exit 8
    }
  }
  validate_durable_ack() {
    receipt="$1"
    expected_operation_sha256="$2"
    expected_manifest_sha256="$3"
    [ -f "${receipt}" ] && [ ! -L "${receipt}" ] || {
      printf 'external durable acknowledgement receipt is missing or unsafe\n' >&2
      exit 10
    }
    [ "$(wc -c <"${receipt}")" -le 65536 ] || {
      printf 'external durable acknowledgement receipt exceeds 64 KiB\n' >&2
      exit 10
    }
    jq -e --arg operation_sha256 "${expected_operation_sha256}" \
      --arg manifest_sha256 "${expected_manifest_sha256}" \
      --arg issuer "${DPONE_RUNTIME_POD_WITHDRAW_ACK_ISSUER}" \
      --arg verifier_sha256 "${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256}" '
        .schema == "dpone.airflow-runtime-pod-retention-withdrawal-durable-ack.v1"
        and .status == "acknowledged"
        and .durability == "external_durable_acknowledged"
        and .evidence_classification == "restricted"
        and .encryption == "at_rest_and_in_transit"
        and .verification_method == "credentialed_remote_readback"
        and .issuer == $issuer
        and .verifier_sha256 == $verifier_sha256
        and .operation_sha256 == $operation_sha256
        and .evidence_manifest_sha256 == $manifest_sha256
        and (.sink_uri | type == "string" and test("^(s3|gs|az|https)://")
          and length <= 2048)
        and (.acknowledgement_id | type == "string" and length > 0 and length <= 256)
        and (.acknowledged_at | type == "string"
          and test("^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\\.[0-9]+)?Z$"))
      ' "${receipt}" >/dev/null || {
        printf 'external durable acknowledgement is unauthenticated, unencrypted, or identity-mismatched\n' >&2
        exit 10
      }
  }
  verify_durable_ack() {
    receipt="$1"
    expected_operation_sha256="$2"
    expected_manifest_sha256="$3"
    "${ack_command}" verify-receipt \
      --receipt "${receipt}" \
      --expected-operation-sha256 "${expected_operation_sha256}" \
      --expected-manifest-sha256 "${expected_manifest_sha256}" \
      --expected-issuer "${DPONE_RUNTIME_POD_WITHDRAW_ACK_ISSUER}" \
      --classification restricted \
      --encryption at_rest_and_in_transit
    validate_durable_ack "${receipt}" "${expected_operation_sha256}" \
      "${expected_manifest_sha256}"
  }
  preflight_permissions() {
    for permission in \
      'get cronjobs.batch' 'patch cronjobs.batch' 'delete cronjobs.batch' \
      'list jobs.batch' 'watch jobs.batch' 'get jobs.batch' 'delete jobs.batch' \
      'list pods' 'watch pods' 'get pods' 'get pods/log' 'list events' 'get namespaces' \
      'get serviceaccounts' 'delete serviceaccounts' \
      'get roles.rbac.authorization.k8s.io' 'delete roles.rbac.authorization.k8s.io' \
      'get rolebindings.rbac.authorization.k8s.io' 'delete rolebindings.rbac.authorization.k8s.io'; do
      set -- ${permission}
      kube auth can-i "$1" "$2" >/dev/null || {
        printf 'missing Kubernetes permission: %s %s\n' "$1" "$2" >&2
        exit 8
      }
    done
    if [ "${DPONE_RUNTIME_POD_RETENTION_ALERTS}" = prometheus ]; then
      for verb in get delete; do
        kube auth can-i "${verb}" prometheusrules.monitoring.coreos.com >/dev/null || {
          printf 'missing Kubernetes permission: %s prometheusrules.monitoring.coreos.com\n' \
            "${verb}" >&2
          exit 8
        }
      done
    fi
  }

  permissions_preflight_done=0
  acknowledgement_ready=0
  if [ -f "${evidence_dir}/pre/SHA256SUMS" ]; then
    (cd "${evidence_dir}/pre" && sha256sum -c SHA256SUMS)
    operation_sha256="sha256:$(sha256sum "${evidence_dir}/pre/operation.json" | awk '{print $1}')"
    forensic_manifest_sha256="sha256:$(sha256sum \
      "${evidence_dir}/pre/FORENSIC-SHA256SUMS" | awk '{print $1}')"
    verify_durable_ack "${evidence_dir}/pre/durable-ack.json" \
      "${operation_sha256}" "${forensic_manifest_sha256}"
  else
    if [ -f "${evidence_dir}/pre/FORENSIC-SHA256SUMS" ]; then
      (cd "${evidence_dir}/pre" && sha256sum -c FORENSIC-SHA256SUMS)
      if [ -f "${evidence_dir}/pre/ACKNOWLEDGED-SHA256SUMS" ]; then
        (cd "${evidence_dir}/pre" && sha256sum -c ACKNOWLEDGED-SHA256SUMS)
        operation_sha256="sha256:$(sha256sum \
          "${evidence_dir}/pre/operation.json" | awk '{print $1}')"
        forensic_manifest_sha256="sha256:$(sha256sum \
          "${evidence_dir}/pre/FORENSIC-SHA256SUMS" | awk '{print $1}')"
        verify_durable_ack "${evidence_dir}/pre/durable-ack.json" \
          "${operation_sha256}" "${forensic_manifest_sha256}"
        acknowledgement_ready=1
      elif [ -e "${evidence_dir}/pre/durable-ack.json" ] \
        || [ -e "${evidence_dir}/pre/cronjob-suspended.json" ]; then
          printf 'unsealed acknowledgement evidence requires incident review\n' >&2
          exit 10
      fi
      cronjob_uid="$(jq -er '.metadata.uid' "${evidence_dir}/pre/cronjob.json")"
      cronjob_rv="$(jq -er '.metadata.resourceVersion' "${evidence_dir}/pre/cronjob.json")"
      suspend_patch="$(jq -cn --arg uid "${cronjob_uid}" --arg rv "${cronjob_rv}" '[
        {op:"test", path:"/metadata/uid", value:$uid},
        {op:"test", path:"/metadata/resourceVersion", value:$rv},
        {op:"replace", path:"/spec/suspend", value:true}
      ]')"
    else
      storage_tmp="${evidence_dir}/pre/storage-verification.json.tmp.$$"
      trap 'rm -f "${storage_tmp}"' EXIT
      "${ack_command}" verify-storage \
        --directory "${evidence_dir}" \
        --classification restricted \
        --required-encryption at_rest \
        --output "${storage_tmp}"
      jq -e --arg directory "${evidence_dir}" \
        --arg verifier_sha256 "${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256}" '
          .schema == "dpone.airflow-runtime-pod-retention-storage-verification.v1"
          and .passed == true and .status == "verified"
          and .directory == $directory
          and .evidence_classification == "restricted"
          and .encryption_at_rest == true
          and .verifier_sha256 == $verifier_sha256
        ' "${storage_tmp}" >/dev/null || {
          printf 'restricted evidence directory encryption was not verified\n' >&2
          exit 10
        }
      mv "${storage_tmp}" "${evidence_dir}/pre/storage-verification.json"
      trap - EXIT
      preflight_permissions
      permissions_preflight_done=1
      capture_json_bounded "${evidence_dir}/pre/cronjob.json" get cronjob \
      dpone-runtime-pod-retention --ignore-not-found -o json
      cronjob_uid="$(jq -er '.metadata.uid' "${evidence_dir}/pre/cronjob.json")"
      cronjob_rv="$(jq -er '.metadata.resourceVersion' "${evidence_dir}/pre/cronjob.json")"
      suspend_patch="$(jq -cn --arg uid "${cronjob_uid}" --arg rv "${cronjob_rv}" '[
        {op:"test", path:"/metadata/uid", value:$uid},
        {op:"test", path:"/metadata/resourceVersion", value:$rv},
        {op:"replace", path:"/spec/suspend", value:true}
      ]')"
      capture_json_bounded "${evidence_dir}/pre/namespace.json" get namespace \
      "${AIRFLOW_NAMESPACE}" -o json
    namespace_uid="$(jq -er '.metadata.uid' "${evidence_dir}/pre/namespace.json")"
    for kind in serviceaccount role rolebinding; do
      capture_json_bounded "${evidence_dir}/pre/${kind}.json" get "${kind}" \
        dpone-runtime-pod-retention -o json
    done
    if [ "${DPONE_RUNTIME_POD_RETENTION_ALERTS}" = prometheus ]; then
      capture_json_bounded "${evidence_dir}/pre/prometheusrule.json" get prometheusrule \
        dpone-runtime-pod-retention -o json
    fi
    capture_json_bounded "${evidence_dir}/pre/jobs.json" get jobs \
      --selector "${withdraw_selector}" \
      --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
    require_complete_inventory "${evidence_dir}/pre/jobs.json"
    jq -e --arg uid "${cronjob_uid}" '
      [.items[]
        | select(any(.metadata.ownerReferences[]?;
            .kind == "CronJob" and .uid == $uid and .controller == true))
        | select((.status.active // 0) != 0
            or (any(.status.conditions[]?;
              (.type == "Complete" or .type == "Failed") and .status == "True") | not))]
      | length == 0
    ' "${evidence_dir}/pre/jobs.json" >/dev/null || {
      printf 'owned retention Jobs must be terminal and inactive before withdrawal\n' >&2
      exit 8
    }
    jq -er --arg uid "${cronjob_uid}" '
      [.items[]
        | select(any(.metadata.ownerReferences[]?;
            .kind == "CronJob" and .uid == $uid and .controller == true))
        | {name: .metadata.name, uid: .metadata.uid,
            resource_version: .metadata.resourceVersion}]
    ' "${evidence_dir}/pre/jobs.json" >"${evidence_dir}/pre/owned-jobs.json"
    jq -r '.[].name' "${evidence_dir}/pre/owned-jobs.json" \
      >"${evidence_dir}/pre/job-names.txt"
    capture_json_bounded "${evidence_dir}/pre/pods.json" get pods \
      --selector "${withdraw_selector}" \
      --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
    require_complete_inventory "${evidence_dir}/pre/pods.json"
    jq -er --slurpfile jobs "${evidence_dir}/pre/owned-jobs.json" '
      ($jobs[0] | map(.uid) | INDEX(.)) as $job_uids
      | [.items[]
        | .metadata.ownerReferences[]? as $owner
        | select($owner.kind == "Job" and $owner.controller == true
            and $job_uids[$owner.uid] != null)
        | {name: .metadata.name, uid: .metadata.uid,
            resource_version: .metadata.resourceVersion, owner_job_uid: $owner.uid}]
    ' "${evidence_dir}/pre/pods.json" >"${evidence_dir}/pre/owned-pods.json"
    : >"${evidence_dir}/pre/pod-log-captures.jsonl"
    while IFS= read -r pod; do
      [ -n "${pod}" ] || continue
      pod_json="${evidence_dir}/pre/pod-${pod}.json"
      capture_json_bounded "${pod_json}" get "pod/${pod}" -o json
      expected_uid="$(jq -er --arg pod "${pod}" '.[] | select(.name == $pod) | .uid' \
        "${evidence_dir}/pre/owned-pods.json")"
      jq -e --arg uid "${expected_uid}" '.metadata.uid == $uid' "${pod_json}" >/dev/null
      jq -r '
        ((.spec.initContainers // []) | map(["init", .name] | @tsv))
        + ((.spec.containers // []) | map(["container", .name] | @tsv))
        + ((.spec.ephemeralContainers // []) | map(["ephemeral", .name] | @tsv))
        | .[]
      ' "${pod_json}" | while IFS=$'\t' read -r container_kind container; do
        for log_kind in current previous; do
          log_file="pod-${pod}.${container_kind}.${container}.${log_kind}.log"
          stderr_file="${log_file}.stderr"
          set +e
          if [ "${log_kind}" = current ]; then
            kube logs "pod/${pod}" -c "${container}" \
              --tail="${DPONE_RUNTIME_POD_WITHDRAW_LOG_TAIL_LINES}" \
              --limit-bytes="${DPONE_RUNTIME_POD_WITHDRAW_LOG_LIMIT_BYTES}" \
              >"${evidence_dir}/pre/${log_file}" 2>"${evidence_dir}/pre/${stderr_file}"
          else
            kube logs "pod/${pod}" -c "${container}" --previous \
              --tail="${DPONE_RUNTIME_POD_WITHDRAW_LOG_TAIL_LINES}" \
              --limit-bytes="${DPONE_RUNTIME_POD_WITHDRAW_LOG_LIMIT_BYTES}" \
              >"${evidence_dir}/pre/${log_file}" 2>"${evidence_dir}/pre/${stderr_file}"
          fi
          log_status=$?
          set -e
          jq -cn --arg pod "${pod}" --arg uid "${expected_uid}" \
            --arg container_kind "${container_kind}" --arg container "${container}" \
            --arg log_kind "${log_kind}" --arg log_file "${log_file}" \
            --arg stderr_file "${stderr_file}" --argjson status "${log_status}" \
            '{pod:$pod,uid:$uid,container_kind:$container_kind,container:$container,
              log_kind:$log_kind,status:$status,log_file:$log_file,stderr_file:$stderr_file}' \
            >>"${evidence_dir}/pre/pod-log-captures.jsonl"
        done
      done
    done < <(jq -r '.[].name' "${evidence_dir}/pre/owned-pods.json")
    : >"${evidence_dir}/pre/event-captures.jsonl"
    capture_relevant_events cronjob dpone-runtime-pod-retention "${cronjob_uid}"
    while IFS=$'\t' read -r kind name uid; do
      [ -n "${name}" ] || continue
      capture_relevant_events "${kind}" "${name}" "${uid}"
    done < <(jq -r '.[] | ["job", .name, .uid] | @tsv' "${evidence_dir}/pre/owned-jobs.json"; \
      jq -r '.[] | ["pod", .name, .uid] | @tsv' "${evidence_dir}/pre/owned-pods.json")
    while IFS= read -r job; do
      [ -z "${job}" ] || kube logs "job/${job}" --all-containers \
        --tail="${DPONE_RUNTIME_POD_WITHDRAW_LOG_TAIL_LINES}" \
        --limit-bytes="${DPONE_RUNTIME_POD_WITHDRAW_LOG_LIMIT_BYTES}" \
        >"${evidence_dir}/pre/${job}.log"
    done <"${evidence_dir}/pre/job-names.txt"
    capture_json_bounded "${evidence_dir}/pre/jobs-quiescence.json" get jobs \
      --selector "${withdraw_selector}" \
      --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
    require_complete_inventory "${evidence_dir}/pre/jobs-quiescence.json"
    jq -e --arg uid "${cronjob_uid}" '
      [.items[]
        | select(any(.metadata.ownerReferences[]?;
            .kind == "CronJob" and .uid == $uid and .controller == true))
        | select((.status.active // 0) != 0
            or (any(.status.conditions[]?;
              (.type == "Complete" or .type == "Failed") and .status == "True") | not))]
      | length == 0
    ' "${evidence_dir}/pre/jobs-quiescence.json" >/dev/null || {
      printf 'owned retention Jobs became active or non-terminal during forensic capture\n' >&2
      exit 8
    }
    jq -er --arg uid "${cronjob_uid}" '
      [.items[]
        | select(any(.metadata.ownerReferences[]?;
            .kind == "CronJob" and .uid == $uid and .controller == true))
        | {name: .metadata.name, uid: .metadata.uid}]
      | sort_by(.name, .uid)
    ' "${evidence_dir}/pre/jobs-quiescence.json" >"${evidence_dir}/pre/owned-jobs-quiescence.json"
    jq -S '[.[] | {name, uid}] | sort_by(.name, .uid)' \
      "${evidence_dir}/pre/owned-jobs.json" >"${evidence_dir}/pre/owned-jobs-identity.json"
    cmp -s "${evidence_dir}/pre/owned-jobs-identity.json" \
      "${evidence_dir}/pre/owned-jobs-quiescence.json" || {
        printf 'owned Job inventory changed during forensic capture\n' >&2
        exit 8
      }
    capture_json_bounded "${evidence_dir}/pre/pods-quiescence.json" get pods \
      --selector "${withdraw_selector}" \
      --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
    require_complete_inventory "${evidence_dir}/pre/pods-quiescence.json"
    jq -er --slurpfile jobs "${evidence_dir}/pre/owned-jobs.json" '
      ($jobs[0] | map(.uid) | INDEX(.)) as $job_uids
      | [.items[]
        | .metadata.ownerReferences[]? as $owner
        | select($owner.kind == "Job" and $owner.controller == true
            and $job_uids[$owner.uid] != null)
        | {name: .metadata.name, uid: .metadata.uid}]
      | sort_by(.name, .uid)
    ' "${evidence_dir}/pre/pods-quiescence.json" >"${evidence_dir}/pre/owned-pods-quiescence.json"
    jq -S '[.[] | {name, uid}] | sort_by(.name, .uid)' \
      "${evidence_dir}/pre/owned-pods.json" >"${evidence_dir}/pre/owned-pods-identity.json"
    cmp -s "${evidence_dir}/pre/owned-pods-identity.json" \
      "${evidence_dir}/pre/owned-pods-quiescence.json" || {
        printf 'owned Pod inventory changed during forensic capture\n' >&2
        exit 8
      }
    jq -cn --arg removal_commit "${DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT}" \
      --arg context "${KUBE_CONTEXT}" --arg namespace "${AIRFLOW_NAMESPACE}" \
      --arg namespace_uid "${namespace_uid}" \
      --arg alert_topology "${DPONE_RUNTIME_POD_RETENTION_ALERTS}" \
      --arg cronjob_uid "${cronjob_uid}" \
      --arg acknowledgement_issuer "${DPONE_RUNTIME_POD_WITHDRAW_ACK_ISSUER}" \
      --arg ack_verifier_sha256 "${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256}" \
      '{schema:"dpone.airflow-runtime-pod-retention-withdrawal-operation.v1",
        removal_commit:$removal_commit,kube_context:$context,namespace:$namespace,
        namespace_uid:$namespace_uid,alert_topology:$alert_topology,
        cronjob_uid:$cronjob_uid,acknowledgement_issuer:$acknowledgement_issuer,
        ack_verifier_sha256:$ack_verifier_sha256}' \
      >"${evidence_dir}/pre/operation.json"
  cat >"${evidence_dir}/pre/exact-delete.py" <<'PY'
import concurrent.futures
import json
import pathlib
import sys
import time

from kubernetes import client, config, watch
from kubernetes.client.exceptions import ApiException

phase, capture_root, context, namespace, *phase_args = sys.argv[1:]
capture_root = pathlib.Path(capture_root)
config.load_kube_config(context=context)
batch = client.BatchV1Api()
core = client.CoreV1Api()
rbac = client.RbacAuthorizationV1Api()
custom = client.CustomObjectsApi()

def load(name):
    return json.loads((capture_root / name).read_text(encoding="utf-8"))

def identity(payload):
    metadata = payload["metadata"]
    uid = metadata["uid"]
    resource_version = metadata["resourceVersion"]
    if not isinstance(uid, str) or not uid or not isinstance(resource_version, str) or not resource_version:
        raise SystemExit("captured Kubernetes identity is incomplete")
    return uid, resource_version

def request_timeout(deadline):
    remaining = max(1, int(deadline - time.monotonic()))
    return min(10, remaining), min(30, remaining)

def controlled_by(payload, kind, owner_uids):
    references = payload.metadata.owner_references or []
    return any(
        reference.kind == kind and reference.uid in owner_uids and reference.controller is True
        for reference in references
    )

def watch_for_late_object(list_call, resource_version, quiet_seconds, predicate, description):
    watcher = watch.Watch()
    try:
        for event in watcher.stream(
            list_call,
            namespace=namespace,
            label_selector=(
                "app.kubernetes.io/name=dpone-runtime-pod-retention,"
                "app.kubernetes.io/managed-by=dpone"
            ),
            resource_version=resource_version,
            timeout_seconds=quiet_seconds,
            _request_timeout=(10, quiet_seconds + 10),
        ):
            if event.get("type") == "ERROR":
                raise SystemExit(f"Kubernetes watch failed while excluding late {description}")
            payload = event.get("object")
            if payload is not None and predicate(payload):
                raise SystemExit(f"late owned {description} observed during quiescence watch")
    finally:
        watcher.stop()

def delete_exact(delete_call, read_call, name, payload, propagation):
    uid, resource_version = identity(payload)
    body = client.V1DeleteOptions(
        propagation_policy=propagation,
        preconditions=client.V1Preconditions(uid=uid, resource_version=resource_version),
    )
    deadline = time.monotonic() + 60
    try:
        delete_call(
            name=name,
            namespace=namespace,
            body=body,
            _request_timeout=request_timeout(deadline),
        )
    except ApiException as exc:
        if exc.status != 404:
            raise
    while time.monotonic() < deadline:
        try:
            observed = read_call(
                name=name,
                namespace=namespace,
                _request_timeout=request_timeout(deadline),
            )
        except ApiException as exc:
            if exc.status == 404:
                return
            raise
        if observed.metadata.uid != uid:
            raise SystemExit(f"replacement object appeared while deleting {name}")
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    raise SystemExit(f"timed out waiting for exact deletion of {name}")

def delete_custom_exact(name, payload):
    uid, resource_version = identity(payload)
    body = {
        "apiVersion": "v1",
        "kind": "DeleteOptions",
        "propagationPolicy": "Foreground",
        "preconditions": {"uid": uid, "resourceVersion": resource_version},
    }
    arguments = {
        "group": "monitoring.coreos.com",
        "version": "v1",
        "namespace": namespace,
        "plural": "prometheusrules",
        "name": name,
    }
    deadline = time.monotonic() + 60
    try:
        custom.delete_namespaced_custom_object(
            body=body,
            _request_timeout=request_timeout(deadline),
            **arguments,
        )
    except ApiException as exc:
        if exc.status != 404:
            raise
    while time.monotonic() < deadline:
        try:
            observed = custom.get_namespaced_custom_object(
                _request_timeout=request_timeout(deadline),
                **arguments,
            )
        except ApiException as exc:
            if exc.status == 404:
                return
            raise
        if observed["metadata"]["uid"] != uid:
            raise SystemExit(f"replacement object appeared while deleting {name}")
        time.sleep(min(1, max(0, deadline - time.monotonic())))
    raise SystemExit(f"timed out waiting for exact deletion of {name}")

if phase == "controller-jobs":
    cronjob = load("cronjob-suspended.json")
    delete_exact(
        batch.delete_namespaced_cron_job,
        batch.read_namespaced_cron_job,
        "dpone-runtime-pod-retention",
        cronjob,
        "Orphan",
    )
    for item in load("owned-jobs.json"):
        payload = {"metadata": {"uid": item["uid"], "resourceVersion": item["resource_version"]}}
        delete_exact(batch.delete_namespaced_job, batch.read_namespaced_job, item["name"], payload, "Foreground")
elif phase == "quiescence":
    if len(phase_args) != 3:
        raise SystemExit("quiescence requires Job/Pod resourceVersions and a timeout")
    job_resource_version, pod_resource_version, quiet_seconds_text = phase_args
    quiet_seconds = int(quiet_seconds_text)
    cronjob_uid = load("operation.json")["cronjob_uid"]
    captured_job_uids = {item["uid"] for item in load("owned-jobs.json")}
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        futures = (
            executor.submit(
                watch_for_late_object,
                batch.list_namespaced_job,
                job_resource_version,
                quiet_seconds,
                lambda payload: controlled_by(payload, "CronJob", {cronjob_uid}),
                "Job",
            ),
            executor.submit(
                watch_for_late_object,
                core.list_namespaced_pod,
                pod_resource_version,
                quiet_seconds,
                lambda payload: controlled_by(payload, "Job", captured_job_uids),
                "Pod",
            ),
        )
        for future in futures:
            future.result()
elif phase == "access":
    for filename, delete_call, read_call in (
        ("rolebinding.json", rbac.delete_namespaced_role_binding, rbac.read_namespaced_role_binding),
        ("role.json", rbac.delete_namespaced_role, rbac.read_namespaced_role),
        ("serviceaccount.json", core.delete_namespaced_service_account, core.read_namespaced_service_account),
    ):
        delete_exact(delete_call, read_call, "dpone-runtime-pod-retention", load(filename), "Foreground")
elif phase == "monitoring":
    delete_custom_exact("dpone-runtime-pod-retention", load("prometheusrule.json"))
else:
    raise SystemExit(f"unsupported withdrawal phase: {phase}")
PY
    (cd "${evidence_dir}/pre" && \
      sha256sum -- * > FORENSIC-SHA256SUMS && sha256sum -c FORENSIC-SHA256SUMS)
    chmod 0400 "${evidence_dir}/pre"/*
    fi
    if [ "${permissions_preflight_done}" -eq 0 ]; then
      preflight_permissions
      permissions_preflight_done=1
    fi
    if [ "${acknowledgement_ready}" -eq 0 ]; then
      operation_sha256="sha256:$(sha256sum "${evidence_dir}/pre/operation.json" | awk '{print $1}')"
      forensic_manifest_sha256="sha256:$(sha256sum \
        "${evidence_dir}/pre/FORENSIC-SHA256SUMS" | awk '{print $1}')"
      ack_tmp="${evidence_dir}/pre/durable-ack.json.tmp.$$"
      trap 'rm -f "${ack_tmp}"' EXIT
      "${ack_command}" publish-and-ack \
      --evidence-directory "${evidence_dir}/pre" \
      --manifest "${evidence_dir}/pre/FORENSIC-SHA256SUMS" \
      --operation "${evidence_dir}/pre/operation.json" \
      --issuer "${DPONE_RUNTIME_POD_WITHDRAW_ACK_ISSUER}" \
      --classification restricted \
      --encryption at_rest_and_in_transit \
        --output "${ack_tmp}"
      (cd "${evidence_dir}/pre" && sha256sum -c FORENSIC-SHA256SUMS)
      verify_durable_ack "${ack_tmp}" "${operation_sha256}" "${forensic_manifest_sha256}"
      mv "${ack_tmp}" "${evidence_dir}/pre/durable-ack.json"
      trap - EXIT
      (cd "${evidence_dir}/pre" && sha256sum -- \
        FORENSIC-SHA256SUMS durable-ack.json operation.json storage-verification.json \
        > ACKNOWLEDGED-SHA256SUMS && sha256sum -c ACKNOWLEDGED-SHA256SUMS)
      chmod 0400 "${evidence_dir}/pre/ACKNOWLEDGED-SHA256SUMS" \
        "${evidence_dir}/pre/durable-ack.json"
    fi

    capture_json_bounded "${evidence_dir}/pre/cronjob-before-suspend.json" get cronjob \
      dpone-runtime-pod-retention -o json
    jq -e --arg uid "${cronjob_uid}" '.metadata.uid == $uid' \
      "${evidence_dir}/pre/cronjob-before-suspend.json" >/dev/null || {
        printf 'CronJob occurrence changed after durable acknowledgement\n' >&2
        exit 10
      }
    if jq -e '.spec.suspend == true' \
      "${evidence_dir}/pre/cronjob-before-suspend.json" >/dev/null; then
      install -m 0400 "${evidence_dir}/pre/cronjob-before-suspend.json" \
        "${evidence_dir}/pre/cronjob-suspended.json"
    else
      jq -e --arg rv "${cronjob_rv}" '.metadata.resourceVersion == $rv' \
        "${evidence_dir}/pre/cronjob-before-suspend.json" >/dev/null || {
          printf 'CronJob changed between forensic capture and suspension\n' >&2
          exit 10
        }
      kube patch cronjob dpone-runtime-pod-retention --type json -p "${suspend_patch}"
      capture_json_bounded "${evidence_dir}/pre/cronjob-suspended.json" get cronjob \
        dpone-runtime-pod-retention -o json
    fi
    jq -e --arg uid "${cronjob_uid}" '
      .metadata.uid == $uid and .spec.suspend == true
      and (.metadata.resourceVersion | type == "string" and length > 0)
    ' "${evidence_dir}/pre/cronjob-suspended.json" >/dev/null
    (cd "${evidence_dir}/pre" && sha256sum -- * > SHA256SUMS && sha256sum -c SHA256SUMS)
    chmod 0400 "${evidence_dir}/pre"/*
  fi

  cronjob_uid="$(jq -er '.metadata.uid' "${evidence_dir}/pre/cronjob.json")"
  namespace_uid="$(jq -er '.metadata.uid' "${evidence_dir}/pre/namespace.json")"
  jq -e --arg removal_commit "${DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT}" \
    --arg context "${KUBE_CONTEXT}" --arg namespace "${AIRFLOW_NAMESPACE}" \
    --arg namespace_uid "${namespace_uid}" \
    --arg alert_topology "${DPONE_RUNTIME_POD_RETENTION_ALERTS}" \
    --arg acknowledgement_issuer "${DPONE_RUNTIME_POD_WITHDRAW_ACK_ISSUER}" \
    --arg ack_verifier_sha256 "${DPONE_RUNTIME_POD_WITHDRAW_ACK_COMMAND_SHA256}" '
      .schema == "dpone.airflow-runtime-pod-retention-withdrawal-operation.v1"
      and .removal_commit == $removal_commit
      and .kube_context == $context
      and .namespace == $namespace
      and .namespace_uid == $namespace_uid
      and .alert_topology == $alert_topology
      and .acknowledgement_issuer == $acknowledgement_issuer
      and .ack_verifier_sha256 == $ack_verifier_sha256
      and (.cronjob_uid | type == "string" and length > 0)
    ' "${evidence_dir}/pre/operation.json" >/dev/null || {
      printf 'sealed withdrawal identity does not match this retry\n' >&2
      exit 9
    }
  post_attempt_dir="$(mktemp -d "${evidence_dir}/post/attempt.XXXXXX")"
  capture_json_bounded "${post_attempt_dir}/namespace.json" get namespace \
    "${AIRFLOW_NAMESPACE}" -o json
  jq -e --arg uid "${namespace_uid}" '.metadata.uid == $uid' \
    "${post_attempt_dir}/namespace.json" >/dev/null || {
      printf 'namespace occurrence changed since withdrawal review\n' >&2
      exit 9
    }
  if [ "${permissions_preflight_done}" -eq 0 ]; then
    preflight_permissions
  fi

  python3 "${evidence_dir}/pre/exact-delete.py" controller-jobs "${evidence_dir}/pre" \
    "${KUBE_CONTEXT}" "${AIRFLOW_NAMESPACE}"

  capture_json_bounded "${post_attempt_dir}/jobs-after-controller-delete.json" get jobs \
    --selector "${withdraw_selector}" \
    --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
  require_complete_inventory "${post_attempt_dir}/jobs-after-controller-delete.json"
  jq -e --arg uid "${cronjob_uid}" '
    [.items[]
      | select(any(.metadata.ownerReferences[]?;
          .kind == "CronJob" and .uid == $uid and .controller == true))]
    | length == 0
  ' "${post_attempt_dir}/jobs-after-controller-delete.json" >/dev/null || {
    printf 'withdrawal_verified=false\nreason=late_owned_jobs\n' \
      >"${post_attempt_dir}/verification.txt"
    (cd "${post_attempt_dir}" && sha256sum -- * > SHA256SUMS && sha256sum -c SHA256SUMS)
    chmod 0400 "${post_attempt_dir}"/*
    exit 7
  }
  capture_json_bounded "${post_attempt_dir}/pods-after-controller-delete.json" get pods \
    --selector "${withdraw_selector}" \
    --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
  require_complete_inventory "${post_attempt_dir}/pods-after-controller-delete.json"
  jq -e --slurpfile captured "${evidence_dir}/pre/owned-jobs.json" '
    ($captured[0] | map(.uid) | INDEX(.)) as $captured_uids
    | [.items[]
      | select(any(.metadata.ownerReferences[]?;
          .kind == "Job" and .controller == true and $captured_uids[.uid] != null))]
    | length == 0
  ' "${post_attempt_dir}/pods-after-controller-delete.json" >/dev/null || {
    printf 'withdrawal_verified=false\nreason=late_owned_pods\n' \
      >"${post_attempt_dir}/verification.txt"
    (cd "${post_attempt_dir}" && sha256sum -- * > SHA256SUMS && sha256sum -c SHA256SUMS)
    chmod 0400 "${post_attempt_dir}"/*
    exit 7
  }
  capture_json_bounded "${post_attempt_dir}/jobs-final-before-access.json" get jobs \
    --selector "${withdraw_selector}" \
    --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
  require_complete_inventory "${post_attempt_dir}/jobs-final-before-access.json"
  jq -e --arg uid "${cronjob_uid}" '
    [.items[]
      | select(any(.metadata.ownerReferences[]?;
          .kind == "CronJob" and .uid == $uid and .controller == true))]
    | length == 0
  ' "${post_attempt_dir}/jobs-final-before-access.json" >/dev/null || {
    printf 'withdrawal_verified=false\nreason=late_owned_jobs\n' \
      >"${post_attempt_dir}/verification.txt"
    (cd "${post_attempt_dir}" && sha256sum -- * > SHA256SUMS && sha256sum -c SHA256SUMS)
    chmod 0400 "${post_attempt_dir}"/*
    exit 7
  }
  capture_json_bounded "${post_attempt_dir}/pods-final-before-access.json" get pods \
    --selector "${withdraw_selector}" \
    --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
  require_complete_inventory "${post_attempt_dir}/pods-final-before-access.json"
  jq -e --slurpfile captured "${evidence_dir}/pre/owned-jobs.json" '
    ($captured[0] | map(.uid) | INDEX(.)) as $captured_uids
    | [.items[]
      | select(any(.metadata.ownerReferences[]?;
          .kind == "Job" and .controller == true and $captured_uids[.uid] != null))]
    | length == 0
  ' "${post_attempt_dir}/pods-final-before-access.json" >/dev/null || {
    printf 'withdrawal_verified=false\nreason=late_owned_pods\n' \
      >"${post_attempt_dir}/verification.txt"
    (cd "${post_attempt_dir}" && sha256sum -- * > SHA256SUMS && sha256sum -c SHA256SUMS)
    chmod 0400 "${post_attempt_dir}"/*
    exit 7
  }
  job_watch_resource_version="$(jq -er '
    .metadata.resourceVersion | select(type == "string" and length > 0)
  ' "${post_attempt_dir}/jobs-final-before-access.json")"
  pod_watch_resource_version="$(jq -er '
    .metadata.resourceVersion | select(type == "string" and length > 0)
  ' "${post_attempt_dir}/pods-final-before-access.json")"
  if ! python3 "${evidence_dir}/pre/exact-delete.py" quiescence \
    "${evidence_dir}/pre" "${KUBE_CONTEXT}" "${AIRFLOW_NAMESPACE}" \
    "${job_watch_resource_version}" "${pod_watch_resource_version}" \
    "${DPONE_RUNTIME_POD_WITHDRAW_QUIESCENCE_SECONDS}"; then
    printf 'withdrawal_verified=false\nreason=late_work_or_quiescence_watch_failure\n' \
      >"${post_attempt_dir}/verification.txt"
    (cd "${post_attempt_dir}" && sha256sum -- * > SHA256SUMS && sha256sum -c SHA256SUMS)
    chmod 0400 "${post_attempt_dir}"/*
    exit 7
  fi
  capture_json_bounded "${post_attempt_dir}/jobs-after-quiescence.json" get jobs \
    --selector "${withdraw_selector}" \
    --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
  require_complete_inventory "${post_attempt_dir}/jobs-after-quiescence.json"
  jq -e --arg uid "${cronjob_uid}" '
    [.items[]
      | select(any(.metadata.ownerReferences[]?;
          .kind == "CronJob" and .uid == $uid and .controller == true))]
    | length == 0
  ' "${post_attempt_dir}/jobs-after-quiescence.json" >/dev/null || {
    printf 'withdrawal_verified=false\nreason=late_owned_jobs\n' \
      >"${post_attempt_dir}/verification.txt"
    (cd "${post_attempt_dir}" && sha256sum -- * > SHA256SUMS && sha256sum -c SHA256SUMS)
    chmod 0400 "${post_attempt_dir}"/*
    exit 7
  }
  capture_json_bounded "${post_attempt_dir}/pods-after-quiescence.json" get pods \
    --selector "${withdraw_selector}" \
    --limit="${DPONE_RUNTIME_POD_WITHDRAW_INVENTORY_LIMIT}" -o json
  require_complete_inventory "${post_attempt_dir}/pods-after-quiescence.json"
  jq -e --slurpfile captured "${evidence_dir}/pre/owned-jobs.json" '
    ($captured[0] | map(.uid) | INDEX(.)) as $captured_uids
    | [.items[]
      | select(any(.metadata.ownerReferences[]?;
          .kind == "Job" and .controller == true and $captured_uids[.uid] != null))]
    | length == 0
  ' "${post_attempt_dir}/pods-after-quiescence.json" >/dev/null || {
    printf 'withdrawal_verified=false\nreason=late_owned_pods\n' \
      >"${post_attempt_dir}/verification.txt"
    (cd "${post_attempt_dir}" && sha256sum -- * > SHA256SUMS && sha256sum -c SHA256SUMS)
    chmod 0400 "${post_attempt_dir}"/*
    exit 7
  }
  python3 "${evidence_dir}/pre/exact-delete.py" access "${evidence_dir}/pre" \
    "${KUBE_CONTEXT}" "${AIRFLOW_NAMESPACE}"

  require_absent() {
    kind="$1"
    name="$2"
    output_file="$3"
    if ! observed="$(kube get "${kind}/${name}" --ignore-not-found -o name \
      2>"${output_file}.stderr")"; then
      printf 'Kubernetes API could not verify absence of %s/%s\n' "${kind}" "${name}" >&2
      exit 6
    fi
    printf '%s\n' "${observed}" >"${output_file}"
    if [ -n "${observed}" ]; then
      printf '%s/%s still exists after withdrawal\n' "${kind}" "${name}" >&2
      exit 5
    fi
  }
  require_absent cronjob dpone-runtime-pod-retention "${post_attempt_dir}/cronjob.txt"
  for kind in serviceaccount role rolebinding; do
    require_absent "${kind}" dpone-runtime-pod-retention "${post_attempt_dir}/${kind}.txt"
  done
  while IFS= read -r job; do
    [ -z "${job}" ] || require_absent job "${job}" "${post_attempt_dir}/job-${job}.txt"
  done <"${evidence_dir}/pre/job-names.txt"
  if [ "${DPONE_RUNTIME_POD_RETENTION_ALERTS}" = prometheus ]; then
    python3 "${evidence_dir}/pre/exact-delete.py" monitoring "${evidence_dir}/pre" \
      "${KUBE_CONTEXT}" "${AIRFLOW_NAMESPACE}"
    require_absent prometheusrule.monitoring.coreos.com dpone-runtime-pod-retention \
      "${post_attempt_dir}/prometheusrule.txt"
  fi
  printf 'withdrawal_verified=true\nlate_owned_jobs=0\nlate_owned_pods=0\n' \
    >"${post_attempt_dir}/verification.txt"
  (cd "${post_attempt_dir}" && sha256sum -- * > SHA256SUMS && sha256sum -c SHA256SUMS)
  chmod 0400 "${post_attempt_dir}"/*
  result_tmp="${evidence_dir}/withdrawal.json.tmp.$$"
  jq -cn --arg removal_commit "${DPONE_RUNTIME_POD_WITHDRAW_REMOVAL_COMMIT}" \
    --arg context "${KUBE_CONTEXT}" --arg namespace "${AIRFLOW_NAMESPACE}" \
    --arg namespace_uid "${namespace_uid}" \
    --arg alert_topology "${DPONE_RUNTIME_POD_RETENTION_ALERTS}" \
    --arg operation_sha256 "sha256:$(sha256sum "${evidence_dir}/pre/operation.json" | awk '{print $1}')" \
    --arg durable_ack_sha256 "sha256:$(sha256sum \
      "${evidence_dir}/pre/durable-ack.json" | awk '{print $1}')" \
    --arg cronjob_uid "${cronjob_uid}" --arg post_attempt "$(basename "${post_attempt_dir}")" \
    '{schema:"dpone.airflow-runtime-pod-retention-withdrawal.v1",passed:true,
      status:"withdrawn",removal_commit:$removal_commit,cronjob_uid:$cronjob_uid,
      kube_context:$context,namespace:$namespace,namespace_uid:$namespace_uid,
      alert_topology:$alert_topology,
      operation_sha256:$operation_sha256,durable_ack_sha256:$durable_ack_sha256,
      evidence_durability:"external_durable_acknowledged",
      evidence_classification:"restricted",encryption:"at_rest_and_in_transit",
      post_attempt:$post_attempt,
      late_owned_jobs:0,late_owned_pods:0}' \
    >"${result_tmp}"
  mv "${result_tmp}" "${evidence_dir}/withdrawal.json"
  (cd "${evidence_dir}" && sha256sum -- withdrawal.json > SHA256SUMS && sha256sum -c SHA256SUMS)
  chmod 0400 "${evidence_dir}/withdrawal.json" "${evidence_dir}/SHA256SUMS"
  printf 'withdrawal evidence retained at %s\n' "${evidence_dir}"
  ```

## Rollback and incident response

- Withdrawal never accepts a caller-supplied manifest or a public label as
  deletion authority. Labels only bound discovery; the exact controller owner
  chain is rechecked before capture. It captures CronJob UID/resourceVersion,
  records complete restricted forensics, validates encrypted external durable
  acknowledgement of the sealed manifest, then suspends that exact occurrence
  with JSON Patch `test` operations as the first mutation. It recaptures the
  post-suspend resourceVersion and deletes
  through official Kubernetes `V1DeleteOptions` UID/resourceVersion
  preconditions. Only Jobs captured by controller owner reference to that exact
  UID are deleted. Every owned Job must be terminal and inactive in both Job
  snapshots, and the exact owned Job/Pod UID sets must remain unchanged across
  forensic capture. A replacement object therefore blocks instead of being
  removed. Ordered post-controller recaptures establish Job and Pod
  resourceVersions, a concurrent watch must remain quiet from both versions,
  and one more recapture must pass before access mutation. This closes object
  creation between sequential LIST calls rather than treating two snapshots as
  a transaction. Any late owned Job or Pod makes withdrawal incomplete
  and preserves access and monitoring resources for incident handling; it is
  never reported as verified. Role,
  RoleBinding and ServiceAccount deletion is likewise bound to captured
  UID/resourceVersion values in the explicitly reviewed namespace.
- Reuse the same absolute `DPONE_RUNTIME_POD_WITHDRAW_EVIDENCE_DIR` after an
  interruption. A sealed `pre/SHA256SUMS` is the only resume authority when the
  CronJob is already absent. It includes the exact classified/encrypted durable
  acknowledgement; legacy evidence without that receipt cannot authorize a
  retry. The sealed context, namespace, alert topology, namespace UID, CronJob
  UID and removal commit must match the retry inputs and current namespace
  occurrence before any delete.
  Each attempt gets its own `post/attempt.*`; final `withdrawal.json` is
  checksum-sealed and bound to the same operation identity.
- JSON captures default to 8 MiB and 100 inventory items, logs to the latest
  1,000 lines and 1 MiB, and Kubernetes calls to 30 seconds. Tune the explicit
  bounds only during review; a paginated, oversized, or timed-out response
  blocks deletion.
- Do not rerun committed data tasks just because Pod cleanup failed.
- If a selector concern appears, stop the full-withdrawal procedure before
  mutation and compare apply item UIDs with Airflow run/task evidence.
- On growing stale inventory, inspect RBAC/API errors, CronJob failures and the
  provider cleanup path. Raising the TTL is not a fix for delete failures.
- Do not set `keep_pod` ad hoc in workload configuration; strict provider policy
  owns `delete_succeeded_pod`.
