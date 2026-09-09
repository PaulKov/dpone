# Promote an Airflow cache deployment

**Purpose.** Select immutable desired state and atomically promote one reviewed deployment for the parser.

**Audience.** Platform promotion controllers, CI engineers, and Airflow operators.

[Back to cache sync and recovery overview](airflow-cache-sync.md) · **Next likely task:** [verify and recover cache state](airflow-cache-sync-recovery.md).

## Promote

### Remote desired-state pull

For the complete first-success command, rollback journey, supported Airflow
component matrix, and future Airflow 3.3 migration criteria, see
[Exact Airflow desired-state delivery](airflow-desired-state.md).

The recommended production topology installs one synchronization mechanism
beside the Airflow parse authority:

```text
immutable object-storage release/deployment
  -> conditionally updated desired-state document
  -> fail-open init container and watcher sidecar
  -> cache-materialize
  -> guarded cache-sync
  -> local current/airflow-index.json
  -> provider parse, loader ACK, REST convergence
```

The init container runs one bounded cycle after pod creation. It always exits
successfully so an object-storage incident cannot block the Airflow main
container. The watcher repeats the same operation at a configurable interval
and skips downloads when the desired-state revision is unchanged.

Only the parse-authority pod receives the watcher:

- the shipped Airflow 2.10 profile covers scheduler-managed DAG processing;
- a standalone Airflow 2.10 DAG processor requires a separately rendered and
  certified profile and is `UNVERIFIED` in this repository;
- the shipped Airflow 3.2 profile covers the dedicated `dagProcessor`;
- Airflow 3.3 package compatibility is tested, but no 3.3 Helm profile is
  shipped or operationally certified;
- API server, webserver, triggerer, and KubernetesExecutor workers: no
  scheduler-cache watcher or mount.

This is similar operationally to `git-sync`, but the source is an exact
precompiled dpone deployment rather than Git source. A failed cycle preserves
the last known good `current`. If the cache is empty and object storage is
unavailable, Airflow still starts; the dpone loader reports a visible import
diagnostic instead of silently returning zero DAGs.

This placement is deliberate across supported Airflow lines:

- the exact shipped 2.10 profile runs beside scheduler-side DAG processing;
- the exact shipped 3.2 profile runs beside the dedicated `dagProcessor`;
- Airflow 3.3 may later use a versioned custom DAG Bundle, but the official
  `S3DagBundle` is not a drop-in replacement while it lacks bundle versioning.

The migration criterion is behavioral, not version-number based: a replacement
must preserve exact deployment selection, checksums, last-known-good recovery,
rollback identity, loader acknowledgement, and REST convergence evidence.

The desired-state object is not a mutable `latest` artifact. It is a small
versioned control document selecting immutable IDs. Create and replace use
object-store conditional writes, object revisions remain opaque, and SHA-256
identifies content. Rollback writes a new desired-state occurrence selecting a
retained deployment.

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

The read-only init/watcher cycle fetches one bounded snapshot:

```bash
dpone airflow desired-state fetch \
  --connection-id s3_dpone_artifacts_reader \
  --connection-type airflow \
  --output /opt/airflow/.dpone-cache/control/desired.json \
  --status-output /opt/airflow/.dpone-cache/status/desired-state.json
```

The snapshot is written atomically only after schema, environment, identity,
size, canonical JSON, and checksum fields are valid. `fetch` never changes
`current`; the cache materializer and guarded cache sync remain separate
steps. Failure preserves the previous snapshot and the last-known-good active
deployment.

Production watchers use the atomic one-cycle command instead of composing
`fetch`, `cache-materialize`, and `cache-sync` in shell:

```bash
dpone airflow desired-state reconcile \
  --connection-type airflow \
  --connection-id s3_dpone_artifacts_reader \
  --artifact-connection-type airflow \
  --artifact-connection-id s3_dpone_artifacts_reader \
  --cache-root /opt/airflow/.dpone-cache
```

This command holds one cache-root lock for fetch, validation, materialization,
remote precommit recheck, local CAS activation, and checkpoint commit. Infra
must not reimplement the desired-state parser or split this transaction across
independent shell commands. Snapshot, checkpoint, and reconcile status have
fixed paths under `<cache-root>/status`; callers cannot alias them to active
cache control files.

Machine-readable contracts:

- [trusted authority](schemas/gitops/airflow-desired-state-authority.schema.json);
- [desired deployment](schemas/gitops/airflow-desired-deployment.schema.json);
- [durable publish intent](schemas/gitops/airflow-desired-state-publish-intent.schema.json);
- [publish evidence](schemas/gitops/airflow-desired-state-publish.schema.json);
- [fetch evidence](schemas/gitops/airflow-desired-state-fetch.schema.json);
- [reconcile evidence](schemas/gitops/airflow-desired-state-reconcile.schema.json);
- [local checkpoint](schemas/gitops/airflow-desired-state-checkpoint.schema.json).

See the [approved design](feature-design-airflow-s3-desired-state-pull-v1.md)
and [ADR 0033](adr/0033-airflow-s3-desired-state-pull.md) for concurrency,
failure, retention, Airflow-version, and certification rules.

For the first promotion, assert that no current deployment exists. Omitting the
guard cannot protect two racing first promotions:

```bash
: "${DPONE_SCHEDULER_CACHE_ROOT:?set the absolute scheduler cache root}"
: "${DPONE_FIRST_DEPLOYMENT_DIR:?set the reviewed sha256-... deployment directory name}"

dpone airflow cache-sync \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --deployment-dir "${DPONE_SCHEDULER_CACHE_ROOT}/deployments/prod/${DPONE_FIRST_DEPLOYMENT_DIR}" \
  --environment prod \
  --promoted-by ci://your-platform/dpone-airflow \
  --allowed-promoter ci://your-platform/dpone-airflow \
  --expect-current-absent \
  --confirm-promote
```

For a later update, read `current_path_deployment_id` from
`dpone airflow cache-recovery-plan --environment prod --format json`, review
it, and use it as the CAS guard:

```bash
: "${DPONE_NEXT_DEPLOYMENT_DIR:?set the reviewed sha256-... deployment directory name}"
: "${DPONE_REVIEWED_CURRENT_DEPLOYMENT_ID:?set the current canonical sha256 digest}"
: "${DPONE_SCHEDULER_CACHE_ROOT:?set the absolute scheduler cache root}"

dpone airflow cache-sync \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --deployment-dir "${DPONE_SCHEDULER_CACHE_ROOT}/deployments/prod/${DPONE_NEXT_DEPLOYMENT_DIR}" \
  --environment prod \
  --promoted-by ci://your-platform/dpone-airflow \
  --allowed-promoter ci://your-platform/dpone-airflow \
  --expected-current-deployment-id "${DPONE_REVIEWED_CURRENT_DEPLOYMENT_ID}" \
  --confirm-promote
```

When infrastructure projects desired-state evidence into the pod, bind the
activation to the exact projected activation record as well:

```bash
: "${DPONE_DESIRED_GUARD_PATH:?set the absolute bounded guard path}"
: "${DPONE_DESIRED_GUARD_SHA256:?set the sha256:<64 hex> digest captured before materialization}"

dpone airflow cache-sync \
  --cache-root "${DPONE_SCHEDULER_CACHE_ROOT}" \
  --deployment-dir "${DPONE_SCHEDULER_CACHE_ROOT}/deployments/prod/${DPONE_NEXT_DEPLOYMENT_DIR}" \
  --environment prod \
  --promoted-by ci://your-platform/dpone-airflow \
  --allowed-promoter ci://your-platform/dpone-airflow \
  --expected-current-deployment-id "${DPONE_REVIEWED_CURRENT_DEPLOYMENT_ID}" \
  --precommit-guard-path "${DPONE_DESIRED_GUARD_PATH}" \
  --expected-precommit-guard-sha256 "${DPONE_DESIRED_GUARD_SHA256}" \
  --confirm-promote
```

The two precommit options are one contract and must be supplied together. The
file must be regular, no larger than 64 KiB, and unchanged since its expected
digest was captured. Verification occurs inside the cache-root promotion lock
before pointer mutation. A missing, oversized, or changed guard returns a
structured blocker and leaves `current` unchanged. The guard is intentionally
opaque to dpone: infrastructure owns its schema, while dpone owns the bounded
digest precondition and transaction boundary.

After successful argument parsing, success exits `0` and prints a topology-free
summary. Validation/integrity failure exits `4`, writes no promotion state,
prints the stable error code and safe action to stdout, and leaves stderr empty.
Argparse usage errors exit `2`, write usage to stderr, and do not run promotion.
On a first promotion there is no previous deployment; failed integrity
verification leaves `current` absent.

Use `--format json` for automation. It includes the exact failing local path in
`errors[0].path`, while ordinary text output intentionally omits cache topology.
The command is still a promotion attempt, not a read-only doctor: after an
operator repairs the cache, a repeated invocation can promote it.
