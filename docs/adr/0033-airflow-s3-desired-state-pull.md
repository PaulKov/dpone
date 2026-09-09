# ADR 0033: Airflow deployments use S3 desired-state pull

## Status

Accepted.

## Context

ADR 0009 separates immutable publication, local cache materialization, atomic
activation, and network-free DAG parsing. The first external activation
integration projected desired state through Kubernetes ConfigMaps updated by a
per-release infrastructure pipeline.

That integration couples the DAG repository to infrastructure repository
tokens, makes deployment depend on an infrastructure mutation for every
release, and does not restore a pod-local `emptyDir` after restart unless the
projection and controller are both available. It also creates two authorities
when a historical mutable pack cache remains enabled.

Astronomer Cosmos demonstrates the value of consuming compiled artifacts and
cache invalidation. Its remote manifest and remote cache modes can perform
object-storage reads during DAG parsing. Airflow recommends avoiding network
I/O in top-level DAG code, and Airflow 3.3 object-storage DAG bundles do not
currently provide versioned bundle identity.

## Decision

Use one bounded, versioned desired-state object per environment:

```text
protected DAG CI
  -> immutable release/deployment publication
  -> conditional desired-state write
  -> parser-pod init/watcher
  -> exact cache materialization
  -> guarded local cache activation
  -> network-free provider parse
  -> loader acknowledgement and REST convergence
```

The desired-state object:

- selects exact immutable release and deployment IDs;
- includes the expected index checksum, runtime image digest, and DAG IDs;
- is serialized as canonical JSON under a strict schema;
- is the only mutable control object in the flow;
- is created with expected-absent semantics and replaced with the exact
  previously observed object-store revision;
- treats an S3 ETag or object VersionId as an opaque token, never a content
  digest or ordering clock;
- uses SHA-256 for content identity;
- never contains credentials, signed URLs, connection payloads, Kubernetes
  secret names, or Vault paths.

The object-storage adapter must fail closed when conditional writes are not
available. It must not fall back to an unconditional overwrite. Conditional
mutation is enabled only for an explicitly certified HTTPS endpoint matching
the actual SDK endpoint; unknown/custom S3 endpoints fail before `PutObject`.

Environment, desired-state key, artifact registry root/ref, watcher identity,
source repository, and protected ref form one trusted composition authority.
Infrastructure mounts that stable authority as read-only configuration.
Per-release jobs cannot override those values with CLI flags. Promotion
evidence carries the registry scope identity, and GitLab source authorization
proves the selected SHA is still the configured protected-ref head immediately
before mutation. The immutable registry is an endpoint-bound scope and must be
a sibling of, not a parent of, the mutable desired-state key. Reconciliation
also requires the materialized `airflow_bundle_ref` to equal
`git:<source.git_sha>`.

Cross-job retry uses a create-once canonical preparation artifact. It binds the
complete credential-free write-authority fingerprint, immutable promotion
evidence, predecessor revision, source pipeline, occurrence ID, and timestamp.
The mutating job cannot redirect that occurrence to another desired-state key,
endpoint, watcher, registry, repository, or protected ref. Repeating
preparation at the same artifact path reuses equal bytes and rejects a
different candidate. A rejected candidate or any CLI failure must never
overwrite the create-once winner, promotion evidence, authority file, intent,
or preparation. Canonical cross-job publication requires a separate failure
status; success output, failure status, and every input/control path are
pairwise distinct before any remote client or CAS side effect. The retained
single-job `--intent` compatibility path preserves its v0.73.24 failure-output
behavior when a separate status is not supplied.

The desired deployment remains byte-identical across a retried GitLab job.
Its `source.job_id` identifies the job that durably originated the promotion
occurrence. Publication evidence v2 separately records
`preparation_job_id` and the current `publisher_job_id`, so audit consumers can
identify both the stable retry origin and the job that actually performed or
reconciled the CAS mutation.

Infrastructure installs one generic fail-open synchronization mechanism:

- a one-shot init container restores an empty pod-local cache at startup;
- a watcher sidecar checks desired state at a bounded configurable interval;
- unchanged desired state performs no artifact download or activation;
- a changed state is staged and verified before the local atomic switch;
- the exact desired bytes and opaque revision are durably staged in a bounded
  pre-activation recovery record before cache mutation;
- missed intermediate occurrences converge to the latest CAS-protected desired
  state and emit `predecessor_status=skipped`;
- an already-active verified projection validates and reuses its immutable
  activation receipt; a missing receipt is reconstructed as an activation fact
  before the checkpoint, while the cycle itself emits `status=recovered`;
- if activation D2 succeeds while receipt/checkpoint still name D1, the next
  cycle repairs D2 from that recovery record before reading a newer remote D3;
- `status=unchanged` requires the canonical local desired snapshot, current,
  checkpoint, immutable receipt, and complete artifact projection to agree;
- each cycle writes a non-passing in-progress status before any cache mutation,
  so a failed final status write cannot expose stale success;
- any clean failure preserves the last known good `current`;
- an empty cache plus remote failure does not block the Airflow pod, but the
  dpone loader emits a visible import diagnostic.

The watcher runs only beside the parse authority:

- Airflow 2.10: scheduler/DAG processing component;
- Airflow 3.2: dedicated `dagProcessor`;
- Airflow 3.3+: the same mechanism remains valid until a custom versioned DAG
  Bundle proves equivalent semantics.

The API server, webserver, triggerer, and KubernetesExecutor workers do not
mount or mutate this scheduler cache. Workers fetch exact runtime artifacts
through the separate strict `init_fetch` contract.

### Compatibility rationale

The desired deployment, authority, recovery record, checkpoint, publish-intent,
activation receipt, and reconcile evidence schemas are introduced together by
this ADR for their first public release in `0.73.24`. None of these desired-state
v1 contracts exists on public tag `v0.73.23` or its protected `master`
baseline, so there is no released desired-state checkpoint or receipt to
migrate. Intermediate commits on the unreleased feature branch are not a
compatibility baseline. Existing immutable pack releases, `cache-materialize`,
`cache-sync`, and provider contracts remain compatible.

Version `0.73.25` adds
`dpone.airflow-desired-state-publish-preparation.v1` and
`dpone.airflow-desired-state-publish.v2`. The existing desired deployment v1
wire format is unchanged; its `source.job_id` is formally defined as the
occurrence-origin job. In the 0.73.24 single-job flow that job also performed
the mutation. Publish evidence v1 remains registered and documented for
existing consumers. The legacy same-job `--intent` flow continues to emit v1;
only the canonical cross-job `--preparation` flow emits v2 with separate
preparation and publisher provenance. This is an additive producer migration,
not a silent wire replacement.

For Airflow 2.10 and 3.2, the watcher is an intentional compatibility
mechanism, analogous to `git-sync` for immutable dpone deployments. It performs
remote synchronization outside DAG parsing and leaves the parser with a local
atomic `current`.

Airflow 3.3 adds native DAG Bundles and an official `S3DagBundle`. The official
S3/GCS implementations do not currently support bundle versioning, so tasks use
the latest object-store contents instead of an exact historical bundle version.
They cannot yet replace the dpone release ID, deployment ID, conditional desired
state, checksum, last-known-good, rollback occurrence, loader acknowledgement,
and convergence contracts.

A future dpone custom DAG Bundle may absorb the watcher. That migration is
allowed only when the bundle implements version-specific retrieval, preserves
all existing identities and evidence, and passes the same restart, outage,
concurrency, rollback, and convergence certification.

The decision follows the official
[Airflow 3.3 DAG Bundles documentation](https://airflow.apache.org/docs/apache-airflow/stable/administration-and-deployment/dag-bundles.html):
`S3DagBundle` exposes the latest bucket contents, while a custom bundle can
implement `get_current_version`, version-specific initialization, refresh, and
concurrency locking. The watcher remains the supported compatibility mechanism
until that custom bundle passes equivalent certification.

Cosmos is a design input, not the activation control plane. dpone adopts its
compiled-artifact boundary, hash invalidation, cache reuse, and stale-cache
cleanup patterns. It does not adopt remote object reads during every DAG parse;
the dpone watcher keeps those reads outside Airflow's parse critical path.

Dev promotion is automatic from the protected default branch. Production
promotion is a protected manual environment action. Neither promotion calls an
infrastructure pipeline or receives kubeconfig.

## Identity and evidence

ADR-0032 supersedes the original occurrence/activation separation in this
section. For the canonical desired-state route, one trusted UUIDv4
`occurrence_id` is persisted as `activation_id` by every pod-local switch that
applies that occurrence. Standalone local promotion, which has no trusted
desired-state occurrence, still generates its own activation UUID.

The remaining identities are separate:

- `release_id`: environment-neutral immutable content;
- `deployment_id`: environment-specific immutable projection;
- desired-state SHA-256: selected control document bytes;
- object-store revision: opaque conditional-write token;
- promotion occurrence ID / canonical `activation_id`: one trusted
  desired-state activation occurrence, shared by its pod-local switches;
- immutable activation receipt: exact evidence persisted before checkpoint;
- loader acknowledgement: one successful parse of that activation;
- Airflow convergence evidence: expected serialized DAGs visible for that
  activation.

An immutable publication pass or desired-state write is not an activation pass.
Production readiness requires the complete evidence chain.

## Consequences

- DAG parsing remains independent from object-storage latency and outages.
- Infrastructure is deployed independently and does not participate in every
  DAG release.
- Cross-project activation tokens and per-release ConfigMap mutations are
  removed after cutover.
- Restart recovery works with disposable bounded `emptyDir`.
- Rollback is a new conditional desired-state occurrence selecting a retained
  immutable deployment; restoring an old object version or copying local cache
  is not the normal rollback path.
- Remote retention must protect desired state, referenced immutable objects,
  successful activation evidence, and the configured rollback window.
- The watcher adds one small long-running container to the parse-authority pod,
  so poll jitter, resource limits, status, and stale-cache alerts are required.
- Object-store conditional-write behavior must be certified against each
  claimed provider. Fake S3 evidence does not certify Yandex Object Storage.

## Rejected alternatives

- Direct remote read during every DAG parse: rejected because it puts network
  availability and latency in the scheduler critical path.
- Shared writable PVC for scheduler and workers: rejected as the default
  because it introduces RWX availability, cross-pod mutation, and stale-mount
  failure modes without solving immutable identity.
- Mutable `latest` artifact: rejected because it has no exact deployment or
  stale-writer protection.
- Per-release infrastructure pipeline: rejected because application promotion
  should not mutate infrastructure or require infra repository credentials.
- Two independently mutable desired-state files: rejected because a consumer
  can observe a split generation.
- Unconditional object overwrite after a conditional conflict: rejected
  because it converts concurrency into hidden last-writer-wins behavior.

## Compatibility and migration

Existing release, deployment, publication receipt, cache materialization,
cache activation, loader acknowledgement, and convergence contracts are
preserved. The desired-state source changes from ConfigMap projection to one
conditional object-storage document.

During migration, the canonical and historical caches may be materialized side
by side, but only one loader authority can be active. After dev certification
and a stability window, remove the mutable `latest` cache, ConfigMap desired
state, and cross-project activation jobs.

The complete algorithm, test matrix, market comparison, rollout, and live
certification gates are defined in
[the approved feature specification](../feature-design-airflow-s3-desired-state-pull-v1.md).
