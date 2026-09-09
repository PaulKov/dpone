# ADR 0009: Artifact delivery and cache materializer

## Status

Accepted.

> Amended by [ADR 0024](0024-airflow-executable-init-fetch-wire-boundary.md).
> Its strict v2 wire, fixed pod commands, runtime receipt, and trust semantics
> govern executable `init_fetch`.
>
> The publication-receipt amendment below is additive. Exact activation
> occurrence and convergence remain governed by
> [ADR 0032](0032-airflow-exact-activation-occurrence.md).
>
> External desired state and restart recovery are governed by
> [ADR 0033](0033-airflow-s3-desired-state-pull.md). The local cache transaction
> defined here remains unchanged.
> ADR 0033 also replaces release-specific projected ConfigMap input with a
> connector-neutral conditional object-store control plane. A create-once
> preparation artifact binds each cross-job publish occurrence before the
> non-interruptible CAS mutation; this amendment does not change the local cache
> activation ordering below.

## Context

The Airflow scheduler, worker, and Kubernetes runtime pod usually do not share one filesystem. The Airflow loader must not synchronize remote artifacts during DAG parsing.

## Decision

Separate four roles:

- Publisher writes immutable release/deployment artifacts.
- Cache Materializer verifies a local candidate, serializes promotion through
  one cache-root inter-process lock, checks CAS under that lock, and switches
  `current` only as the final activation commit point.
- Recovery Planner independently validates active state and every candidate.
  Recovery Apply requires an allowed actor and the active-state CAS value from
  the reviewed plan.
- Airflow Loader reads only local `current/airflow-index.json`.

For KPO runtime, the primary production delivery mode is strict-v2
`init_fetch`: an init container downloads pinned artifacts by digest into an
`emptyDir`, validates `deployment-set.v2` as the runtime deployment receipt,
independently validates `release-set.v1` and the selected pack, applies the
effective trust policy, then publishes `runtime-fetch-ready.json` last. The base
launcher reads that immutable ready state and fetched artifacts; it never
resolves `current` or reopens init-only ConfigMap mounts.

## Consequences

- Airflow parse remains network-free.
- Remote desired-state publication remains outside DAG parsing and uses
  pairwise-distinct immutable inputs, success evidence, and optional failure
  status paths. A failed preparation/publish cannot overwrite a concurrent
  create-once winner or its source evidence.
- Runtime pods can fetch pinned artifacts without resolving mutable `current`.
- Artifact credentials stay out of KPO arguments and generated packs.
- `deployment_id` is recomputed before promotion. Environment-specific fields
  mirrored into `airflow-index.json` must equal `deployment.json`.
- The pinned release-set and each DAG/pack pass confinement, size, and SHA-256
  validation before control-state mutation. File opens walk from an anchored
  cache-root descriptor and reject intermediate symlink replacement.
- The materializer prepares a unique relative symlink, durably replaces pointer
  JSON, appends promotion authorization, then atomically replaces `current`.
- `current` targets a sealed `activations/<environment>/<deployment_id>`
  snapshot, not the mutable build candidate. The materializer copies, seals,
  compares the complete candidate/staging tree, and revalidates that snapshot
  and the pinned release tree before pointer or audit mutation. Existing
  activations are reused only after exact tree comparison. Unreadable subtrees
  fail closed, and non-regular entries are opened nonblocking before rejection.
- Lock, pointer, and audit control files reject symlinks. Pointer temporary files
  are unique/exclusive and fsynced; audit append uses no-follow semantics.
- Promotion, recovery apply, and retention apply hold the same cache-root
  transaction lock from fresh validation/plan through their mutation.
- Cache-path and control identities are canonical lowercase SHA-256 strings.
  Every current consumer validates publisher and offset-aware promotion time in
  addition to environment, release, and deployment identity.
- `--expect-current-absent` protects the first promotion. Later promotions use
  `--expected-current-deployment-id`. Concurrent guarded promotions therefore
  have one winner; stale callers receive `DPONE_CURRENT_POINTER_CAS_MISMATCH`.
- An optional bounded precommit guard binds the final activation to external
  desired-state bytes such as a projected ConfigMap activation record. Its
  expected SHA-256 is captured before materialization and revalidated while
  holding the same promotion lock, immediately before pointer mutation.
  Deployment CAS and this activation-event guard are complementary: the first
  protects active cache state, while the second proves that one pod used one
  stable projected event throughout its local transaction. ConfigMap
  projection is eventually consistent across pods; authoritative parse ACK and
  REST convergence, not the local guard alone, prove cluster rollout.
- A pointer-write failure leaves active `current` unchanged, but can leave a
  visible pointer when failure occurs after rename and before directory fsync.
  Audit or activation failure can leave prepared metadata ahead of active state and returns
  `failed_step`, `state_may_have_changed`, and `recovery_required`.
- A failure while preparing the `current` symlink leaves pointer, audit, and
  active current unchanged, but the sealed activation or release permissions
  may already have changed. It therefore reports possible artifact mutation
  without claiming control-state recovery is required.
- Retention validates the full delete set before mutation. A later filesystem
  failure reports completed deployment IDs, the failed ID, and the failed step;
  it never converts partial deletion into an opaque exception.
- Retention fully validates active `current` before classifying or deleting any
  other deployment. A corrupt current fingerprint, index, release, or artifact
  therefore preserves all potential recovery candidates.
- Recovery never offers an unverified candidate. It refuses healthy-state
  mutation and stale-plan rollback. Pointer and active projection environment,
  deployment, and release identities must agree before audit-only repair.
  Existing noncanonical `current` paths with no physical CAS identity block
  automatic recovery. Audit-only repair preserves original publisher, commit,
  and attestation fields.
- Malformed pointer and directory identities are represented as `null` plus a
  structured issue/quarantine record, never as invalid digest text in public
  recovery or retention reports. Retention schemas prohibit `null` identities
  on destructive actions, and human output marks quarantine as
  `NEEDS_ATTENTION` instead of reporting an apparently healthy plan.
- Historical malformed audit lines are warnings and are never silently erased.
- CLI actor/allowlist values enforce policy consistency but do not authenticate
  the process. CI/workload identity and cache write permissions are the trust
  boundary.
- Remote publication and pinned materialization may obtain an
  `ArtifactRegistry` through workload identity or a bounded logical credential
  reference at the CLI composition root. The application service receives only
  the registry port; credential values and resolver metadata never enter
  release/deployment content, cache evidence, or DAG parse.
- Logical S3 references must resolve an explicit access-key/secret-key pair;
  missing or partial credentials fail before SDK client construction. Ambient
  SDK credential chains are available only through the explicit
  `workload_identity` access mode, so one mode cannot silently become another.
- Materialization depends on the read-only `ArtifactRegistryReader` port.
  Publication alone receives the combined read/write registry capability.
- The default `dpone airflow publish --publication-mode compatible` preserves
  the historical v1 receipt and registry port. Canonical CI explicitly selects
  `--publication-mode exact`; only that mode emits
  `dpone.airflow-artifact-publish.v2`. The exact publisher first requires a
  matched v2 deployment/index pair, then reads back every object,
  verifies exact SHA-256 and size, reconstructs and semantically validates the
  release/deployment projection, and commits the release root,
  `deployment.json`, `airflow-index.json`, and a credential-free registry
  scope fingerprint.
- Exact promotion rejects historical v1 publication evidence. In the internal
  GitLab flow, the v2 receipt is trusted only as an artifact of the exact
  protected publish job; infrastructure independently validates source project,
  pipeline/job identity, exact job SHA, and protected branch head before
  activation.
- The exact-publication registry scope v2 binds provider, the canonical
  endpoint authority observed from the constructed storage SDK client, account
  where applicable, bucket/container, and immutable root path. It never
  contains credentials. The historical root-only scope v1 remains a
  compatibility helper but cannot authorize exact publication.
- Exact publication checks the explicit connector-neutral
  `ArtifactRegistryAuthority.authority_scope_id` capability before the first
  registry write. The collision-free member name deliberately differs from the
  historical `scope_id`; therefore a valid root-only v1 digest cannot satisfy
  exact authorization accidentally.
  Compatible publication continues to accept the historical reader/writer
  port without authority identity.
- The S3, GCS, and Azure Blob adapters expose the endpoint selected by their
  constructed SDK client; the local emulator exposes a digest-derived
  filesystem-root authority. Azure SAS query material is stripped, while a
  routing-significant Azurite account path remains part of authority identity.
  Authority is evaluated lazily only when exact publication requests it, so
  compatible publication retains historical behavior. Custom adapters that
  cannot expose their actual
  endpoint remain usable in compatible mode but cannot authorize exact
  publication. They migrate by implementing the narrow
  `ObjectStorageEndpointAuthority` capability; rollback is to compatible mode,
  never to a caller-asserted endpoint label.
- Rejected alternative: accepting endpoint text beside credentials or the
  registry URI. That would let configuration claim one authority while the SDK
  writes to another endpoint.
- A publication receipt proves remotely observable content at publication
  time. It does not prove a cache activation. UUIDv4 `activation_id`, provider
  ACK v2, and Airflow REST convergence remain the activation source of truth.
- Materialization revalidates remote bytes immediately before local promotion.
  A previously valid publication receipt cannot override a later materializer
  integrity failure.
- Materialization accepts only matched v1/v1 or v2/v2 deployment/index wires;
  mixed or unknown schema pairs fail closed.

## Promotion State Machine

```text
unlocked
  -> locked
  -> candidate_verified
  -> activation_snapshot_sealed
  -> cas_verified
  -> optional_precommit_guard_verified
  -> pointer_prepared
  -> audit_authorized
  -> current_activated
  -> unlocked/success
```

Any failure before `current_activated` is not a successful deployment
activation. If pointer or audit metadata was already prepared, the operator
must run the plan/apply workflow instead of retrying promotion blindly.

## Rejected Alternatives

- Check-then-act CAS without a lock: rejected because two promoters can both
  report success for different deployments.
- Re-reading projected desired state only in an outer controller: rejected
  because the projection can change after that read and before the cache
  materializer commits `current`.
- Fixed temporary filenames: rejected because concurrent writers collide and a
  symlink can redirect writes outside the cache root.
- Activate `current` before pointer/audit: rejected because a command can fail
  after making an unaudited deployment visible to the loader.
- Directory-copy fallback for `current`: rejected because it has no atomic
  activation boundary. Copying into an immutable activation snapshot before
  the atomic symlink switch is accepted because the switch remains the single
  activation boundary.
- Recovery by arbitrary deployment ID: rejected because it bypasses candidate
  integrity, promoter policy, and reviewed-state concurrency guards.

## Compatibility

This security correction is intentionally fail-closed. Existing caches must be
rematerialized when deployment identity does not match its content, when the
release-set is absent, or when `current` is not a relative symlink. Rolling back
to the former materializer removes these integrity and concurrency guarantees.
The operational migration is specified in the
[cache sync and recovery runbook](../airflow-cache-sync.md).
