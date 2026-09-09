# ADR 0040: Legacy Airflow pack cache uses receipt authority and bounded generations

## Status

Accepted.

## Context

The compatibility `dpone-airflow-pack-sync` command consumes a mutable remote
`latest/pack-index.json`. It predates exact release/deployment activation but
must remain safe while installations migrate. A cache may be shared by an init
container, watch sidecar, scheduler, and DAG processor with different process
IDs or UIDs. Concurrent syncs, pod termination, filesystem failures, slow
object reads, restrictive umasks, and bounded ephemeral storage create failure
states that a plain `current` pointer cannot distinguish.

Exact deployment caches use a symlink and occurrence-bound deployment index.
Legacy caches use a text generation pointer. Sharing one root would make
`current` ambiguous and can silently remove all generated DAGs after restart.

## Decision

- Every writable Airflow cache root declares one durable layout marker:
  `exact_deployment_v1` or `legacy_pack_index_v1`. Layouts never share a root.
- A legacy generation is downloaded into an fsGroup-managed stage protected by a
  process-independent file lease. It is checksum-verified, inventoried,
  fsynced, and sealed before publication.
- `status/current-commit.json` is the sole legacy activation authority. It
  binds commit UUID, monotonic sequence, generation, index digest, and time.
  Text `current` and `current_generation` are compatibility projections and
  must match the receipt before parsing or pruning.
- Commit uses a durable `pending-commit.json`. An interrupted commit is
  fail-visible; the next single writer validates the immutable generation and
  completes the prepared commit before accepting new remote bytes.
- DAG-spec loading holds a shared cache lease from receipt validation through
  file reads. Retention takes the exclusive lease only to detach paths.
- Download capacity is reserved before stage payload writes. Declared artifact
  sizes are verified; missing sizes reserve the configured per-artifact limit.
- Retention protects the union of receipt, text pointer, and compatibility
  symlink generations. Authority disagreement blocks generation pruning.
- Active stage leases override expired heartbeat evidence. Uncertain active
  work is preserved and reported instead of deleted.
- Commit and mutable diagnostic publication share a separate evidence lease.
  Slow Airflow metadata publication cannot block DAG parsing and cannot
  overwrite a newer current-generation projection.
- Shared control paths use explicit `02775` directories and `0664` files when
  owned by the writer. Foreign-owned PVC roots are checked for the same
  minimum shared bits without chmod, independent of umask. Attempt work
  directories use `02770`, their controls use `0660`, and published generation
  bytes use `02770` directories with `0440` files. Kubernetes deployments
  provide one common `fsGroup`; cache bytes are non-secret by contract.
  Historical foreign-owned generations are adopted by a shared receipt
  sidecar rather than by modifying their inode.
- An unmarked historical cache is adopted and receives durable commit authority
  before the layout marker is written. A crash therefore leaves either the old
  readable cache or the fully marked managed cache, never a marker that hides
  an unadopted last-known-good generation.
- `status=blocked` and command exceptions return non-zero in one-shot mode.
  Helm may wrap init/watch commands fail-open, but loader readiness remains
  fail-visible.

## Consequences

- A stale or crashed writer cannot roll back or delete the active generation.
- A cache may temporarily retain more bytes when ownership is uncertain; the
  hard budget then becomes a blocker rather than silent cleanup.
- The default 512 MiB budget is enforced before payload writes and after safe
  retention. Users with larger compiled packs must explicitly size the volume
  and policy.
- Historical unmarked roots remain readable. Once a managed writer adds a
  layout marker and lock, all readers use receipt authority.
- Airflow Variable status is diagnostic only. Local receipt, index digest, and
  loader evidence are the source of truth.
- Exact deployment activation remains the target architecture; this ADR does
  not make mutable `latest` a production deployment identity.

## Rejected alternatives

- Trust text `current` alone: cannot distinguish partial pointer updates or
  bind the selected index digest.
- Delete any stage older than TTL: can interrupt a slow active object read.
- Hold the parse-blocking lock during S3 or Airflow metadata I/O: increases DAG
  parse latency and creates avoidable availability coupling.
- Let retention failure degrade to warning: violates a hard storage budget.
- Mix exact and legacy layouts under one root: the pointer formats and rollback
  identities are incompatible.
- Introduce a generic cache plugin system: two closed, versioned layouts do not
  justify the added extension surface.

## References

- [Feature design](../feature-design-airflow-legacy-pack-cache-hardening-v07332.md)
- [Operations runbook](../airflow-legacy-pack-cache-operations.md)
- [ADR 0008: parse-safe provider](0008-airflow-parse-safe-provider.md)
- [ADR 0009: artifact delivery and cache materializer](0009-artifact-delivery-and-cache-materializer.md)
- [ADR 0032: exact activation occurrence](0032-airflow-exact-activation-occurrence.md)
