# ADR 0042: Runtime Pod deletion requires pre-mutation evidence

## Status

Accepted.

## Context

Airflow runtime Pod retention conditionally deletes terminal Kubernetes Pods.
An aggregate report created after the deletion loop is insufficient: process
termination, eviction or node failure can happen after Kubernetes accepts a
delete and before the report reaches stdout. Fresh inventory can prove that a
Pod is absent, but cannot prove which controller requested the deletion.

The cleanup Job has an infrastructure-owned logging boundary. The application
service must expose sufficient structured events for that boundary without
importing Kubernetes, Airflow or one logging backend. The default CLI publisher
can establish process ordering, but it cannot acknowledge storage durability.

## Decision

Apply requires two injected capabilities in addition to inventory and deletion:

1. a credential-source descriptor supplied by the Kubernetes composition root;
2. an event evidence publisher supplied by the command or embedding application.

The service creates a content-addressed operation identity and publishes:

```text
operation_started
  -> delete_intent (published and flushed before each API call)
  -> delete_outcome (immediately after each API call)
operation_completed
```

Failure to publish `operation_started` or a `delete_intent` is fail-before-
mutation. Failure after an API call stops the batch and makes the aggregate
report failed with `evidence_status=incomplete`. When an acknowledged durable
publisher is injected, an abrupt death can leave a persisted intent without an
outcome; that is an explicit recovery fact and the next run performs fresh
inventory instead of assuming success.

Every publisher declares `process_ordered` or `durable_acknowledged` capability,
which is recorded as `evidence_durability` in the apply report. The CLI
publisher emits one bounded JSON object per line and flushes after each event.
This proves ordering at the application/process boundary only: it does
not call `fsync` and does not receive acknowledgement from an external
collector. Infrastructure owns collection, retention and immutability. A
deployment may claim crash-durable mutation evidence only when its injected
publisher returns after the configured durable sink has acknowledged the
event. The core service depends only on the publisher port. Python callers must
inject their own publisher for apply; plan remains read-only and requires no
publisher.

Credential evidence is read from the injected adapter capability. Read-only
planning may resolve `auto`, but destructive apply requires either explicit
`in-cluster` authority or `kubeconfig` plus one explicit context. Apply never
falls back to the ambient current kubeconfig context. The report records the
resolved mode and the reviewed kubeconfig context; in-cluster reports use a
null context because rollout evidence supplies the cluster occurrence.

## Consequences

- Every possible Pod mutation has a successful publisher call before the API
  call; persistence is guaranteed only by an acknowledged durable publisher.
- With a certified durable publisher, a missing outcome is distinguishable
  from a clean no-op or successful cycle.
- Evidence publication failure can reduce cleanup throughput; this is an
  intentional fail-closed tradeoff.
- The default CLI publisher does not provide crash durability. Production apply
  remains `UNVERIFIED` until the infrastructure collector and its acknowledgement
  boundary are certified; local tests do not claim cluster-level durability.
- The shipped acknowledgement adapter validates operation, sequence and the
  canonical event digest returned by an injected durable sink. It supplies a
  runnable composition boundary without claiming that any particular sink is
  certified by dpone.
- The event schema is a public additive contract. The apply report includes
  operation identity, evidence completeness and the publisher's explicit
  durability capability; process flushing is never reported as durable sink
  acknowledgement.

## Rejected alternatives

- Catch only `KeyboardInterrupt` or `SystemExit`: it does not cover SIGKILL,
  eviction or node loss.
- Write only the final aggregate report: mutation can precede report creation.
- Put a filesystem receipt on Pod `emptyDir`: it is lost with the Pod.
- Make the service depend on S3, Kubernetes logs or a vendor SDK: this reverses
  dependency direction and makes the core connector-specific.

## References

- [Runtime Pod retention feature design](../feature-design-airflow-runtime-pod-retention-control-v0732.md)
- [Runtime Pod retention operations](../airflow-runtime-pod-retention.md)
- [ADR 0024: executable Airflow runtime boundary](0024-airflow-executable-init-fetch-wire-boundary.md)
