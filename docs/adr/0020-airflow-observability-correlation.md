# ADR 0020: Airflow observability correlation composes immutable and observed identity

## Status

Accepted.

## Context

dpone needs one join key across an Airflow task attempt, dpone runtime run,
release, deployment, workload pack, runtime evidence and Kubernetes pod.
`dpone.airflow-run-identity.v1` already identifies immutable artifacts and is
shared by every mapped item. Airflow attempt coordinates, dpone run ID, evidence
checksum and pod UID become known at different execution stages.

Putting every field into the immutable run identity would make artifact identity
retry- and pod-specific. Using only a dpone run ID would not prove which release,
deployment or Airflow attempt ran. Generating trace IDs without an actual trace
context would misrepresent telemetry, while propagating arbitrary OTel baggage
would create a data-leak surface.

## Decision

- Keep `AirflowRunIdentity` unchanged as immutable release/deployment/DAG/pack
  identity.
- Add `dpone.airflow-correlation.v1` for one Airflow workload attempt.
- Derive `correlation_id` from Airflow attempt coordinates plus immutable
  release, deployment and workload-pack identity.
- Exclude dpone run, evidence and pod observations from that digest so the ID is
  available before runtime and survives pod replacement within one attempt.
- Require the final release evidence bundle to associate the correlation with
  a dpone run ID, runtime evidence digest and observed pod UID.
- Treat all mismatches as blockers. Missing observations are blockers in release
  policy and warnings in advisory/PR policy.
- Make the existing Airflow evidence bundle the authoritative correlation
  source for exporter projections.
- Project the same correlation into an OpenLineage custom run facet and OTel
  metric artifacts. Use deterministic UUIDv5 only for OpenLineage `runId`; do
  not invent OTel trace/span IDs.
- Keep OpenLineage/OTel imports optional and outside Airflow DAG parse.
- Keep high-cardinality attempt IDs out of Prometheus labels.

## Consequences

- Operators can join Airflow, dpone, artifact, evidence and pod records with one
  validated ID.
- Retries have distinct IDs while mapped items remain tied to the same immutable
  release/deployment/pack identity.
- Pod replacement does not change the logical attempt identity.
- Exporter backends cannot silently reinterpret identity fields.
- Release evidence becomes stricter: missing dpone run, evidence digest or pod
  metadata prevents a full-correlation claim.
- The provider remains parse-safe and does not require observability packages.
- A future genuine OTel SDK integration can propagate trace context alongside,
  rather than instead of, the dpone correlation model.

## Alternatives rejected

- Expand `AirflowRunIdentity` with attempt and pod fields: mixes immutable and
  observed identity and breaks mapped reuse.
- Use Airflow run ID or dpone run ID alone: cannot prove all execution inputs.
- Include pod UID in the correlation digest: pod replacement changes logical
  attempt identity.
- Put correlation in OTel baggage: unnecessary propagation and leakage risk.
- Derive fake trace/span IDs: creates telemetry that looks traced but is not.
- Let each exporter compute its own key: duplicates policy and permits drift.

## References

- [Feature design: Airflow observability correlation v1](../feature-design-airflow-observability-correlation-v1.md)
- [ADR 0008: Airflow parse-safe provider](0008-airflow-parse-safe-provider.md)
- [ADR 0011: Canonical fingerprints](0011-canonical-fingerprints.md)
- [ADR 0013: Reproducible rerun and retention](0013-reproducible-rerun-and-retention.md)
- [ADR 0019: Bounded Airflow mapping](0019-bounded-airflow-backfill-mapping.md)
- [OpenLineage facets](https://openlineage.io/docs/spec/facets/)
- [OpenTelemetry context propagation security](https://opentelemetry.io/docs/concepts/context-propagation/)
