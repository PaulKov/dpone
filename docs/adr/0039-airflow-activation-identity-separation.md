# ADR 0039: Airflow activation occurrence is separate from immutable run identity

## Status

Accepted.

## Context

`dpone.airflow-run-identity.v1` is a strict, immutable identity for release,
deployment, DAG, workload pack, runtime image and binding artifacts. ADR 0020
explicitly keeps execution observations outside that object. A cache
`activation_id` is an occurrence: the same deployment bytes receive a new UUID
when activated again or rolled back.

Adding `activation_id` to the strict v1 object would make old v1 readers reject
new payloads and would mix immutable artifact identity with an observed cache
switch. Runtime acceptance still needs all three values to prove which exact
activation executed.

## Decision

- Keep `dpone.airflow-run-identity.v1` byte- and schema-compatible.
- Introduce strict `dpone.airflow-deployment-identity.v1` with `release_id`,
  `deployment_id` and canonical UUIDv4 `activation_id`.
- Inject it through `DPONE_AIRFLOW_DEPLOYMENT_IDENTITY` beside
  `DPONE_AIRFLOW_RUN_IDENTITY`.
- Emit it as `deployment_identity` in bounded runtime and terminal workflow
  XCom evidence.
- Publish deployment-bound provider attempt evidence as
  `dpone.dbt-airflow-attempt-evidence.v2` and terminal evidence as
  `dpone.dbt-workflow-evidence-outcome.v2`. Their immutable v1 predecessors do
  not gain new fields; readers accept both versions and require identity only
  for v2.
- Derive both identities only from one verified local deployment index; runtime
  code must not copy expected values from trigger configuration.
- Hold one shared cache read lease while status/provenance reads the pointer,
  active symlink, index and pack checksum. A promotion takes the exclusive
  lease, so readers observe either the previous committed activation or the new
  committed activation, never an intermediate mix.
- Treat missing activation identity as backward-compatible for ordinary
  execution and as a blocker for deployment-bound acceptance.

## Consequences

- Historical strict v1 readers remain compatible.
- Historical attempt/workflow evidence remains schema-compatible and is
  classified as legacy evidence, not exact activation proof.
- Artifact fingerprints remain stable across reactivation.
- Acceptance can distinguish repeated activation of identical bytes.
- Provider and runtime carry two small typed values, each with one semantic
  responsibility.
- Any future evolution uses an explicit schema version instead of adding
  unknown fields to a strict contract.

## Alternatives rejected

- Add an optional field to run-identity v1: incompatible with strict old
  readers and contrary to ADR 0020.
- Create run-identity v2: technically valid, but forces every artifact and
  rerun consumer to migrate for an execution occurrence it does not own.
- Trust serialized DAG tags after execution: cannot prove which activation the
  runtime pod actually received.

## References

- [ADR 0020: Airflow observability correlation](0020-airflow-observability-correlation.md)
- [ADR 0032: exact activation occurrence](0032-airflow-exact-activation-occurrence.md)
- [Feature design](../feature-design-airflow-runtime-activation-evidence-v0732.md)
