# ADR 0013: Reproducible rerun and retention

## Status

Accepted.

## Context

Reruns must be able to use the same release/deployment identities that produced the original evidence, but Airflow DAG Bundle backends vary in versioning support.

## Decision

Evidence records release ID, deployment ID, pack fingerprints, binding-set fingerprint, connection-registry fingerprint, runtime image digest, and Airflow bundle metadata. Non-versioned DAG bundle backends require a dpone content-addressed snapshot or emit `DPONE_RERUN_NOT_REPRODUCIBLE`.

Retention/GC protects current deployments, active runs, and reproducible evidence references before deletion.

## Consequences

- Critical pipelines can default to reproducible rerun.
- GC becomes plan-first and evidence-aware.
- Broken retention yields explicit release/deployment errors.
