# ADR 0007: Release-set and deployment-set

## Status

Accepted.

> Amended by [ADR 0024](0024-airflow-executable-init-fetch-wire-boundary.md)
> for the strict executable `init_fetch` wire. The v1 deployment contract
> remains the non-runnable preview/legacy authority; executable indexed
> deployments use the v2 contract defined by that amendment.

## Context

dpone Airflow artifacts must be promoted across environments without rebuilding, while environment-specific bindings vary by dev, stage, and prod.

## Decision

Use two immutable identities:

- `dpone.release-set.v1` is environment-neutral and contains DAG specs, workload packs, and canonical schemas.
- `dpone.deployment-set.v1` binds a release to the non-runnable preview and
  compatibility projection.
- `dpone.deployment-set.v2` binds one release to environment-specific
  binding-set, connection registry, credential runtime, exact runtime image,
  trust tier, registry/trust snapshots, selected workload inventory, and the
  strict Airflow runtime-delivery configuration.

The active Airflow cache points to a deployment, not directly to a release.

## Consequences

- `Build once, promote by digest` is preserved.
- Environment changes alter deployment identity but not release identity.
- Evidence records both `release_id` and `deployment_id`.
