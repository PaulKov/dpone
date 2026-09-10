# ADR 0058: Verified release composition preserves constituent authority

- Status: Accepted
- Date: 2026-09-10

## Context

ADR 0052 deliberately requires complete workspace source authority. Independently
authored ordinary workloads are not part of that source. A single immutable
publication must not weaken that invariant or assign dbt authority by filename.

## Decision

Introduce the explicit `dpone.release-set.v3` composition envelope. Keep native
release-set.v2 and dbt wire-v2 unchanged inside a typed constituent. Bind the
complete ordinary producer inventory, exact flat union, ownership and supported
transport in parent identity. Preserve native bytes and verify its detached
original tree using the existing complete-source verifier. Runtime resolves the
selected workload's authority through the verified parent/constituent binding.

Require already compact native input in the initial scope; the existing public
materializer provides that transformation. Only semantically empty ordinary
connection projections can use RuntimeConnectionContext transport. Register all
source sidecars in delivery inventory and apply aggregate parent budgets.

Explicitly block composition activation until physical admission covers every
constituent. Delivery, signatures and offline source checks cannot grant SQL or
activation authority. Existing dbt-only mirror/evidence producers reject this
outer envelope until they support parent-scoped evidence.

## Consequences

Old readers reject v3. Current v1/v2 consumers retain their contracts. Users
upgrade readers first and regenerate from complete authoring inputs. Source
metadata grows, but native runtime payload bytes are not duplicated. Independent
DAG boundaries remain; cross-constituent DAG splicing is outside this decision.

See the [approved feature specification](../feature-specs/verified-release-composition.md)
for algorithms, failure states, consumer audit, validation and migration.
