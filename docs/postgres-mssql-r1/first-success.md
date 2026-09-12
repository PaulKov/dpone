<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Plan the PostgreSQL → MSSQL R1 route

> Current migration scope: planning remains non-mutating and activation is blocked. Historical source design approval does not certify this candidate. Follow only the planning steps below until current implementation and certification are established.

This tutorial gives a data engineer the first safe, non-mutating R1 result.
Current repository evidence is `UNVERIFIED`, so the tutorial ends at planning.

## Prerequisites

- install `dpone` with the PostgreSQL and MSSQL extras;
- clone the repository so the validated example is available;
- obtain logical connection references from the platform team;
- do not put connection strings or credentials in the manifest or profile
  catalog.

The current compatibility-only planning manifest is
`examples/batch/postgres-mssql-r1-full-refresh.batch.yaml`.
Open this file from the repository root after cloning.
It is deliberately nonactivating and still contains legacy physical choices
such as staging schema, BCP mode and state provisioning. It demonstrates the
existing planner boundary; it is not the future certified semantic-only
manifest. Receipt tables, database identities and provider registration remain
platform-owned. The certified profile will reject these physical knobs rather
than silently accepting them.

## Validate

```bash
dpone manifest validate \
  examples/batch/postgres-mssql-r1-full-refresh.batch.yaml
```

Exit `0` means the manifest is structurally valid. It does not mean R1 is
certified, semantic-only or active.

## Plan one route

Point `DPONE_POSTGRES_MSSQL_CORRECTNESS_CATALOG` at the reviewed environment
catalog, then run:

```bash
dpone plan \
  examples/batch/postgres-mssql-r1-full-refresh.batch.yaml \
  --selector public.orders \
  --format json
```

The expected R1 section while certification is unavailable is:

```json
{
  "selected": true,
  "capability_id": "postgres_mssql_target_uow_v2",
  "source_mode": "batch_full_refresh",
  "target_topology": "standalone_same_database",
  "implementation_status": "absent",
  "certification_status": "unverified",
  "activation_status": "blocked",
  "mutates": false
}
```

A selected but blocked R1 plan exits with status `1`; inspect the JSON blockers. This is an expected non-mutating planning result while activation remains unavailable.

`selected: false` means the route remains on its compatibility profile. A
blocker or invalid catalog is not permission to edit the user manifest with
physical receipt or topology fields; the platform owner must correct the exact
registration or capability evidence.

`implementation_status: absent` is intentional in the current repository. The
target-local contracts and unit-of-work foundation exist, but the signed target
registration, exact staging/quality providers and production composition are
still governed by the
[R1 provider-closure design](../feature-design-postgres-mssql-r1-provider-closure-v1.md).
Until that design is approved and implemented, `dpone run` cannot activate R1.

Next, read the [profile reference](reference.md). Return to
the [R1 overview](overview.md) to check scope.
