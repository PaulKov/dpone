# PostgreSQL → MSSQL R1 correctness profile

> Migration status: historical source document retained for contract context. Source approvals, commit references, test counts and architecture exceptions do not establish approval or passing evidence for the current migration candidate. Current validation and public binding remain separately tracked.

This page helps data engineers and platform operators decide whether the bounded
R1 profile fits a route. It is not a WAL CDC guide.

!!! warning "Current availability"

    The executable implementation is currently `absent`, activation is blocked,
    and vendor-live evidence is `UNVERIFIED`. `dpone plan` may select and explain
    R1, but selection is not a production-readiness claim.

R1 covers PostgreSQL 16 Batch full refresh and XMin current-state polling into
one ordinary SQL Server 2022 disk rowstore table. Target data, row hashes,
receipt, writer fence and XMin checkpoint are committed in one transaction in
the same standalone database.

Use R1 only when all of these conditions hold:

- one non-null unique `int2`, `int4`, `int8` or UUID business key;
- the platform exclusively owns target business DML;
- target, staging and authority objects share one SQL Server database;
- SQL Server delayed durability is disabled;
- the target has no trigger, foreign-key, temporal, memory-optimized, ledger,
  target-CDC or other unsupported physical feature;
- a bounded target transaction fits the configured row, byte, log and time
  budgets.

R1 does not cover WAL streaming, event history, DTC, availability groups,
cross-database state, arbitrary target SQL or large resumable shadow loads.
Existing routes remain compatibility routes until a platform registration
selects R1 explicitly.

## Journey

1. [Validate and plan the example](first-success.md).
2. Ask the platform owner to register the exact route using the
   [environment profile contract](reference.md).
3. Before activation, complete provisioning, target-object validation and
   current vendor-live certification.
4. Use the [recovery runbook](recovery.md) for ambiguous
   outcomes; never delete a receipt or checkpoint to force a retry.
5. Use the [V1 → V2 migration guide](migration.md) for a
   controlled cutover.

For compatibility strategies and existing wide-route evidence, return to the
[Postgres → MSSQL route guide](../source-sink/postgres-to-mssql.md). The normative design is
[PostgreSQL → MSSQL Batch/XMin correctness R1](../feature-design-postgres-mssql-r1-correctness-v1.md).
