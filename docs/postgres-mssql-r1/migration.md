# Migrate PostgreSQL → MSSQL from V1 to R1 V2

> Historical source contract context: approvals, source bindings and evidence do not transfer to this migration candidate. Current validation remains separately tracked.

This guide is for platform engineers performing a controlled writer cutover.
The transition is per target and becomes roll-forward-only at principal
revocation.

!!! warning "Procedure is not executable yet"

    The target-local authority foundation exists, but the production
    provisioner, signed target registration, staging attestor and concrete
    Batch/XMin provider composition are still absent. Do not begin this cutover
    until the approved provider closure has complete Migration/Renderer/Catalog,
    aggregate and installer implementations, explicit activation, and current
    exact vendor-live evidence.

## Preconditions

- exact V2 schema and target registration are provisioned and validated;
- current vendor-live evidence matches the deployed commit, dependency closure,
  PostgreSQL minor, SQL Server CU and ODBC build;
- a distinct least-privilege V2 runtime principal exists;
- all Batch, XMin, shadow, backfill and maintenance writers can be paused;
- the full baseline fits the R1 bounded transaction profile.

## Cutover

1. Pause and drain every old writer; prove there is no active V1 operation.
2. Extract and seal one snapshot-bound Batch or XMin baseline.
3. Have the provisioner verify the seal and issue the exact signed generation
   authority.
4. Under the canonical target lock, revoke the V1 principal's target DML/DDL,
   V1 authority, shadow, backfill and maintenance mutation rights.
5. Rotate deployment credentials and prove the old runtime is denied both by
   registry preflight and SQL Server.
6. Apply the sealed baseline through the V2 one-session UoW.
7. Verify target/hash parity and atomically committed receipt, checkpoint when
   applicable, and generation head.
8. Activate only the exact certified profile registration.

The revocation transaction is the point of no return. Before it, inactive V2
preparation may be discarded and V1 retained. At or after it, keep the route
paused and roll forward with a V2-capable binary and the same sealed intent.
Do not re-grant V1 rights or synthesize V2 row hashes from current target data.

V1 receipts/checkpoints remain read-only compatibility evidence. Dual-write is
forbidden. A binary rollback is allowed only if the older binary can read the
current V2 authority; otherwise leave the route paused.

For failures, follow the [R1 recovery runbook](recovery.md).
Return to the [R1 overview](overview.md).
