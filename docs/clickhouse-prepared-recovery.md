# Recovering an original ClickHouse `PREPARED` publication

This command is for a fully staged, unchanged full-refresh candidate whose
**original** authority operation is still `PREPARED`. It is not a general retry,
an authority reset, or a way to mark a failed DAG green. No source data is
read and no new candidate is created by the command.

## Admission checklist

1. Stop competing writers by the normal scheduler procedure and record the
   exact operation ID, authority version, target, cluster and original start
   time from authenticated run evidence. Verify there is no active target DDL.
2. Confirm that all writers use the same externally provisioned strict
   KeeperMap authority, on the same Keeper service. Verify the authority
   schema/path on **every** replica and approved all-writer cutover. A
   ReplicatedReplacingMergeTree with a version column is **not** sufficient.
3. Confirm all candidate rows, schemas, target/predecessor generations and
   replica health are unchanged. The preflight independently checks them.
4. Ensure `system.query_log` covers the entire interval from before the
   operation start on every replica. A queue lookup alone is insufficient.
5. Use a logical `connection_ref` in the binding-set/connection registry;
   never put passwords on the CLI. The standalone command supports normal
   projected credentials. A Vault-only reference without its pinned runtime
   credential context fails closed.

If any fact is missing, stop. Do not delete, rewrite or import an authority row
to make the command pass. In particular, a legacy `PREPARED` record without a
strict-origin write identity cannot be recovered by this command; a separate
reviewed infrastructure migration is required.

## Plan and execute

Use a private directory for the plan file. The example uses synthetic names:

```bash
dpone ops clickhouse-prepared-recovery plan \
  --binding-set binding-set.yaml --connection-registry connection-registry.yaml \
  --connection-ref analytics_sink --cluster analytics_cluster \
  --database analytics --target daily_fact --operation-id original-operation-id \
  --authority-version 1 --operation-started-at 2026-09-30T08:00:00+00:00 \
  --plan-file ./prepared-recovery-plan.json --format json
```

The JSON response contains redacted digests, not physical table identifiers.
The local plan file is created once with mode `0600`; keep it in restricted
storage. It contains operation metadata but no credentials. Do not replace it
with a fresh plan after dispatch. Review the returned `plan_digest`, then:

```bash
dpone ops clickhouse-prepared-recovery execute \
  --binding-set binding-set.yaml --connection-registry connection-registry.yaml \
  --connection-ref analytics_sink --cluster analytics_cluster \
  --database analytics --target daily_fact --operation-id original-operation-id \
  --authority-version 1 --operation-started-at 2026-09-30T08:00:00+00:00 \
  --plan-file ./prepared-recovery-plan.json \
  --confirmation-digest '<digest from plan>' --format json
```

Exit `0` means the exact operation reached proven `COMPLETED`. Exit `2` is a
proved safety block (for example legacy authority, drift or conflict). Exit
`1` means the outcome is unknown or the observation failed. In either error
case, inspect exact authority, DDL queue and physical generations read-only.
Never perform a blind second dispatch. If execute crashed, retry **with the
same plan file and digest**; it resumes reconciliation and does not send a
second DDL. A completed `EXCHANGE` cannot be rolled back by this CLI.

After `COMPLETED`, verify the DAG outcome and run independent source/target
quality and freshness checks before declaring the business load restored.
Aggregate parity does not establish full row-wise identity.

For design and migration constraints see [ADR 0076](adr/0076-clickhouse-strict-prepared-recovery.md).
