# Recovering an original ClickHouse `PREPARED` publication

This command is for a fully staged, unchanged full-refresh candidate whose
**original** authority operation is still `PREPARED`. It is not a general retry,
an authority reset, or a way to mark a failed DAG green. No source data is
read and no new candidate is created by the command.

## Admission checklist

1. Enforce exclusion of competing writers, including existing processes and
   their database permissions, through the approved platform procedure. Pausing
   a scheduler alone is insufficient. Record the
   exact operation ID, authority version, target, cluster and original start
   time from authenticated run evidence. Verify there is no active target DDL.
2. Confirm that all writers use the same externally provisioned strict
   KeeperMap authority, on the same Keeper service. Verify the authority
   schema/path on **every** replica and approved all-writer cutover. A
   ReplicatedReplacingMergeTree with a version column is **not** sufficient.
3. Confirm all candidate rows, schemas, target/predecessor generations and
   replica health are unchanged. The preflight independently checks them.
4. Check `system.query_log` for a pre-start event on every replica, enabled
   logging now, and no matching DDL in query history, processes or queue.
   These are negative corroboration only: retained logs do not by themselves
   prove continuous historical logging. The operation-scoped strict preparation
   provenance and approved all-writer cutover provide the no-dispatch argument.
5. Use a logical `connection_ref` in the binding-set/connection registry;
   never put passwords on the CLI. The standalone command supports normal
   projected credentials. A Vault-only reference without its pinned runtime
   credential context fails closed.

If any fact is missing, stop. Do not delete, rewrite or import an authority row
to make the command pass. In particular, a legacy `PREPARED` record without a
strict-origin write identity cannot be recovered by this command; a separate
reviewed infrastructure migration is required.

## Repeated generations and compatibility

New strict acquisitions carry a store-produced `prepared_origin`: the Keeper
version, epoch and canonical payload digest of that operation's preparation.
Recovery accepts the exact unadvanced preparation even when a target slot has
already served earlier loads. It does not accept an arbitrary nonzero version.
The producer preserves that origin through publication and cleanup, and CAS
matches the prior payload digest as well as version, operation and fence.
Same-phase quality governance writes never issue another DDL permit.

Initial-version strict records without this field retain the conservative
version-zero/epoch-zero path. Noninitial unmarked records and legacy imports
remain blocked. Upgrade all readers/writers of a strict authority together:
older binaries cannot interpret the extended record envelope. The legacy
ReplacingMergeTree payload remains unchanged when the field is absent.

Quality-bearing and empty-candidate recovery remain unsupported by this CLI.
Their implementation requires authenticated original policy, not an operator
override or a parsed `passed` flag. This limitation must not be advertised as a
completed recovery feature for such operations.

## Plan and execute

Use a private directory for the plan file. The example uses synthetic names:

```bash
dpone ops clickhouse-prepared-recovery plan \
  --binding-set binding-set.yaml --connection-registry connection-registry.yaml \
  --connection-ref analytics_sink --cluster analytics_cluster \
  --database analytics --target daily_fact --operation-id original-operation-id \
  --authority-version 0 --operation-started-at 2026-09-30T08:00:00+00:00 \
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
  --authority-version 0 --operation-started-at 2026-09-30T08:00:00+00:00 \
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

For design and migration constraints see [ADR 0078](adr/0078-clickhouse-strict-prepared-recovery.md).

## Disposable three-replica acceptance

The opt-in fixture uses only synthetic local data and fixed loopback ports
39100, 49100 and 59100. Check those ports are free. It is separate from the
older two-replica legacy-authority fixture and must not reuse a shared cluster.

```bash
docker compose -p dpone-prepared-recovery-local \
  -f tests/integration/clickhouse_cluster/strict-recovery-compose.yml up -d
DPONE_RUN_STRICT_PREPARED_RECOVERY=1 uv run pytest -q \
  tests/integration/clickhouse_cluster/test_strict_prepared_recovery_live.py
docker compose -p dpone-prepared-recovery-local \
  -f tests/integration/clickhouse_cluster/strict-recovery-compose.yml down
```

The profile tests real KeeperMap competing CAS, a lost write acknowledgement,
and recovery/replay of three consecutive interrupted generations with real
rows on three replicas. It is not production writer-fencing, Keeper quorum-loss,
quality-bearing recovery or legacy-migration certification. Preserve test
output with the exact source commit and image version; no synthetic result
may replace deployment-specific acceptance.
