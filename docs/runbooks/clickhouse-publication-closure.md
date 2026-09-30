# Diagnose and retain uncertain ClickHouse publication

For operators of the [native publication building block](../clickhouse-native-publication.md).
This runbook inspects transport closure only. It does not unlock the target,
prove physical outcome or provide a complete production recovery backend.

## Stop and preserve

If a reply is lost, the process dies, a journal ACK is ambiguous or the publisher
raises `PublicationUnknown`, preserve the original journal/WAL/SHM, lock files,
candidate, object inventory and target. Stop new admissions to that target.
Do not repeat EXCHANGE/REPLACE, change operation IDs, delete owners, remove lock
files or provision another journal. A lost reply may follow a successful swap.

The execution mutex disappears on process exit; durable ownership does not.
`safe_to_retry=False` applies to mutation dispatch. Repeating a protected
read-only inspection is different from repeating SQL publication.

## Inspect the original operation

1. Reopen the same authority path and deployment identity using
   `SQLitePublicationAuthority`. A missing, corrupt, replaced or unsafe store is
   a stop condition, not an invitation to initialize an empty replacement.
2. Read `diagnostics(original_operation_id)` or use `write_diagnostics` with a
   new destination in a separate report directory. Output is atomic UTF-8 JSON,
   no overwrite; it omits credentials, grants and source rows. A diagnostic is
   not importable authority. Keep operational identifiers access-controlled.
3. Inspect `transport`, `state`, `owner_retained` and immutable history. A
   `claimed` publication can coexist with durable terminal transport closure:
   physical observation/resolution is a later component, not performed here.
4. Invoke `close_and_drain(original_operation_id)` only on the correctly wired
   publisher, exclusion and original authority. It does not connect to the
   source or replay publication SQL.

| Original transport | Closure result | What it does not permit |
|---|---|---|
| `not_started` | Atomically close without send, revoking delayed claimants | Reconstruct a grant or start a successor |
| `closed_without_send` | Idempotent success, zero SQL | Claim target data was published |
| `closed_terminal` | Idempotent success from original persisted terminal proof | Claim COMMITTED or advance a checkpoint |
| `may_have_sent` | `PublicationUnknown`; retain quarantine | Infer no-send from timeout, absent PID/query log or desired rows |

If a terminal write committed but its ACK was lost, a new protected read may
find `closed_terminal`; closure can then succeed without SQL. If that state was
not persisted, even an apparently successful server effect remains unknown.
The digest in the journal is not a reconstructible native protocol receipt.

## Escalation and restore

Provide the maintainer with the redacted original report, exact source and
driver versions, node/image identity and the observed failure boundary. Do not
send credentials, volatile grant secrets, raw vendor exceptions or source rows.
Missing driver/version mismatch should be corrected in deployment preflight,
but installing the right driver does not turn an already possible-send operation
into a retryable one.

There is no force-unlock flag, TTL takeover, automatic KILL recovery or operator
boolean override. Closure alone cannot admit a second operation on this target.
A separately implemented and reviewed offline recovery/finalization protocol is
required to prove ingress isolation and server quiescence before any release.

A valid old backup cannot be detected from SQLite alone. Before restoring or
replacing a volume, stop admissions and revoke/isolate publisher access outside
the store being restored. Do not resume an old journal with still-active database
credentials. Copying only a live main SQLite DB is not a consistent backup.

## Upgrade and rollback

Drain new admissions before replacing code. Preserve the existing authority
schema and original deployment/operation identity. Never fall back to the legacy
writer while an operation is unresolved. Code rollback does not undo an actual
ClickHouse mutation. Table rollback requires a new guarded publication after
the original authority is safely resolved; REPLACE retains the candidate, not
the old target partition.

Return to the [method overview](../clickhouse-publication-methods.md) for the
future complete backend's method-specific outcome rules, or the
[journal reference](../clickhouse-authority-journal.md) for storage diagnostics.
