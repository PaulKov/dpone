# Extraction lifecycle and artifact ownership

dpone records source-extraction time separately from target load time. The
runtime contract is designed for eager files, lazy row streams, batched files,
partitioned COPY, and same-database internal queries without inventing a
timestamp before data has actually been read.

## Receipt semantics

`ExtractionLifecycleAuthority` owns the current immutable
`ExtractionLifecycleReceipt`:

- `extraction_started_at` is the UTC start of the observed source-read window;
- `clock_authority` identifies the clock that issued the extraction-window
  timestamps. Generic adapters use `dpone.orchestrator.utc` and do not claim a
  database snapshot;
- `extraction_completed_at` is `null` until every source row or byte has been
  consumed and its row/byte receipt has been published;
- `snapshot_acquired_at` and `snapshot_authority` are optional and are issued
  only after a real vendor/session snapshot exists;
- `source_token` is allowed only with proven snapshot evidence. PostgreSQL
  strategies expose a digest of the server-issued snapshot token;
- `complete()` atomically replaces the in-progress immutable receipt. It does
  not mutate a receipt already observed by another component.

A lazy artifact may be returned while its authority is still pending. This is
expected for batched export: acquisition occurs when the generator opens its
repeatable-read transaction, not when Python constructs the generator object.

PostgreSQL boundaries are strategy-specific:

- whole COPY opens `REPEATABLE READ READ ONLY`, obtains
  `txid_current_snapshot()`, writes and receipts the file, then commits the
  read-only source transaction;
- streaming rows keep that transaction open until the target-side owner issues
  a terminal outcome;
- batched COPY acquires on first generator consumption and completes only after
  the terminal empty batch proves exhaustion;
- partition workers import one `pg_export_snapshot()` from a coordinator
  session. Automatic MIN/MAX/count bounds are resolved inside that same
  snapshot;
- `InternalQueryArtifact` has no source file or cursor. Its extraction window
  starts and completes at the target session's `INSERT ... SELECT` execution
  boundary; it does not claim a snapshot unless that session supplies a native
  snapshot authority.

`ExtractResult.extraction_receipt` always reads the authority's latest frozen
value, so wrappers and schema rebinds do not copy stale timing data.

## Terminal outcomes

Artifacts accept one idempotent terminal decision:

| Outcome | Meaning | File evidence | Source cursor/transaction |
| --- | --- | --- | --- |
| `success` | Target commit is acknowledged | cleaned | commit/close |
| `abort` | Load did not commit | cleaned | rollback/close |
| `retain_commit_unknown` | Target commit acknowledgement is ambiguous | retained | rollback/close |

Retention applies only to durable diagnostic evidence. A live cursor or a
PostgreSQL read-only transaction is never evidence and is always closed. The
first terminal decision wins; repeated calls return the same
`ArtifactTerminalReceipt`.

Cleanup is best-effort and never replaces a primary load exception. A strict
artifact release path records a stable error type in
`cleanup_error_code`; `cleanup_succeeded=true` is never emitted when file
deletion actually failed. Compatibility `cleanup()` methods may continue to log
release failures for legacy callers.

File rebinds and wrappers share one terminal authority and one exactly-once
physical-file release authority. A projected view therefore cannot reverse a
root `retain_commit_unknown` decision or publish a contradictory receipt.

Every artifact that creates a target staging table also uses the shared
`owned_staging_handle` construction guard. Ownership transfers to the caller
only when `materialize()` returns. If row insertion, file import, contract
validation, logging, or cancellation fails before that return, the guard drops
the otherwise unreachable staging table. A secondary drop failure is attached
as a redacted exception note and never replaces the primary failure.

## Connector integration checklist

New source implementations must:

1. Call `acquire()` at the truthful source-read/materialization boundary. Use
   `acquire_snapshot()` only after a real vendor snapshot exists.
2. Bind the same authority to the artifact and `ExtractResult`.
3. Call `complete()` only after iterator exhaustion and authoritative row/byte
   evidence.
4. Provide separate success and abort handlers for transactional streams.
5. Treat `retain_commit_unknown` as rollback/close for live source resources.
6. Publish `snapshot_authority` and `source_token` only from a native source
   primitive; generic MSSQL, MySQL, Kafka, and REST adapters keep both empty.
7. Use server-safe SQL composition for snapshot tokens. PostgreSQL does not
   accept a bind parameter in `SET TRANSACTION SNAPSHOT`; dpone uses
   `psycopg.sql.Literal` with a server-issued token.

The vendor-live regression
`tests/integration/postgres/test_postgres_exported_snapshot_lifecycle_live.py`
certifies distinct worker sessions, snapshot import, concurrent source
mutation, automatic-bounds ordering, and whole-COPY timing against real
PostgreSQL.
