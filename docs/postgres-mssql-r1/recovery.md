# PostgreSQL → MSSQL R1 recovery runbook

> Historical source contract context: approvals, source bindings and evidence do not transfer to this migration candidate. Current validation remains separately tracked.

This runbook is for on-call operators handling an R1 route. Preserve target
authority and sealed staging until the outcome is proved.

!!! warning "Future production runbook"

    No production R1 route can currently be activated: the provider
    composition is absent and vendor-live evidence is `UNVERIFIED`. Use this
    runbook to review the recovery contract and test the implemented UoW
    foundation only; it is not evidence that an executable route exists.

## First response

1. Pause the route and prevent a different Batch, XMin or maintenance writer
   from using the target.
2. Record the stable error code, operation/effect identity and redacted
   authority projection.
3. Do not delete or edit receipts, the writer head, checkpoint, row hashes,
   sealed intent or staging.
4. Do not rerun source extraction under the same effect identity when sealed
   staging is missing or mismatched.

## Outcome classes

| Outcome | Meaning | Action |
| --- | --- | --- |
| `replay_suppressed` | Exact historical receipt and descendant chain prove the effect | Return the receipt-backed result; no source read or DML |
| `committed_after_receipt_probe` | COMMIT response was lost, but a fresh session proves the receipt | Treat the target effect as committed; repair secondary evidence only |
| `known_not_committed` | Fresh locked proof shows no receipt and unchanged authority | Retry only the same sealed intent and incremented operation epoch |
| `DPONE_POSTGRES_MSSQL_KNOWN_NOT_COMMITTED_RETRY_EXHAUSTED` | The one bounded same-intent retry also proved non-commit | Keep staging; return a failed result and require an explicit same-intent retry |
| `DPONE_POSTGRES_MSSQL_COMMIT_OUTCOME_UNKNOWN` | Receipt, chain, recovery domain or target authority is inconclusive | Keep paused; escalate for signed recovery/rebaseline |

A commit probe must use a new SQL Server session. It acquires the target and
operation locks in canonical order and verifies receipt, head, operation,
recovery identity, checkpoint and staging seals. A same-session query after an
ambiguous COMMIT is not evidence.

## Common blockers

- `...WRITER_FENCE_STALE`: stop the stale worker; verify the current owner and
  operation epoch.
- `...RECEIPT_CONFLICT`: permanent identity/body conflict; do not retry DML.
- `...ROW_HASH_BASELINE_MISMATCH`: investigate unauthorized target DML and
  reconcile the actual target before any new generation.
- `...EMPTY_FULL_REFRESH_AUTHORITY_REQUIRED`: obtain a signed, one-shot
  `allow_empty=true` authority bound to the sealed zero-row snapshot.
- `...RECOVERY_DOMAIN_CHANGED`: restore/PITR/clone invalidated absence proof;
  resolve old ambiguous work and perform signed rebaseline.
- `...SEALED_INTENT_UNAVAILABLE`: retain evidence and escalate; never reread a
  changing source using the same effect key.

After recovery, verify target/row-hash parity, receipt/head/checkpoint
agreement, source-I/O suppression for replay, and absence of another active
writer. Only then resume.

Source-I/O suppression applies to a replay found during pre-source admission.
If an exact concurrent receipt is found inside the target UoW after staging,
the effect is still suppressed, but the result truthfully reports
`source_io_performed=true`.

Batch and XMin counters are reconstructed only from the verified typed
receipt. The XMin body retains exact before/after, inserted, updated,
hard-deleted, unchanged, delta-row and payload-byte metrics, so commit-response
recovery and historical replay return the same `LoadResult` without rescanning
PostgreSQL or the SQL Server target.

Continue with the [migration guide](migration.md), or return
to the [R1 overview](overview.md).
