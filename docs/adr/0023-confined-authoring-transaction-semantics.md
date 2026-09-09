# ADR 0023: Confined authoring transactions use atomic exchange and durable recovery

- Status: Accepted
- Date: 2026-07-18

## Context

Self-service scaffold, migration, and catalog updates operate on user-editable
repository files. They must tolerate concurrent editors, CI workers, process
termination, symlinks, and partial cleanup without losing bytes or claiming a
rollback that did not occur.

The v0.73.1 implementation moved an existing file to a recovery name and then
linked a replacement into the authoritative name. That sequence preserved the old
bytes but introduced a namespace gap. A crash could leave the authoritative path
missing. Content-based rollback also could not distinguish an operation-owned file
from a pre-existing or concurrent identical file.

Portable `os.replace` is atomic but cannot express compare-and-replace. A writer can
change the path after the digest check and be silently overwritten. Cooperative
locks do not protect against normal editors and external tools.

## Decision

Existing-file authoring replacement uses:

1. descriptor-confined, no-follow validation;
2. one deterministic, bounded, fsynced sibling transaction journal;
3. a certified native atomic name exchange;
4. post-exchange validation of the displaced file;
5. idempotent recovery based on expected, desired, and observed digests;
6. exact ownership receipts for creation and rollback.

The atomic exchange is the only commit linearization point. The authoritative
pathname is therefore always bound to a complete old, concurrent, or desired file;
it is never intentionally absent.

Supported POSIX adapters are Linux `renameat2(RENAME_EXCHANGE)` and macOS
`renameatx_np(RENAME_SWAP)`. An unsupported or uncertified platform fails before
mutation. There is no fallback to `os.replace`, unlink-then-link, or a cooperative
lock as a correctness boundary.

The journal is:

```text
.<leaf>.dpone-transaction.json
```

It records only schema, leaf names, phase, and expected/desired digests. It contains
no file payload, absolute path, or secret. Recovery checks this one deterministic
path; it does not scan the repository.

After exchange:

- displaced digest equals expected: replacement is committed;
- displaced digest differs: exchange back and report `source_changed`;
- a third writer or corrupt/incomplete journal makes the state ambiguous: preserve
  every version and report `recovery_required`.

A cleanup error after the exchange has proven expected bytes were displaced is a
committed transaction with cleanup required. Callers must not roll back dependent
files or report the root replacement as uncommitted.

Created files carry an exact ownership receipt. Rollback removes only the owned
inode and never relies on equal content alone. A pre-existing identical file is a
no-op and remains user-owned.

Multi-file authoring operations validate dependencies immediately before root
activation, compensate every owned receipt in reverse order, continue after an
individual compensation failure, and return all recovery artifacts.

## Consequences

Positive consequences:

- no intentional authoritative-path gap;
- no silent overwrite of a concurrent pathname winner;
- process interruption leaves bounded, discoverable recovery state;
- post-commit cleanup is not misreported as rollback;
- identical pre-existing fragments are not deleted;
- receipts remain truthful when several rollback actions fail.

Costs and limitations:

- native platform adapters and CI certification are required;
- atomic exchange preserves bytes but cannot prevent external observers from seeing
  a complete desired file between exchange and mismatch recovery;
- ambiguous third-writer races require manual recovery;
- unsupported platforms can plan and validate but cannot apply until certified;
- filesystem and directory `fsync` semantics still depend on the underlying
  filesystem and mount configuration.

## Invariants

1. Dpone never knowingly removes the authoritative name before binding a complete
   replacement.
2. No unknown or user-owned file is unlinked.
3. Every mutation after journal creation is recoverable or explicitly
   `recovery_required`.
4. A successful root activation observes the expected root and current dependent
   fragment snapshots.
5. A post-commit cleanup failure never triggers a false rollback claim.
6. Plan mode performs zero filesystem writes.

## Related

- [Approved filesystem transaction bug-spec](../feature-design-self-service-filesystem-transactions-v0731.md)
- [ADR 0006: Authority and canonical IR](0006-authority-and-canonical-ir.md)
- [ADR 0008: Airflow parse-safe provider](0008-airflow-parse-safe-provider.md)
