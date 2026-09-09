# Feature design: CI shadow PR6A pair remediation

- Status: RESEARCHED
- Owner: dpone maintainers
- Issue: [#512](https://github.com/PaulKov/dpone/issues/512)
- Parent: [PR6A readiness core](feature-design-ci-shadow-pr6a-readiness-core.md)
Last verified: 2026-08-28

## Executive summary

Fresh review found that the first PR6A implementation could overwrite or remove
foreign bytes and did not preserve exact-subject identity. A create-only repair
removed those races but changes ADR 0048's accepted retained-prior-byte and
atomic-exchange semantics. This amendment therefore chooses the safe algorithm
before any implementation can merge.

## Scope and public boundary

The implementation remains private internal code: no CLI, public Python import,
schema, workflow, credentials, readiness `PASS`, merge, or release authority is
added. Users see no new file format. A remaining journal is always internal
`UNVERIFIED` evidence, never a success result.

## Required decision

### Alternative A — retain ADR 0048 semantics

Implement the existing durable-prior-byte, native-exchange pair transaction.
The journal retains backup leaves plus their inode/digest receipts. Before every
recovery action it authenticates the journal, stages, backups, and ownership;
foreign/missing/torn state is preserved as `UNVERIFIED`. Every deletion uses the
existing ownership/quarantine primitive. Exact subject identity needs a durable
private receipt or a parent-approved deterministic per-subject output root.

### Alternative B — amend ADR 0048 to create-only publication

Amend ADR 0048 and the parent PR6A design so an exact subject can only create an
empty output root. Public JSON/Markdown and a private identity receipt are linked
from confined stages without overwrite. Existing/racing names are preserved and
return `UNVERIFIED`; no prior bytes or exchange path exists. Recovery removes
only proven same-inode staged hard-links through ownership quarantine.

### Recommendation

Choose **Alternative A**. It preserves the accepted ADR, supports future
replacement/recovery requirements, and avoids weakening the already-approved
transaction contract. Alternative B is sound only after an explicit ADR
amendment and changes the feature's operational capability.

## Algorithm for Alternative A

1. Validate a strict SHA-40 subject, report kind, bounded leaves and confined
   directory; acquire a validated nonblocking private lock.
2. Authenticate/recover a prior journal before any new mutation. Unknown fields,
   noncanonical bytes, foreign inode, missing or digest-mismatched backup return
   `UNVERIFIED` and preserve all evidence.
3. Stage JSON and Markdown with `O_EXCL`, fsync each, snapshot pre-existing
   regular leaves into durable backup leaves, and record both backup digest and
   ownership receipt in the closed journal.
4. Persist `PREPARED`; native-exchange JSON then persist `JSON_REPLACED`;
   native-exchange Markdown then persist `PAIR_REPLACED`.
5. Verify both intended inode/digests and durable exact-subject identity receipt;
   persist `COMMITTED`. Only then remove owned backups/stages and journal.
6. Recovery validates all retained objects first. It rolls forward only exact
   staged/target/backup state; otherwise it restores exact owned backups with
   exchange/quarantine. Any race or uncertain operation retains evidence and is
   `UNVERIFIED`.

## State and failure semantics

`PREPARED`, `JSON_REPLACED`, `PAIR_REPLACED`, and `COMMITTED` remain the only
journal states. Lock contention, unsupported native exchange, Windows lock
adapter absence, external winner, backup mismatch, malformed journal, I/O/fsync
fault, and cancellation are non-successes. No retry overwrites a foreign winner.

## Architecture and quality

Reuse `confined_mutations`, `confined_atomic_exchange`, `confined_files`, and
the existing ownership receipt; add only pair-specific codec/state modules under
400 SLOC. ADR amendment is **not needed for Alternative A**; it is mandatory
for Alternative B. The POSIX lock boundary must either gain a Windows adapter or
fail closed before any mutable leaf is created, with a platform contract test.

## Validation and certification

Tests must cover existing/absent pair members; every write, exchange, fsync,
journal-state, identity-receipt and cleanup fault; symlink/foreign inode/race
preservation; lock contention; every recovery state; same and distinct subject
idempotency; Windows unsupported/adapter behavior; and legacy confined-mutation
compatibility. Hosted required checks and exact-head receipt remain mandatory;
live certification is N/A because PR6A has no external service.

## Documentation, rollout, and rollback

Only developer design/runbook text changes. No user migration exists. Before
PR6B, rollback removes these internal modules and unreferenced private artifacts;
it never deletes uncertain journals or user bytes.

## Approval checklist

- [x] Review findings and alternatives are explicit.
- [x] Exact failure and recovery semantics are specified.
- [x] Public boundary and compatibility are unchanged.
- [x] Test, quality, and documentation impact are explicit.
- [ ] Maintainer selected an alternative and changed status to `APPROVED`.
