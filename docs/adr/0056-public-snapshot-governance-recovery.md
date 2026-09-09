# ADR 0056: Public snapshot debt has observable provenance

## Status

Accepted by maintainer instruction to repair the enumerated public-snapshot CI
blockers on 2026-09-09. Required GitHub checks and normal protected merge remain
mandatory. This does not authorize retrospective certification or admin bypass.

## Decision

The root `f8c6a4a5e75d167829c05f65d5d3033acb193878` begins the public
history. Its ledger SHA256 is
`0858ac904defebd34c5b8fc94d886da802c78d81565aa286747b660dfd7b821f`.
All 51 exact LOC/SLOC caps equal sources in that root. Prior unavailable commits
cannot be ancestors of this new root and remain historical UNVERIFIED evidence.

An audited producer may replace only `baseline_commit` with this public root
for continuously retained entries from that exact ledger. It verifies immutable
source sizes, keeps owner/reason/ADR/target/deadline and exact caps, writes with
HEAD/byte compare-and-swap, and emits original provenance/source digests. This is
an explicit one-time amendment to ADR 0047's provenance transition rules.
Ordinary ancestry validation is unchanged: the public root must be an ancestor
of the evaluated base. No generic missing-history exception exists. New debt,
changed metadata, increasing caps and resurrected retired entries cannot adopt.
The usual tightening, retirement and exact rename rules apply after adoption.

Imported historical CI-shadow contracts remain immutable at the public root.
Current CI validates that continuity plus active/synthetic behavior; a separate
retrospective evaluator must remain UNVERIFIED/nonzero without the original Git
objects. Public-root validation cannot be used as a merge/release historical
receipt. This distinction prevents both false historical PASS and a permanently
unreproducible public source test suite.

## Consequences

Debt does not gain headroom. The migration can be reproduced from public inputs.
Rollback restores the old ledger and its truthful failing ancestry result. The
producer's candidate status is not gate acceptance; commit and exact-head checks
are required. See [recovery specification](../feature-design-public-snapshot-ci-recovery.md)
and [module-size runbook](../module-size-ratchet.md).
