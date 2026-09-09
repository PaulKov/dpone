# ADR 0047: Module-size debt is governed by exact non-regression caps

## Status

Accepted.

## Approval evidence

Repository acceptance is completed only when the implementing pull request
records the maintainer's exact-head attestation and merges without bypass.

## Context

The repository-wide module-size gate has absolute limits of 600 LOC and 400
SLOC, but its version-1 baseline treats every listed path as fully exempt.  A
listed module can therefore grow without detection, and the baseline cannot
prove whether a recorded allowance matches the debt that was actually reviewed.

At the migration base
`e1d93822b47234e940829319cac0dc9678f6906c`, 69 modules exceed at least one
warning threshold.  One of them also exceeds the absolute 400-SLOC limit and
must be split before the ratchet is enabled.  Requiring all warning-level debt
to disappear in the same change would mix unrelated refactors into the CI
repair; preserving it without exact caps would weaken the gate.

## Decision

1. `docs/module_size_baseline.json` uses a closed version-2 schema.  Each debt
   entry is keyed by its canonical repository-relative Python path and records
   its exact reviewed `max_lines` and `max_sloc`, owner, reason, reduction
   target, target date, accepted ADR, and full lowercase `baseline_commit`.
2. Caps have no headroom.  A module above either recorded value fails.  A module
   above an absolute project limit fails even when it has a baseline entry.
3. New warning-level debt, stale entries, deleted paths that retain entries,
   missing history, invalid ancestry, duplicate keys, unknown fields, unsafe
   paths, symlinks, and malformed values fail closed.
   Warning thresholds come from the immutable exact-head
   `docs/benchmarks/quality_budgets.yml`; callers may make them stricter but
   cannot raise them to hide debt.
4. An improvement must be persisted by lowering the exact cap.  Once both
   warning thresholds are met, the entry must be removed.  The writer may only
   tighten or remove existing debt after the one-time migration.
5. `accepted_adr: null` is allowed only for entries migrated from the exact
   `e1d93822b47234e940829319cac0dc9678f6906c` baseline.  Those grandfathered
   entries have a target date of `2026-12-31`.  New debt requires an Accepted
   ADR whose commit is an ancestor of the evaluated head. If the pull-request
   base advances before this one-time migration merges, continuity remains
   valid only when that exact base retains the legacy v1 ledger, the audited
   commit is its ancestor, and every null-ADR cap is no larger than the
   corresponding module measured at both the audited commit and the exact
   descendant base. Missing ledgers, rewritten provenance, regrowth over the
   immediate base, and new warning debt still fail closed.
6. The target date is operational: an entry passes through that UTC date and
   blocks the gate on the next day if it remains unresolved.
7. A rename can carry the same or lower caps only in a rename-only change with
   exactly one old and one new path.  No persistent rename mapping is kept.  A
   split removes the old entry; every resulting module is evaluated
   independently and does not inherit the old allowance.
8. Authoritative evaluation requires explicit, distinct full base and head
   commit SHAs. The comparison uses the checked-out repository history, loads the baseline,
   budgets, ADRs, and Python sources from one immutable exact-head snapshot,
   and fails when either identity or the required ancestry cannot be proven.
   The unconditional compatibility codec may create a missing parent directory,
   but exact-byte compare-and-swap authoring and post-replacement canonical
   verification are read-only with respect to parent namespaces. A missing,
   replaced, or symlinked parent fails without recreating it, certifying the
   candidate, or overwriting concurrent bytes.
9. The legacy five-field `ModuleSizeBaselineEntry` and v1 load/write Python
   helpers remain import-compatible for downstream tooling. They do not feed
   the v2 CI authority; new governance code uses the v2 debt model and codec.
10. The report envelope declares `debt_model` so legacy-v1, ratchet-v2, and
    no-baseline results cannot be confused. The public analyzer retains its
    historical advisory-warning default; the authoritative service opts into
    fail-closed warning debt explicitly. `--no-baseline` is forbidden for
    canonical `src/dpone` and remains a scoped hard-budget check elsewhere.
11. Manual CI resolves a real comparison commit from the default branch and,
    when dispatched on the default-branch head, its parent. It never substitutes
    the evaluated head as its own comparison base merely because the event is a
    manual dispatch.

## Clean source-root continuity

After the approved source-history replacement, the parentless root
`f8c6a4a5e75d167829c05f65d5d3033acb193878` contains 51 remaining entries
with exact source caps but unavailable original ancestry. A repository-specific
producer may reanchor only this frozen cohort to that root. It verifies the
root identity, baseline and budget digests, original provenance, and exact
root source measurements. It may tighten or retire debt but cannot introduce
entries, increase caps, alter metadata or extend deadlines. The verifier
normalizes only the prior provenance field for that exact root comparison;
all ordinary ancestry, deadline, metadata and no-growth checks remain active.

This exception does not permit arbitrary rewritten history, a different root,
changed budgets, missing objects, or self-comparison. A root-only CI dispatch
cannot certify continuity; the first reviewed successor provides the distinct
head. Candidate writing returns a non-certifying outcome and requires a
committed exact-head verification. The v2 public schema and legacy bootstrap
remain unchanged. See the [migration runbook](../module-size-ratchet.md#clean-root-provenance-migration).

## Consequences

- Existing warning debt remains visible and cannot grow.
- Improvements monotonically tighten the baseline instead of creating hidden
  capacity for later growth.
- The migration is deterministic and auditable against one historical commit.
- Renames and splits cannot multiply an allowance.
- The baseline is a public quality-policy contract; schema changes require an
  ADR and migration guidance.
- The gate depends on sufficient Git history.  Shallow CI checkouts must fetch
  the explicit comparison commits before evaluation.

## Links

- Tracking issue: <https://github.com/PaulKov/dpone/issues/512>
- Grandfathered-debt burn-down: <https://github.com/PaulKov/dpone/issues/520>
- Quality budgets: `docs/benchmarks/quality_budgets.yml`
- Quality tooling: `docs/quality-tooling.md`
- Self-service runbook: `docs/module-size-ratchet.md`
- Baseline v2 schema: `docs/schemas/quality/module-size-baseline-v2.schema.json`
- Analysis: `src/dpone/metrics/module_size.py`
- Baseline codec: `src/dpone/metrics/module_size_baseline.py`
- Git continuity policy: `src/dpone/metrics/module_size_policy.py`
