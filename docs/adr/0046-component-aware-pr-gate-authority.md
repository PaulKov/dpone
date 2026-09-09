# ADR 0046: CI shadow is diagnostic and future PR Gate authority is App-bound

## Status

Accepted.

Acceptance is evidenced by the exact-head owner attestation and successful
`Agent PR receipt` for the PR that lands this ADR, plus the merge commit recorded
in [issue #512](https://github.com/PaulKov/dpone/issues/512). The status line does
not by itself prove approval, implementation, or live activation.

## Context

The repository requires the GitHub Actions contexts resolved from the canonical
active branch-protection policy. They are
expensive and do not expose a single component-routing explanation, but they
are the established merge authority. Frozen PR #511 prototyped an Actions job
named `PR Gate`, union/final protection, and apply-capable synchronization. That
prototype is non-authoritative and its authority model conflicts with the
approved closure.

GitHub rulesets can require a check from a specific GitHub App. A check with the
same name from another source does not satisfy an App-bound requirement. GitHub
Actions uses App ID `15368` in the current repository observation, so allowing
Actions to publish the future authority name would weaken the intended source
boundary.

An always-present diagnostic context cannot safely use workflow-level path
filters. Unknown or control-surface paths need a fail-closed full route, and
missing, cancelled, timed-out, or unexpectedly skipped work must remain visible.

## Decision

1. GitHub Actions may publish exactly one job context named
   `PR Gate shadow`.
2. `PR Gate shadow` is always-on for pull requests, has no workflow-level
   `paths`/`paths-ignore`, and remains non-required throughout
   `DPONE-CI-SHADOW-CLOSURE`.
3. A single `if: always()` aggregator evaluates a closed exact-base/head plan.
   `N/A` is allowed only for a job the trusted plan marks non-applicable.
   Missing, failed, cancelled, timed-out, or unexpectedly skipped expected work
   blocks a green shadow decision.
   Every evidence matrix sets `strategy.fail-fast: false`; cancellation or
   timeout of any required cell is incomplete evidence rather than an inherited
   sibling failure.
   The PR-head producer alone publishes this untrusted diagnostic context. The
   read-only default-branch auditor emits a separate receipt and never creates,
   updates, or recolors a PR check; its receipt gates acceptance evidence only.
4. Static policy rejects a second `PR Gate shadow` and rejects any Actions job
   named `PR Gate`.
5. The canonical active required contexts remain the only merge authority. Their
   workflows stay unconditional and each child PR merges without bypass.
6. The name `PR Gate` is reserved for a future separately administered GitHub
   App. Its App ID must be positive and different from `15368`; authority is the
   pair `(context=PR Gate, trusted_app_id)`.
7. The future App, union/final activation, ruleset/classic mutation, repository
   variables, tags, release workflows, and publication are outside this goal.
8. A dormant read-only observation overlay may derive `legacy` from the exact
   canonical active-v1 bytes/digest, model a non-active `union` containing
   `PR Gate shadow`, and a disabled `final` target. It copies no context list,
   creates no second production policy/consumer, and cannot be applied. It must
   be absorbed or removed by any future ADR 0028 atomic migration.
9. Unknown paths, classifier ambiguity, and control-surface changes select the
   full shadow route. Diff, identity, or policy errors fail closed.
10. Pull-request concurrency may cancel superseded heads for the same PR, but
    every attempt remains immutable and concurrency is not a durable lock.

### Authority-count amendment (2026-08-30)

The original accepted decision observed 19 required contexts. The canonical
policy, ruleset `18806829`, and classic protection were re-read on 2026-08-30
and resolved 21 contexts after the approved Windows doctor expansion. The
decision is intentionally count-independent: consumers resolve exact names from
`.agents/policy/github-branch-protection.yml`, and no diagnostic shadow or
exact-SHA receipt replaces any member of that active set.

## Consequences

- Contributors receive one stable diagnostic result without changing what can
  merge.
- A compromised or modified Actions workflow cannot preempt the name intended
  for the future App authority.
- Shadow and legacy work coexist temporarily; cost reduction is measured before
  any later optimisation objective.
- The repository can investigate shadow/legacy disagreement without bypass or
  protection mutation.
- Completing this ADR's implementation does not authorize a cutover. A future
  authority objective needs a real App identity, new repository approval, and
  live readback evidence.
- Frozen #511 checks and artifacts remain historical counterexamples, not
  migration inputs.

## Links

- [CI shadow closure feature design](../feature-design-ci-pr-gate-exact-sha-evidence.md)
- [Tracking and frozen audit ledger](https://github.com/PaulKov/dpone/issues/512)
- [ADR 0025: Quality-gate authority is explicit and process-local](0025-process-local-quality-gate-authority.md)
- [ADR 0028: Frozen release policy and publication boundary](0028-frozen-release-policy-and-publication-boundary.md)
- [ADR 0037: Immutable agent PR merge closure](0037-immutable-agent-pr-merge-closure.md)
- [GitHub ruleset required-check source](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets)
