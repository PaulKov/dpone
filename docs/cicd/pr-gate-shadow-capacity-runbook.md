# PR Gate shadow capacity runbook

Use this runbook to inspect the diagnostic-only capacity probe. It never changes
merge authority: the existing required checks remain the only gate.

## Prerequisites

- GitHub CLI authenticated for read access to `PaulKov/dpone`.
- The trusted default branch is `master`; do not run the probe from a PR branch.
- Do not add a personal token, secret, cache, or write permission.

## Dispatch and retrieve

```bash
gh workflow run pr-gate-shadow-capacity.yml --repo PaulKov/dpone --ref master
gh run list --repo PaulKov/dpone --workflow pr-gate-shadow-capacity.yml --branch master --limit 1
gh run download <run-id> --repo PaulKov/dpone --dir capacity-receipt
```

The workflow is expected to be red after a successful upload because the
receipt is always `UNVERIFIED`. `gh run download` is inspection-only: it
extracts the JSON but does not by itself authenticate the provider ZIP digest.
Use the Actions artifact metadata and the supported verifier/API path before
treating an artifact as evidence.

## Inspect the JSON

```bash
jq '{schema, source, decision, code, complete, two_observations_match,
     within_thresholds, limits_crossed, counters, hard_maxima,
     approval_thresholds, valid_until}' \
  capacity-receipt/**/pr-gate-shadow-reconciliation-capacity-*.json
```

Confirm the source workflow path is `.github/workflows/pr-gate-shadow-capacity.yml`,
the source SHA is the dispatched `master` commit, and the receipt has not
expired. Confirm `schema=dpone.ci-shadow-reconciliation-capacity.v2`: V1 is
historical only and cannot qualify the amended prerequisite. The amended request values are 3,000 hard and 1,500 qualifying;
byte and wall thresholds remain 64 MiB and 450 seconds.

## Decision matrix

| Observation | Meaning | Safe action |
| --- | --- | --- |
| `complete=true`, equal snapshots, no crossed limit, counters within threshold | Narrow calibration prerequisite only | Record the immutable evidence; proceed only after the separate fork-lifecycle prerequisite |
| `limits_crossed=["total_http_requests"]` | The value is a saturating lower bound, not an exact total | Retain receipt; do not hand-edit, retry into green, or raise limits without a parent amendment |
| Complete but over threshold | Exact diagnostic result is too costly | Preserve it and seek a reviewed parent decision |
| Unequal or incomplete observations | Provider history was not stable/complete | Dispatch a fresh trusted run; never reuse the first observation |
| Stale receipt | Approval validity is 24 hours | Dispatch a new `master` run |
| No artifact or archive/payload mismatch | Evidence did not persist or authenticate | Fix trusted source/transport and dispatch a new run |

Any source, policy, or acquisition-bundle change invalidates previous calibration
for reconciler approval. It requires a new default-branch dispatch; never upload
or alter a replacement receipt manually.

See the [capacity overview](pr-gate-shadow-capacity.md),
[PR Gate shadow overview](pr-gate-shadow.md), and
[PR Gate shadow runbook](pr-gate-shadow-runbook.md).
