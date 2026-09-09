# Agent PR merge-receipt runbook

Use this runbook when `Agent PR receipt` is missing or failing on an exact
integration or release commit. The workflow has two evidence stages:

- the edited-PR run records reviewed head `H`, the reviewed body snapshot,
  exact changed paths, required checks, and governance evidence;
- the merged-closed run derives an immutable closure from `H` to integration
  commit `C`.

`H` and `C` are intentionally different identities. The closure never treats
the current, editable PR body as reviewed evidence.

## Discover

Set a frozen tag or an explicit full commit and resolve it once. GitHub binds
the native closed-event workflow run to reviewed head `H`, so discover closure
through the explicit exact-commit check on `C`, not through workflow-run
`headSha`. Do not derive the candidate from a mutable branch checkout:

```bash
repository=PaulKov/dpone
release_ref="vX.Y.Z"
integration_commit="$(git rev-parse "${release_ref}^{commit}")"
test "$(printf '%s' "${integration_commit}" | wc -c | tr -d ' ')" = 40

evidence_dir="test_artifacts/agent-policy/merge-closure-${integration_commit}"
required_check_report="${evidence_dir}/exact_commit_checks.json"
mkdir -p "${evidence_dir}"
uv run python tools/agent_policy/release_commit_gate.py \
  --repo "${repository}" \
  --commit-sha "${integration_commit}" \
  --policy .agents/policy/github-branch-protection.yml \
  --github-token-env GITHUB_TOKEN \
  --timeout-seconds 300 \
  --poll-interval-seconds 15 \
  > "${required_check_report}"
uv run python tools/agent_policy/release_merge_receipt_gate.py \
  --repo "${repository}" \
  --commit-sha "${integration_commit}" \
  --github-token-env GITHUB_TOKEN \
  --required-check-report "${required_check_report}" \
  --output "${evidence_dir}/exact_commit_merge_receipt.json"
run_id="$(jq -r '.workflow_run_id' "${evidence_dir}/exact_commit_merge_receipt.json")"
```

A missing/non-success exact-commit check, an ambiguous check selection, or a
check whose `external_id` does not identify its closed-event producer
run/attempt is classified as `UNVERIFIED` by the release review and blocks
release. GitHub Actions may preserve the submitted Actions run URL or normalize
`details_url` to `/runs/<check-id>`; the gate accepts only those two exact,
identity-derived values. The
merge-receipt JSON itself has the closed machine contract `PASS`/`FAIL`;
unavailable evidence therefore writes `FAIL`, while the surrounding release
verdict records `UNVERIFIED`.

## Prepare

You need:

- `gh` authenticated with read access to Actions, checks, attestations, and
  repository contents;
- the canonical repository and full 40-character integration SHA;
- the original pre-merge `agent-pr-receipt` artifact, retained for 90 days;
- the checked-in branch policy from the exact integration commit.

Never pass a token as a CLI argument, write it to an evidence file, or print it.

## Execute and observe

The normal path is automatic: merging or squashing a PR into a protected branch
emits `pull_request: closed`, checks out `C`, and creates the closure. There is
no manual semantic backfill.

When two overlapping PRs close on the same merge commit, GitHub can mark the
inner PR as merged because its reviewed head is already an ancestor of the
directly merged PR head. Before the check publisher performs any Checks API
write, it classifies ownership from the immutable Git graph. Only the PR whose
reviewed head is the exact second parent (or exact squash tree) may publish the
integration check. The transitively included run retains its failed derivation
artifact and writes an `N/A` projection diagnostic, but does not publish or
replace an exact-commit check. A reviewed head that is neither the direct owner
nor a proven ancestor fails closed without a target check write.

The gate derives the producer run/attempt from `external_id`, corroborates the
exact submitted or provider-normalized `details_url`, and binds the GitHub
Actions App, provider digest, downloaded bytes, and retained receipt before
returning `PASS`. The producer run may report `headSha == H`; the retained
receipt and projected check are the authoritative `C` binding. Download the
two artifacts only for inspection:

```bash
gh run download "${run_id}" \
  --repo "${repository}" \
  --name agent-pr-receipt \
  --dir "${evidence_dir}/receipt"
gh run download "${run_id}" \
  --repo "${repository}" \
  --name agent-pr-merge-check \
  --dir "${evidence_dir}/projection"

jq -e \
  --arg commit "${integration_commit}" \
  '.status == "PASS" and .integration_commit_sha == $commit' \
  "${evidence_dir}/receipt/agent_pr_merge_receipt.json"
jq -e \
  --arg commit "${integration_commit}" \
  '.status == "PASS"
   and .integration_commit_sha == $commit
   and .projected_conclusion == "success"' \
  "${evidence_dir}/projection/agent_pr_merge_check.json"
```

The durable `agent-pr-receipt` artifact is uploaded before a check may become
successful and contains:

- `agent_pr_merge_receipt.json`, validated by
  `evals/agent/pr-merge-receipt.schema.json`;
- `source-agent-pr-receipt.zip`, byte-identical to the selected pre-merge
  artifact;
- `pr-receipt-exit-code.txt`.

The separate `agent-pr-merge-check` artifact contains
`agent_pr_merge_check.json`, the credential-free projection diagnostic. Release
acceptance trusts the live provider identity plus the durable receipt gate, not
a previously downloaded projection report.

The JSON records `H`, `C`, merge method, trees, first parent, changed paths,
source check/run/artifact IDs, provider and local archive digests and sizes,
inner receipt/body/audit digests, and producer run identity.

## Diagnose

| Failure | Meaning | Safe action |
| --- | --- | --- |
| A successful direct receipt is followed by a failure from an overlapping PR | Legacy workflow allowed a transitively included PR to publish against the shared merge SHA. | Apply the integration-owner classifier fix through a new reviewed PR. Do not rerun or manually replace the existing check. |
| No eligible source check | No successful edited-PR receipt completed before merge. | Open a corrective reviewed PR; do not reconstruct a PASS. |
| Source artifact missing or expired | Immutable reviewed evidence is unavailable. | Treat the commit as `UNVERIFIED`; release a newer reviewed commit. |
| Body digest mismatch | Closed-event snapshot and immutable source disagree. | Stop; do not query or copy the current PR body. |
| Path mismatch | Source paths differ from exact rename-disabled `C^1..C`. | Inspect merge identity and rename/delete handling; do not edit evidence. |
| Parent or tree mismatch | `C` does not contain exactly the reviewed tree `H`. | Use a new reviewed PR/commit with a supported merge or squash result. |
| Artifact or attestation mismatch | Provider metadata, downloaded bytes, governance JSON, or provenance disagree. | Preserve the failure receipt and investigate the exact source run. |

Inspect the failed step and its retained failure receipt without exposing the
token:

```bash
gh run view "${run_id}" --repo "${repository}" --log-failed
failure_dir="test_artifacts/agent-policy/merge-closure-failure-${run_id}"
gh run download "${run_id}" \
  --repo "${repository}" \
  --name agent-pr-receipt \
  --dir "${failure_dir}"
jq '.errors' "${failure_dir}/agent_pr_merge_receipt.json"
```

## Recover

Rerun the original closed-event workflow only for a transient GitHub API,
timeout, or partial-download failure and only while the exact source artifact
still exists:

```bash
gh run rerun "${run_id}" --repo "${repository}"
gh run watch "${run_id}" --repo "${repository}" --exit-status
```

GitHub rerun preserves the original event payload. The derivation selects only
successful pre-merge evidence completed no later than `merged_at`, so a retry
cannot substitute a post-merge body edit or later receipt.

Do not rerun a deterministic body, path, tree, parent, provenance, provider
identity, or policy mismatch expecting a different verdict. Those failures,
and deleted or expired source evidence, require a new reviewed PR and a new
integration commit. Never manufacture historical `PASS` evidence. If an
immutable version was already tagged or published, increment the version
instead of moving or rewriting it.

## Operate and upgrade

Keep both source and closure artifacts for the configured 90 days and record
their run IDs and SHA-256 digests in release evidence. Alert before evidence
needed by an unreleased candidate reaches retention expiry. Historical receipts
are not migrated to newer schemas and are never upgraded from `UNVERIFIED` to
`PASS`.

See also [Agent governance](agent-governance.md),
[GitHub branch protection](github-branch-protection.md), and
[Release evidence](release-evidence.md).
