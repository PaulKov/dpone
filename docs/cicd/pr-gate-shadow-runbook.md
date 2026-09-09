# PR Gate shadow runbook

`PR Gate shadow` is diagnostic-only. It must never be substituted for the
required contexts resolved from the canonical active policy or used as merge
authorization. `PR Gate` is reserved for a future separately administered App;
it is not the name of the current required checks.

## Reading a result

The plan is calculated from the pull request's base SHA (`B`), head SHA (`H`)
and GitHub's merge SHA (`M`). Product jobs check out `H`; a result belongs only
to the recorded run id and attempt. A source push changes `H` and creates a
new diagnostic subject. Rerunning an unchanged head only creates a new attempt.

`N/A` means the closed route policy did not select that product. `UNVERIFIED`
means a selected product was missing, cancelled, skipped, timed out, nonterminal
or ambiguous. It is not a pass and cannot be repaired by editing an artifact.

## Common recovery actions

| Symptom | Meaning | Safe action |
| --- | --- | --- |
| Unknown path selects all products | The classifier could not prove a narrower route | Let the full diagnostic run finish; add a reviewed route rule only in a later source change. |
| Plan fails on `B`, `H` or `M` | The exact identity or diff was unavailable | Push a new head or rerun after GitHub has refreshed the PR; do not reuse an old claim. |
| Product is cancelled or missing | The attempt does not have complete evidence | Rerun the workflow or push a new head. Treat the old claim as `UNVERIFIED`. |
| `PR Gate shadow` is red | A diagnostic product failed | Investigate its named product job; the legacy required checks remain the merge authority. |

## Recovering immutable evidence

Open the exact Actions run and download only the artifact whose name includes
that run id and attempt. Confirm its plan digest, `B`, `H`, `M`, repository id
and PR number before comparing it with another run. Artifacts are create-only;
never overwrite, amend, or manually create an artifact to make a result pass.

If provider history or an artifact is unavailable, record the result as
`UNVERIFIED`. The merged PR4B auditor, not this producer, establishes independent
trust in a claim.

## Trusted-auditor receipt

The default-branch auditor is diagnostic-only and does not change the canonical
active required contexts. It writes
`pr-gate-shadow-audit-<producer-run>-<producer-attempt>-<auditor-run>-<auditor-attempt>.json`
as a create-only 90-day artifact. `AUDITOR_MERGE_REF_UNVERIFIED` means GitHub's
current PR merge ref, its ordered `(B,H)` parents, or the complete eligible PR
set could not be proved stable. Do not rerun an old producer identity after a
head/ref movement: push a new head and let GitHub create a new producer run.

`AUDITOR_RUN_UNVERIFIED`, `AUDITOR_CLAIMS_UNVERIFIED`, and
`AUDITOR_JOBS_UNVERIFIED` respectively identify an exact-run API mismatch,
missing or malformed claims, and an incomplete or disagreeing exact-attempt
Jobs population. Each is intentionally a persisted `UNVERIFIED` result, not a
successful diagnostic fallback.

An incoming `workflow_run` event with `action_required` is deliberately
receipt-free telemetry. Approval lifecycle continuity is not assumed; only a
separately authenticated completed producer attempt may be audited.

The auditor reads the exact producer-run record and the complete Jobs API list
for that run attempt. It derives product outcomes from the provider's terminal
job conclusions; a `PASS` value embedded in producer claims is never enough.
Missing, duplicate, cancelled, or disagreeing job rows produce `UNVERIFIED`.

Before a receipt can pass, the default-branch bundle manifest's SHA-256 values
must match every trusted file fetched from immutable Git objects at `H`; the
producer workflow must also be the same regular blob in `B`, `H`, and `M`.
Changing a classifier, policy, audit helper, or workflow byte therefore yields
`AUDITOR_BUNDLE_UNAPPROVED`, even if producer claims retain an old digest.
