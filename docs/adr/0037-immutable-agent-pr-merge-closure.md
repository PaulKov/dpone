# ADR 0037: Agent PR merge closure derives from immutable reviewed-head evidence

## Status

Accepted.

Accepted 2026-07-29 after maintainer approval of
[`feature-design-agent-pr-merge-closure-receipt.md`](../feature-design-agent-pr-merge-closure-receipt.md).

### PR 3B execution-envelope amendment

- Amendment status: ACCEPTED
- Approval identity: PaulKov
- Approval date: 2026-08-11

The `PROPOSED` state has no authority while the linked
[PR 3B child specification](../feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md)
is `RESEARCHED`. The only valid approval transition changes the child to
`APPROVED`, this status to `ACCEPTED`, identity to `PaulKov`, and date to
`2026-08-11` on the same exact reviewed commit. It becomes part of this accepted
decision only after that commit passes exact-head governance and merges before
the implementation that consumes it. The implementation cannot approve or edit
this exception itself.

The legacy `.github/workflows/ci.yml` job `quality` receives job-wide
`attestations: write` and `id-token: write` while checking out and executing
PR-controlled repository code. A workflow-envelope hash does not bind that
transitive code. This amendment therefore retires, rather than accepts, the
legacy exception. Its two hashes remain historical negative fixtures:

- the unchanged security projection has SHA-256
  `55b80fe3c773f0313e07288083403e32a8fa002813e96da8c61953c4fede0679`;
- the resolved workflow/job execution envelope at commit
  `518537e616033ecf86bae96d831b8deb23eb8bca` has SHA-256
  `734cd4a8eb1b6105bff06af68e616108ab03f0b0b7f203eab3e66d5a859c9b32`;
- canonicalization is the exact JSON/SHA-256 formula in the linked child; and
- either historical fixture is a deterministic
  `PRIVILEGE_ADR0037_PROFILE_DRIFT`, never PASS authority.

The consuming PR3B implementation atomically performs the replacement. The
existing matrix `quality` job becomes read-only. A single non-matrix,
`contents: read` producer `governance-source` waits for quality, generates and
uploads exactly `agent_governance_gate.json`, and exposes only the immutable
provider artifact ID and digest. A source-free `governance-attestation` job
needs that producer and has exact job permissions `actions: read`,
`attestations: write`, `contents: read`, and `id-token: write`. Its resolved
action-only envelope SHA-256 is
`c5832e92a84ddd4eaa44cbf009d45eb7de2c4a7c3bcfc4258da1702b5aa0b3af`;
the producer transport projection SHA-256 is
`0bf8016384b224f58cd6be67fc892531cfdad21c114b48fb9f0d90a5d3c9f97f`.

The finalizer has exactly two pinned steps: download the same-run artifact by
provider ID with digest mismatch fatal, then attest the exact downloaded JSON
path. It has no checkout, `run`, local/Docker action, `github-script`, setup,
package installation, cache, matrix, service/container, environment, secret,
cross-run token, or subject glob/directory. Artifact content is data and is
never executed. Attestation proves byte provenance, not semantic truth; the
existing receipt validation remains decision authority. The artifact name,
raw JSON subject, 90-day retention, signer workflow path, and receipt schema
remain compatible.

The full normalized workflow trigger mapping and mandatory subject/route
occurrence are part of each PR3B profile. Missing, renamed, copied, duplicate,
branch/path/type drift, removed dependency, or extra matching route is
`PRIVILEGE_ADR0037_PROFILE_DRIFT`. Safe rollback keeps producer jobs read-only
and may disable the finalizer only as emergency containment. That state has no
attestation, keeps the mandatory-profile scanner and integration blocked, and
returns to green only by restoring the exact approved finalizer or merging a
prior approved amendment. Rollback never restores OIDC or attestation authority
to code from the candidate repository.

The merged-closure publisher remains a different post-merge capability. Its
resolved execution-envelope SHA-256 at the same base is
`92c783c76af4f23ccbce06892fd297f73c830120a030b739a8b05055dec406f3`;
its merged-event, exact-checkout, permission, and receipt rules below remain
unchanged.

## Context

The protected branch requires `Agent PR receipt`. Its existing public contract
is correctly bound to the reviewed pull-request head `H`: required checks,
governance artifact content, artifact provenance, body attestations, and the
evidence chain all describe `H`.

After GitHub integrates the pull request, the integration commit `C` needs the
same required check for exact-commit release certification. Re-running the
reviewed-head validator with `C` as `head_sha` is incorrect. Looking up the
associated PR and reading its current body is also incorrect because GitHub
permits body edits after merge. Those approaches conflate two authorities and
allow retrospective evidence to change.

The repository already retains a successful pre-merge `agent-pr-receipt`
artifact for 90 days. GitHub provides provider digest/size metadata, workflow
run identity, and exact downloaded archive bytes. Git provides an immutable
parent/tree relationship between `H` and `C`. These sources are sufficient for
a derived closure without reinterpreting the original receipt.

Review also exposed three false-pass boundaries in the source evidence path:

- rename detection can omit the old control-surface path;
- governance content parses but does not enforce `head_commit`; and
- validation can accept one artifact while the evidence chain records another,
  while incomplete normalized attestation fields can still be labeled `PASS`.

## Decision

### Reviewed-head and integration identities remain separate

`agent_pr_receipt.json` v2 continues to mean reviewed-head receipt and is not
changed. A new closed `agent_pr_merge_receipt.json` v1 records:

- reviewed head `H`;
- first parent/base and integration commit `C`;
- merge/squash method and tree identities;
- exact changed paths;
- immutable source check, workflow run, artifact, archive, and inner-file
  identities; and
- the producer run for the derived closure.

No field called `head_sha` is repurposed to mean `C`.

### Merge closure is automatic and event-bound

The existing workflow also listens for `pull_request: closed`. It runs closure
only when the immutable event says `merged == true`, the canonical protected
base matches frozen policy, and the event merge SHA equals the checked-out
commit.

There is no semantic `workflow_dispatch` backfill from mutable PR metadata.
Safe replay is a rerun of the original closed-event workflow and immutable
source evidence. Missing source evidence is `UNVERIFIED`.

### The exact-commit check is an explicit least-privilege projection

GitHub records the native workflow run and job check for
`pull_request: closed` against reviewed head `H`, even when the job checks out
and validates integration commit `C`. The native job check is therefore not
exact-commit evidence.

After producing and schema-validating the immutable receipt, the merged-only
job creates a same-name `Agent PR receipt` check-run on `C` through the Checks
API. The target SHA comes only from the immutable closed event. Receipt
`PASS` maps to `success`; missing, invalid, failed, or identity-mismatched
receipts map only to `failure`. The projection recomputes the canonical
`binding_id` and requires the receipt producer to equal the current
run/attempt. The returned check name, SHA, conclusion, `external_id`,
check-run ID, GitHub Actions App identity, and bounded `details_url` are
validated. GitHub Actions may preserve the submitted producer-run URL or
normalize it to the provider-owned check-run URL. Only those two exact URLs,
derived from the validated producer run ID or returned check-run ID, are
accepted. The `external_id` is the run/attempt pointer; `details_url` is
corroborating provider metadata rather than the producer identity authority.
Creation is two-phase and fail-closed: the check first exists with conclusion
`failure`; only a matching provider response permits updating that exact
check-run ID to `success`. A failed receipt never executes the success update.
The immutable receipt artifact is uploaded successfully before the success
update is attempted. A failed or ambiguous update is reverted to `failure`;
release verification also requires the producer workflow run to be successful,
so an unconfirmed green response cannot be accepted.

The reviewed-head job remains read-only. Only the merged-closed job receives
job-scoped `checks: write`; it executes code from the already integrated commit
and receives no repository secret.

Release preflight independently re-fetches the latest exact-`C` check, derives
the producer run/attempt from the exact `external_id`, corroborates the
provider-normalized or submitted `details_url`, binds the check/App identity,
and downloads the unique durable receipt
artifact from that run, verifies provider and local digest/size, and validates
the receipt, canonical binding, producer exit code, and preserved source bytes.
A stale projection report is diagnostic only and cannot authorize release.

### The source receipt is selected before its artifact is consumed

Closure selects the latest successful pre-merge `Agent PR receipt` check run on
`H`, completed no later than `merged_at`. It fetches `agent-pr-receipt` through
that exact workflow run ID and requires one non-expired artifact created no
later than merge.

Provider digest and size must equal locally downloaded bytes. The archive must
contain exactly one required source file of each kind. The archived body digest,
head, PR/base identity, paths, receipt status, evidence chain, and audit manifest
must agree with the closed event and `C`.

The current PR body is never an authority after merge.

### Git object identity closes merge and squash

For a two-parent merge, `C^2` must equal `H`. For a one-parent squash, exact tree
equality binds the result to `H`. In both cases:

- `C^1` is an ancestor of `H`;
- `tree(C) == tree(H)`; and
- the rename-disabled exact delta `C^1..C` equals the source receipt path set.

Zero-parent, octopus, mismatched-tree, or unrelated commits fail closed.

### Source evidence validation has one authoritative artifact

Changed paths are enumerated with rename detection disabled so old and new
paths are visible. Governance content must contain `head_commit == H`.

Artifact selection returns the exact validated object. Evidence-chain and audit
projection receive that object and may not rescan the candidate list.

A normalized attestation can be `PASS` only with the expected predicate,
subject digest, exact source repository, exact
`refs/pull/<current PR number>/merge` ref and canonical source Git digest,
signer workflow, GitHub OIDC issuer, positive verified timestamp count, and
GitHub-hosted runner identity. GitHub's source digest for the `pull_request`
workflow is the synthetic merge commit `M`, not reviewed head `H`. The
provider workflow-run head and signed governance content bind `H`; the
attestation retains `M` as provenance without relabeling it.

### Historical absence is not repaired

Historical integration commits without immutable source closure stay
`UNVERIFIED`. They are not upgraded to `PASS`, and a current PR body cannot
repair them. The operator must create a new reviewed corrective commit when
source evidence is unavailable.

This decision is a tactical v1 receipt closure. It does not activate or claim
completion of ADR 0028's external release controller, append-only evidence
store, lease/fencing, immutable-release parity, or Trusted Publisher boundary.

## Consequences

Positive:

- exact release commits can receive the required check without mutable evidence;
- retries are deterministic at the semantic `binding_id`;
- audit evidence preserves both `H` and `C`;
- rename-out and mismatched provenance no longer create false `N/A`/`PASS`;
- existing reviewed-head schemas and consumers remain compatible.

Costs:

- closure depends on retained GitHub artifact availability;
- the workflow performs additional read-only API and archive verification;
- the merged-only job needs narrowly documented `checks: write` permission;
- merge and squash need separate Git-object tests;
- a failed post-merge closure requires another reviewed commit if the source
  artifact cannot be recovered.

Risks:

- GitHub provider semantics remain an external dependency;
- public-fork token downgrades may prevent the exact-commit check projection;
  those commits remain `UNVERIFIED` until a maintainer-owned corrective PR or
  future external controller produces new reviewed evidence;
- the source receipt artifact itself is not separately attested, so closure
  relies on exact workflow/check identity plus provider/local artifact digests;
- this tactical design remains below ADR 0028's future external-controller
  assurance level.

## Rejected alternatives

- Current REST PR body as reviewed authority: mutable after merge.
- `workflow_dispatch --ref master`: branch race and no immutable body event.
- Reusing `agent_pr_receipt.json` with merge SHA as `head_sha`: public semantic
  break and evidence conflation.
- `pull_request_target`: forbidden by repository workflow security policy and
  unnecessary for this read-only post-merge path.
- Treating a missing artifact as permission to reconstruct: false certification.

## Related material

- [Feature design: immutable Agent PR merge-closure receipt](../feature-design-agent-pr-merge-closure-receipt.md)
- [Agent governance](../agent-governance.md)
- [GitHub branch protection](../github-branch-protection.md)
- [Release protocol](../agent-release-protocol.md)
- [ADR 0028: frozen release policy and irreversible publication boundary](0028-frozen-release-policy-and-publication-boundary.md)
