# ADR 0028: Frozen release policy and irreversible publication boundary

## Status

Accepted.

Accepted 2026-07-19 after independent architect `APPROVE_SPEC` and maintainer
approval of the linked feature design. Live provider, controller, evidence-store,
and Trusted Publisher inventories remain `UNVERIFIED` activation blockers; they
do not reopen this decision.

## Context

The v0.73.1 release remediation introduced exact annotated-tag, package,
candidate, required-check producer, attestation, and PyPI byte checks. The fresh
global review identified connected authority and ordering gaps:

- SS-45: ~~the exact-commit gate reads policy from the mutable worktree and does
  not bind the committed policy digest~~ — local tactical fix on this branch:
  frozen `git show` load + `policy_sha256` evidence; full v2 policy migration
  still belongs to this ADR;
- SS-46: live comparison omits the complete ruleset target, conditions, bypass
  actors, ruleset version, and workflow producer/run identity;
- SS-47: jobs holding attestation, OIDC, signing-key, or repository-write
  authority execute checked-out repository code;
- SS-48: ~~annotated-tag identity is checked before a potentially long build but
  not immediately before each external public commit~~ — local tactical fix on
  this branch: GitHub API tag recheck before PyPI and `release_identity_gate`
  before GitHub Release; immutable-tag ruleset and privileged-job redesign still
  belong to this ADR; and
- SS-51: a caller-selected protected base can make ancestry verification
  tautological.

The first architecture proposal addressed those findings directionally but did
not pass independent review. SS-68 found that it still:

- replaced a shared branch-governance policy without atomically migrating PR,
  drift, setup, package-release, runtime-image, schema, and test consumers;
- used two point-in-time reads without bounded internal stability, a closure
  snapshot, or one coherent protected-base advancement rule;
- left the privileged composition graph in candidate-controlled workflow code;
- staged provider state before publication serialization and treated runner
  concurrency as if it were a durable lock;
- omitted queue, attempt, candidate, authorization, recovery, cancellation,
  lease, and fencing identities;
- lacked a schema-complete append-only receipt envelope, producer union,
  durable store, retention, and incident hold;
- called pre-publication authorization `PASS`, allowed token publication into
  the v2 route, and did not require per-file PyPI Integrity API verification;
  and
- staged the GitHub draft before an exact verified public-bundle handoff and
  did not state the lifecycle of every existing supply-chain artifact.

PyPI accepts release files one at a time. A workflow can therefore stop after a
strict subset is public, and accepted files cannot be treated as a reversible
transaction. GitHub immutable releases protect assets and their associated tag
only after a draft is published. The release workflow must expose these
provider boundaries rather than summarize them as one atomic publish.

The current v1 policy contains a branch ruleset ID, but this review did not use
an approved live-administration credential to prove its current ruleset
version, complete live state, producer IDs, or workflow identities. No tag
ruleset ID/version ID is available. Repository immutable-release parity and
PyPI Trusted Publisher parity are also `UNVERIFIED`. The external controller,
GitHub App, evidence store, retention enforcement, and provider release IDs do
not exist as verified design inputs. These unknowns block activation and live
publication, not review of this decision.

### SS-68 revision closure (2026-07-19)

A docs-only revision of
[Feature design: frozen release policy and privileged publication boundary](../feature-design-release-trust-boundary-v0732.md)
addresses each SS-68 rejection reason. The mapping below is normative for the
Accepted decision; it does not claim live provider parity.

| SS-68 gap | Decision response |
|---|---|
| Shared policy migration without atomic consumer cutover | One in-place `dpone.github-governance-policy.v2` at `.agents/policy/github-branch-protection.yml`; one parser; branch and release views; all listed consumers migrate in one integrator commit with full rollback on failure |
| Missing final snapshot and contradictory protected-base rules | Bounded stable snapshots A, B, and closure C; two reads ≥5s apart; start/end identity; single fast-forward rule `base_A ≤ base_B ≤ base_C` with release commit reachable from every base |
| Privileged graph in candidate-controlled workflow | Externally administered, revision-pinned controller is the only privileged composition root; candidate `release.yml` is unprivileged input only; mutation jobs are action-only without checkout or repository code |
| Serialization after provider mutation; runner concurrency as lock | Durable append-only store lease CAS and fencing token before first controlled mutation; GitHub Actions queue is advisory admission only; domain-separated queue, attempt, recovery, cancellation, and lease identities |
| Incomplete append-only evidence | Closed `dpone.release-receipt-envelope.v2` envelope, producer union, payload union, transactional chain append, retention, and incident hold outside the candidate repository |
| Pre-publication `PASS`; token path into v2; missing Integrity API closure | Pre-publication state is `AUTHORIZED` only; `PASS`/`GO` only at `CLOSED`; API-token bootstrap is permanently `LEGACY_UNVERIFIED`; each PyPI file requires cryptographically verified Integrity API provenance |
| Draft before verified public bundle; unstated supply-chain artifact lifecycle | `PUBLIC_BUNDLE_VERIFIED` precedes draft staging; immutable GitHub publication follows PyPI exact closure; every current supply-chain artifact has an explicit preserve/deprecate/disposition |

**Fact:** SS-45 and SS-48 local tactical remediations on this branch remain
accurate interim controls (frozen policy bytes; pre-publication tag recheck).
**Inference:** they do not substitute for SS-46, SS-47, or the full boundary
above.

**SS-46 and SS-47 after this revision:** the specification for both findings
is complete within this ADR and the linked feature design. With this ADR
Accepted and the feature `APPROVED`, implementation proceeds as one coordinated
cutover with policy migration, external controller provisioning, and evidence
store setup. Isolated workflow patches in the candidate repository cannot close
SS-46 or SS-47 without violating the external-controller and lease-before-mutation
contract. Live inventories remain required before activation and public mutation.

## Decision

### One unified policy migrates atomically and frozen bytes are authority

Adopt `dpone.github-governance-policy.v2` at the existing fixed path
`.agents/policy/github-branch-protection.yml`. It preserves branch-governance
semantics and adds release-trust fields. One strict parser returns immutable
`BranchGovernancePolicyView` and `ReleaseTrustPolicyView` projections; the
release view reuses, rather than copies, the branch ruleset and check objects.

The policy, schema, parser, and every semantic consumer move in one integrator
commit:

- branch validator, setup checks, governance gate, and check selector;
- PR receipt and GitHub-settings drift tools/workflows;
- package release and runtime-image release gates/workflows; and
- branch/drift/release/Airflow-matrix plus workflow/setup tests.

No second policy file, direct YAML field reader, copied context list, active v1
parser fallback, or partial consumer migration is allowed. Any failing consumer
aborts and rolls back the entire cutover.

For release commit `C`, policy bytes are read only as
`git show C:.agents/policy/github-branch-protection.yml`. Their exact SHA-256
is evidence. Worktree bytes, caller paths, environment defaults, or fallback
policy are never authority. Compatibility arguments may only assert equality
with the frozen view.

Policy v2 owns target repository numeric identity, canonical remote/base,
branch and tag ruleset IDs/versions/security projections, exact check producer
identity, candidate workflow, external controller identity, immutable-release
requirement, publication-control bounds, four PyPI projects and one exact
controller Trusted Publisher, append-only store identity, and retention.
Placeholders are invalid in active policy. Every absent provider-assigned value
remains `UNVERIFIED` until approved live evidence supplies it.

### Governance snapshots A, B, and closure C are bounded and stable

The controller captures:

1. A after external admission and before accepting a candidate;
2. B after the verified public bundle is staged on the exact draft and
   immediately before authorization; and
3. closure C after exact PyPI/GitHub observation and immediately before final
   closure.

Each A/B/C attempt performs two independent complete provider reads at least
five seconds apart. It records start/end times, read count, pagination,
repository/tag/base/check/ruleset/immutable state, and canonical digest. Start
and end projections, including protected-base SHA, must match. At most three
attempts and 180 seconds are allowed per snapshot; instability or incomplete
pagination is `UNVERIFIED`.

Non-base governance, tag object/peeled commit, and required-check identities
must equal policy and A across A/B/C. Protected-base SHA may advance only
between snapshots, with `base_A <= base_B <= base_C` under the Git
ancestor-or-equal relation, and the release commit must remain an ancestor of
every base.
A rewind, sideways replacement, changed remote/ref, or unreachable release
commit blocks closure. This single rule replaces simultaneous equality and
advancement requirements.

Legacy statuses, hidden bypass actors, null producer IDs, unknown rule fields,
cache substitution, API failure, or pagination truncation cannot satisfy a
snapshot. GitHub currently requires repository `Administration: write` for
ruleset history/version reads. A full-SHA-pinned external reader receives that
technically write-capable permission but exposes no mutation operation.

### An external controller owns privilege and a fenced lease precedes mutation

The only privileged composition root is a separately administered,
revision-pinned controller repository/workflow named exactly in frozen policy.
The candidate repository's `.github/workflows/release.yml` remains an
unprivileged tag-push candidate producer and cannot dispatch, select, or provide
code to the controller.

The normative authority split is:

| Job class | Exact authority |
|---|---|
| candidate build/import | target `contents/actions: read`; no publication, attestation, administration, or OIDC write |
| governance A/B/C | target metadata/contents/actions/checks/statuses read plus isolated ruleset-history `administration: write`; no provider mutation |
| queue/lease/receipt | append-only store CAS/append only; no GitHub/PyPI mutation |
| attestation | controller `id-token: write`, `attestations: write`, `contents: read`; action-only |
| draft stage / GitHub publish | target `contents: write` only; separate action-only jobs |
| PyPI publish | controller `id-token: write` for the exact `pypi` environment/audience only |
| observers/closure | provider read plus store append; no provider mutation |

No row inherits workflow-level write permissions. Privileged jobs use only
reviewed full-SHA actions and immutable IDs; they have no candidate checkout,
candidate action/container, package installation, cache, repository script, or
arbitrary shell.

The append-only store assigns a queue sequence and permits at most 32 pending
attempts per repository/tag lease key. GitHub Actions queueing is advisory
runner admission, not ordering or locking authority. Before the first
attestation, draft, PyPI, or GitHub Release mutation, the selected attempt
acquires a 300-second lease by compare-and-swap; it renews at least every 60
seconds. Acquisition increments a fencing token and appends the lease receipt
atomically.

Each mutation requires an active attempt-specific authorization, current
fencing token, append-only one-use intent, and a short-lived capability minted
after that check. A stale attempt receives no new capability. Provider APIs do
not understand dpone fencing, so a call already accepted at lease loss is
observed and reconciled rather than assumed rolled back. Candidate artifact
upload may precede the lease because it is unprivileged input preparation and
cannot mutate attestations, drafts, tags, releases, or PyPI.

Release, frozen-authority, queue entry, attempt, candidate, authorization,
recovery, cancellation, lease, and receipt IDs are domain-separated canonical
SHA-256 identities. Following ADR 0011, semantic release identity excludes
source commit; the separate authority identity binds tag-object SHA, peeled
commit SHA, policy digest, and protected-base ref. Retry uses a new attempt and
fencing token but the same authority/candidate/recovery identities.
Cancellation appends an event before renewal stops; after any external commit
it becomes `RECOVERY_REQUIRED`.

### A verified public bundle precedes draft staging

The controller imports the eight-file candidate by exact workflow run/attempt,
artifact ID, and provider digest, then recomputes the closed bounded inventory.
The unprivileged candidate job generates supplemental SBOM/provenance without a
signing secret; those files remain public context, not authorization.
The action-only attestation job signs those exact subjects. A separate
unprivileged verifier checks subject bytes, the external controller's signer
repository/workflow/ref/full SHA, predicate, and receipt. It separately proves
that the authenticated append-only handoff binds the target authority ID, tag
object, peeled commit, and policy digest.

Only then does it create `release-public-bundle.v2`: a canonical closed
manifest of eight distributions, checksums, candidate inventory, retained
SBOM/provenance/attestation JSON, release-body digest, exact GitHub attestation
receipts, prior receipt IDs, and expected public asset names. Signature and
timestamp bytes need not reproduce across newly generated attestations; one
selected attestation set is deterministic by immutable bytes and canonical
manifest digest.

An independent verifier downloads by exact artifact ID/provider digest,
rejects extras/missing members, recomputes all bytes, and appends
`PUBLIC_BUNDLE_VERIFIED`. Draft staging is downstream of that receipt and the
active fenced lease. Globs, name-only artifact lookup, regenerated evidence, or
post-verification assets are forbidden.

### Tags and immutable GitHub Releases use provider-exact semantics

An active tag ruleset protects canonical `refs/tags/v*.*.*` tags from update
and deletion with an exact approved bypass set. The controller rechecks the
annotated tag object and peeled commit immediately before PyPI upload and
GitHub publication. A stale preflight receipt is insufficient.

The draft is created only for an existing tag. GitHub ignores
`target_commitish` when that tag exists, so Release metadata never substitutes
for Git object/ref evidence. Before publication, the exact draft release ID,
body digest, and every unique asset ID/name/size/`state: uploaded`/provider
`sha256:` digest must match the public-bundle manifest.

The same draft is published only after PyPI closes exact. No later asset
operation is allowed. Closure requires the same release ID/tag,
`draft: false`, `prerelease: false`, `immutable: true`, exact assets/body,
unchanged tag identity, successful immutable-release verification, and
verification of each local uploaded asset. GitHub-generated source archives
are not uploaded assets and are excluded.

An immutable release locks uploaded assets and its tag while the release
exists. GitHub can still delete the release; afterward the tag may be deleted
but its name cannot be reused. This decision does not claim the release object
is undeletable. Existing releases remain `LEGACY_UNVERIFIED`.

### External publication is a provenance-closed state machine

The ordered states are:

```text
QUEUED
  -> GOVERNANCE_A
  -> CANDIDATE_HANDOFF
  -> LEASE_ACQUIRED
  -> ATTESTED
  -> PUBLIC_BUNDLE_VERIFIED
  -> DRAFT_STAGED
  -> GOVERNANCE_B
  -> AUTHORIZED
  -> PYPI_PUBLISHING
  -> PYPI_PARTIAL_EXACT | PYPI_VERIFIED
  -> DRAFT_VERIFIED
  -> GITHUB_IMMUTABLE_PUBLISHED
  -> GOVERNANCE_C
  -> CLOSED / PASS / GO
```

`AUTHORIZED` is the only positive pre-publication state. It is bound to
attempt, candidate, public bundle, A/B, lease, and fencing token; it is neither
`PASS` nor `GO`.

Each PyPI file is an irreversible external commit. Before upload it is absent,
exact with acceptable provenance, or conflict. Only absent files from the
original candidate enter a sealed subset; duplicate skipping is disabled. The
action-only publisher uses the exact direct external-controller Trusted
Publisher for all four projects. API tokens do not enter v2.

After every upload observation, closure downloads and hashes the file, queries
`GET /integrity/<project>/<version>/<filename>/provenance`, and
cryptographically verifies certificate/signature, single-file subject/digest,
PyPI publish-attestation predicate, and exact controller publisher claims.
Presence of provenance JSON is not verification. Token-bootstrap bytes are
permanently `LEGACY_UNVERIFIED`, even when their hashes match.

An exact partial result activates the stable recovery ID and resumes only from
the original candidate under a new attempt/lease/authorization. Digest,
yanked-state, duplicate, or publisher/provenance conflict places an incident
hold and stops the version. Automatic deletion or overwrite is forbidden.

### Append-only evidence closes after snapshot C

Every transition uses a closed `dpone.release-receipt-envelope.v2` with
content-addressed receipt ID, release stream/sequence/previous digest,
scope-tagged identities, attempt, lease/fencing token, timestamps, a closed
producer union, closed payload union, and payload digest. The payload union
includes one-use `MUTATION_INTENT` receipts (lease ID, fencing token, mutation
kind, subject identity digest, attempt ID) that must precede short-lived
provider capabilities. Producers are exactly external controller jobs, trusted
controller services, or authenticated maintainer hold actions. Candidate jobs
can emit only candidate handoff receipts.

The authoritative ledger is outside the candidate repository and provides
transactional chain append, lease CAS, idempotent same-byte replay,
read-after-write verification, versioning/WORM, encryption, and audit logging.
GitHub Actions artifacts and public release assets are transport/mirrors.
Pre-mutation failures are retained at least 365 days; any external commit,
recovery, cancellation after commit, closure, and required candidate or public
bundle bytes are retained at least 2,557 days.

An append-only incident `HOLD` blocks garbage collection. Releasing it appends a
distinct event and never erases history or shortens baseline retention.

`dpone.release-evidence.v2` writes `status: PASS`, `decision: GO`, and
`state: CLOSED` only after stable C, exact A/B/C governance and base ancestry,
all eight cryptographically provenance-verified PyPI files, exact immutable
GitHub release integrity, unchanged tag/policy/candidate/public bundle, active
fencing token, and the complete receipt chain. Partial, stale, inaccessible,
skipped, or unattributable evidence is `FAIL`, `SKIP`, or `UNVERIFIED`, never
`PASS`.

## Compatibility and migration

- Canonical tags, four package names, exact eight-file candidate set, and the
  candidate repository's tag-push entrypoint remain stable. That entrypoint
  becomes unprivileged input production only.
- Unified policy v2 activates only with its parser, schema, every current
  consumer, fixtures, and workflows in one commit. There is no active v1 policy
  fallback or duplicate release policy.
- Existing authority-like CLI arguments become equality-only deprecated
  assertions for one minor release.
- Evidence v1 remains an explicit diagnostic projection for one minor release;
  it cannot authorize publication.
- Existing `release-preflight`, candidate check reports, attestation artifact,
  and PyPI smoke report remain diagnostic mirrors for one minor release.
  Those names record the v0.73.2 baseline. Starting in v0.74, release audit and
  diagnostic uploads use `<base>-<run-id>-<attempt>`, while immutable
  `release-candidates` and `release-supply-chain-local` retain fixed names as
  no-rebuild tripwires. Downstream jobs consume the exact producer artifact ID
  plus provider digest rather than selecting by name.
- Eight distributions, checksums, SPDX/CycloneDX SBOMs, supplemental in-toto
  provenance, deterministic supply-chain bundle index, and eight GitHub
  attestation receipts remain manifest-bound public assets. Release notes
  remain the release body, not a duplicate asset.
- `supply_chain_attestation.md` remains diagnostic only. No new v2
  `signature.hmac-sha256.json` is produced; existing copies are readable for one
  minor release only as `LEGACY_UNVERIFIED`. Generated GitHub source archives
  are explicitly outside the asset contract.
- Trusted Publishing from the exact external controller is mandatory for v2.
  Any API-token bootstrap is a separately invoked first-project maintenance
  action, permanently `LEGACY_UNVERIFIED`, and removed after one minor release.
- Provider IDs and ruleset version IDs are populated only from an approved live
  snapshot; placeholders are not committed to production policy.
- No earlier release receives manufactured v2 or immutable-release `PASS`
  evidence.

## Failure and recovery consequences

- Before the first controlled provider mutation: release the lease and roll
  back the complete policy/parser/consumer cutover.
- After attestation or draft mutation but before PyPI: stop renewal, observe
  exact state, close the failed attempt, and repair/abandon the draft; do not
  continue under v1.
- After an exact PyPI subset: retain the original candidate and upload only
  missing exact files under a new attempt, fencing token, and authorization.
- After a PyPI conflict: stop, investigate, normally yank the version, and use a
  new version; hold retention and do not delete automatically.
- After GitHub immutable publication: publish a corrective new release; do not
  replace assets or move the tag.
- After cancellation or process loss: reconstruct from provider reads and
  append-only receipts; missing attribution is `RECOVERY_REQUIRED/UNVERIFIED`.
- After closure-C drift: public provider facts remain recorded, but no final
  `PASS` is written.

No design can atomically roll back both providers. The system's guarantee is
truthful state, fail-closed ordering, exact idempotent recovery where the
providers allow it, and no false success.

## Alternatives considered

| Alternative | Decision |
|---|---|
| Mutable worktree policy with a recorded path | Rejected; path identity does not bind bytes |
| Separate release policy/parser | Rejected; duplicates shared governance semantics and consumers drift |
| Policy context-name equality only | Rejected; producer, workflow, target, bypass, and version drift remain |
| A/B snapshots without closure C | Rejected; post-authorization drift can hide behind final success |
| Candidate workflow as privileged root | Rejected; candidate revision can select its own authority graph |
| GitHub concurrency as the lock | Rejected; runner admission is not durable fencing or recovery state |
| Repository scripts in privileged jobs | Rejected; candidate code would inherit publication authority |
| Directly create a published GitHub Release | Rejected; assets cannot be completely verified before immutable commit |
| `skip-existing` without exact pre/post reconciliation | Rejected; duplicate handling is not byte authorization |
| Exact token-uploaded bytes close v2 | Rejected; they lack acceptable Trusted Publisher publish provenance |
| Automatic deletion to simulate rollback | Rejected; destructive and not a transaction |
| Generic multi-provider transaction abstraction | Rejected; neither provider exposes the required atomic primitive |

## Consequences

Positive:

- release authority is attributable to exact committed policy bytes;
- branch and release governance use one parser and one ruleset/check model;
- ruleset and producer drift cannot hide behind matching context names;
- stable A/B/C evidence catches pre-authorization and pre-closure drift;
- candidate-controlled code and graph are separated from publication
  credentials;
- stale attempts are fenced before receiving new provider capabilities;
- stale tag checks cannot authorize later publication;
- partial PyPI state is resumable only with exact bytes and publisher
  provenance;
- GitHub assets are complete before immutable publication;
- append-only receipts, retention, and incident hold survive runner artifacts;
- final evidence distinguishes `AUTHORIZED` and public partial state from
  `CLOSED/PASS`.

Costs and limitations:

- additional jobs and provider API reads increase latency;
- an external controller, capability broker, and WORM evidence store add
  operational ownership;
- ruleset history may require a technically write-capable credential even
  though the job performs reads;
- cross-provider publication remains non-atomic;
- exact candidate/public-bundle bytes must be retained for partial recovery;
- immutable releases prevent convenient in-place correction by design;
- live target/controller/App/store IDs, ruleset versions, immutable-release
  parity, retention enforcement, and four controller Trusted Publishers remain
  `UNVERIFIED` until approved inspection and configuration.

## Primary platform basis

Checked 2026-07-19:

- [GitHub repository rulesets and version history](https://docs.github.com/en/rest/repos/rules?apiVersion=2026-03-10)
- [GitHub available rules for branch and tag rulesets](https://docs.github.com/en/repositories/configuring-branches-and-merges-in-your-repository/managing-rulesets/available-rules-for-rulesets)
- [GitHub immutable releases](https://docs.github.com/en/code-security/concepts/supply-chain-security/immutable-releases)
- [GitHub immutable-release repository endpoint](https://docs.github.com/en/rest/repos/repos?apiVersion=2026-03-10#check-if-immutable-releases-are-enabled-for-a-repository)
- [GitHub Releases API](https://docs.github.com/en/rest/releases/releases?apiVersion=2026-03-10)
- [GitHub release-assets API](https://docs.github.com/en/rest/releases/assets?apiVersion=2026-03-10)
- [GitHub release-integrity verification](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/secure-your-dependencies/verify-release-integrity)
- [GitHub Actions concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency)
- [GitHub secure Actions guidance](https://docs.github.com/en/actions/reference/security/secure-use)
- [PyPI per-file Upload API](https://docs.pypi.org/api/upload/)
- [PyPI JSON and file digest API](https://docs.pypi.org/api/json/)
- [PyPI Integrity API](https://docs.pypi.org/api/integrity/)
- [PyPI Trusted Publishers](https://docs.pypi.org/trusted-publishers/)
- [PyPI publish-attestation contract](https://docs.pypi.org/attestations/publish/v1/)
- [PyPI attestation verification](https://docs.pypi.org/attestations/consuming-attestations/)
- [PyPI release yanking](https://docs.pypi.org/project-management/yanking/)

## Related

- [Feature design: frozen release policy and privileged publication boundary](../feature-design-release-trust-boundary-v0732.md)
- [Release process](../release.md)
- [Agent release protocol](../agent-release-protocol.md)
- [ADR 0011: canonical fingerprints](0011-canonical-fingerprints.md)
- [ADR 0013: reproducible rerun and retention](0013-reproducible-rerun-and-retention.md)
- [ADR 0026: digest-first runtime image promotion](0026-digest-first-runtime-image-promotion.md)
- [v0.73.1 global review](../../test_artifacts/airflow-self-service-global-review-v0731/review-report.md)
