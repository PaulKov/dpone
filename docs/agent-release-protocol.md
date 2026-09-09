# Agent release protocol

This protocol supplements `docs/release.md`. It converts the maintainer's release
expectations into auditable gates for a named version and frozen commit.

## Operation, authority, and scope

Start with [the current release runbook](release.md), not a historical workflow
or the longest available checklist. Record the requested operation: readiness
audit, authorized publication, retrospective verification, or a separately
scoped route/production certification. Freeze the source version/commit and
record the reviewed controller revision independently.

Ordinary PyPI publication belongs only to `PaulKov/dpone-release-controller`
and its manually dispatched `pypi-release.yml`. It accepts only the version,
builds from the matching dpone tag, uploads via OIDC, and verifies public archive
hashes. It does not execute R1–R9, the source merge-receipt gates, or the legacy
paired-tag campaign. Those distinctions are not permission to skip applicable
source-readiness checks. A `GO` recommendation is not upload authorization;
the release-auditor role remains read-only.

For an already published version, use the controller's read-only
`tools.retro_pypi_verification` and report `PASS`, `FAIL`, or `UNVERIFIED` for
that observation. Do not run publication to obtain a receipt, require a GitHub
Release or GHCR image for a PyPI-only claim, or infer production certification
from matching public hashes.

Classify every R1–R9 category before scheduling work:

- Source required checks, compatibility, immutable merge evidence, package
  identity, and publication authority are not waived by a small patch.
- Documentation-only patches need normal CI and strict docs evidence; do not
  launch unrelated live matrices. Runtime/CLI/connector changes additionally
  need focused evidence appropriate to their risks and approved environment.
- Route, state, recovery, performance, Airflow/dbt, image, and vendor-live
  campaigns apply when the changed scope or claimed certification requires
  them. Record the source of each requirement and the reason for `N/A`.
- Legacy minor/major paired-run rules remain documented for that workflow
  contract in [Release evidence](release-evidence.md); they are not the
  ordinary controller's publication gate.
- CI-shadow diagnostics and unfinished backlog are not publication authority.
  Keep their actual status separate; a policy update does not complete them.

## Evidence vocabulary

Every check has one status:

- `PASS`: executed successfully against the frozen commit and required environment;
- `FAIL`: executed and did not satisfy acceptance criteria;
- `SKIP`: required but not executed; include reason and owner;
- `N/A`: not applicable to the release scope; include rationale;
- `UNVERIFIED`: evidence is stale, incomplete, inaccessible, or not attributable
  to the frozen commit.

`SKIP`, mocked success, and stale evidence are never `PASS`.

For each item record command/workflow, source and controller identity where
relevant, environment, timestamp, observed result, artifact path, owner,
applicability, requirement source, and blocking decision. Keep the readiness
decision separate from the post-publication observation.

## R1 — CLI correctness and UX

Build the command/option inventory from the executable CLI and generated
reference. Do not attempt an unbounded Cartesian product. Use:

1. equivalence classes for values and modes;
2. boundary values;
3. negative and mutually exclusive combinations;
4. pairwise coverage for interacting options;
5. exhaustive coverage only for small, high-risk groups.

Verify help/version/import without optional SDKs, exit codes, stdout/stderr,
JSON and file output, encoding, atomicity/overwrite policy, non-TTY behavior,
invalid configuration, actionable errors, and no durable side effect before
validation succeeds.

## R2 — `run` CLI/Python parity

For representative manifests compare semantic behavior, not decorative output:

- normalized manifest and execution plan;
- selected source, sink, strategy, and capabilities;
- identities, counts, state/checkpoint changes, and evidence;
- dry-run side effects;
- error classification and recovery guidance.

## R3 — hierarchical identity and nested normalization

Verify deterministic root/row/parent identity, lineage at every depth, naming and
collision rules, missing/null/empty object/empty array semantics, arrays of
objects/primitives, deep nesting, type fidelity, schema evolution, retry/replay
stability, and absence of orphaned children.

## R4 — source-to-sink × strategy matrix

Generate the matrix from authoritative capability metadata. Every cell is
`supported`, `unsupported` with reason, `experimental`, or
`certification-required`.

For supported scope verify capability negotiation, type/schema mapping, row
counts or reconciliation, strategy semantics, checkpoint/evidence ordering,
retry/resume, quarantine/recovery, and understandable artifacts. Separate mocked
contract evidence from live route certification.

## R5 — contracts and guardrails

Run import, architecture, module-size, graph, compatibility, schema, state,
evidence, security, and fail-closed checks. Confirm no gate or threshold was
weakened to make the release pass.

## R6 — documentation and CJM

Run strict docs checks, validate YAML/examples/links, compare CLI and schema
reference to executable behavior, update architecture and diagrams, and walk the
first-time-user journey. Decompose monolithic pages when audience/task boundaries
justify it; do not split mechanically.

## R7 — Airflow and dbt

Verify supported version ranges, import/parse behavior, serialization/templating,
configuration and secret handling, retries/cancellation/logs/artifacts, upgrade
compatibility, first-run UX, and documented production deployment. Use relevant
current official Airflow, dbt, Astronomer Cosmos, and gusty behavior as research
inputs, not as unsupported marketing claims.

## R8 — packaging, dependency, security, and supply chain

Verify clean builds, metadata, wheels/sdists, fresh-environment installs, base and
extras, optional import isolation, dependency review, secret scanning, and
scope-required SBOM/provenance evidence according to `docs/release.md`.
For source readiness, retain the canonical annotated-tag report and live
required-check report for the exact commit. For publication observation,
compare the controller's retained four-package wheel/sdist inventory with
PyPI by filename and SHA-256; local candidate bytes are not a substitute.
Report public resolver visibility separately. For a runtime image in scope,
retain the GHCR digest, pull and smoke by digest, verify the installed dpone
version and `pip check`, and retain package inventories, SPDX
SBOM, provenance, and SBOM attestations. A visible version or mutable image tag
without byte identity is `UNVERIFIED`, not `PASS`.

The exact commit must also have a successful automatic
`agent_pr_merge_receipt.json` closure. Reconcile its
`integration_commit_sha` with the release identity and exact-commit reports,
retain its byte-identical `source-agent-pr-receipt.zip`, and follow the
[merge-receipt runbook](agent-pr-merge-receipt-runbook.md). Missing immutable
source evidence is a release blocker, not permission for manual reconstruction.

The exact-commit evidence is produced by
`tools/agent_policy/release_identity_gate.py` and
`tools/agent_policy/release_commit_gate.py`. The first binds the annotated tag,
four package versions, exact internal dependency pins, changelog and protected
base ancestry. The protected base is always `origin/<branch>` from the frozen
branch-protection policy (never a caller-selected `HEAD`); evidence records
`protected_base_sha` and `policy_sha256`. It reads package, changelog, and
policy bytes directly from the frozen commit and never falls back to the
worktree. The second reads
`.agents/policy/github-branch-protection.yml` only via
`git show <commit>:<path>`, binds `policy_sha256` in its JSON report, queries
the live ruleset plus check-runs for that full SHA, and validates the configured
producer identity; legacy commit statuses are diagnostic only. Caller-selected
policy bytes and worktree policy edits cannot influence the gate. Static policy
files or a screenshot of green checks cannot replace either machine-readable
report.

Before archive inspection, attestations, or upload, run the closed candidate
inventory gate. It must prove exactly eight regular artifacts: one wheel and
one `.tar.gz` sdist for each of the four public distributions, all at the
requested version. Duplicate variants, `.zip`, unrelated files, directories,
and symlinks are blockers. Keep the deterministic JSON inventory with filenames
and SHA-256 digests as release evidence, then validate archive members, run
bounded tenant hygiene on frozen source and exact archive bodies with the
CI-owned `TENANT_HYGIENE_POLICY` secret, and run a fresh `dpone[full,accel]`
install plus `pip check`.

These are maintainer/source-readiness checks; do not describe them as checks
implemented by the external controller. Tenant-hygiene credentials stay within
their approved CI environment. Do not retrieve secrets to complete an audit.
Configure the deny list using [Tenant hygiene policy](cicd/tenant-hygiene-policy.md).
The publisher builds its own archives; a local or source-workflow candidate
inventory does not prove identity with the controller's bytes. Retain its
`release-manifest.json` and exact run/artifact identities separately.

The source repository's `source-release-readiness.yml` workflow produces this
packaging/hygiene evidence independently of the legacy GHCR candidate campaign:

```bash
gh workflow run source-release-readiness.yml --repo PaulKov/dpone --ref master
```

Dispatch only after integration; retain the resulting run's exact `head_sha` as
the assessed C. The workflow accepts no source/version/path inputs and runs only
on protected `master`. A skipped job is not certification. If `master` moved
before dispatch, the run assesses the new C, not the intended earlier commit;
do not transfer its result to another SHA. Require both jobs to succeed.

The build job records C/tree/version/run/attempt, synchronizes validation
dependencies from `uv.lock`, builds and inspects all eight archives, then checks
a fresh installed `full,accel` environment and revalidates the closed inventory
before upload. A separate runner downloads the
candidate artifact by immutable ID with digest mismatch treated as an error.
It verifies producer context and archive checksums before running the bounded
stdlib-only hygiene scanner. It never installs or executes candidate packages.
The CI policy exists only in that scan step and its private temporary file is
removed on success or failure. An absent policy or scanner error blocks the run;
do not substitute a local policy, retrieve the secret, or mark the scan skipped.

Retain the original `source-readiness-candidates-<run>-<producer-attempt>` and
`source-readiness-hygiene-<run>-<scan-attempt>` artifact ZIPs, their provider
IDs/digests, context, inventory, install results and both actual hygiene reports
before the 90-day retention expires. Failed build diagnostics use a separate
artifact and do not imply readiness. Rerunning only a failed scan preserves the
original candidate attempt in `handoff.json`. This workflow has read-only
repository permissions: it does not tag, publish, dispatch the controller, or
replace exact-C required checks, automatic merge closure or deployment acceptance.

Before authorized publication, re-observe the tag binding to the frozen
commit. The controller's ordinary tag/name/version check is not the stronger
annotated-object, protected-ancestry, dependency, and changelog audit above.
An already-published version instead needs the read-only retrospective
receipt, not another upload. See [Release](release.md) for the exact command,
artifact retention, and partial-upload recovery boundary.

## R9 — recovery, observability, and performance

Verify crash/retry/resume/replay, cancellation, state/evidence recovery,
quarantine/repair, log/metric/lineage completeness, performance regressions,
resource bounds, and documented operating limits for changed critical paths.

## Existing executable checklist

When the chosen certification contract requires `dpone ops pre-release-checklist`,
follow its [diagnostic contract](release-evidence.md#pre-release-checklist)
only after gathering the underlying evidence. Its booleans summarize executed
checks; they do not create proof or replace the controller's public-byte
verification. Do not fill an all-true checklist to manufacture a release GO.

## Release decision

- Any blocking `FAIL` or required `SKIP/UNVERIFIED` means `NO-GO`.
- `N/A` requires scope rationale.
- `GO` names the operation, exact commit, applicable gates, and residual
  non-blocking risks. It is not permission to publish.
- A different proposed/tagged commit invalidates the release decision until
  affected gates are rerun. Later `master` advancement alone does not
  invalidate provider evidence for frozen commit C.
- Raw `live-certification.yml` success and standalone behavioral
  `release_evidence_pack.json` are not publication authorization.
- A retrospective `PASS` states only what the verifier observed; it does not
  retroactively prove source readiness or authorize a republish.
- Missing, deleted, expired, ambiguous, or unattributable evidence is
  `UNVERIFIED`; observed byte mismatches are failures. Preserve actual producer
  statuses instead of relabeling them to obtain GO.
- A publish failure or partial upload requires reconciliation and an explicit
  recovery decision. Never substitute artifacts, enable `skip-existing`,
  restore a second publisher, or bypass exact-head PR checks.
