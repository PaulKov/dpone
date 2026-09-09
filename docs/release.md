# Release

This is the current maintainer runbook for the four dpone PyPI distributions.
It separates source readiness, authorized publication, and read-only observation
of a version that is already published.

## Current authority

As reviewed on 2026-08-28, the sole ordinary PyPI publisher is
`PaulKov/dpone-release-controller`, workflow `pypi-release.yml`, environment
`pypi`. Its manual input is only `version=X.Y.Z`, without the `v` prefix.
It builds from `PaulKov/dpone` tag `vX.Y.Z`; it does not accept a repository,
source SHA, ref, package, or artifact path from the caller.

The approved authority and implementation were checked at controller commit
`3b6ff638e2dbefc092447c584084a75ac72fd117`:

- [Approved OIDC publisher specification](https://github.com/PaulKov/dpone-release-controller/blob/3b6ff638e2dbefc092447c584084a75ac72fd117/docs/feature-specs/oidc-pypi-release-controller.md).
- [Active publisher contract](https://github.com/PaulKov/dpone-release-controller/blob/3b6ff638e2dbefc092447c584084a75ac72fd117/config/oidc-pypi-publisher.json).
- [Publication workflow](https://github.com/PaulKov/dpone-release-controller/blob/3b6ff638e2dbefc092447c584084a75ac72fd117/.github/workflows/pypi-release.yml).
- [Controller operating guide and retrospective verifier](https://github.com/PaulKov/dpone-release-controller/blob/3b6ff638e2dbefc092447c584084a75ac72fd117/README.md).

Re-observe the controller revision and provider configuration before a new
release. These source references are not proof of current PyPI permissions or
of any particular version's successful publication.

The controller does not create a GitHub Release or publish a GHCR image.
Source-repository `release.yml` and `runtime-image.yml` still exist and were
active when inspected; a tag push can start them. The source `publish_pypi`
job only prints a handoff message: it neither uploads to PyPI nor dispatches
the controller. Its candidate artifacts, attestations, GitHub Release code
paths, and paired-run rules are separate from the ordinary PyPI publisher.
Do not describe those workflows as disabled or use their success as controller
publication evidence. Do not dispatch a tag as a harmless configuration probe.

## Choose the operation

| Request | Work to perform | Completion evidence |
| --- | --- | --- |
| Prepare or audit a release | Freeze version, source commit, changed scope, and applicable gates. Do not publish. | Scoped `GO` or `NO-GO` report. |
| Publish an authorized version | Verify prerequisites, then manually dispatch the controller once. | Exact controller run, retained build manifest, and public verification result. |
| Verify an already published version | Use the controller's read-only retrospective verifier. Never dispatch publication. | `retro_pypi_verification.json` and `fresh_install.log`. |
| Certify a route, runtime image, or production rollout | Use its explicitly selected certification/deployment contract. | Evidence for that scope, not a replacement PyPI receipt. |

A readiness `GO` is not publication authorization. Documentation changes,
backlog closure, and a request to inspect a release do not authorize uploads,
tag changes, provider configuration changes, or announcements.

## Release policy and scope

Keep version selection and compatibility decisions separate from publication
mechanics. Apply [Compatibility](compatibility.md) and document changes in
[CHANGELOG.md](https://github.com/PaulKov/dpone/blob/master/CHANGELOG.md).
Patch releases preserve backward compatibility; any approved 0.x minor break
needs explicit migration guidance. The controller only validates a numeric
`X.Y.Z` triplet: it does not choose the next version, enforce monotonicity,
classify patch/minor/major changes, or accept prerelease suffixes.

Use [Agent release protocol](agent-release-protocol.md) to classify R1–R9
before running checks. Source PR requirements and exact-commit merge evidence
remain mandatory. Documentation-only patches need their normal CI/docs gates;
runtime, connector, or data/state changes additionally need relevant focused
evidence in an approved environment. A production or route-certification claim
still requires its own live proof. Do not waive an applicable gate merely
because the controller does not implement it.

R1–R9 is an audit inventory, not an instruction to run every historical
campaign for every PyPI upload. The legacy minor/major paired-tag campaign in
[Release evidence](release-evidence.md#canonical-pre-tag-workflow) is not the
current controller's publication gate. Unfinished CI-shadow work remains a
separate backlog under [ADR 0046](adr/0046-component-aware-pr-gate-authority.md);
it neither certifies nor blocks ordinary publication by itself. Report it
honestly instead of closing it merely because release authority changed.

## Prerequisites

Record version, exact source commit, annotated tag object, reviewed controller
commit, operation, and approval in the
[release evidence report](agent-templates/release-evidence-report.md).

1. Merge the source/version changes through normal required checks; preserve
   the immutable merge receipt described below.
2. Confirm all four project versions and internal dependency pins match the
   chosen version. A complete release has one wheel and one sdist each for
   `dpone`, `dpone-native-accel`, `dpone-airflow-pack`, and
   `apache-airflow-providers-dpone`.
3. Validate the exact source identity, required checks, changed-scope evidence,
   and annotated tag. Do not move or replace an existing release tag.
4. Check the controller revision, permission boundary, and publisher parity.
   Resolve uncertainty before authorizing a production dispatch.
5. Preserve run/artifact IDs and immutable bytes promptly: current controller
   build and manifest artifacts expire after 14 days.

### Trusted Publisher parity gate

Each of the four PyPI projects must trust this exact tuple:

| Setting | Required value |
| --- | --- |
| Owner | `PaulKov` |
| Repository | `dpone-release-controller` |
| Workflow filename | `pypi-release.yml` |
| Environment | `pypi` |

Inspect the authenticated PyPI publishing settings and retain dated,
credential-free evidence for every project. Missing access or an unobserved
row is `UNVERIFIED`, not `PASS`. Do not restore `dpone/release.yml` or the
historical `release-controller.yml` as a second publisher. No API-token
fallback or `skip-existing` recovery is authorized.

The controller's publish job has only `id-token: write`, consumes the retained
build artifact, and performs no checkout or source execution. The name `pypi`
alone does not prove required reviewers, environment protection, or revocation
of historical credentials. Provider changes and credential revocation need
their own authorization; never print or persist credentials in evidence.

## Pre-release checks

Select checks using `tools/agent_policy/select_checks.py` and the changed scope.
Keep local build smoke separate from the bytes later built by the controller.
In a clean disposable checkout with an empty `dist/`, reproduce the four-package
build and inspect its archives:

```bash
uv sync --locked --all-extras
uv build
uv build packages/dpone-native-accel --out-dir dist
uv build packages/dpone-airflow-pack --out-dir dist
uv build packages/apache-airflow-providers-dpone --out-dir dist
rm -f dist/.gitignore
uv run python tools/pypi_release_smoke_dist.py \
  --dist-dir dist --expected-version X.Y.Z --inventory-only --format json
uv run python tools/agent_policy/package_archive_gate.py dist/*.whl dist/*.tar.gz
uv run twine check dist/*
```

For a fresh local install, use the following path only if it does not already
exist; otherwise choose a new temporary directory. Do not overwrite an existing
environment:

```bash
uv venv /tmp/dpone-release-smoke
uv pip install --python /tmp/dpone-release-smoke/bin/python pip
/tmp/dpone-release-smoke/bin/pip install dist/*.whl
/tmp/dpone-release-smoke/bin/pip check
/tmp/dpone-release-smoke/bin/dpone --help
```

These are diagnostic source/build checks, not public-byte proof. Extra-specific
smokes and live route tests are required when the selected scope calls for them.
A checklist boolean or a manually edited JSON file cannot replace executed
evidence.

### Merge-receipt sequence for release pull requests

1. Use `.github/pull_request_template.md` and link the approved specification,
   issue, or an explicit `N/A` justification.
2. Wait for required checks on the final reviewed PR head. Complete the four
   owner attestations truthfully and link `agent-governance-gate` when agent
   controls changed.
3. Wait for the final-body `Agent PR receipt` on that exact head. Pending,
   stale, skipped, and failed checks are not successful checks.
4. Merge without bypass, then retain the automatic
   `agent_pr_merge_receipt.json` and byte-identical
   `source-agent-pr-receipt.zip` for the exact integration commit.

Before tagging, reconcile `integration_commit_sha` with source identity and
exact-commit required-check evidence. Use `release_identity_gate.py`,
`release_commit_gate.py`, and `release_merge_receipt_gate.py` under
`tools/agent_policy/`, following the
[merge-receipt runbook](agent-pr-merge-receipt-runbook.md).
Those maintainer checks are not executed by the external publisher. Missing
immutable source evidence blocks readiness; do not reconstruct it from a
merged PR's mutable body or fabricate a backfill.

After the annotated tag exists, retain the frozen-source identity report as
well as the merge-receipt reports. This command is read-only with respect to
GitHub and PyPI; it only writes local evidence:

```bash
release_tag=vX.Y.Z
release_commit="$(git rev-parse "${release_tag}^{commit}")"
mkdir -p "test_artifacts/release/${release_tag}"
uv run python tools/agent_policy/release_identity_gate.py \
  --root . \
  --tag "${release_tag}" \
  --commit-sha "${release_commit}" \
  --remote-ref origin/master \
  --output "test_artifacts/release/${release_tag}/release_identity.json"
```

Confirm `PASS` and reconcile the exact commit across all three reports.
The identity gate reads the package versions, pins, changelog, and policy
from the frozen commit, not from uncommitted worktree edits.

## Rehearse without publication

After confirming the tag and controller revision, an authorized artifact-only
rehearsal uses this separate manual workflow:

```bash
gh workflow run pypi-rehearsal.yml \
  --repo PaulKov/dpone-release-controller \
  --ref master \
  -f version=X.Y.Z
```

It builds and checks eight archives, re-downloads and hashes the inventory,
installs the four retained wheels, and runs `pip check` and `dpone --help`.
It has no OIDC, publishing action, or publishing environment. Retain
`dpone-pypi-rehearsal-X.Y.Z` and
`dpone-pypi-rehearsal-receipt-X.Y.Z`; the latter contains
`rehearsal-receipt.json`. Rehearsal does not prove live Trusted Publisher parity,
public PyPI availability, or byte identity with a later publication build.

## Publish an authorized version

Only after explicit publication authorization and prerequisite evidence:

```bash
gh workflow run pypi-release.yml \
  --repo PaulKov/dpone-release-controller \
  --ref master \
  -f version=X.Y.Z
```

`--ref master` selects the controller workflow revision, not the dpone source
ref. Resolve and record the created run ID, attempt, actual controller SHA,
input version, and source tag/commit; do not select an unrelated newest run.
If dispatch acknowledgement is uncertain, inspect runs before trying again.
Do not issue a duplicate production dispatch just to discover its status.

The actual sequence is build → publish → verify-published:

1. Check out the fixed dpone tag; compare the root project name/version and
   build the four distributions. Validate metadata and the eight-file set.
2. Retain `dpone-pypi-X.Y.Z` and `dpone-pypi-manifest-X.Y.Z` with
   `overwrite: false`. The manifest is `release-manifest.json` with schema
   `dpone.pypi-release-manifest.v1`.
3. Publish only those built files through the OIDC-only artifact job.
4. Compare exact filenames and SHA-256 hashes against all four
   version-specific PyPI JSON endpoints. Public verification uses bounded
   retries (up to ten observations, six seconds apart).

Ordinary verification currently records its result in run logs/conclusion,
not a separate uploaded post-publication JSON receipt. Preserve those logs and
the immutable manifest. Success proves the observed archive inventory, not
Simple API propagation, a production route certification, a GitHub Release,
or a runtime image. Use retrospective verification for a durable detailed
publication observation.

## Verify an already published version

Use a reviewed checkout of the **controller repository**, not the dpone CLI.
Download the original controller distribution artifact ZIP without rebuilding
or repacking it. Obtain its run ID, artifact ID, and provider SHA-256 digest
from GitHub metadata; do not substitute a digest of replacement local files.

```bash
uv run --frozen python -m tools.retro_pypi_verification \
  --tag vX.Y.Z \
  --commit-sha EXACT_40_CHARACTER_SHA \
  --controller-run-id RUN_ID \
  --artifact-id ARTIFACT_ID \
  --artifact-zip /secure/download/dpone-pypi-X.Y.Z.zip \
  --artifact-sha256 PROVIDER_SHA256 \
  --output test_artifacts/release/vX.Y.Z/OBSERVATION_ID/retro_pypi_verification.json
```

Replace the identity placeholders with observed values and choose a fresh,
non-existing `OBSERVATION_ID` directory for every invocation. The output JSON
atomically replaces an existing file; do not reuse a prior observation path.
The verifier makes
fixed read-only GitHub/PyPI requests, checks the annotated tag and successful
publisher run, artifact identity/non-expiry, and exact public filenames,
hashes, sizes, and non-yanked state. It then creates a temporary environment,
installs the four retained wheels, runs `pip check` and `dpone --help`, and
writes `fresh_install.log` beside the atomic JSON receipt only when the install
stage is reached. It also prints the JSON result to stdout. A failed early
verification has no fresh install transcript; a leftover log from another
invocation is not evidence for the new receipt. Keep each observation together
with its command, timestamps, stdout/stderr, and exit code.

The current verifier requires a **successful overall controller run**, not
only a successful upload job. If `publish` succeeded but `verify-published`
failed, the run is still unsuccessful: rerunning this read-only CLI cannot
turn it into a `PASS`, even after PyPI propagation recovers. Preserve the
failed run and perform diagnostic manifest/public-index comparisons without
calling them an authoritative receipt. A verifier/recovery-contract change
needs a separate approved controller change; do not rerun the upload or
weaken the success condition to obtain green evidence.

| Result | Meaning and next step |
| --- | --- |
| `PASS` / exit 0 | The requested publication identity, public bytes, and retained-wheel install checks agree. Record the receipt; do not republish. |
| `FAIL` / exit 1 | An observed contract or verification check failed. Preserve evidence and investigate; never overwrite PyPI files. |
| `UNVERIFIED` / exit 1 | Required observations are unavailable or incomplete after bounded retries. Restore read access/evidence and repeat only verification. |
| Exit 2 | Invalid invocation; error on stderr and no new receipt. Correct inputs; no publication occurred. |

An expired/missing original artifact is not permission to rebuild evidence.
This verifier installs retained wheels, not packages resolved from PyPI's
Simple API. Use the [index-visibility diagnostic](cicd/runbooks.md#pypi-upload-succeeds-but-installers-cannot-see-the-version)
when public installation is the question. `dpone ops release-verify` includes
legacy GitHub Release expectations and is not the verdict for this PyPI-only
method.

## Failure and recovery

Stop after an upload failure, duplicate version, conflicting archive, partial
upload, or uncertain dispatch. Preserve the original run, artifact ZIP,
manifest, and public observations. PyPI uploads are not transactional and
accepted files cannot be overwritten. Do not rerun publication, upload a
missing subset, enable `skip-existing`, restore a second publisher, or delete
and recreate a version as automatic recovery. Reconcile first; obtain an
explicit recovery decision, normally a new version if new bytes are needed.

If only visibility/verification failed, repeat read-only diagnostics, not
the upload. The retrospective CLI can produce PASS only for an eligible
successful run as described above. A verified publication does not prove that all
source-readiness gates ran. Keep those results separate in the report.
See [Release and PyPI failures](cicd/runbooks.md#release-and-pypi-failures).

## Advanced and historical evidence

- [Release RC collector](release-rc-collector.md), `release-rc-collect`, and
  [Release RC finalizer](release-rc-finalizer.md), `release-rc-finalize`, remain
  explicit evidence workflows, not automatic ordinary-upload requirements.
- [Release evidence](release-evidence.md) documents diagnostic packs and the
  legacy paired-tag campaign; it does not grant controller upload authority.
- [Release and Pages automation](cicd/release-and-pages.md) describes the
  current publication boundary and the independent documentation deployment.
- [Single-publisher handoff decision](feature-specs/single-pypi-publisher-handoff.md)
  records the superseded source-workflow sequence. Historical broker/writer
  designs do not reactivate credentials or authorize a second publication path.
