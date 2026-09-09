# Feature design: PR 3A CI hygiene

- Status: APPROVED
- Owner: dpone maintainers
- Issue: [#512](https://github.com/PaulKov/dpone/issues/512)
- Parent specification: [CI shadow closure and exact-SHA evidence](feature-design-ci-pr-gate-exact-sha-evidence.md)
- Design base: `6fdbcf90f38a88d5aa1793213160e0eb0fb6939d`
- Target release: TBD
- Last verified: 2026-08-10

## Executive summary

PR 3A is a repository-control hygiene change. It makes non-release CI consume
the checked-in lock file without rewriting it, bounds Dependabot and expensive
compatibility work, separates pull-request cancellation from retained nightly
work, removes synthetic Dependency Review success publication, and confines
GitHub Pages deploy authority to the deploy job.

This document closes the executable child contract required by the approved
parent design. It does not add `PR Gate shadow`, change the nineteen required
checks, or change runtime/release behavior. No production workflow or Dependabot
configuration is changed by this specification PR. A later implementation PR
may start only after all approval conditions in this document are satisfied.

This security amendment is reviewed and merged independently of the PR 3A
implementation. It closes two provider-boundary details discovered during
implementation review: every Pages freshness lookup names `github.com`
explicitly, and durable label evidence separates its exact-name decision from
diagnostic provider metadata. The implementation PR must rebase onto the merged
amendment and keep this specification byte-identical to that new base.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
| --- | --- | --- | --- |
| Contributor | Reproduce CI from committed dependency bytes | CI may update a stale lock implicitly and superseded runs consume capacity | locked install either succeeds exactly or fails with a repair command; stale same-PR heads cancel |
| Maintainer | Retain nightly compatibility coverage | default concurrency keeps only one pending run | a stable nightly group retains up to the platform limit without cancelling running work |
| Security reviewer | Minimize token authority | PR Pages builds and Dependency Review receive write scopes they do not need | PR validation is read-only; only the protected Pages deploy job can deploy |
| Dependency owner | Receive bounded, correctly labelled update PRs | current `pip`/Actions schedules and limits do not match the approved plan | exact `uv` and Actions schedules use pre-existing owner-managed labels |
| Operator | Diagnose a red required check without manufacturing success | Dependency Review has a manual check-run backfill path | the native PR or exact-master run is fixed/rerun; no check is synthesized |

The first-time journey is:

- **Discover** — PR 3A is CI hygiene only. It does not add a merge authority,
  release path, runtime feature, connector, or evidence decision.
- **Prepare** — verify the exact base, pinned actionlint, the three owner-managed
  labels, and the release-workflow byte freeze. The label probe requires an
  authenticated GitHub CLI session that can read repository metadata; verify it
  first with `gh auth status --active --hostname github.com` and never print its
  account details or token.
- **Configure** — apply only the closed Dependabot, workflow, permission,
  concurrency, and matrix values below.
- **Execute** — run locked sync, changed-workflow actionlint, governance tests,
  and the selected repository gates.
- **Observe** — stale heads cancel only inside one pull request; nightly work
  queues; Pages PRs build without deploy authority; current-master Pages runs
  deploy monotonically; Dependency Review reports natively on both the PR test
  merge and the exact `master` commit.
- **Diagnose** — distinguish a stale lock, missing label, invalid workflow,
  queue saturation, dependency advisory, and Pages build/deploy failure.
- **Recover** — regenerate and review `uv.lock`; for Pages, never rerun an
  existing non-PR run and dispatch a new run on current `master`. Restore a
  missing label through an owner-authorized operation or revert the
  implementation. Operators never rerun all Pages jobs, never rerun only its
  deploy job, never manufacture a successful check, or replay an obsolete Pages
  subject.
- **Operate** — monitor Dependabot open-PR limits, nightly queue depth, matrix
  ceilings, and the Pages deployment environment.
- **Upgrade** — retain the exact values and release exclusions in reviewed
  tests/docs; remove the temporary queue waiver when a pinned released
  actionlint version parses `concurrency.queue`.

## Scope

### In scope

The implementation allowlist is exact:

- `.github/dependabot.yml`;
- `.github/workflows/ci.yml`;
- `.github/workflows/airflow-pack-compat.yml`;
- new `.github/workflows/airflow-pack-compat-nightly.yml`;
- `.github/workflows/pages.yml`;
- `.github/workflows/dependency-review.yml`;
- `.agents/policy/workflow-security.yml`;
- focused workflow-governance tests;
- contributor, workflow-reference, recovery, Pages, branch-protection,
  testing, changelog, and generated-metrics documentation named below.

For this child, the parent phrase “non-release CI dependency installation”
means the install steps in the exact allowlist above. It does not authorize a
repository-wide mechanical rewrite. The current intentional isolated `uv pip`
environments are unchanged.

### Non-goals

- changing `.github/workflows/release.yml`;
- changing `.github/workflows/runtime-image.yml`;
- changing `.github/workflows/certification-release-summary.yml`;
- changing `.github/workflows/route-certification-release.yml`;
- changing `.github/workflows/route-release-finalize.yml`;
- changing required check names, branch protection, rulesets, App bindings,
  tags, variables, environments, publication, or release credentials;
- adding `PR Gate` or `PR Gate shadow`;
- changing the ADR 0037 governance attestation exception;
- creating GitHub labels from repository automation;
- refactoring unrelated actionlint or shellcheck debt;
- introducing a generic workflow/concurrency framework or production Python
  package.

The five listed release-sensitive workflow files must remain byte-identical to
the PR3A implementation base. A base-to-head blob comparison is a merge gate.

### Assumptions and constraints

- The exact design base is current `master`
  `6fdbcf90f38a88d5aa1793213160e0eb0fb6939d`; it contains PR 2 merge commit
  `ec0323feba829d547e5f9c2d23acaac5b686f1a4` as an ancestor.
- GitHub documents that `queue: max` retains up to 100 pending members and
  cannot be combined with `cancel-in-progress: true`.
- actionlint `1.7.12` is the latest released upstream version observed on
  2026-08-10. It rejects the new GitHub `concurrency.queue` key while upstream
  support remains unreleased.
- Live read-only label inspection on 2026-08-10 found `dependencies` and
  `python:uv`; exact `github-actions` returned HTTP 404. Design review and the
  `APPROVED` specification transition may proceed with that evidence marked
  `UNVERIFIED`. Production implementation work cannot start, and its PR cannot
  merge, until an authorized owner creates the missing label and a fresh
  readback proves all three exact names.
- Repository automation does not create or mutate labels. Label description
  and color are administrative metadata; only the exact name is consumed.

## Public repository contract

There is no dpone CLI, Python API, manifest, state, or data-plane change. GitHub
configuration and its documented operator journey are repository public
contracts.

### Locked installs

The exact commands are:

- `.github/workflows/ci.yml`: every existing `uv sync --all-extras` becomes
  `uv sync --locked --all-extras`;
- `.github/workflows/pages.yml`: `uv sync --frozen` becomes `uv sync --locked`;
- any new project-environment sync in the nightly wrapper is forbidden; the
  wrapper contains only a local reusable-workflow call.

`--locked` checks that `uv.lock` agrees with project metadata and fails instead
of rewriting it. Existing extras remain selected. `--frozen` is rejected here
because it does not prove the lock is current. `uv build` and isolated
`uv pip install --python ...` steps are not project synchronization and remain
byte-unchanged unless another explicit requirement below changes their enclosing
line.

### Dependabot

`.github/dependabot.yml` retains schema version 2, root directory `/`, and the
minor/patch groups, with this exact effective configuration:

```yaml
updates:
  - package-ecosystem: "github-actions"
    directory: "/"
    schedule:
      interval: "weekly"
      day: "wednesday"
      time: "08:00"
      timezone: "Europe/Berlin"
    labels: ["dependencies", "github-actions"]
    open-pull-requests-limit: 2
    groups:
      github-actions-minor-patch:
        patterns: ["*"]
        update-types: ["minor", "patch"]

  - package-ecosystem: "uv"
    directory: "/"
    schedule:
      interval: "weekly"
      day: "monday"
      time: "08:30"
      timezone: "Europe/Berlin"
    labels: ["dependencies", "python:uv"]
    open-pull-requests-limit: 3
    groups:
      uv-minor-patch:
        patterns: ["*"]
        update-types: ["minor", "patch"]
```

No `cooldown`, registry, target-branch, allow, ignore, assignee, reviewer, or
milestone override is introduced. GitHub's documented defaults remain provider
behavior, not a dpone guarantee. The two configured open-PR limits apply to
version updates; this child does not relabel the platform's separate security
update limit.

Before the PR3A implementation task may start or its PR may merge, three
read-only exact-label GETs must return the required names `dependencies`,
`python:uv`, and `github-actions`. Missing, case-variant, inaccessible, partial,
or extra evidence is `UNVERIFIED` and blocks implementation. It does not block
review or approval of this specification. No fallback label is accepted.

The copyable read-only check is:

```bash
set -euo pipefail
if ! gh auth status --active --hostname github.com >/dev/null 2>&1; then
  echo "UNVERIFIED: active github.com authentication is unavailable." >&2
  exit 1
fi

if ! actual="$(
  for label in dependencies python%3Auv github-actions; do
    value="$(
      gh api --hostname github.com \
        "repos/PaulKov/dpone/labels/${label}" --jq '.name' 2>/dev/null
    )" || exit 1
    printf '%s\n' "${value}"
  done
)"; then
  echo "UNVERIFIED: exact GitHub label readback failed." >&2
  exit 1
fi

expected=$'dependencies\npython:uv\ngithub-actions'
if [[ "${actual}" != "${expected}" ]]; then
  echo "UNVERIFIED: GitHub label names or ordering differ from the approved contract." >&2
  exit 1
fi
printf '%s\n' "${actual}"
```

The auth check must exit zero without exposing credentials or adding stdout.
Auth failure performs zero API calls. Every label request explicitly binds
`--hostname github.com`; an ambient `GH_HOST` cannot redirect readiness
evidence. Every API failure is fail-fast even when a later label would exist.
Successful stdout is exactly three newline-terminated names in the approved
order; any other stdout or nonzero exit is `UNVERIFIED`.

The durable receipt uses two distinct decisions:

- `receipt_status` is `VERIFIED` only when the bounded JSON artifact is
  duplicate-free, internally consistent, and bound to the canonical collector;
- `label_readiness` is `PASS` only when authenticated read-only GETs explicitly
  bound to `github.com` and `repos/PaulKov/dpone/labels/{encoded}` return the
  exact ordered label-name projection `dependencies`, `python:uv`, and
  `github-actions`. Every other result is `UNVERIFIED`, never `FAIL`.

Raw response bytes and digests, numeric and node IDs, echoed URLs, color,
description, default status, GitHub CLI version, observation time, PR number,
and implementation base are diagnostic only. The producer may retain and
integrity-check those fields, but coherent diagnostic drift never changes label
readiness. The GitHub CLI version observation is best-effort: unavailable,
malformed, or changed output is stored as bounded diagnostic text or `null` and
never blocks collection, offline integrity, or live readiness. Offline receipt
verification proves artifact integrity, not current provider state.
`verify-live` re-authenticates and repeats the three host-bound GETs, then
compares only the canonical name projection. Missing, case-variant, foreign,
ambiguous, malformed, or unavailable name evidence produces one controlled
`UNVERIFIED` result without traceback or partial PASS output.

The approved producer is `tools/ci/pr3a_label_readback_evidence.py`; its
copyable collect, offline-verify, and live-verify commands are documented in
the CI/CD runbook. Generated evidence changes only through that producer.

### Pull-request and nightly concurrency

`.github/workflows/ci.yml` preserves its exact PR-number-scoped group and
conditional cancellation:

```yaml
concurrency:
  group: ci-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.run_id }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}
```

`.github/workflows/airflow-pack-compat.yml` preserves the corresponding
`airflow-pack-...` direct-event behavior and adds `workflow_call` with one
optional numeric input, `airflow_max_parallel`, with `required: false` and
`default: 2`. Its `airflow-compat` strategy uses the exact expression
`max-parallel: ${{ inputs.airflow_max_parallel || 2 }}`. It must not branch on
`github.event_name`: GitHub preserves the caller event in a called workflow.
Direct `push`, `pull_request`, and `workflow_dispatch` invocations therefore
resolve the fallback/default `2`, while the nightly wrapper passes `4`. An
early validation step in the build prerequisite receives the raw
`${{ inputs.airflow_max_parallel }}` through `env` before the strategy applies
`|| 2`. It accepts an empty value only for a direct event, or the exact integer
`2` or `4`; explicit `0`, any other number, a string, or an empty value on a
called run stops before the Airflow matrix. This keeps a caller-supplied `0`
from becoming `2` through truthiness fallback. Static tests prove that the
nightly wrapper is the only local caller passing `4`, no caller passes another
value, and raw boundaries `0/1/2/4/5` have the stated decisions.
`runtime-wheel-smoke` remains `max-parallel: 1` for direct and called
execution. Every matrix retains `fail-fast: false` and its exact current cells.

The new `.github/workflows/airflow-pack-compat-nightly.yml` is source-free: it
has no checkout, `run`, external action, secret, write permission, artifact, or
deployment. It calls only
`./.github/workflows/airflow-pack-compat.yml`, passes
`airflow_max_parallel: 4`, and has this exact trigger/concurrency contract:

```yaml
on:
  schedule:
    - cron: '23 1 * * *'
      timezone: Europe/Berlin
  workflow_dispatch:

permissions:
  contents: read

concurrency:
  group: airflow-pack-compat-nightly
  queue: max
  cancel-in-progress: false
```

The stable group deliberately joins scheduled and manual nightly-full runs.
GitHub may retain up to 100 pending runs; the 101st is provider-cancelled, not
called PASS. When that cancelled run has a
`RUN_ID`, wait until the group has capacity and run
`gh run rerun "$RUN_ID" --repo PaulKov/dpone`; the same run gains a new attempt
and retains its original SHA/ref. If no queryable run exists, record the absence
as `UNVERIFIED` and, after capacity returns, create a new run with
`gh workflow run airflow-pack-compat-nightly.yml --repo PaulKov/dpone --ref master`.
The new dispatch is a different run, not a repair of the absent one. A direct PR
run never enters the nightly group, and two different PR numbers never cancel
each other.

### GitHub Pages capability split

`.github/workflows/pages.yml` uses top-level `permissions: {}`. Job `build`
receives only `contents: read`. Job `deploy` receives only `pages: write` and
`id-token: write`, retains its protected `master`/non-PR condition and
`github-pages` environment, and remains dependent on successful build.

The entire Pages workflow uses this exact concurrency object. PR runs group by
PR number and cancel only an older head of that PR. `master` and manual runs
group by their ref; they never cancel the running workflow, retain only the
newest pending workflow through GitHub's default single-pending behavior, and
serialize build, freshness verification, and deploy as one unit:

```yaml
concurrency:
  group: pages-${{ github.event_name == 'pull_request' && github.event.pull_request.number || github.ref }}
  cancel-in-progress: ${{ github.event_name == 'pull_request' }}
```

No Pages job uses `queue`. After `build`, a separate read-only
`verify_current_master` job must query `refs/heads/master` with the workflow
token and require its exact 40-hex SHA to equal `github.sha`. API failure,
malformed/multiple output, a non-master event, a malformed
`GITHUB_RUN_ATTEMPT`, or a stale SHA fails closed. The job emits the verified
SHA and exact provider `GITHUB_RUN_ATTEMPT`. The privileged `deploy` job needs
both `build` and `verify_current_master` and accepts those outputs only when the
verification result succeeded in the same `github.run_attempt`; the freshness
query never runs inside the job that owns Pages/OIDC write authority. The exact
job boundary is:

```yaml
build:
  if: github.event_name == 'pull_request' || github.run_attempt == 1

verify_current_master:
  needs: build
  if: >-
    github.event_name != 'pull_request' &&
    github.ref == 'refs/heads/master' &&
    github.run_attempt == 1
  runs-on: ubuntu-latest
  permissions:
    contents: read
  outputs:
    verified_sha: ${{ steps.verify.outputs.sha }}
    verified_attempt: ${{ steps.verify.outputs.attempt }}
  steps:
    - name: Verify current master subject
      id: verify
      env:
        GH_TOKEN: ${{ github.token }}
        EXPECTED_SHA: ${{ github.sha }}
      run: |
        set -euo pipefail
        actual="$(
          gh api --hostname github.com \
            "repos/${GITHUB_REPOSITORY}/git/ref/heads/master" \
            --jq '.object.sha'
        )"
        if [[ ! "${actual}" =~ ^[0-9a-f]{40}$ ]] || \
           [[ "${actual}" != "${EXPECTED_SHA}" ]]; then
          echo "::error::Pages subject is not current master."
          exit 1
        fi
        if [[ ! "${GITHUB_RUN_ATTEMPT}" =~ ^[1-9][0-9]*$ ]]; then
          echo "::error::Pages run attempt is invalid."
          exit 1
        fi
        {
          printf 'sha=%s\n' "${actual}"
          printf 'attempt=%s\n' "${GITHUB_RUN_ATTEMPT}"
        } >> "${GITHUB_OUTPUT}"

deploy:
  name: Deploy GitHub Pages documentation
  needs: [build, verify_current_master]
  if: >-
    github.event_name != 'pull_request' &&
    github.ref == 'refs/heads/master' &&
    github.run_attempt == 1 &&
    needs.build.result == 'success' &&
    needs.verify_current_master.result == 'success' &&
    needs.verify_current_master.outputs.verified_sha == github.sha &&
    needs.verify_current_master.outputs.verified_attempt == format('{0}', github.run_attempt)
  permissions:
    pages: write
    id-token: write
  steps:
    - name: Configure Pages
      uses: actions/configure-pages@45bfe0192ca1faeb007ade9deae92b16b8254a0d
    - name: Deploy Pages
      id: deployment
      uses: actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128
```

Only non-PR build runs upload the Pages artifact. A PR never executes upload,
configure, or deploy actions and never receives deploy permissions. A skipped
deploy job on a PR is expected and must not replace the required docs build.
For non-PR subjects, `build` is eligible only on attempt `1`; `Re-run all jobs`
therefore skips build before a second upload can reuse the immutable
`github-pages` name, and its dependent verification/deploy jobs do not run. PR
build reruns remain eligible because they never upload or deploy Pages.

Every non-PR job that can upload, verify, or deploy also requires exact
`github.run_attempt == 1`. A specific rerun of `deploy` cannot reuse the older
verification output because its attempt differs, while a specific rerun of
`verify_current_master` and `Re-run all jobs` both stop before verification and
deploy. No non-PR rerun is a supported recovery path. The component never
selects, deletes, or overwrites a prior artifact to manufacture eligibility.
The only Pages recovery uses the versioned workflow-dispatch REST endpoint
because it returns the new exact run ID and URLs. `PRIOR_RUN_ID` is the failed
positive integer retained by the operator. Before running the command, the
operator installs `gh` and `jq`, authenticates `gh` to `github.com` with an
approved credential that has Actions write permission for `PaulKov/dpone`, and
exports `PRIOR_RUN_ID`. The command validates every local prerequisite and the
prior identity before the mutating request. It emits exactly two success lines
only after the workflow object, current `master`, dispatch response, new
identity, and URLs validate; prerequisite/API/parser/identity failure exits
nonzero and emits no success lines. A rejected dispatch does not create a run;
a successful POST followed by an invalid provider response is `UNVERIFIED`, and
the operator must preserve the response and inspect the repository run list
rather than blindly repeat the POST:

```bash
set -euo pipefail
API_VERSION=2026-03-10
: "${PRIOR_RUN_ID:?export PRIOR_RUN_ID as the failed positive integer}"
[[ "${PRIOR_RUN_ID}" =~ ^[1-9][0-9]*$ ]]
command -v gh >/dev/null
command -v jq >/dev/null
gh auth status --active --hostname github.com >/dev/null 2>&1
workflow_json="$(
  gh api --hostname github.com \
    -H "X-GitHub-Api-Version: ${API_VERSION}" \
    repos/PaulKov/dpone/actions/workflows/pages.yml
)"
WORKFLOW_ID="$(
  jq -er '
    select(.path == ".github/workflows/pages.yml" and .state == "active")
    | .id | select(type == "number" and . > 0 and . == floor) | tostring
  ' <<<"${workflow_json}"
)"
[[ "${WORKFLOW_ID}" =~ ^[1-9][0-9]*$ ]]
MASTER_SHA="$(
  gh api --hostname github.com \
    -H "X-GitHub-Api-Version: ${API_VERSION}" \
    repos/PaulKov/dpone/git/ref/heads/master \
    --jq '.object.sha'
)"
[[ "${MASTER_SHA}" =~ ^[0-9a-f]{40}$ ]]
dispatch_json="$(
  gh api --hostname github.com --method POST \
    -H "X-GitHub-Api-Version: ${API_VERSION}" \
    "repos/PaulKov/dpone/actions/workflows/${WORKFLOW_ID}/dispatches" \
    -f ref=master
)"
RUN_ID="$(
  jq -er '
    .workflow_run_id
    | select(type == "number" and . > 0 and . == floor) | tostring
  ' \
    <<<"${dispatch_json}"
)"
[[ "${RUN_ID}" =~ ^[1-9][0-9]*$ ]]
RUN_URL="$(jq -er '.run_url | select(type == "string")' <<<"${dispatch_json}")"
HTML_URL="$(jq -er '.html_url | select(type == "string")' <<<"${dispatch_json}")"
[[ "${RUN_ID}" != "${PRIOR_RUN_ID}" ]]
[[ "${RUN_URL}" == "https://api.github.com/repos/PaulKov/dpone/actions/runs/${RUN_ID}" ]]
[[ "${HTML_URL}" == "https://github.com/PaulKov/dpone/actions/runs/${RUN_ID}" ]]
printf 'PAGES_RECOVERY_RUN_ID=%s\n' "${RUN_ID}"
printf 'PAGES_RECOVERY_EXPECTED_SHA=%s\n' "${MASTER_SHA}"
```

The later exact-run observation must bind that returned ID to repository
`PaulKov/dpone`, the numeric workflow ID and path, event `workflow_dispatch`,
provider-visible `head_branch == "master"`, attempt `1`, and `head_sha` equal
to the emitted expected SHA. The Workflow Runs API does not expose a `ref`
field. Ref authorization instead comes from the authenticated workflow blob's
exact `github.ref == 'refs/heads/master'` job conditions; a successful observed
job proves that condition admitted the run. It receives a new artifact
namespace. Missing, malformed,
ambiguous, concurrent, prior-run, or foreign dispatch identity is
`UNVERIFIED`; the workflow's own attempt/ref/freshness conditions remain the
authorization boundary and fail closed independently. If
`master` advances after a run passes freshness, the stable workflow group keeps
the newer run pending until the older deploy completes, so the newer successful
run remains the final deployment.

A false job-level deploy condition is provider `SKIPPED` and may be displayed
as check `Success`; it is never deployment PASS. It is `NOT_RUN` with
`UNVERIFIED` deployment evidence. PR 3A deliberately does not select a Pages
REST deployment by SHA because repeated runs of one SHA are not an exact-attempt
identity. Deployment PASS requires one authenticated `deploy` job from the
exact repository/workflow/run ID, attempt `1`, provider-visible
`head_branch == "master"`, event, `head_sha`, and job ID; that job must conclude
`success`, and its single pinned
`actions/deploy-pages` step must conclude `success`. The pinned action creates
and polls the Pages deployment; its exact-step conclusion is the attempt-bound
provider source. A workflow conclusion, environment state, older Pages REST
record, or same-SHA record from another run is never deployment evidence.
The implementation observer authenticates the exact workflow blob first. It
reads `workflow_id` and `head_sha` from the exact run, requires
`GET /repos/PaulKov/dpone/actions/workflows/{workflow_id}` to return the active
path `.github/workflows/pages.yml`, then reads
`GET /repos/PaulKov/dpone/contents/.github/workflows/pages.yml?ref={head_sha}`.
The response must be one regular file; its strict base64-decoded bytes must
match the returned Git blob identity and parse as duplicate-free YAML without
aliases or unknown job/step structure. Any source/API/transport ambiguity is
`UNVERIFIED`. From those authenticated bytes the observer
requires `jobs.deploy.name == "Deploy GitHub Pages documentation"`, the ordered
step named `Deploy Pages` at provider step number `3`, and the exact
`actions/deploy-pages@cd2ce8fcbc39b97be8ca5fce6e763baed58fa128` source pin.
It then authenticates the exact run with
`GET /repos/PaulKov/dpone/actions/runs/{run_id}` and completely paginates
`GET /repos/PaulKov/dpone/actions/runs/{run_id}/attempts/1/jobs?per_page=100`.
Because the Jobs API does not expose YAML job keys or action `uses`, the
observer selects exactly one provider job by the authenticated display name and
requires its provider-visible step name `Deploy Pages`, number `3`, and
conclusion `success`. Missing, duplicate, nonterminal, foreign, source-drifted,
or partially paginated run, job, or step identity is `UNVERIFIED`. No Pages REST
record selection is part of this decision.

The exact Pages recovery/evidence policy is:

```yaml
pages_recovery:
  rerun_all_jobs: FORBIDDEN
  rerun_deploy_only: FORBIDDEN
  rerun_verify_current_master: FORBIDDEN
  only_recovery:
    action: DISPATCH_NEW_RUN_ON_CURRENT_MASTER
deployment_evidence:
  skipped_job:
    provider_check: SKIPPED_SUCCESS
    deployment_outcome: NOT_RUN
    evidence_status: UNVERIFIED
  pass_requires:
    - authenticated_workflow_deploy_display_name_order_and_action_pin
    - exact_repository_workflow_run_attempt_one_head_branch_event_sha_and_job
    - unique_provider_display_name_job_and_ordered_step_succeeded
```

The implementation governance tests parse the actual
`.github/workflows/pages.yml` and assert the workflow concurrency, non-PR
attempt-1 build eligibility, read-only freshness outputs, exact deploy
condition, and job dependencies; matching text from another workflow is not
evidence.

`.agents/policy/workflow-security.yml` moves the existing Pages write exception
from workflow scope to the exact `deploy` job and removes the obsolete
Dependency Review write exception. No other allowlist entry changes.

### Read-only Dependency Review

`.github/workflows/dependency-review.yml` keeps its workflow and job display
name `Dependency Review`, pinned checkout and dependency-review action,
`fail-on-severity: high`, and native triggers for `pull_request` and `push` to
`master`. The push run is required because the unchanged release preflight
queries all nineteen required checks on the exact `master`/tag commit, while a
PR run is attached to `refs/pull/<N>/merge`. Its closed surface becomes:

- only `pull_request` and `push`, each restricted to `master`;
- top-level `contents: read` and no job override;
- `comment-summary-in-pr: never` on both action invocations;
- PR invocation uses the native test-merge comparison;
- push invocation requires `github.event.before` to be a nonzero exact 40-hex
  SHA, then supplies `base-ref: ${{ github.event.before }}` and
  `head-ref: ${{ github.sha }}` to the pinned action;
- `workflow_dispatch`, arbitrary requested refs, `gh api`, and the synthetic
  check-run backfill are deleted;
- `pull-requests: write` and `checks: write` are deleted.

`Dependency Review` remains the native job/check result on two different
subjects. PR PASS applies only to the PR merge SHA. Push PASS applies only to
the exact `master` commit and supplies release exact-commit evidence; neither
relabels the other. Manual invocation is absent and therefore `N/A`, never a
recovery path. Missing/unreadable provider evidence for either eligible event is
`UNVERIFIED`, not PASS. An existing failed run may be rerun by exact run ID. If
the PR run is absent, provider recovery requires a new reviewed PR head/event;
the missing prior subject remains `UNVERIFIED`. If the push run is absent, there
is no in-band backfill: after provider recovery, use a new reviewed successor
commit as the release candidate. No documented or executable recovery may
publish a substitute success.

### actionlint compatibility boundary

The implementation pins actionlint `1.7.12`. The official Linux amd64 archive
SHA-256 is
`8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8`.
The official Darwin arm64 archive SHA-256 is
`aba9ced2dee8d27fecca3dc7feb1a7f9a52caefa1eb46f3271ea66b6e0e6953f`.
CI downloads only
`https://github.com/rhysd/actionlint/releases/download/v1.7.12/actionlint_1.7.12_linux_amd64.tar.gz`
with HTTPS failure enabled, verifies the exact checksum before extraction into
`$RUNNER_TEMP/actionlint-1.7.12`, and executes that absolute binary path.
Developer documentation gives the corresponding Darwin checksum and permits an
already-installed binary only when the first line of `actionlint -version` is
exactly `1.7.12`; its later installation/compiler lines are diagnostic only. A
missing binary, failed download, checksum mismatch, extraction failure, or
version mismatch exits nonzero before linting. The Linux CI sequence is exact:

```bash
set -euo pipefail
ACTIONLINT_VERSION=1.7.12
ACTIONLINT_DIR="${RUNNER_TEMP}/actionlint-${ACTIONLINT_VERSION}"
ACTIONLINT_ARCHIVE="${RUNNER_TEMP}/actionlint_${ACTIONLINT_VERSION}_linux_amd64.tar.gz"
mkdir "${ACTIONLINT_DIR}"
curl --fail --silent --show-error --location --proto '=https' --tlsv1.2 \
  --output "${ACTIONLINT_ARCHIVE}" \
  "https://github.com/rhysd/actionlint/releases/download/v${ACTIONLINT_VERSION}/actionlint_${ACTIONLINT_VERSION}_linux_amd64.tar.gz"
printf '%s  %s\n' \
  '8aca8db96f1b94770f1b0d72b6dddcb1ebb8123cb3712530b08cc387b349a3d8' \
  "${ACTIONLINT_ARCHIVE}" | sha256sum --check --status
LC_ALL=C tar -xOf "${ACTIONLINT_ARCHIVE}" actionlint \
  > "${ACTIONLINT_DIR}/actionlint.tmp"
test "$(wc -c < "${ACTIONLINT_DIR}/actionlint.tmp" | tr -d ' ')" = "6074530"
printf '%s  %s\n' \
  'c872d6db8c6bf83a8eaa704fc93999f027d55dffbc63b8a6abdccb47df5f4cd4' \
  "${ACTIONLINT_DIR}/actionlint.tmp" | sha256sum --check --status
chmod 0755 "${ACTIONLINT_DIR}/actionlint.tmp"
mv "${ACTIONLINT_DIR}/actionlint.tmp" "${ACTIONLINT_DIR}/actionlint"
test "$("${ACTIONLINT_DIR}/actionlint" -version | sed -n '1p')" = "${ACTIONLINT_VERSION}"
```

Streaming only the exact `actionlint` archive member plus verifying its exact
6,074,530-byte length and binary SHA-256 prevents traversal, symlink, or
unrelated archive members from becoming executable. Tests cover wrong member,
directory/symlink substitution, changed size/digest, truncated stream, and
pre-existing destination. Installation uses a new runner-temp directory and
never overwrites a repository path.

Changed workflows without `queue` run through actionlint with no ignore. While
1.7.12 lacks the released grammar, only
`.github/workflows/airflow-pack-compat-nightly.yml` may use this exact
diagnostic waiver:

```text
unexpected key "queue" for "concurrency" section
```

The waiver is file-scoped, used only on that one original workflow byte, and
paired with YAML contract tests that require exactly one allowed `queue: max`
at the specified workflow concurrency location and forbid it beside a true
`cancel-in-progress`. All other diagnostics remain fatal. The waiver must be
removed in the first approved change after an upstream actionlint release
supports `concurrency.queue`; using an unreleased actionlint commit is forbidden.

Repository-wide pre-existing actionlint findings outside the implementation
allowlist are not silently reclassified and are not remediated in PR 3A. The
gate runs actionlint on every changed workflow in the exact allowlist. Every
invocation uses `-no-color -format '{{json .}}' -shellcheck '' -pyflakes ''`.
The non-queue invocation passes `ci.yml`, `airflow-pack-compat.yml`,
`pages.yml`, and `dependency-review.yml` together without an ignore and requires
exit `0`, stdout exactly `[]\n`, and empty stderr. The one queue-bearing file is
then checked twice.
Unwaived actionlint
must exit `1`, write one JSON array to stdout, and leave stderr empty. The array
must contain exactly one object with the exact original filepath,
`kind: "syntax-check"`, and full message
`unexpected key "queue" for "concurrency" section. expected one of "cancel-in-progress", "group"`.
Line, column, and snippet remain diagnostic. The waived invocation adds only
`-ignore '^unexpected key "queue" for "concurrency" section\. expected one of "cancel-in-progress", "group"$'`
to the common flags and exact filepath; it must exit `0`, write exactly `[]\n`
to stdout, and leave stderr empty. If the unwaived invocation exits `0`,
the compatibility test fails with `ACTIONLINT_QUEUE_WAIVER_OBSOLETE`; this is
the machine-enforced removal tripwire. Any extra object, extra stream byte, or
other exit code is a hard failure. The existing repository Ruff, mypy,
shell-focused tests, and workflow contract tests remain separate required gates.

The pin is not self-updating. An Upgrade PR changes the version, both applicable
archive checksums, version assertion, and release source atomically. Before it
may retain the waiver, it runs the new pinned binary unwaived. A queue-aware
release returns exit `0`, which deliberately triggers
`ACTIONLINT_QUEUE_WAIVER_OBSOLETE` until that same PR removes the ignore and old
expected-error branch. A still-incompatible release must reproduce the one exact
JSON object above. No floating `latest`, upstream branch, or unreleased commit is
accepted.

## Detailed implementation algorithm

1. Resolve and verify the exact implementation base; require that it contains
   this child specification with status `APPROVED`.
2. Snapshot SHA-256 and Git blob IDs for the five frozen release-sensitive
   workflows.
3. Perform the three exact-name label GETs defined above; require all three
   responses before changing Dependabot.
4. Modify `.github/dependabot.yml` to the exact two-entry contract.
5. Add `--locked` to only the declared project sync steps.
6. Make `airflow-pack-compat.yml` reusable without changing its direct triggers,
   cells, commands, action SHAs, artifact names, retention, or permissions.
7. Add the source-free nightly wrapper and validate the local call boundary.
8. Apply the Pages job-level capability split, stable whole-workflow
   concurrency, read-only current-master freshness guard, and same-attempt
   verification-output binding on deploy.
9. Reduce Dependency Review to native read-only PR and exact-master push
   contracts and delete dispatch/backfill.
10. Reconcile the existing workflow-security policy with those exact permission
    changes.
11. Update focused governance tests and every documentation path in the plan.
12. Run pinned changed-workflow actionlint, workflow/security tests, selected
    checks, strict docs, and the full non-live suite.
13. Re-query labels and compare frozen workflow blobs immediately before commit.
14. Push an immutable candidate, require the unchanged nineteen checks, owner
    attestation, and Agent PR receipt; do not bypass.

### Deterministic checks

The implementation contract includes commands equivalent to:

```bash
BASE_COMMIT=<merged-approved-child-spec-head>
git diff --exit-code "$BASE_COMMIT" HEAD -- \
  .github/workflows/release.yml \
  .github/workflows/runtime-image.yml \
  .github/workflows/certification-release-summary.yml \
  .github/workflows/route-certification-release.yml \
  .github/workflows/route-release-finalize.yml

uv run pytest \
  tests/test_ci_shadow_pr3a_contracts.py \
  tests/test_github_workflow_governance.py \
  tests/agent_policy/test_workflow_security.py -q
```

The implementation task contract cites the merged approved child-specification
commit, not this document's future branch SHA. The merge-base, selected
workflows, actionlint version/output, label readback, release blob identities,
test output, and hosted check run IDs are retained in the PR evidence.

### Failure and recovery matrix

| Failure | Decision | Recovery |
| --- | --- | --- |
| `uv.lock` stale or absent | FAIL; no implicit rewrite | run `uv lock --check` to reproduce, then `uv lock`; inspect `git diff -- pyproject.toml uv.lock`, commit the intended lock change, and rerun `uv sync --locked --all-extras` (or the Pages-specific `uv sync --locked`) |
| required label missing/inaccessible | UNVERIFIED; implementation merge blocked | owner creates/restores exact label, then repeats the three exact-label GETs |
| invalid workflow/actionlint finding | FAIL | correct the original workflow; never broaden the ignore |
| nightly queue reaches provider limit | cancelled run is not PASS | inspect exact `RUN_ID`; after capacity returns use `gh run rerun "$RUN_ID" --repo PaulKov/dpone`, or create a new dispatch only when no run exists and retain the earlier absence as UNVERIFIED |
| dependency vulnerability at high+ | FAIL | remediate or explicitly review dependency policy in a separate change |
| existing PR Dependency Review run failed or became unreadable before an authenticated native conclusion | UNVERIFIED/FAIL; PR merge blocked | after provider recovery, rerun that exact run ID; retain the interrupted attempt as UNVERIFIED; no backfill |
| PR Dependency Review run is absent | UNVERIFIED; PR merge blocked | after provider recovery, create a new reviewed PR head/event; the missing prior subject remains UNVERIFIED and receives no dispatch/backfill |
| exact-master Dependency Review run is absent, pending, failed, or unreadable | UNVERIFIED/FAIL; release exact-commit gate blocked | rerun an existing exact run by ID; if none exists, use a new reviewed successor commit after provider recovery; never synthesize or backfill a check |
| Pages PR build fails | FAIL required docs check | reproduce locked build and fix docs/lock |
| Pages subject is proven stale | FAIL; no deploy | preserve the obsolete run and dispatch a new run on current `master`; never rerun any job of the obsolete subject |
| Pages current-master lookup is unavailable or malformed | UNVERIFIED; no deploy | after provider recovery, preserve the run and dispatch a new run on current `master` |
| Pages build/upload failed | FAIL/UNVERIFIED according to the authenticated provider conclusion; no deploy | never rerun or delete/overwrite evidence; fix the cause and dispatch a new run on current `master` |
| Any non-PR rerun is requested | upload, verification, and deploy are ineligible or `SKIPPED`; deployment is `NOT_RUN/UNVERIFIED` | preserve the run and dispatch a new run on current `master` |
| Pages deploy fails after a current-master build | authenticated deployment failure, not PR retroactive failure | inspect environment/permissions; never rerun any existing non-PR job; dispatch a new run on current `master` |
| Pages deploy attempt differs from `1`, differs from `verified_attempt`, or verification outputs are missing | provider job `SKIPPED/SUCCESS`; deployment `NOT_RUN`; evidence `UNVERIFIED` | do not treat workflow/check success as deployment PASS; preserve the run and dispatch a new run on current `master` |
| release-sensitive workflow differs | hard FAIL | remove unrelated diff or use a separate approved release change |
| unexpected permission/write scope | hard FAIL | restore exact least-privilege map |

No state migration exists. GitHub run attempts and artifacts remain immutable
provider records. Every Pages recovery creates a new run and never relabels
prior evidence.

## Architecture

### Components and dependency direction

| Component | Responsibility | Boundary |
| --- | --- | --- |
| Dependabot YAML | hosted update scheduling | no runtime or label mutation logic |
| direct CI/Airflow workflows | PR/push/manual orchestration | thin composition roots; PR cancellation only |
| nightly wrapper | schedule, stable queue, reusable call | source-free and read-only |
| Pages workflow | read-only attempt-1 upload, attempt-1 freshness guard, protected deploy | stable whole-workflow serialization; write/OIDC isolated to deploy job |
| Dependency Review | native PR and exact-master dependency results | read-only; no manual or synthetic publication |
| workflow-security policy | exact write-capability exceptions | existing agent-policy module owns interpretation |
| governance tests | static public-contract proof | parse original YAML and compare frozen values |

No new dpone package, port, adapter, service locator, plugin, or public CLI is
needed. The existing workflow-security policy remains the single capability
authority. The Airflow compatibility implementation is reused through
`workflow_call` instead of copied.

### Alternatives and tradeoffs

| Alternative | Decision | Reason |
| --- | --- | --- |
| Put PR cancellation and nightly queue in one concurrency object | Reject | GitHub forbids `queue: max` with `cancel-in-progress: true` |
| Copy the Airflow compatibility workflow | Reject | duplicates a large security- and evidence-sensitive composition root |
| Call a reusable Airflow workflow from a source-free wrapper | Adopt | isolates concurrency policy and keeps one compatibility implementation |
| Queue only the Pages deploy job | Reject | build completion can invert commit order and publish an older site after a newer one |
| Serialize the whole Pages workflow and revalidate current master before deploy | Adopt | preserves least privilege and prevents an obsolete run or rerun from deploying |
| Trust a prior-attempt successful freshness prerequisite on deploy-only rerun | Reject | a specific-job rerun does not rerun prerequisites; same-attempt output binding is required |
| Re-run all non-PR Pages jobs with the default artifact name | Reject | the artifact is immutable within one run and a same-name upload can fail or become ambiguous |
| Use attempt-specific artifact names to support full rerun | Reject for PR 3A | deploy can select a dynamic name, but a new current-master dispatch is simpler |
| Rerun only the freshness job after artifact qualification | Reject | it adds provider-inventory and cross-attempt state without improving the safe new-run recovery |
| Dispatch a new current-master run after any non-PR failure | Adopt | one run ID, attempt `1`, and artifact namespace provide the smallest exact recovery identity |
| Keep Dependency Review comments/backfill writes | Reject | unnecessary write authority and false-certification surface |
| Use `--frozen` in CI | Reject | it does not prove the lock matches project metadata |
| Consume unreleased actionlint main/PR | Reject | unstable validator supply chain |
| Narrowly waive only released actionlint's known queue parse error | Adopt temporarily | all other lint and exact queue semantics remain enforced |

### ADR requirement

[ADR 0048](adr/0048-exact-sha-readiness-evidence.md) is amended by this design
to own the long-lived Pages attempt, recovery, artifact, and truthful evidence
semantics. ADR 0046 continues to own diagnostic PR-gate authority and least
privilege. Any change to required-check authority or release behavior would
exceed this child and require another prior design/ADR amendment.

### Quality-budget impact

Production Python SLOC and import edges do not change. New/changed test modules
must remain cohesive and below the repository hard limits. The implementation
must not place policy inside shell snippets when an existing agent-policy test
can own the invariant.

## Market and platform research

The named ETL/ELT products are N/A: this child changes dpone's repository CI,
not an end-user data integration capability.

| System | Relevance | Decision |
| --- | --- | --- |
| dlt | N/A — no GitHub repository control contract comparison | exclude |
| Informatica | N/A — no dpone contributor CI contract | exclude |
| Airbyte | N/A — no connector/runtime behavior changes | exclude |
| Fivetran | N/A — no managed ingestion behavior changes | exclude |
| Pentaho | N/A — no pipeline runtime change | exclude |
| Microsoft SSIS | N/A — no package/runtime change | exclude |
| gusty | N/A — no DAG authoring behavior change | exclude |
| Astronomer Cosmos | N/A — Airflow execution semantics remain unchanged | exclude |
| Apache Beam | N/A — no data-processing semantics change | exclude |

Official platform sources, verified 2026-08-10:

- [GitHub concurrency](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-workflow-concurrency): `queue: max`, provider limit 100, incompatibility with active cancellation, default replacement of an older pending run, and no guaranteed execution order;
- [GitHub workflow events](https://docs.github.com/en/actions/reference/workflows-and-actions/events-that-trigger-workflows): cron schedules, IANA timezones, provider delay semantics, and pull-request `GITHUB_SHA` binding to the test-merge commit rather than the later integration commit;
- [GitHub workflow REST API](https://docs.github.com/en/rest/actions/workflows?apiVersion=2026-03-10): exact workflow lookup and a versioned dispatch response containing `workflow_run_id`, `run_url`, and `html_url`;
- [GitHub workflow reruns](https://docs.github.com/en/actions/how-tos/manage-workflow-runs/re-run-workflows-and-jobs?tool=cli): exact run-ID rerun, retained SHA/ref, new attempt history, and specific-job reruns;
- [GitHub Actions workflow-run REST API](https://docs.github.com/en/rest/actions/workflow-runs?apiVersion=2026-03-10): an exact run exposes `head_branch`, `head_sha`, event, workflow ID, and attempt but no `ref`; a specific-job rerun runs that job and its dependent jobs, not its prerequisite jobs;
- [GitHub job conditions](https://docs.github.com/en/actions/how-tos/write-workflows/choose-when-workflows-run/control-jobs-with-conditions): a false job condition is skipped and reports check success rather than deployment success;
- [GitHub upload-artifact](https://github.com/actions/upload-artifact): artifacts are immutable, same-name upload fails by default, and overwrite is opt-in;
- [GitHub upload-pages-artifact](https://github.com/actions/upload-pages-artifact) and [deploy-pages](https://github.com/actions/deploy-pages): the default artifact name is `github-pages`, and deploy supports an explicit artifact name;
- [GitHub Dependabot options](https://docs.github.com/en/code-security/reference/supply-chain-security/dependabot-options-reference): `uv`, labels, schedule timezone, and open-PR limits;
- [GitHub Pages custom workflows](https://docs.github.com/en/pages/getting-started-with-github-pages/using-custom-workflows-with-github-pages): deploy job requires `pages: write` and `id-token: write`;
- [GitHub Dependency Review configuration](https://docs.github.com/en/code-security/how-tos/secure-your-supply-chain/manage-your-dependency-security/configure-dependency-review-action): a PR workflow can operate with `contents: read`;
- [uv locking and syncing](https://docs.astral.sh/uv/concepts/projects/sync/): `--locked` fails when the lock is stale; `--frozen` skips that check;
- [actionlint releases](https://github.com/rhysd/actionlint/releases/tag/v1.7.12) and [open queue support](https://github.com/rhysd/actionlint/pull/654): latest release and unreleased grammar gap.

## Measurable outcome

```yaml
axis: deterministic non-release CI with bounded hosted capacity
scenario: one stale PR head, one nightly burst, one stale lock, docs PR, dependency update PR
baseline: implicit lock update; one-pending concurrency; broad write permissions; synthetic check backfill
target:
  locked_sync: all declared project sync steps fail rather than mutate a stale lock
  cancellation: only an older head of the same PR is cancelled
  nightly_queue: up to 100 pending provider members, no running-work cancellation
  pages_monotonicity: an older SHA never deploys after a newer successful SHA
  pages_rerun_safety: deploy requires freshness outputs from the same run_attempt
  pages_artifact_recovery: every non-PR Pages job is attempt 1 only; recovery creates a new current-master run
  pages_evidence: skipped deploy is NOT_RUN/UNVERIFIED, never deployment PASS
  dependency_review: native read-only checks exist on both PR test-merge and exact master subjects
  permissions: zero Pages deploy scopes on PR build; zero Dependency Review write scopes
  release_diff: zero bytes across five frozen workflows
procedure: static contracts, actionlint, hosted burst/rerun observations, exact Git blob comparison
artifact: PR logs plus owner-attested exact-head receipt
limitations: GitHub queue and hosted scheduling are provider services, not durable dpone evidence
```

## Security, privacy, and operations

- No new secret, OIDC, environment, self-hosted runner, or write authority is
  introduced.
- The nightly wrapper executes only a local reusable workflow and declares
  `contents: read`.
- Pages OIDC/write authority is inaccessible to PR build steps.
- The current-master lookup executes in a separate `contents: read` job; the
  deploy job receives no token capability to reinterpret freshness.
- Pages deploy checks the provider attempt and verified SHA from the direct
  freshness dependency; missing or prior-attempt outputs cannot authorize OIDC.
- Non-PR Pages build/upload is ineligible after attempt `1`, preventing
  same-run reuse of the immutable `github-pages` name.
- Pages recovery never inspects, deletes, overwrites, or reuses a prior run's
  artifact; every recovery receives a new run ID and artifact namespace.
- Dependency Review cannot comment or publish checks through the token.
- Label evidence contains names and public metadata only; credentials are never
  printed or stored.
- Action SHAs remain pinned. PR 3A does not update action versions.
- Queue/cancellation identity is operational scheduling only, never evidence or
  merge authority.

## Test and certification plan

| Layer | Required scenario | Expected evidence |
| --- | --- | --- |
| Contract | exact Dependabot ecosystems/schedules/labels/limits/groups | parsed YAML assertions |
| Contract | direct PR groups, two PRs, stale same-PR head, nightly stable group | parsed expressions and fixture simulation |
| Contract | Airflow direct 2/nightly 4; runtime wheel 1; fail-fast false | matrix assertions |
| Security | Pages build/freshness/deploy scopes; Dependency Review read-only PR+push; policy parity | workflow-security tests |
| Contract | Pages attempt-1-only execution, new-run-only recovery, exact run/jobs/step deployment identity, skipped-evidence taxonomy | parsed policy plus transition simulation |
| Negative | release blob changed; unknown workflow diff; queue beside active cancel; stale Pages SHA/API failure; prior/missing attempt outputs; deploy-only/verify-only/full rerun; foreign ref/event/run/job/attempt/SHA; skipped false-PASS; missing exact-master Dependency Review | deterministic rejection |
| Tooling | pinned actionlint on each changed workflow; exact temporary queue waiver | zero unwaived diagnostics |
| Documentation | every CJM/recovery surface and release exclusion | docs contracts, check-docs, MkDocs strict |
| Broad | change-selected static, docs, governance, and full non-live pytest | exact-head logs |
| Live | all labels; PR cancellation; queued nightly burst/rerun; Pages PR; two rapid master Pages subjects; PR and exact-master dependency advisory fixtures | run IDs, attempts, conclusions, final deployed SHA, no-bypass receipt |

Boundary tests cover queue members 100/101, matrix values 0/1/2/4/5, Pages
attempt 1/output 1, attempt 2/output 1, missing output, attempt 2/output 2,
same-attempt stale SHA, removed/inverted attempt comparison, deploy-only rerun
after a newer deployment, non-PR build/verify/deploy attempt 1/2, PR build
attempt 1/2, foreign ref/event/run/job/attempt/SHA, zero/one/two deploy jobs,
first/middle/last Jobs pages and incomplete/API-failed pagination, skipped check
success versus deployment `NOT_RUN/UNVERIFIED`, auth and
first/middle/last label-GET failures, missing/extra/case-variant labels, stale
lock, missing lock, Pages PR/current/stale/default-branch events, current-ref API
failure, Dependency Review PR/push/manual decisions, exact-master missing versus
native check, actionlint waiver exact/non-exact messages, and
unchanged/one-byte-changed release blobs.

The implementation tests parse each original YAML file rather than searching
global repository text. For Pages they parse the actual `pages.yml`, not this
Markdown snippet, and require the non-PR attempt-1 build condition, exact verify
outputs/deploy expression, immutable artifact policy, and evidence taxonomy.
They mutate nightly and Pages concurrency independently:
missing/wrong/second `queue`, `cancel-in-progress: true`, an expression that can
be true, wrong group/location, or a value borrowed from the other workflow must
fail the exact owning-file assertion.

## Documentation plan

The implementation PR updates these exact user/developer surfaces:

- `docs/ci-cd.md`: workflow inventory, locked default CI, scheduling summary;
- `docs/cicd/workflows.md`: exact triggers, commands, concurrency, permissions,
  matrix ceilings, Dependabot schedules, monotonic Pages publication, and native
  PR plus exact-master Dependency Review results;
- `docs/cicd/runbooks.md`: stale-lock, missing-label, queue saturation,
  pinned-actionlint installation/streams/obsolete-waiver recovery, dependency
  advisory, stale Pages subject, prohibition of every non-PR rerun, new
  current-master versioned dispatch with returned run-ID validation, and
  skipped deployment evidence;
- `docs/developer-ci-cd.md`: locked sync, concurrency choice, least privilege,
  pinned actionlint version and exact per-path invocation, one-file waiver and
  removal tripwire, and release exclusion for workflow authors;
- Pages-only sections of `docs/cicd/release-and-pages.md` and
  `docs/github-pages.md`; release instructions remain unchanged;
- `docs/github-branch-protection.md`: read-only native PR and exact-master
  Dependency Review with explicit prohibition of manual/synthetic backfill;
- `docs/testing/overview.md` and `docs/testing/index.md`: local locked gate and
  exact actionlint exits/stdout/stderr expectations;
- `CHANGELOG.md`: one Unreleased CI-hygiene entry;
- `docs/quality-metrics.md`: regenerate through its producer.

This child specification is linked from its parent. It does not need a separate
top-level MkDocs navigation entry before approval; the implementation docs are
already navigable pages. If reviewers require a child-spec nav entry,
`mkdocs.yml` remains integrator-owned.

## Rollout and rollback

1. Review this `RESEARCHED` candidate and record the maintainer's authorization
   to transition the exact content.
2. Change this document to `APPROVED` in a new immutable candidate; rerun every
   selected gate and obtain exact-head owner attestation and Agent PR receipt.
   Approval is not inferred from chat or issue labels, and a `RESEARCHED` head
   never merges as the executable implementation authority.
3. Merge only that exact `APPROVED` specification commit without bypass.
4. Complete the owner-authorized `github-actions` label creation and exact
   readback. This is an implementation-readiness gate, not a specification
   approval gate.
5. Only after the label gate passes, create and validate
   `test_artifacts/agent-policy/dpone-ci-shadow-closure-pr3a-ci-hygiene.yml`
   against the merged approved spec/base.
6. Implement the allowlisted paths in one PR; update docs and generated metrics.
7. Run focused/broad/live evidence and require the unchanged nineteen checks,
   owner attestation, Agent PR receipt, and no bypass.
8. Observe at least one PR cancellation, one nightly run/rerun, one Pages PR,
   monotonic current-master deployment, and native Dependency Review on both a
   PR test-merge and exact master commit before closing PR 3A.

Rollback is a reviewed revert of the PR3A implementation commit. The revert
restores the prior workflows, Dependabot file, security policy, tests, and docs
as one unit. If a release-sensitive workflow changed, stop instead of trying to
repair it inside rollback. Hosted run history is retained; no result is deleted
or relabelled.

## Agent execution plan

The original specification used
`test_artifacts/agent-policy/dpone-ci-shadow-pr3a-spec.yml`. This reopened
security amendment uses the separately scoped
`test_artifacts/agent-policy/dpone-ci-shadow-pr3a-security-amendment.yml`; it
adds only the provider-bound contract, read-only receipt producer, focused
tests, and operator documentation. It changes no workflow or production path.

After merge, the implementation task contract assigns one integrator the shared
workflow, policy, changelog, and navigation files. Read-heavy architecture,
security, docs/CJM, and certification reviewers work independently; no parallel
writer may edit those shared semantic files.

| Role | Future implementation responsibility | Forbidden expansion |
| --- | --- | --- |
| Explorer | current workflow paths, tests, exact blob inventory | edits |
| Architect | reusable boundary, permission/cancellation review | release/authority redesign |
| Test certifier | actionlint, static/broad/live matrix | false PASS for skipped live evidence |
| Docs/UX reviewer | first-time journey and recovery | release prose changes |
| Integrator | all approved implementation paths and final reconciliation | paths outside allowlist |

## Approval checklist

- [x] User problem and CJM are explicit.
- [x] Exact paths, values, algorithms, failure/recovery, and rollback are frozen.
- [x] Permission, concurrency, validator, and release boundaries are explicit.
- [x] Platform research uses current primary sources.
- [x] Tests, live evidence, docs, and ownership are planned.
- [x] Maintainer changes the amended status to `APPROVED` on the exact reviewed commit.
- [ ] Exact-head Agent PR receipt and required checks pass without bypass.
