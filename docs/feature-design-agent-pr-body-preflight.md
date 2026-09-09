# Feature design: agent PR body preflight

- Status: APPROVED
- Owner: PaulKov
- Origin: #275 agent governance hardening
- Target release: next patch
- Last verified: 2026-07-13

## Executive summary

Agent-control PRs now have a strict `Agent PR receipt`, but the maintainer or
agent can still write a PR body that is correct in intent and wrong in grammar.
PR #308 exposed that failure mode: the approved source was present as a heading,
but not in the exact machine-readable bullet form required by
`pr_traceability.py`.

This change adds a local PR body preflight and generator for agent-control
changes. The generator emits the exact Markdown grammar expected by the receipt.
The preflight validates draft bodies before opening a PR and final bodies before
asking GitHub to rerun `Agent PR receipt`.

## Personas and journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Maintainer | Open an agent-control PR with a valid body on the first try. | A nearly-correct PR body can burn one CI cycle. | Local preflight catches grammar errors before GitHub CI. |
| Agent integrator | Produce owner-attestation Markdown without memorizing regexes. | Must copy exact labels from docs and template. | `render` prints the exact required sections and labels. |
| Auditor | Understand whether a PR body failed because evidence is unavailable or because grammar is malformed. | Receipt errors are only visible after workflow execution. | `check --phase draft` reports traceability errors locally. |

The intended journey is:

1. Generate a draft PR body locally with the approved source, scope, validation
   rows, and default unchecked owner attestation.
2. Run draft preflight against the body and changed paths.
3. Open the PR and wait for CI/governance artifacts.
4. Update the owner attestation and governance artifact metadata.
5. Run final preflight locally, then let `Agent PR receipt` run on the PR body
   edit.

## Scope

### In scope

- Add a focused `tools/agent_policy/pr_body.py` helper with `render` and
  `check` subcommands.
- Keep draft preflight limited to PR-body traceability grammar and validation
  evidence statuses.
- Keep final preflight aligned with `pr_receipt.validate_pr_receipt()` without
  requiring live GitHub evidence.
- Add focused tests for generator output, draft preflight, final preflight, and
  CLI JSON/text output.
- Document the local workflow in agent governance and branch-protection docs.

### Non-goals

- No change to branch protection, required checks, GitHub Actions triggers, or
  merge policy.
- No live GitHub API calls from the new preflight helper.
- No replacement of the authoritative `Agent PR receipt` CI check.
- No runtime ETL, connector, Airflow, dbt, schema, or packaging behavior.

## Public contract

This is a repository-local agent tooling contract, not a dpone runtime API.

`tools/agent_policy/pr_body.py render` writes Markdown to stdout or `--output`.
It uses exact labels required by `Agent PR receipt`, including:

```markdown
- Approved specification or issue: docs/feature-design-example.md
```

`tools/agent_policy/pr_body.py check --phase draft` validates only local
traceability grammar:

- approved source is a link, issue id, docs path, ADR, or `N/A: reason`;
- validation evidence includes at least one recognized status;
- `SKIP`, `N/A`, and `UNVERIFIED` rows include a reason.

`tools/agent_policy/pr_body.py check --phase final` validates the same PR body
shape used by `Agent PR receipt` without fetching live GitHub evidence. GitHub
CI remains the source of truth for live required-check and artifact freshness.

Exit codes:

- `0`: preflight status is `PASS` or `N/A`;
- `1`: preflight status is `FAIL`.

## Algorithm

### Render

1. Read CLI fields for problem, solution, impact, approved source, scope,
   non-goals, validation rows, and governance receipt text.
2. Normalize validation rows into `Check | Status | Command/workflow |
   Artifact/notes` table rows.
3. Emit the PR template sections with exact receipt-compatible labels.
4. Leave owner attestation unchecked by default so the generated draft cannot
   falsely claim final CI evidence.

### Draft check

1. Normalize changed paths through `control_surface.py`.
2. Return `N/A` when no agent-control path changed.
3. Extract traceability through `pr_traceability.extract_traceability()`.
4. Validate traceability through `pr_traceability.validate_traceability_payload()`.
5. Return `PASS` when errors are empty, otherwise `FAIL`.

### Final check

1. Call `pr_receipt.validate_pr_receipt()` with the PR body and changed paths.
2. Do not set `require_github_evidence`; local final preflight checks Markdown,
   attestation, and governance-reference shape only.
3. Return the same status/errors as the receipt validator.

## Failure semantics

- A malformed draft body fails locally and does not imply GitHub CI state.
- A `final` PASS does not mean the PR is mergeable; live GitHub evidence still
  belongs to `Agent PR receipt`.
- Non-agent PRs return `N/A` so the helper can be used safely in broad scripts.
- Missing files, malformed validation rows, and placeholder sources fail closed.

## Compatibility

The helper is additive. Existing PR templates, workflows, receipt JSON schemas,
and generated artifacts keep their current contracts. Rollback is a normal
revert; CI receipt enforcement remains authoritative.

## Documentation and CJM

Update the agent governance docs to show the local preflight loop before opening
or finalizing an agent-control PR. Update branch-protection docs to include the
preflight as a local diagnosis step for PR-body failures.

## Market and platform comparison

| System | Applicability | Fact | Adopted | Rejected | Source |
|---|---|---|---|---|---|
| GitHub pull request templates | Relevant platform | GitHub supports repository PR templates to standardize PR body content. | Keep `.github/pull_request_template.md` as the human-facing template and generate the same grammar locally. | Do not rely on the template alone; humans and agents can still edit it incorrectly. | https://docs.github.com/en/communities/using-templates-to-encourage-useful-issues-and-pull-requests/about-issue-and-pull-request-templates, checked 2026-07-13 |
| GitHub `pull_request` events | Relevant platform | Workflows can run on PR activity including body edits when configured. | Keep final authority in `Agent PR receipt` on PR-body edit. | Do not add a separate workflow for preflight. | https://docs.github.com/actions/using-workflows/events-that-trigger-workflows, checked 2026-07-13 |
| GitHub Actions artifacts | Relevant platform | Upload-artifact exposes SHA-256 digest metadata for workflow artifacts. | Preserve final artifact validation in `Agent PR receipt`. | Do not duplicate live artifact fetching in local preflight. | https://docs.github.com/en/actions/tutorials/store-and-share-data, checked 2026-07-13 |
| dlt | N/A | Data loading framework, not GitHub PR governance tooling. | N/A | N/A | N/A, reason: no comparable PR-body preflight layer. |
| Informatica | N/A | Data integration/governance platform, not repository PR receipt grammar. | N/A | N/A | N/A, reason: no comparable GitHub PR-body preflight layer. |
| Airbyte | N/A | ELT connector platform, not repository PR receipt grammar. | N/A | N/A | N/A, reason: no comparable PR-body preflight layer. |
| Fivetran | N/A | Managed ELT platform, not repository PR receipt grammar. | N/A | N/A | N/A, reason: no comparable PR-body preflight layer. |
| Pentaho | N/A | ETL/BI tooling, not GitHub PR governance tooling. | N/A | N/A | N/A, reason: no comparable PR-body preflight layer. |
| SSIS | N/A | ETL runtime/design tooling, not GitHub PR governance tooling. | N/A | N/A | N/A, reason: no comparable PR-body preflight layer. |
| gusty | N/A | Airflow DAG generation helper, not PR governance tooling. | N/A | N/A | N/A, reason: no comparable PR-body preflight layer. |
| Astronomer Cosmos | N/A | dbt/Airflow orchestration integration, not PR governance tooling. | N/A | N/A | N/A, reason: no comparable PR-body preflight layer. |
| Apache Beam | N/A | Distributed data processing model, not GitHub PR governance tooling. | N/A | N/A | N/A, reason: no comparable PR-body preflight layer. |

## Validation plan

- Focused TDD tests for render and check behavior.
- Agent-policy test suite.
- Agent-policy strict module-size guards for tools and tests.
- Standard ruff, format, mypy, import/layer/module guards, docs checks, strict
  MkDocs, and broad non-live pytest selected by `select_checks.py`.

## Rollout and rollback

Roll out through a normal agent-control PR. The PR body for this change should
use the generated grammar and final receipt evidence. Rollback is a normal
revert of the helper, tests, and docs; CI receipt enforcement remains intact.
