# Agent governance

Purpose: define how dpone uses AI agents safely, who owns the controls, and
what evidence proves the controls worked.

Audience: maintainers, reviewers, release auditors, and security reviewers.

## Scope

This page governs repository-local AI agents, skills, prompts, issue templates,
review prompts, generated evidence, and workflow settings that influence how
agents plan, edit, validate, and report changes.

The goal is operational control, not ceremony. A useful agent may write code,
documentation, tests, and release evidence, but every important action must have
a clear owner, permission boundary, and verification trail.

## Operating model

| Area | Owner | Required evidence |
|---|---|---|
| Durable repository instructions | Maintainer | `AGENTS.md`, nested `AGENTS.md`, `validate_setup.py` |
| Agent roles and skills | Maintainer | `.codex/agents/**`, `.agents/skills/**`, metadata tests |
| Agent permissions and tools | Maintainer | `.agents/policy/agent-permission-profile.yml`, `.agents/policy/tool-registry.yml`, `agent_governance_gate.json` |
| MCP connector onboarding | Maintainer | `.agents/policy/mcp-connector-onboarding.yml`, scope review, connector evidence receipt |
| Path ownership | Integrator | validated task contract, CODEOWNERS, PR changed files |
| Change-aware validation | Implementer and integrator | `select_checks.py` output and executed commands |
| Security and supply chain | Maintainer | branch protection, semantic PR privilege report, compatible workflow-security guard, drift summary, secret scan, hosted CodeQL |
| PR review receipt | Owner | approved source, validation evidence, checked owner attestation, `agent_pr_receipt.json`, `agent_governance_gate.json` |
| Release evidence | Release auditor | R1-R9 evidence report from a frozen commit |

## Solo-maintainer mode

dpone currently has one human owner. GitHub does not allow a pull-request author
to approve their own pull request, so requiring a CODEOWNER approval from the
same sole owner creates an impossible merge condition. The repository therefore
uses solo-maintainer mode:

- `master` still requires a pull request;
- required GitHub checks remain strict and up to date;
- force pushes and branch deletion remain blocked;
- review conversation resolution remains required;
- CODEOWNERS remains the ownership map, but not a required approval gate;
- the PR must include owner attestation when no independent reviewer exists;
- `Agent PR receipt` must pass for PRs that change agent-control files.

The desired state is recorded in
`.agents/policy/github-branch-protection.yml` and validated by the agent
governance gate. Switch to multi-reviewer mode only after adding a second trusted
maintainer or team that can approve PRs independently.

## Control lifecycle

1. **Inventory**: record agent-facing files in `tools/agent_policy/validate_setup.py`.
2. **Classify**: route changed paths through `tools/agent_policy/select_checks.py`.
3. **Constrain**: use CODEOWNERS, branch protection, task contracts, worktrees,
   permission profiles, and the tool registry.
4. **Verify**: run focused checks, docs checks, and broad CI gates.
5. **Record**: completion reports list commands, status, artifacts, skipped work,
   and remaining risk.
6. **Review**: revisit the risk register whenever an agent incident, false pass,
   prompt injection, workflow change, or release blocker occurs.
7. **Red-team**: rerun the prompt injection, secret disclosure, workflow
   tampering, verification laundering, excessive agency, unbounded consumption,
   unregistered tool use, and connector scope escalation scenarios when agent
   controls change. The
   executable scenario catalog is `evals/agent/red_team_scenarios.yml`; validate it with
   `uv run python tools/agent_policy/red_team.py evals/agent/red_team_scenarios.yml`.
8. **Gate**: generate an executable governance receipt whenever agent controls,
   workflows, release evidence, or supply-chain policy change. Prepare the
   locked project environment once with `uv sync --locked --all-extras`; that
   prerequisite may access the network, write the environment or uv cache, and
   use stderr, so it is outside the scanner process contract. Then run:

   ```bash
   uv run python tools/agent_policy/governance_gate.py \
     --base-ref origin/master \
     --output test_artifacts/agent-policy/agent_governance_gate.json
   uv run python tools/agent_policy/task_contract.py \
     docs/agent-templates/agent-task-contract.yml \
     --template
   uv run --locked --no-sync --offline --no-python-downloads python -B \
     tools/agent_policy/workflow_security_privileged.py \
     --root . \
     --format text
   uv run --locked --no-sync --offline --no-python-downloads python -B \
     tools/agent_policy/workflow_security.py .
   uv run dpone docs check-module-size \
     --package tools/agent_policy \
     --no-baseline \
     --warn-lines 350 \
     --max-lines 400 \
     --warn-sloc 300 \
     --max-sloc 350
   uv run dpone docs check-module-size \
     --package tests/agent_policy \
     --no-baseline \
     --warn-lines 350 \
     --max-lines 400 \
     --warn-sloc 300 \
     --max-sloc 350
   ```

   The local command records the checked-out commit. In pull-request CI,
   `--head-commit` is reserved for the trusted event head `H`, because GitHub
   checks out and attests a synthetic `refs/pull/<number>/merge` commit `M`.
   An explicit empty, short, uppercase, or otherwise non-canonical Git SHA is
   rejected; do not use the option to relabel local evidence.

   The standalone semantic scanner reads a stable bounded snapshot and proves
   the complete pull-request-reachable workflow graph. A local `PASS` requires
   every reachable job to be unprivileged or one exact mandatory closed
   profile. `FAIL` and `UNVERIFIED` both block integration and cannot be
   overridden by the legacy write-scope policy. The existing umbrella remains
   the required compatibility surface and appends semantic findings after its
   existing errors.

   The scanner's read-only, no-network, no-file-mutation, and stream guarantees
   are process-scoped. For exact evidence, bypass the uv wrapper with the direct
   `.venv/bin/python -B` commands in the
   [testing reference](testing/index.md#semantic-pr-privilege-boundary-gate).

   CI keeps repository-controlled governance production read-only. The
   `governance-source` job generates and uploads the exact JSON subject with
   provider artifact ID/digest outputs; the source-free
   `governance-attestation` job only downloads that same-run artifact by ID and
   attests the exact file. The attestation proves byte provenance, not semantic
   `PASS`. `Agent PR receipt` still validates the JSON and reviewed-head
   binding. Neither internal job creates a new required branch-protection
   context or publisher.

   The receipt ties agent inventory, permission profiles, tool registry, red-team
   coverage, MCP connector onboarding, task-contract template validity,
   workflow-security policy, agent-policy module-size guards, release
   attestations, OSSF Scorecard posture, and SLSA self-assessment into one
   auditable artifact. The scheduled `Agent Governance Drift` workflow also
   uploads `agent-governance-drift-summary.json`; use that summary as the final
   repository-settings drift status, and use the component receipts only for
   diagnosis.
9. **Receipt**: when the PR changes the agent control surface, the
   `Agent PR receipt` check validates that the PR body has an approved
   specification, issue, ADR, docs path, or explicit `N/A: reason`; at least one
   validation evidence row with `PASS`, `FAIL`, `SKIP`, `N/A`, or `UNVERIFIED`;
   a reason for `SKIP`, `N/A`, and `UNVERIFIED`; checked owner attestation;
   checked required-checks attestation; checked admin-bypass attestation;
   checked governance-receipt attestation; and a reference to
   `agent_governance_gate.json` or `agent-governance-gate`. For agent-control
   PRs the check also reads live GitHub evidence for the reviewed head commit:
   required checks from the live ruleset, check-run/status results for the PR
   head SHA, and the `agent-governance-gate` artifact uploaded from that same
   head SHA. The governance artifact must also expose a GitHub artifact id,
   workflow run id, `sha256:<digest>` metadata, and positive metadata size; a
   stale, expired, digestless, or sizeless governance artifact is a receipt
   failure. The receipt then downloads the artifact archive, computes the local
   downloaded archive SHA-256 and byte length, requires both to match GitHub
   artifact metadata, and validates exactly one `agent_governance_gate.json`:
   schema version 1, `status: PASS`, `control_surface_changed: true`, changed
   paths matching the PR changed paths, and
   `changed_control_surface_red_team: PASS`. Missing, duplicate, invalid,
   stale, or non-PASS governance content is a receipt failure. The receipt also
   verifies a GitHub Artifact Attestation for that extracted JSON subject with
   `gh attestation verify`, requiring the dpone repository, the
   `PaulKov/dpone/.github/workflows/ci.yml` signer workflow, GitHub's OIDC
   issuer, the SLSA provenance predicate, GitHub-hosted runner provenance, and
   source ref `refs/pull/<current PR number>/merge`. The source digest is the
   canonical synthetic merge commit `M`; reviewed head `H` remains bound by
   workflow-run metadata and the signed JSON `head_commit`.
   Missing, failing, wrong-signer, wrong-predicate, or self-hosted-runner
   attestation evidence is a receipt failure. The
   `agent_pr_receipt.json` artifact includes a structured `traceability` object
   with the approved source, source kind, validation rows, validation statuses,
   non-pass reasons, owner-attestation booleans, and governance-receipt
   reference state. It also includes a compact `evidence_chain` object that
   links the reviewed head SHA to required checks, check ids and workflow run ids
   where GitHub exposes them, and the governance artifact id, run id, head SHA,
   metadata digest, local archive SHA-256, local archive size, content status,
   content changed paths, key governance check statuses, and compact attestation
   fields: status, predicate type, subject SHA-256, source repository, source
   ref, source digest, signer workflow, issuer, timestamp count, runner
   environment, and errors. Non-agent PRs record `N/A` with `traceability: null` and
   `evidence_chain: null` instead of weakening the check. The V2 receipt
   observes `opened`, `reopened`, `synchronize`, and `edited`. It captures the
   reviewed head `H`, polls only bounded retryable publication lag (five to
   sixty second backoff, thirty-minute deadline), rereads the live PR head
   before body refresh and again before PASS, and records `STALE_HEAD`,
   `TIMEOUT`, or terminal failure as a non-PASS result. Per-PR concurrency
   cancels an obsolete receipt run; provider cancellation is never evidence.
   Push a new head and obtain its own receipt instead of rerunning stale work.

   The semantic privilege scanner selects immutable V1 when no V2 file exists.
   When `.agents/policy/workflow-security-privileged-v2.yml` is present it
   records V2 in the generated report and requires its exact schema, the
   byte-exact V1 digest, and the approved V2-amendment merge binding. A missing
   or malformed V2 binding is `UNVERIFIED`; it never falls back to V1. Repair
   by reverting to the last approved policy commit or by submitting a new
   approved amendment—never by editing V1.
   Before opening or finalizing an agent-control PR, generate and preflight the
   body locally:

   ```bash
   uv run python tools/agent_policy/pr_body.py render \
     --approved-source docs/feature-design-agent-pr-body-preflight.md \
     --output test_artifacts/agent-policy/pr-body.md
   uv run python tools/agent_policy/pr_body.py check \
     --phase draft \
     --body-file test_artifacts/agent-policy/pr-body.md \
     --changed-paths tools/agent_policy/pr_body.py
   ```

   Use `--phase final` after CI has produced the reviewed head's required checks
   and `agent-governance-gate` artifact, and after owner attestation has been
   updated. The local final preflight checks Markdown and attestation grammar;
   the GitHub `Agent PR receipt` check remains authoritative for live required
   checks and artifact freshness.
   After merge, the same workflow automatically derives
   `agent_pr_merge_receipt.json` from the immutable pre-merge artifact. It keeps
   reviewed head `H` separate from integration commit `C`, requires exact
   parent/tree/path identity, and records the source check, workflow run,
   artifact, archive, and inner-file digests. It does not query the current PR
   body. GitHub keeps the native closed-event workflow run on `H`, so the
   merged-only job uses documented job-scoped `checks: write` to project the
   validated required check onto `C`; missing or invalid evidence can project
   only failure. A missing or expired source artifact remains `UNVERIFIED` and
   blocks release; it is never reconstructed as `PASS`. See the
   [Agent PR merge-receipt runbook](agent-pr-merge-receipt-runbook.md).
10. **Retention**: agent governance evidence is retained as GitHub Actions
    artifacts for 90 days. The required artifacts are `agent-pr-receipt`,
    `agent-pr-merge-check`, `agent-governance-gate`, and
    `agent-governance-drift`; the
    workflow-security policy fails CI if one of these artifacts loses explicit
    `retention-days: 90`. The PR receipt artifact also includes
    `agent_audit_manifest.json`, a compact index with PR number, head SHA,
    optional merge SHA, receipt status, required-check snapshot, and referenced
    governance artifacts. It also copies compact traceability fields:
    `traceability_source`, `traceability_source_kind`, `traceability_statuses`,
    and `traceability_non_pass_reasons`. The manifest also copies compact
    evidence-chain fields: `evidence_chain_head_sha`,
    `evidence_chain_required_checks`, and
    `evidence_chain_governance_artifact`, including metadata digest, local
    archive SHA-256 and size, governance content status, changed paths, and key
    check statuses plus compact attestation fields. Use the manifest first for
    audit triage, then open the full receipt JSON for diagnosis.
    The post-merge receipt artifact additionally preserves the exact selected
    source archive as `source-agent-pr-receipt.zip` and is uploaded before a
    success check is possible. The separate projection artifact records the
    safe provider response in `agent_pr_merge_check.json`; release preflight
    re-fetches and verifies the live check/App/run plus durable receipt bytes.

## Standards alignment

This governance model is intentionally lightweight but maps to current industry
control families:

- [NIST AI RMF](https://www.nist.gov/itl/ai-risk-management-framework):
  govern, map, measure, and manage AI risks.
- [NIST AI 600-1 GenAI Profile](https://www.nist.gov/publications/artificial-intelligence-risk-management-framework-generative-artificial-intelligence):
  treat generated content, tool use, provenance, and misuse risk as explicit
  risk-management subjects.
- [ISO/IEC 42001 overview](https://www.iso.org/home/insights-news/resources/iso-42001-explained-what-it-is.html):
  maintain a management system for responsible AI use with policies, objectives,
  risk controls, and continual improvement.
- [NIST SSDF SP 800-218](https://csrc.nist.gov/pubs/sp/800/218/final):
  integrate secure software development practices into normal SDLC work.

## Required maintainer review surfaces

The following changes require maintainer attention even when CI is green. In
solo-maintainer mode that evidence is owner attestation in the PR; in
multi-reviewer mode it is an independent approving review:

- root or nested `AGENTS.md`;
- `.codex/**`, `.agents/**`, and `evals/agent/**`;
- `tools/agent_policy/**`;
- `.github/workflows/**`, `.github/CODEOWNERS`, and PR or issue templates;
- agent governance, risk, security, release, and branch-protection docs;
- changes that alter validation status vocabulary or make a skipped check look
  like a pass.

## Incident triggers

Open or update `docs/agent-risk-register.md` when any of these happens:

- an agent edits outside its owned paths;
- a PR claims `PASS` without fresh evidence;
- live certification is mocked, skipped, or stale but reported as complete;
- a workflow, prompt, or external document tries to override repository rules;
- a secret, token, credential, local path, or private endpoint appears in agent
  output or committed evidence;
- generated metrics, docs, or release evidence drift from their producer.
- an MCP server, connector, OAuth scope, tenant boundary, or write operation is
  added without onboarding evidence.
- GitHub repository settings drift from the checked-in branch-protection policy.
- the semantic privilege scanner reports an unknown route, mandatory-profile
  drift, non-deterministic output, or an internal report failure.

## Related pages

- [Agent-assisted development](agent-development.md)
- [Agent task contracts](agent-task-contracts.md)
- [Agent permissions and tool registry](agent-permissions.md)
- [Agent MCP connector onboarding](agent-mcp-connectors.md)
- [Agent risk register](agent-risk-register.md)
- [Agent security mapping](agent-security-mapping.md)
- [GitHub branch protection](github-branch-protection.md)
- [Semantic PR privilege runbook](cicd/runbooks.md#semantic-pr-privilege-boundary)
- [SLSA release self-assessment](supply-chain-slsa.md)
