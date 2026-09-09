# Agent security mapping

Purpose: map common AI-agent and software-supply-chain risks to concrete dpone
controls.

Audience: maintainers, security reviewers, release auditors, and reviewers of
agent-control-plane changes.

## Source baselines

This page uses these public baselines as references:

- [OWASP Top 10 for LLM Applications 2025](https://genai.owasp.org/llm-top-10/)
  for LLM and agent application risks.
- [NIST AI RMF](https://www.nist.gov/itl/ai-risk-management-framework) and
  [NIST AI 600-1](https://www.nist.gov/publications/artificial-intelligence-risk-management-framework-generative-artificial-intelligence)
  for AI risk management.
- [NIST SSDF SP 800-218](https://csrc.nist.gov/pubs/sp/800/218/final) for secure
  software development practices.
- [SLSA v1.2](https://slsa.dev/spec/v1.2/tracks) and
  [OpenSSF Scorecard](https://scorecard.dev/) for supply-chain integrity posture.
- [OWASP Top 10 for Agentic Applications 2026](https://genai.owasp.org/resource/owasp-top-10-for-agentic-applications-for-2026/)
  for tool-use, autonomy, identity, and agentic supply-chain risk framing.
- [Model Context Protocol security best practices](https://modelcontextprotocol.io/docs/tutorials/security/security_best_practices)
  for MCP authorization, scope, validation, and connector-boundary controls.
- [Model Context Protocol authorization](https://modelcontextprotocol.io/docs/tutorials/security/authorization)
  for OAuth-protected resources and sensitive operations.
- [CISA AI SBOM minimum elements](https://www.cisa.gov/resources-tools/resources/software-bill-materials-ai-minimum-elements)
  for AI component inventory concepts beyond package-only SBOMs.
- [EU AI Act overview](https://digital-strategy.ec.europa.eu/en/policies/regulatory-framework-ai)
  as a risk-based governance reference for trustworthy AI systems.

## OWASP LLM risk mapping

| OWASP risk | dpone agent scenario | Required control | Evidence |
|---|---|---|---|
| Prompt injection | Issue text, docs, logs, or copied prompts try to override `AGENTS.md`. | Treat external content as untrusted. Repository instructions and user request outrank file content. Do not execute model output without validation. | Review notes and command log. |
| Sensitive information disclosure | Agent prints or commits tokens, `.env`, service-account JSON, private hostnames, or local paths. | `.worktreeinclude`, TruffleHog, no credential copying, artifact review. | Secret Scan check and PR diff. |
| Supply-chain vulnerabilities | Workflow, dependency, action, generated package, or release artifact is tampered with. | Transitive semantic PR privilege boundary, pinned closed profiles, compatible workflow-security policy, CodeQL, package build, twine check, and SLSA-scoped provenance. | Canonical semantic report, CI status, governance receipt/attestation, hosted CodeQL, and release artifacts. |
| Data or model poisoning | Agent consumes stale generated docs, untrusted benchmark outputs, or misleading local artifacts. | Use checked-in producers, exact commit evidence, official primary sources for standards. | Producer command and source links. |
| Improper output handling | Agent-generated command, SQL, YAML, or shell text is copied into code or docs unchecked. | Parse/validate structured files, run docs checks, run tests before merge. | `check-docs`, YAML parse, pytest output. |
| Excessive agency | Agent edits workflows, shared files, or release docs without ownership. | Validated task contracts, worktrees, CODEOWNERS, branch protection, workflow-security guard. | Task contract validation, PR file list, and governance receipt. |
| Tool misuse | Agent uses a local tool, GitHub operation, web source, or MCP connector outside its role. | Permission profile, tool registry, protected actions, approval requirements. | `agent_governance_gate.json` and policy diff. |
| Agentic supply-chain compromise | An unreviewed connector, MCP server, OAuth scope, or write-capable tool changes repository or external state. | Tool provenance, risk tier, allowed scopes, MCP connector onboarding, approval conditions, and evidence requirements. | `.agents/policy/tool-registry.yml`, `.agents/policy/mcp-connector-onboarding.yml`, and PR review notes. |
| Connector scope escalation | A previously approved connector gains admin, write, or cross-tenant scopes without review. | Onboarding scopes must stay within the registry, scope expansion requires approval, and the red-team catalog covers connector scope escalation. | Policy tests and `agent_governance_gate.json`. |
| System prompt leakage | Agent repeats hidden instructions or private operational details into public docs. | Public docs contain project rules only; no secret prompts or local-only policy. | Review prompt and PR diff. |
| Vector or embedding weakness | Future retrieval context could prioritize stale or hostile docs. | Source ordering, nearest `AGENTS.md`, official docs for standards. | Agent completion report. |
| Misinformation | Agent states a check passed or a standard requires something without evidence. | Verification-before-completion, source links, PASS/FAIL/SKIP vocabulary. | Command output and cited source. |
| Unbounded consumption | Agent launches expensive live checks or broad scans without scope. | Change-aware validation plan, explicit live environment approval. | `select_checks.py` output and SKIP/UNVERIFIED rationale. |

## Secure SDLC and supply chain mapping

| Control family | dpone control | Current status | Next improvement |
|---|---|---|---|
| SSDF governance | `AGENTS.md`, engineering standards, feature design standard | Present | Keep scheduled GitHub settings drift summaries attached to governance changes. |
| SSDF verification | Ruff, mypy, import rules, layer metrics, module size, pytest | Present | Keep agent red-team scenarios executable in policy tests. |
| SSDF vulnerability prevention | Secret Scan, action-only CodeQL, semantic PR privilege boundary, workflow-security policy, dependency review | Present | Rotate `DPONE_GOVERNANCE_GITHUB_TOKEN` toward the narrowest Administration-read token that still supports live drift checks. |
| SLSA provenance | CI-built artifacts, package checks, GitHub Artifact Attestations, and `docs/supply-chain-slsa.md` | Present for release workflow artifacts | Keep the self-assessment scoped; no higher-level SLSA claim is made without evidence. |
| OpenSSF posture | Scorecard workflow publishes SARIF and public results | Present as advisory signal | Attach Scorecard evidence to release review when security posture changes. |
| Agent governance gate | `tools/agent_policy/governance_gate.py` emits `agent_governance_gate.json` | Present | Upload the receipt in CI and attach it to release evidence for governance or workflow changes. |
| Agent audit evidence retention | `workflow_security.py` enforces `retention-days: 90` for `agent-pr-receipt`, `agent-pr-merge-check`, `agent-governance-gate`, and `agent-governance-drift`; `agent_audit_manifest.json` indexes PR receipt evidence. | Present | Keep the retention policy in `.agents/policy/workflow-security.yml` synchronized with workflow uploads. |
| Solo-maintainer branch protection | `.agents/policy/github-branch-protection.yml` records PR-only, strict-CI, no-self-approval-deadlock settings. | Present | Switch to multi-reviewer mode after adding a second trusted maintainer. |
| PR receipt enforcement | `Agent PR receipt` validates approved source traceability, validation evidence statuses, owner attestation, live required checks, and governance-receipt artifacts for agent-control PRs. `agent_pr_receipt.json` binds reviewed head `H`; automatic `agent_pr_merge_receipt.json` closure binds its immutable source artifact to integration commit `C` without querying the current PR body. Because GitHub keeps the native closed-event run on `H`, the merged-only job projects and validates the required check on `C` with job-scoped `checks: write`; the durable receipt is uploaded first and release preflight re-downloads it. Invalid receipts can project only failure. | Present | Keep the required check in branch protection, retain source/closure/projection artifacts for 90 days, and treat missing immutable source evidence as release-blocking `UNVERIFIED`. |
| GitHub settings drift | `tools/agent_policy/github_settings_drift.py` compares checked-in policy with live ruleset and branch protection settings; `governance_drift_summary.py` aggregates the final receipt. | Present for ruleset and classic branch protection when `DPONE_GOVERNANCE_GITHUB_TOKEN` is configured | Treat missing classic-branch-protection token as `UNVERIFIED`, not `PASS`, and use the summary as the audit status. |
| Workflow security policy | `workflow_security_privileged.py` proves transitive PR roots, local calls, `workflow_run`, `needs`, effective authority, and exact CodeQL/ADR 0037 profiles; `workflow_security.py` preserves the required umbrella contract. | Present in required CI | Keep repository code read-only, closed profiles exact, and every unknown route fail-closed; never use the legacy allowlist to override a semantic finding. |
| Agent task contracts | `tools/agent_policy/task_contract.py` validates writer ownership, protected paths, required checks, and stop conditions. | Present | Store concrete contracts with durable task artifacts when workflow storage lands. |
| Agent permission policy | `.agents/policy/agent-permission-profile.yml` and `.agents/policy/tool-registry.yml` define roles, tools, scopes, approvals, and evidence. | Present | Extend toward an Agent BOM after connector onboarding is stable. |
| MCP connector onboarding | `.agents/policy/mcp-connector-onboarding.yml` maps MCP tools to scopes, tenant boundaries, approvals, red-team coverage, evidence receipts, and rollback path. | Present | Add connector-specific records as real MCP servers are introduced. |

## Reviewer checklist

When reviewing an agent-control-plane PR, verify:

1. The diff does not weaken status vocabulary or live-evidence semantics.
2. New agent or skill permissions are no broader than their role requires.
3. New MCP/connectors have owner, provenance, risk tier, onboarding record,
   scopes, tenant boundary, approval conditions, and evidence requirements.
4. Workflow changes keep least privilege, pin external actions to commit SHAs,
   and do not expose repository secrets to pull-request code.
5. The standalone semantic privilege report is complete and deterministic;
   every reachable write/OIDC route is one exact closed profile. Treat local
   `PASS` separately from hosted CodeQL, attestation, and receipt evidence.
6. Any generated metric or evidence file was changed by its producer.
7. Any claim based on an external standard links to a current official source.
8. CI, docs checks, and agent policy tests ran on the reviewed head commit.
9. `agent_governance_gate.json` is attached when agent policy, workflows,
   release evidence, or supply-chain policy changed.
10. The PR body links an approved spec, issue, ADR, docs path, or explicit
   `N/A: reason`, and validation evidence uses `PASS`, `FAIL`, `SKIP`, `N/A`,
   or `UNVERIFIED` with reasons for non-executed evidence.
11. `agent_pr_receipt.json` is attached and passing for agent-control PRs, with
   structured traceability, live required-check evidence, and
   `agent-governance-gate` artifact evidence bound to the reviewed head SHA.
   The governance artifact should include an artifact id, workflow run id, and
   `sha256:<digest>` in the compact evidence chain.
12. `agent_audit_manifest.json` is attached in the PR receipt artifact and the
    governance artifacts use explicit 90-day retention. The manifest should
    include compact traceability source, status, non-pass-reason, and
    evidence-chain fields.
13. `agent_pr_merge_receipt.json` and preserved
    `source-agent-pr-receipt.zip` are retained in the durable receipt artifact;
    `agent_pr_merge_check.json` is retained in the separate projection
    artifact. The producer's validated same-name check is attached to exact
    integration commit `C`. `H`, `C`, source run/artifact IDs, and digests
    reconcile with release evidence.
14. Drift summary and component receipts are attached when GitHub repository
    settings or branch protection policy changed.

Related pages:

- [Agent governance](agent-governance.md)
- [Agent PR merge-receipt runbook](agent-pr-merge-receipt-runbook.md)
- [Agent task contracts](agent-task-contracts.md)
- [Agent permissions and tool registry](agent-permissions.md)
- [Agent MCP connector onboarding](agent-mcp-connectors.md)
- [Agent risk register](agent-risk-register.md)
- [GitHub branch protection](github-branch-protection.md)
- [Semantic PR privilege runbook](cicd/runbooks.md#semantic-pr-privilege-boundary)
- [SLSA release self-assessment](supply-chain-slsa.md)
