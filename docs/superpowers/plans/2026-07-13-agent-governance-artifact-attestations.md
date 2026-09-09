# Agent governance artifact attestations plan

Date: 2026-07-13
Integrator: Codex
Worktree: `.worktrees/agent-governance-attestations`
Approved spec: `docs/feature-design-agent-governance-artifact-attestations.md`

## Goal

Make agent-control PR receipts fail closed unless the matching
`agent-governance-gate` artifact contains a verified GitHub Artifact
Attestation for the exact `agent_governance_gate.json` subject produced by the
expected dpone CI workflow.

## Steps

1. Add failing tests for attestation normalization, missing-attestation receipt
   failure, compact evidence-chain fields, audit-manifest fields, and workflow
   expectations.
2. Add a small attestation verifier module around `gh attestation verify`.
3. Expose exact governance JSON bytes from downloaded artifact archives.
4. Wire optional attestation verification into live GitHub receipt evidence and
   add `--require-github-attestation`.
5. Copy compact attestation evidence into receipt payloads, evidence chain, and
   audit manifest.
6. Add CI attestation generation and document the new workflow write
   permissions in `.agents/policy/workflow-security.yml`.
7. Update schemas, setup inventory, and operator documentation.
8. Run focused tests, generated change-aware checks, and broad agent-policy
   validation.

## Required checks

- `uv run pytest tests/agent_policy/test_governance_artifact_attestation.py -q`
- `uv run pytest tests/agent_policy/test_pr_receipt.py tests/agent_policy/test_pr_receipt_evidence_chain.py tests/agent_policy/test_audit_manifest.py tests/agent_policy/test_workflow_security.py tests/test_github_workflow_governance.py -q`
- `uv run python tools/agent_policy/workflow_security.py .`
- `uv run python tools/agent_policy/validate_setup.py .`
- `uv run python tools/agent_policy/select_checks.py --base-ref origin/master`
- Broader checks selected by the change-aware plan and AGENTS.md.

## Risks

- GitHub pull-request contexts can restrict `attestations: write` on some fork
  scenarios; if this blocks legitimate internal PRs, roll back the required flag
  while keeping verifier code for manual use.
- `gh attestation verify --format json` output may add fields over time; parsing
  must be tolerant and rely on `gh` exit status plus enforced flags for policy.
- The first live PR is the authoritative integration proof because local tests
  cannot mint GitHub OIDC attestations.
