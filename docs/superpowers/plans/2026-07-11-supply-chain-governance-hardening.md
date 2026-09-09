# Supply-chain governance hardening

## Goal

Close the remaining governance gaps after the agent control-plane rollout:
release artifact provenance, dependency review before merge, auditable
break-glass bypasses, pinned GitHub Actions, and executable agent red-team
scenarios.

## Scope

- Add GitHub Artifact Attestations for release distributions before PyPI
  publishing and GitHub release creation.
- Add a pull-request dependency review workflow and document it as a required
  branch-protection check.
- Add a break-glass/admin-bypass issue template and policy guardrail.
- Require all third-party GitHub Actions in workflows to be pinned to full
  commit SHA refs.
- Promote agent red-team scenarios from prose-only controls to a checked
  machine-readable scenario catalog with a local validator.

## Verification

- Focused contract tests for release workflow, agent policy, dependency review,
  and GitHub Action pinning.
- `tools/agent_policy/validate_setup.py` must validate the new policy files and
  red-team scenario catalog.
- Workflow YAML must parse after action pinning.
- Existing formatting, typing, docs, and pytest gates must remain green.
