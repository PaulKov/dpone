# Agent MCP connector onboarding

Purpose: define the review gate for MCP servers, Codex connectors, document
sessions, and other external tool bridges before an AI agent may use them.

Audience: maintainers, integrators, security reviewers, and anyone adding or
using agent-facing connectors.

## Files

| File | Purpose |
|---|---|
| `.agents/policy/tool-registry.yml` | Registers the tool class and its risk tier, profiles, scopes, approvals, and evidence. |
| `.agents/policy/mcp-connector-onboarding.yml` | Records connector-specific controls: provenance, scopes, tenant boundary, token handling, operation mode, red-team coverage, and rollback path. |
| `evals/agent/mcp-connector-onboarding.schema.json` | Public schema for onboarding records. |
| `tools/agent_policy/permissions.py` | Thin compatibility facade for policy validation. |
| `tools/agent_policy/mcp_onboarding.py` | Validates registry and onboarding records together. |

## When onboarding is required

Create or update an onboarding record before any agent uses:

- a new MCP server;
- a new Codex connector or plugin that bridges to an external system;
- a broader OAuth scope, tenant, workspace, or document session;
- a connector write operation that was not explicitly approved in the task;
- a connector whose output could influence repository instructions, tests, or
  release evidence.

## Required controls

Every approved MCP/connector entry must cover:

| Control | Reviewer question |
|---|---|
| Server identity and provenance review | Do we know what connector/server is being used and who owns it? |
| OAuth scope review | Are scopes minimal, explicit, and no broader than the tool registry allows? |
| Data classification | What data can the connector read or write? |
| Tenant boundary review | Which workspace, tenant, repository, document, or account is in scope? |
| Operation mode split | Are reads, writes, admin actions, and installs separated? |
| Prompt injection boundary | Is connector output treated as untrusted task input? |
| Approval gate | Which actions require explicit maintainer or user approval? |
| Evidence receipt | What artifact proves the connector was used safely? |
| Revocation and rollback path | How do we disable or revert the connector if it misbehaves? |

## Add or expand a connector

1. Add or update the tool class in `.agents/policy/tool-registry.yml`.
2. Add a matching record in `.agents/policy/mcp-connector-onboarding.yml` with
   the same `tool_registry_id`.
3. Keep `allowed_profiles` and `allowed_scopes` no broader than the registry.
4. Copy registry `approval_required_for` and `evidence_required` into the
   onboarding record, then add connector-specific details if needed.
5. Ensure `red_team_scenarios` includes `connector scope escalation`.
6. Run:

   ```bash
   uv run python tools/agent_policy/validate_setup.py .
   uv run python tools/agent_policy/governance_gate.py \
     --base-ref origin/master \
     --output test_artifacts/agent-policy/agent_governance_gate.json
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

## Common failures

| Failure | Meaning | Fix |
|---|---|---|
| `missing required MCP connector onboarding` | The onboarding file is absent or renamed. | Restore `.agents/policy/mcp-connector-onboarding.yml` or intentionally update the gate. |
| `mcp tools require onboarding records` | A registry `category: mcp` tool has no matching onboarding record. | Add a connector entry with the registry tool id. |
| `allowed_scopes exceed tool registry` | The onboarding record grants a scope not present in the registry. | Add a reviewed registry scope first or remove the broader onboarding scope. |
| `missing required MCP onboarding controls` | A connector does not cover all required controls. | Add the missing controls or mark the connector blocked. |
| `red_team_scenarios must include connector scope escalation` | The connector lacks a scope-escalation test path. | Add the red-team scenario reference and rerun validation. |

## Evidence receipt

When an agent uses an approved connector, its completion report or PR body should
include:

- connector name;
- operation type: read, write, metadata inspection, or admin;
- scope or permission summary;
- user approval or task instruction for write operations;
- artifact path when a governance receipt was generated.

Related pages:

- [Agent governance](agent-governance.md)
- [Agent permissions and tool registry](agent-permissions.md)
- [Agent risk register](agent-risk-register.md)
- [Agent security mapping](agent-security-mapping.md)
