# Agent permissions and tool registry

Purpose: define which AI-agent roles may use which tool classes, which actions
need explicit approval, and what evidence reviewers should expect.

Audience: maintainers, integrators, reviewers, and anyone updating agent tools,
skills, prompts, or MCP/connectors.

## Files

| File | Purpose |
|---|---|
| `.agents/policy/agent-permission-profile.yml` | Role-level policy for read-only reviewers, path-scoped writers, integrators, and release auditors. |
| `.agents/policy/tool-registry.yml` | Allowlist of local tools, GitHub operations, web access, validation tools, and MCP/connectors. |
| `.agents/policy/mcp-connector-onboarding.yml` | Review gate for MCP servers, connectors, OAuth scopes, tenant boundaries, write operations, and connector evidence. |
| `evals/agent/agent-permission-profile.schema.json` | Public schema for permission profiles. |
| `evals/agent/tool-registry.schema.json` | Public schema for tool registry entries. |
| `evals/agent/mcp-connector-onboarding.schema.json` | Public schema for MCP/connector onboarding records. |
| `tools/agent_policy/permissions.py` | Thin compatibility facade used by `validate_setup.py` and `governance_gate.py`. |
| `tools/agent_policy/permission_profiles.py` | Focused permission-profile validator. |
| `tools/agent_policy/tool_registry.py` | Focused tool-registry validator. |
| `tools/agent_policy/mcp_onboarding.py` | Focused MCP/connector onboarding validator. |

## Mental model

The permission profile answers: "Which role is this agent acting as?"

The tool registry answers: "What kind of tool is being used, who owns the risk,
which roles may use it, which approvals are required, and what evidence proves it
was used safely?"

The MCP connector onboarding file answers: "Has this external connector, scope,
tenant boundary, write operation, and rollback path been reviewed before an
agent uses it?"

The governance gate fails closed when these files are missing or inconsistent.
For example, a profile cannot allow an unregistered tool, read-only profiles
cannot allow write-capable tools, and high-risk tools must list approval
conditions.

## Standard profiles

| Profile | Typical agents | May write? | Protected actions |
|---|---|---:|---|
| `read_only_reviewer` | `dpone_explorer`, `dpone_architect`, `dpone_docs_ux_reviewer` | No | push, PR creation, admin merge, secrets, live production |
| `path_scoped_writer` | `dpone_implementer`, `dpone_test_certifier` | Owned paths only | push, PR creation, admin merge, secrets, live production |
| `integrator` | `dpone_integrator`, main Codex session acting as integrator | Yes, including shared files when owned | push, PR creation, admin merge, secrets, live production |
| `release_auditor` | `dpone_release_auditor` | No | push, PR creation, admin merge, secrets, live production |

Protected actions are not automatically forbidden in every context. They are the
actions that require explicit user intent, maintainer approval, or break-glass
evidence before use.

## Tool risk tiers

| Tier | Meaning | Review expectation |
|---|---|---|
| `low` | Read-only or deterministic validation with limited blast radius. | Normal PR review. |
| `medium` | Can consume external context or execute local commands. | Evidence must include command/source and result. |
| `high` | Can change remote repository state or create public workflow artifacts. | Explicit approval condition and PR evidence. |
| `critical` | Can bridge to external SaaS, MCP servers, connector sessions, or cross-tenant data. | Scope review, approval, and operation evidence are mandatory. |

## Adding a new tool or MCP connector

1. Add an entry to `.agents/policy/tool-registry.yml`.
2. Set `category`, `provenance`, `risk_tier`, `allowed_profiles`,
   `allowed_actions`, `forbidden_actions`, `approval_required_for`, and
   `evidence_required`.
3. For `github`, `network`, or `mcp` categories, list `allowed_scopes`.
4. For `category: mcp`, add a matching record to
   `.agents/policy/mcp-connector-onboarding.yml`.
5. Keep onboarding `allowed_profiles` and `allowed_scopes` no broader than the
   registry entry, and cover the registry approvals and evidence requirements.
6. Add the tool id to a permission profile only after the registry and
   onboarding records exist.
7. Run:

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
   uv run pytest tests/agent_policy tests/test_module_size_gate.py -q
   ```

## Common failures

| Failure | Meaning | Fix |
|---|---|---|
| `missing required permission profile` | The role policy file is absent or renamed. | Restore `.agents/policy/agent-permission-profile.yml` or update the gate intentionally. |
| `allowed_tools reference unregistered tools` | A profile references a tool id not present in the registry. | Add the registry entry first or remove the profile reference. |
| `tools cannot be both allowed and forbidden` | A profile grants and forbids the same tool/action. | Split tool ids from forbidden action ids and keep only one decision. |
| `read-only profile allows write-capable tools` | A reviewer role can write or open PRs. | Remove write-capable tools or change the role to a writer with a task contract. |
| `high and critical risk tools require approval_required_for` | A privileged tool lacks approval boundaries. | Add concrete approval conditions and evidence requirements. |
| `mcp tools require onboarding records` | A registry MCP tool has no matching onboarding record. | Add `.agents/policy/mcp-connector-onboarding.yml` entry before use. |
| `allowed_scopes exceed tool registry` | The onboarding record grants more than the registry allows. | Review and add the registry scope first or remove the broader onboarding scope. |

## Reviewer checklist

When reviewing permission or tool policy changes, check that:

1. Every new tool has an owner, provenance, risk tier, allowed profiles, and
   evidence requirement.
2. `mcp` and external connector entries include scope review and approval
   conditions.
3. Every `category: mcp` tool has an onboarding record with required controls,
   red-team coverage, and evidence receipt.
4. Read-only profiles cannot write files, push, create PRs, or use write-capable
   connectors.
5. Protected actions still include push, PR creation, admin merge, secret access,
   and live production access.
6. `agent_governance_gate.json` is generated from the reviewed head commit.
7. `tools/agent_policy` and `tests/agent_policy` pass the module-size guard.

Related pages:

- [Agent governance](agent-governance.md)
- [Agent-assisted development](agent-development.md)
- [Agent MCP connector onboarding](agent-mcp-connectors.md)
- [Agent risk register](agent-risk-register.md)
- [Agent security mapping](agent-security-mapping.md)
