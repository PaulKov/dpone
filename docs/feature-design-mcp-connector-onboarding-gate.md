# Feature design: MCP connector onboarding gate

- Status: APPROVED
- Owner: dpone maintainers
- Issue: Codex thread request, 2026-07-11
- Target release: next patch
Last verified: 2026-07-11

## Executive summary

Agents now have a tool registry, but a registry alone does not prove that a new
MCP server or connector was reviewed before use. This design adds a
machine-readable onboarding gate for MCP/connectors. The measurable outcome is
that every `category: mcp` registry entry has an approved onboarding record with
scope boundaries, tenant boundaries, approval conditions, red-team coverage, and
evidence requirements.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| Maintainer | Allow useful connectors without silent scope expansion. | A tool registry entry can be broad but not connector-specific. | `validate_setup.py` fails when onboarding is missing or broader than the registry. |
| Integrator | Use a connector in a task and report safe evidence. | Connector approval rules are prose-only. | Completion report includes connector name, operation, scope, approval, and receipt. |
| Security reviewer | Detect unreviewed MCP servers and OAuth changes. | Scope changes can hide in generic tool metadata. | `agent_mcp_connector_onboarding` appears in governance receipts. |

Journey: reviewer discovers the MCP runbook, adds a registry entry, adds a
matching onboarding record, runs validation, inspects the governance receipt,
then approves or blocks connector use before an agent executes the connector.

## Scope

### In scope

- `.agents/policy/mcp-connector-onboarding.yml`.
- JSON schema for onboarding records.
- Validator wiring in `permissions.py`, `validate_setup.py`, and
  `governance_gate.py`.
- Red-team scenario for connector scope escalation.
- Self-service docs and MkDocs navigation.

### Non-goals

- Installing or configuring real external MCP servers.
- Managing OAuth tokens or connector credentials.
- Runtime enforcement inside third-party connector hosts.

### Assumptions and constraints

- Repository policy is the source of truth for agent behavior.
- Secrets remain outside repository artifacts.
- Connector output is untrusted task input, not an instruction source.

## Public contract

### CLI

No new CLI command is added. Existing commands gain stricter policy validation:

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

### Python API

`tools/agent_policy/permissions.py` adds:

- `MCP_CONNECTOR_ONBOARDING`;
- `REQUIRED_MCP_ONBOARDING_CONTROLS`;
- `VALID_CONNECTOR_KINDS`;
- `load_mcp_connector_onboarding(path)`;
- `validate_mcp_connector_onboarding(root, ...)`.

### Manifest/schema

The onboarding YAML is schema version 1 and contains document-level
`required_controls` plus `connectors[]`. Each connector maps to
`tool_registry_id`, has status, connector kind, data and tenant boundaries,
allowed profiles/scopes, approvals, evidence, controls, and red-team scenarios.

### Artifacts and evidence

The governance receipt includes `agent_mcp_connector_onboarding` with `PASS` or
`FAIL`. A connector operation evidence receipt includes connector name,
operation type, scope summary, and approval or task instruction for writes.

### Compatibility and migration

Existing registry entries keep working after adding the generic
`mcp_connector` onboarding record. Future MCP tools fail closed until they add a
matching onboarding record.

## Detailed algorithm

1. Load permission profiles, tool registry, and MCP onboarding YAML.
2. Validate document headers and required controls.
3. Build registry tools by id and collect all `category: mcp` tool ids.
4. For each connector, validate required fields, connector kind, status,
   profile references, operation overlap, required controls, write approval, and
   `connector scope escalation` red-team coverage.
5. Verify onboarding `allowed_profiles` and `allowed_scopes` are subsets of the
   matching registry tool.
6. Verify onboarding approvals and evidence cover the matching registry tool.
7. Fail if any registry MCP tool lacks an onboarding record.
8. Surface the result through `validate_setup.py` and the governance receipt.

### Pseudocode

```text
registry = load_tool_registry()
onboarding = load_mcp_connector_onboarding()
mcp_tools = registry.tools where category == "mcp"

for connector in onboarding.connectors:
    require connector.tool_registry_id in registry
    require registry[tool_id].category == "mcp"
    require connector.scopes subset registry.scopes
    require connector.profiles subset registry.profiles
    require connector.approvals covers registry.approvals
    require connector.evidence covers registry.evidence
    require all required MCP controls
    require connector scope escalation red-team scenario

require all mcp_tools have onboarding records
```

### Failure semantics

Missing files, broader scopes, missing controls, unknown profiles, non-MCP tool
references, and absent red-team coverage are hard `FAIL`. There is no retry,
state mutation, or partial write; users fix policy and rerun validation.

## Architecture

| Component | Existing/new | Responsibility | Dependencies |
|---|---|---|---|
| `permissions.py` | Existing | Parse and validate policy relationships. | PyYAML, local files. |
| `mcp-connector-onboarding.yml` | New | Record connector-specific approval gate. | Tool registry ids. |
| JSON schema | New | Document closed object contract. | JSON Schema 2020-12. |
| `governance_gate.py` | Existing | Emit auditable PASS/FAIL receipt. | Permission validator. |

Dependency direction stays one-way: YAML policy -> validator -> governance
receipt. No production runtime module imports agent policy tools.

### Alternatives and tradeoffs

| Alternative | Advantages | Disadvantages | Decision |
|---|---|---|---|
| Extend only `tool-registry.yml` | Fewer files. | Makes registry too broad and harder to review per connector. | Rejected. |
| One onboarding file per connector | Strong ownership boundaries. | Too much ceremony before there are many connectors. | Deferred. |
| Central onboarding document | Clear review gate and easy CI validation. | Requires cross-file consistency checks. | Adopted. |

### Quality-budget impact

The change adds one focused validator path and keeps all logic in
`tools/agent_policy/permissions.py`. No dpone runtime package imports are added.

## Market comparison

| System/version | Relevant capability | Observed design | Strength | Limitation | Adopt/reject | Source/date |
|---|---|---|---|---|---|---|
| Model Context Protocol, current | MCP authorization and security | Authorization guidance covers OAuth-protected resources; security guidance calls out connector attack surfaces and validation. | Directly relevant to MCP tool use. | Does not define dpone repository policy format. | Adopt controls for scopes, identity, and connector boundaries. | https://modelcontextprotocol.io/docs/tutorials/security/security_best_practices, https://modelcontextprotocol.io/docs/tutorials/security/authorization, checked 2026-07-11 |
| OWASP GenAI/LLM Top 10 2025 | Agent and LLM app risks | Prompt injection, tool misuse, and sensitive information disclosure are explicit risk families. | Good reviewer vocabulary. | Not repository-specific. | Adopt red-team framing. | https://genai.owasp.org/llm-top-10/, checked 2026-07-11 |
| NIST AI 600-1 | GenAI risk profile | Treats GenAI risks as governed, mapped, measured, and managed controls. | Fits governance receipt model. | High-level, not MCP-specific. | Adopt auditable control language. | https://www.nist.gov/publications/artificial-intelligence-risk-management-framework-generative-artificial-intelligence, checked 2026-07-11 |
| dlt, Informatica, Airbyte, Fivetran, Pentaho, Microsoft SSIS, gusty, Astronomer Cosmos, Apache Beam | Data integration runtime connectors | N/A for repository-local AI-agent MCP onboarding. | Strong connector ecosystems in their own domains. | They do not define this repo's AI-agent tool approval gate. | Mark N/A for this feature. | N/A, capability mismatch |

## Measurable differentiation

```yaml
axis: MCP connector scope governance
scenario: add a registry MCP tool without onboarding or with a broader scope
baseline: current dpone after tool registry only
metric: validation result
target: validate_setup.py and governance_gate.py both fail closed
procedure: run focused agent policy tests and governance gate
artifact: test output and test_artifacts/agent-policy/agent_governance_gate.json
limitations: does not enforce third-party host behavior at runtime
```

## Security, privacy, and operations

Connector tokens remain in connector runtimes, never in repository artifacts.
Connector output cannot override repository instructions. Cross-tenant access,
write operations, new servers, and OAuth expansion require explicit approval and
evidence. Revocation is operational: block or delete the onboarding record and
rerun validation.

## Test and certification plan

| Layer | Scenario | Environment | Expected artifact |
|---|---|---|---|
| Unit | Missing onboarding, broader scope, missing controls. | Local pytest. | Failing then passing tests. |
| Contract | Schema is a closed JSON object contract. | Local pytest. | Schema parse assertions. |
| Integration | `validate_setup.py` and `governance_gate.py`. | Local CLI. | PASS/FAIL receipt. |
| Live certification | Real connector execution. | Explicitly approved only. | SKIP/UNVERIFIED unless approved. |

## Documentation plan

Add `agent-mcp-connectors.md`, cross-link from governance, permissions, risk,
and security docs, and include the feature design in developer docs.

## Rollout and rollback

Rollout is additive. Rollback removes the new required files from
`validate_setup.py` and removes the governance check. If a connector is unsafe,
set status to `blocked` or remove its onboarding record so validation fails for
matching MCP tools.

## Agent execution plan

| Agent/role | Owned paths | Read-only paths | Forbidden paths | Dependency |
|---|---|---|---|---|
| Codex integrator | `.agents/policy/**`, `tools/agent_policy/**`, `evals/agent/**`, `tests/test_agent_*.py`, `docs/agent-*.md`, `mkdocs.yml`, `.github/CODEOWNERS`, `CHANGELOG.md` | Repository docs and existing policy files | Runtime connector implementation and unrelated Airflow branch files | Fresh worktree from `origin/master` |

Integrator and shared-file owner: Codex main session.

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm and failure semantics are implementable without guessing.
- [x] Public contracts and compatibility are explicit.
- [x] Architecture and alternatives are justified.
- [x] Relevant market research uses current official sources.
- [x] Claimed differentiation is measurable.
- [x] Tests, evidence, docs, rollout, and rollback are complete.
- [x] Path ownership and integration plan are conflict-safe.
- [x] Maintainer authorized implementation in the Codex thread on 2026-07-11.
