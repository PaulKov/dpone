"""Shared agent control-surface path classification."""

from __future__ import annotations

CONTROL_SURFACE_FILES = frozenset(
    {
        "AGENTS.md",
        ".github/CODEOWNERS",
        ".github/pull_request_template.md",
        ".worktreeinclude",
        "CONTRIBUTING.md",
        "docs/agent-development.md",
        "docs/agent-governance.md",
        "docs/agent-mcp-connectors.md",
        "docs/agent-release-protocol.md",
        "docs/agent-risk-register.md",
        "docs/agent-security-mapping.md",
        "docs/agent-task-contracts.md",
        "docs/feature-design-agent-task-contract-gate.md",
        "docs/feature-design-mcp-connector-onboarding-gate.md",
        "docs/github-branch-protection.md",
        "docs/supply-chain-slsa.md",
    }
)
CONTROL_SURFACE_PREFIXES = (
    ".agents/",
    ".codex/",
    ".github/ISSUE_TEMPLATE/",
    ".github/codex/",
    ".github/workflows/",
    "evals/agent/",
    "tools/agent_policy/",
)


def normalize_path(path: str) -> str:
    """Normalize a repository path for policy comparisons."""

    return path.replace("\\", "/").strip()


def is_control_surface_path(path: str) -> bool:
    """Return whether a repository path changes the agent control surface."""

    normalized = normalize_path(path)
    if normalized in CONTROL_SURFACE_FILES or normalized.endswith("/AGENTS.md"):
        return True
    return normalized.startswith(CONTROL_SURFACE_PREFIXES)


def control_surface_changed(paths: list[str]) -> bool:
    """Return whether any changed path belongs to the agent control surface."""

    return any(is_control_surface_path(path) for path in paths)
