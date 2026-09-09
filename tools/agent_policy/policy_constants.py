"""Shared constants for repository-local agent policy validation."""

from __future__ import annotations

PERMISSION_PROFILE = ".agents/policy/agent-permission-profile.yml"
TOOL_REGISTRY = ".agents/policy/tool-registry.yml"
MCP_CONNECTOR_ONBOARDING = ".agents/policy/mcp-connector-onboarding.yml"

REQUIRED_PROFILE_IDS = frozenset(
    {
        "read_only_reviewer",
        "path_scoped_writer",
        "integrator",
        "release_auditor",
    }
)
REQUIRED_TOOL_IDS = frozenset(
    {
        "filesystem_read",
        "filesystem_write_owned",
        "shell_local",
        "git_local_read",
        "git_remote",
        "github_read",
        "github_pr",
        "web_official_sources",
        "mcp_connector",
        "docs_quality_tools",
    }
)
REQUIRED_PROTECTED_ACTIONS = frozenset(
    {
        "git.push",
        "github.pr.create",
        "github.admin_merge",
        "secrets.read",
        "live.production",
    }
)
REQUIRED_FORBIDDEN_FOR_ALL = frozenset({"secrets.read"})
READ_ONLY_FORBIDDEN = frozenset(
    {
        "filesystem.write",
        "git.push",
        "github.admin_merge",
        "secrets.read",
        "live.production",
    }
)
VALID_RISK_TIERS = frozenset({"low", "medium", "high", "critical"})
VALID_TOOL_CATEGORIES = frozenset(
    {
        "filesystem",
        "shell",
        "git",
        "github",
        "network",
        "mcp",
        "validation",
    }
)
SCOPE_REVIEW_CATEGORIES = frozenset({"github", "network", "mcp"})
REQUIRED_MCP_ONBOARDING_CONTROLS = frozenset(
    {
        "server identity and provenance review",
        "OAuth scope review",
        "data classification",
        "tenant boundary review",
        "operation mode split",
        "prompt injection boundary",
        "approval gate",
        "evidence receipt",
        "revocation and rollback path",
    }
)
VALID_CONNECTOR_KINDS = frozenset(
    {
        "codex_connector",
        "document_session",
        "generic_mcp_connector",
        "local_mcp_server",
        "remote_mcp_server",
    }
)
VALID_CONNECTOR_STATUSES = frozenset({"approved", "blocked", "deprecated"})
