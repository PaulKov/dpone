"""Standards referenced by the dpone agent governance gate."""

from __future__ import annotations

from typing import TypedDict


class StandardReference(TypedDict):
    """External standard or guidance source used as governance context."""

    name: str
    version: str
    url: str


def agent_governance_standards() -> list[StandardReference]:
    """Return deterministic standards metadata for governance receipts."""

    return [
        {
            "name": "SLSA Build Track",
            "version": "v1.2",
            "url": "https://slsa.dev/spec/v1.2/tracks",
        },
        {
            "name": "OpenSSF Scorecard",
            "version": "current",
            "url": "https://scorecard.dev/",
        },
        {
            "name": "OWASP Top 10 for LLM Applications",
            "version": "2025",
            "url": "https://genai.owasp.org/llm-top-10/",
        },
        {
            "name": "OWASP Top 10 for Agentic Applications",
            "version": "2026",
            "url": "https://genai.owasp.org/resource/owasp-top-10-for-agentic-applications-for-2026/",
        },
        {
            "name": "Model Context Protocol Security",
            "version": "current",
            "url": "https://modelcontextprotocol.io/docs/tutorials/security/security_best_practices",
        },
        {
            "name": "CISA AI SBOM Minimum Elements",
            "version": "2026",
            "url": "https://www.cisa.gov/resources-tools/resources/software-bill-materials-ai-minimum-elements",
        },
    ]
