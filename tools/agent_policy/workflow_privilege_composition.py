"""Default immutable V1/V2/V3 scanner composition outside the evaluation service."""

from __future__ import annotations

import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from tools.agent_policy import workflow_privilege_contracts as contracts
from tools.agent_policy.workflow_privilege_policy_selection import VersionedPolicyReader

_ROOT = Path(__file__).resolve().parents[2]
_V1_SCHEMA = _ROOT / "evals/agent/workflow-security-privileged-policy.schema.json"
_V2_SCHEMA = _ROOT / "evals/agent/workflow-security-privileged-policy-v2.schema.json"
_V3_SCHEMA = _ROOT / "evals/agent/workflow-security-privileged-policy-v3.schema.json"
REPORT_SCHEMA = _ROOT / "evals/agent/workflow-security-privileged-report.schema.json"


def scan_repository(root: Path, *, service: Callable[..., Any]) -> dict[str, Any]:
    """Load exact schemas and select the sole active policy version."""

    v1, v2, v3, report = (
        json.loads(path.read_text(encoding="utf-8")) for path in (_V1_SCHEMA, _V2_SCHEMA, _V3_SCHEMA, REPORT_SCHEMA)
    )
    if not all(isinstance(value, dict) for value in (v1, v2, v3, report)):
        raise TypeError("privilege schemas must be JSON objects")
    reader = VersionedPolicyReader(limits=contracts.V1_LIMITS)
    return service(
        policy_schema=v1, policy_schemas={1: v1, 2: v2, 3: v3}, report_schema=report, acquire_snapshot=reader.acquire
    ).scan(root)
