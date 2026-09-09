"""Reconcile durable merge evidence with the live required-check gate report."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

CHECK_NAME = "Agent PR receipt"


def reconcile_required_check_report(evidence: Any, path: Path) -> None:
    """Bind durable evidence to the exact live ruleset-gate observation."""

    try:
        report = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise ValueError("exact required-check report is unavailable or invalid") from exc
    if not isinstance(report, dict):
        raise ValueError("exact required-check report must be a mapping")
    if (
        report.get("status") != "PASS"
        or report.get("repository") != evidence.repository
        or report.get("commit_sha") != evidence.integration_commit_sha
    ):
        raise ValueError("exact required-check report does not bind the verified release commit")
    contexts = report.get("contexts")
    if not isinstance(contexts, list):
        raise ValueError("exact required-check report contexts are invalid")
    receipt_contexts = [item for item in contexts if isinstance(item, dict) and item.get("context") == CHECK_NAME]
    if len(receipt_contexts) != 1:
        raise ValueError("exact required-check report must contain one Agent PR receipt context")
    context = receipt_contexts[0]
    observations = context.get("current_observations")
    expected = {
        "source": "check_run",
        "evidence_id": evidence.check_run_id,
        "state": "success",
        "integration_id": evidence.github_app_id,
        "commit_sha": evidence.integration_commit_sha,
    }
    matched = isinstance(observations, list) and any(
        isinstance(item, dict) and all(item.get(key) == value for key, value in expected.items())
        for item in observations
    )
    if context.get("status") != "PASS" or context.get("integration_id") != evidence.github_app_id or not matched:
        raise ValueError("exact required-check observation does not match durable receipt evidence")
