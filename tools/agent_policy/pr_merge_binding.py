"""Canonical semantic binding shared by merge-receipt producers and readers."""

from __future__ import annotations

import hashlib
import json
from typing import Any

BINDING_FIELDS = (
    "repository",
    "protected_base_ref",
    "pr_number",
    "merged_at",
    "integration_method",
    "reviewed_head_sha",
    "reviewed_head_tree",
    "base_parent_sha",
    "integration_commit_sha",
    "integration_tree",
    "changed_paths",
    "pr_body_sha256",
    "source_receipt",
)


def compute_binding_id(payload: dict[str, Any]) -> str:
    """Hash the canonical immutable merge-closure semantic fields."""

    binding = {field: payload.get(field) for field in BINDING_FIELDS}
    binding_bytes = json.dumps(binding, sort_keys=True, separators=(",", ":")).encode()
    return f"sha256:{hashlib.sha256(binding_bytes).hexdigest()}"


__all__ = ["BINDING_FIELDS", "compute_binding_id"]
