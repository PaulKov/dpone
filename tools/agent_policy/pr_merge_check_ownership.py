"""Classify integration ownership before an exact check may be published."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_identity() -> Any:
    path = Path(__file__).with_name("pr_merge_identity.py")
    spec = importlib.util.spec_from_file_location("dpone_agent_pr_merge_check_identity", path)
    if spec is None or spec.loader is None:
        raise RuntimeError("cannot load pr_merge_identity.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def classify_event_ownership(
    event: dict[str, Any],
    *,
    root: Path,
    repository: str,
    protected_base_refs: list[str],
) -> Any:
    """Return direct/transitive ownership from event and immutable Git facts."""

    identity = _load_identity()
    checked_out = identity.checked_out_commit(root)
    merge_event = identity.validate_merge_event(
        event,
        repository=repository,
        protected_base_refs=protected_base_refs,
        integration_commit_sha=checked_out,
    )
    return identity.classify_integration_ownership(root, merge_event)
