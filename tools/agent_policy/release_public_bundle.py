"""Build ``dpone.release-public-bundle.v2`` manifests."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


canonical = _load_sibling("dpone_agent_release_canonical", "release_canonical.py")

SCHEMA = "dpone.release-public-bundle.v2"
SCHEMA_VERSION = 2


def build_public_bundle(
    *,
    candidate_id: str,
    distributions: list[dict[str, Any]],
    asset_names: list[str] | None = None,
) -> dict[str, Any]:
    """Return a schema-shaped public bundle with canonical ``manifest_sha256``."""

    if not distributions:
        raise ValueError("distributions must be non-empty")
    normalized = sorted(
        (
            {
                "project": str(item["project"]),
                "filename": str(item["filename"]),
                "size": int(item["size"]),
                "sha256": str(item["sha256"]),
            }
            for item in distributions
        ),
        key=lambda row: (row["project"], row["filename"]),
    )
    if asset_names is not None:
        names = sorted(str(name) for name in asset_names)
    else:
        names = sorted(str(row["filename"]) for row in normalized)
    body = {
        "schema": SCHEMA,
        "schema_version": SCHEMA_VERSION,
        "candidate_id": candidate_id,
        "distributions": normalized,
        "asset_names": names,
    }
    body["manifest_sha256"] = canonical.sha256_id("dpone.release.public-bundle-manifest.v2", body)
    return body
