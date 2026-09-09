"""Discovery helpers for route certification bundle artifacts."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from pathlib import Path

BUNDLE_FILENAME = "route_certification_bundle.json"


class RouteCertificationBundleDiscovery:
    """Find route certification bundles without evaluating release policy."""

    def discover(
        self,
        *,
        roots: Sequence[str | Path] = (),
        explicit: Mapping[str, str | Path] | None = None,
    ) -> dict[str, Path]:
        bundles: dict[str, Path] = {}
        for root in roots:
            bundles.update(_discover_root(Path(root)))
        for route, path in (explicit or {}).items():
            bundles[str(route)] = Path(path)
        return bundles


def _discover_root(root: Path) -> dict[str, Path]:
    if not root.exists():
        return {}
    candidates = (root,) if root.name == BUNDLE_FILENAME else root.rglob(BUNDLE_FILENAME)
    discovered: dict[str, Path] = {}
    for path in candidates:
        if not path.is_file():
            continue
        route = _route_case_id(path)
        if route:
            discovered[route] = path
    return discovered


def _route_case_id(path: Path) -> str:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return ""
    if not isinstance(payload, Mapping):
        return ""
    route = payload.get("route")
    if not isinstance(route, Mapping):
        return ""
    direct = route.get("case_id")
    if isinstance(direct, str) and direct:
        return direct
    source = route.get("source")
    sink = route.get("sink")
    strategy = route.get("strategy")
    if not isinstance(source, str) or not isinstance(sink, str) or not isinstance(strategy, str):
        return ""
    return f"{_slug(source)}_to_{_slug(sink)}__{_slug(strategy)}"


def _slug(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")


__all__ = ["BUNDLE_FILENAME", "RouteCertificationBundleDiscovery"]
