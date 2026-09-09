"""Final route certification release gate and history writer."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dpone.ops.route_certify_release import OSS_SAFE_PROFILE, RouteCertificationReleaseService
from dpone.ops.routes.certify_release_discovery import RouteCertificationBundleDiscovery
from dpone.ops.routes.certify_release_finalizer_models import (
    RouteCertificationReleaseFinalizerReport,
    RouteCertificationReleaseHistoryEntry,
)
from dpone.ops.routes.certify_release_finalizer_policy import RouteCertificationReleaseFinalizerPolicy

DEFAULT_MAX_AGE_HOURS = 24.0


class RouteCertificationReleaseFinalizerService:
    """Finalize route-certified releases without executing route workloads."""

    def __init__(
        self,
        *,
        discovery: RouteCertificationBundleDiscovery | None = None,
        release_service: RouteCertificationReleaseService | None = None,
        policy: RouteCertificationReleaseFinalizerPolicy | None = None,
    ) -> None:
        self._discovery = discovery or RouteCertificationBundleDiscovery()
        self._release_service = release_service or RouteCertificationReleaseService()
        self._policy = policy or RouteCertificationReleaseFinalizerPolicy()

    def finalize(
        self,
        *,
        output_dir: str | Path,
        release: str,
        profile: str = OSS_SAFE_PROFILE,
        bundle_roots: Sequence[str | Path] = (),
        route_bundles: Mapping[str, str | Path] | None = None,
        required_routes: Sequence[str] = (),
        history_dir: str | Path | None = None,
        baseline_json: str | Path | None = None,
        max_age_hours: float = DEFAULT_MAX_AGE_HOURS,
    ) -> RouteCertificationReleaseFinalizerReport:
        directory = Path(output_dir)
        directory.mkdir(parents=True, exist_ok=True)
        bundles = self._discovery.discover(roots=bundle_roots, explicit=route_bundles)
        release_report = self._release_service.evaluate(
            output_dir=directory / "route-certify-release",
            release=release,
            profile=profile,
            route_bundles=bundles,
            required_routes=tuple(required_routes),
        )
        decision = self._policy.evaluate(
            release=release,
            release_report=release_report,
            baseline_scores=_load_baseline_scores(baseline_json),
            max_age_hours=max_age_hours,
            now=datetime.now(tz=timezone.utc),  # noqa: UP017
        )
        json_path = directory / "route_release_finalizer.json"
        markdown_path = directory / "route_release_finalizer.md"
        route_scores = _route_scores(release_report)
        route_levels = {item.route_case_id: item.level for item in release_report.routes}
        history_index_path = _write_history(
            history_dir=Path(history_dir) if history_dir else directory / "history",
            entry=RouteCertificationReleaseHistoryEntry(
                release=release,
                profile=profile,
                passed=decision.passed,
                level=decision.level,
                score=release_report.score,
                route_scores=route_scores,
                route_levels=route_levels,
                finalizer_path=str(json_path),
            ),
        )
        report = RouteCertificationReleaseFinalizerReport(
            release=release,
            profile=profile,
            passed=decision.passed,
            level=decision.level,
            score=release_report.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            discovered_routes=tuple(sorted(bundles)),
            required_routes=release_report.required_routes,
            route_scores=route_scores,
            route_levels=route_levels,
            release_report_path=release_report.json_path,
            release_report=release_report.to_dict(),
            checks=decision.checks,
            artifact_index={
                "route_certification_release": release_report.json_path,
                "route_certification_history": str(history_index_path),
            },
            output_dir=str(directory),
            json_path=str(json_path),
            markdown_path=str(markdown_path),
            history_index_path=str(history_index_path),
        )
        report.write()
        return report


def _route_scores(release_report: Any) -> dict[str, float]:
    scores: dict[str, float] = {}
    for item in release_report.routes:
        if item.score is not None:
            scores[item.route_case_id] = float(item.score)
    return scores


def _write_history(*, history_dir: Path, entry: RouteCertificationReleaseHistoryEntry) -> Path:
    history_dir.mkdir(parents=True, exist_ok=True)
    index_path = history_dir / "route_certification_history_index.json"
    entries = _load_history_entries(index_path)
    entries = [item for item in entries if item.get("release") != entry.release]
    entries.append(entry.to_dict())
    entries.sort(key=lambda item: str(item.get("release", "")))
    index_path.write_text(
        json.dumps({"releases": entries}, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return index_path


def _load_history_entries(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    payload = json.loads(path.read_text(encoding="utf-8"))
    releases = payload.get("releases", []) if isinstance(payload, Mapping) else []
    return [dict(item) for item in releases if isinstance(item, Mapping)]


def _load_baseline_scores(path: str | Path | None) -> dict[str, float]:
    if path is None:
        return {}
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        return {}
    if isinstance(payload.get("route_scores"), Mapping):
        return _float_map(payload["route_scores"])
    if isinstance(payload.get("route_index"), Mapping):
        return _scores_from_route_index(payload["route_index"])
    releases = payload.get("releases")
    if isinstance(releases, list) and releases:
        latest = releases[-1]
        if isinstance(latest, Mapping) and isinstance(latest.get("route_scores"), Mapping):
            return _float_map(latest["route_scores"])
    return {}


def _scores_from_route_index(value: object) -> dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    scores: dict[str, float] = {}
    for route, item in value.items():
        if isinstance(item, Mapping) and (score := _float_value(item.get("score"))) is not None:
            scores[str(route)] = score
    return scores


def _float_map(value: object) -> dict[str, float]:
    if not isinstance(value, Mapping):
        return {}
    result: dict[str, float] = {}
    for key, raw in value.items():
        score = _float_value(raw)
        if score is not None:
            result[str(key)] = score
    return result


def _float_value(value: object) -> float | None:
    if not isinstance(value, int | float | str):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


__all__ = ["DEFAULT_MAX_AGE_HOURS", "RouteCertificationReleaseFinalizerService"]
