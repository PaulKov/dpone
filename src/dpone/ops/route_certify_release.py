"""Release-level route certification gate from immutable route bundles."""

from __future__ import annotations

import json
from collections.abc import Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from dpone.ops.routes.certify_release_evidence import (
    route_evidence_sha256,
    verify_route_bundle_evidence,
)
from dpone.ops.routes.certify_release_models import (
    DEFAULT_REQUIRED_ROUTE_CERTIFICATION_ROUTES,
    RouteCertificationReleaseItem,
    RouteCertificationReleaseReport,
)
from dpone.ops.routes.certify_release_policy import RouteCertificationReleasePolicy

OSS_SAFE_PROFILE = "oss_ci"
VENDOR_LIVE_PROFILE = "vendor_live"
ZERO_SHA256 = "0" * 64


class RouteCertificationReleaseService:
    """Aggregate certified route bundles into one release go/no-go receipt."""

    def __init__(self, *, policy: RouteCertificationReleasePolicy | None = None) -> None:
        self._policy = policy or RouteCertificationReleasePolicy()

    def evaluate(
        self,
        *,
        output_dir: str | Path,
        release: str,
        route_bundles: Mapping[str, str | Path],
        profile: str = OSS_SAFE_PROFILE,
        required_routes: Sequence[str] = (),
    ) -> RouteCertificationReleaseReport:
        directory = Path(output_dir)
        normalized_profile = _profile(profile)
        required = _required_routes(required_routes)
        bundle_paths = {str(name): Path(path) for name, path in route_bundles.items()}
        route_names = tuple(dict.fromkeys((*required, *sorted(bundle_paths))))
        routes = tuple(
            self._read_bundle(
                expected_route=route_name,
                path=bundle_paths.get(route_name),
                required=route_name in required,
                release_profile=normalized_profile,
                expected_release=release,
            )
            for route_name in route_names
        )
        decision = self._policy.evaluate(required_routes=required, routes=routes)
        report = RouteCertificationReleaseReport(
            release=release,
            profile=normalized_profile,
            passed=decision.passed,
            level=decision.level,
            score=decision.score,
            blockers=decision.blockers,
            warnings=decision.warnings,
            next_actions=decision.next_actions,
            required_routes=required,
            routes=routes,
            artifact_index=_artifact_index(routes),
            output_dir=str(directory),
            json_path=str(directory / "route_certification_release.json"),
            markdown_path=str(directory / "route_certification_release.md"),
            release_notes_path=str(directory / "route_certification_release_notes.md"),
        )
        report.write()
        return report

    def _read_bundle(
        self,
        *,
        expected_route: str,
        path: Path | None,
        required: bool,
        release_profile: str,
        expected_release: str,
    ) -> RouteCertificationReleaseItem:
        if path is None or not path.exists():
            return _missing_item(route_case_id=expected_route, path=path, required=required)
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            return _blocked_item(
                route_case_id=expected_route,
                path=path,
                required=required,
                profile="",
                blocker=f"invalid_json:{exc.msg}",
            )
        if not isinstance(payload, Mapping):
            return _blocked_item(
                route_case_id=expected_route,
                path=path,
                required=required,
                profile="",
                blocker="invalid_shape",
            )
        return _item_from_payload(
            expected_route=expected_route,
            path=path,
            required=required,
            release_profile=release_profile,
            expected_release=expected_release,
            payload=payload,
        )


def _item_from_payload(
    *,
    expected_route: str,
    path: Path,
    required: bool,
    release_profile: str,
    expected_release: str,
    payload: Mapping[str, Any],
) -> RouteCertificationReleaseItem:
    profile = _profile(str(payload.get("profile") or ""))
    route_case_id = _embedded_route_case_id(payload) or expected_route
    blockers = list(_string_tuple(payload.get("blockers")))
    if payload.get("schema_version") != "dpone.route_certification_bundle.v1":
        blockers.append("route_certification_bundle.invalid_schema")
    if payload.get("release") != expected_release:
        blockers.append("route_certification_bundle.release_mismatch")
    evidence = verify_route_bundle_evidence(payload, bundle_path=path)
    blockers.extend(evidence.blockers)
    trusted = evidence.passed and not blockers
    level = str(payload.get("level") or ("certified" if trusted else "blocked"))
    artifact_index = _string_map(payload.get("artifact_index"))
    return RouteCertificationReleaseItem(
        route_case_id=expected_route,
        path=str(path),
        required=required,
        missing=False,
        passed=trusted,
        level=level,
        profile=profile,
        bundle_release=str(payload.get("release") or ""),
        score=_score_value(payload.get("score")),
        modified_at=_modified_at(path),
        route_matched=route_case_id == expected_route,
        profile_matched=_profile_matches(release_profile=release_profile, bundle_profile=profile),
        sha256=evidence.sha256,
        summary=_summary(payload),
        blockers=tuple(dict.fromkeys(blockers)),
        artifact_index=artifact_index,
    )


def _missing_item(*, route_case_id: str, path: Path | None, required: bool) -> RouteCertificationReleaseItem:
    return RouteCertificationReleaseItem(
        route_case_id=route_case_id,
        path="" if path is None else str(path),
        required=required,
        missing=True,
        passed=False,
        level="missing",
        profile="",
        bundle_release="",
        score=None,
        modified_at="",
        route_matched=True,
        profile_matched=True,
        sha256=ZERO_SHA256,
        summary="route certification bundle is missing",
        blockers=tuple(),
        artifact_index={},
    )


def _blocked_item(
    *,
    route_case_id: str,
    path: Path,
    required: bool,
    profile: str,
    blocker: str,
) -> RouteCertificationReleaseItem:
    return RouteCertificationReleaseItem(
        route_case_id=route_case_id,
        path=str(path),
        required=required,
        missing=False,
        passed=False,
        level="blocked",
        profile=profile,
        bundle_release="",
        score=None,
        modified_at=_modified_at(path),
        route_matched=True,
        profile_matched=True,
        sha256=route_evidence_sha256(path),
        summary=blocker,
        blockers=(blocker,),
        artifact_index={},
    )


def _required_routes(values: Sequence[str]) -> tuple[str, ...]:
    source = values or DEFAULT_REQUIRED_ROUTE_CERTIFICATION_ROUTES
    return tuple(dict.fromkeys(str(item).strip() for item in source if str(item).strip()))


def _profile(value: str) -> str:
    normalized = str(value or OSS_SAFE_PROFILE).strip().lower().replace("-", "_")
    return normalized or OSS_SAFE_PROFILE


def _profile_matches(*, release_profile: str, bundle_profile: str) -> bool:
    if release_profile == VENDOR_LIVE_PROFILE:
        return bundle_profile == VENDOR_LIVE_PROFILE
    return True


def _embedded_route_case_id(payload: Mapping[str, Any]) -> str:
    route = payload.get("route")
    if not isinstance(route, Mapping):
        return ""
    direct = route.get("case_id")
    if isinstance(direct, str) and direct:
        return direct
    source = route.get("source")
    sink = route.get("sink")
    strategy = route.get("strategy")
    if (
        not isinstance(source, str)
        or not source
        or not isinstance(sink, str)
        or not sink
        or not isinstance(strategy, str)
        or not strategy
    ):
        return ""
    return f"{_slug(source)}_to_{_slug(sink)}__{_slug(strategy)}"


def _slug(value: str) -> str:
    return value.strip().lower().replace("-", "_").replace(" ", "_")


def _summary(payload: Mapping[str, Any]) -> str:
    summary = payload.get("summary")
    if isinstance(summary, str) and summary:
        return summary
    score = payload.get("score")
    level = payload.get("level")
    if score is not None:
        return f"level={level}; score={score}"
    return f"level={level}"


def _score_value(value: object) -> float | None:
    if value is None:
        return None
    if not isinstance(value, int | float | str):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _modified_at(path: Path) -> str:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc).isoformat()  # noqa: UP017


def _string_tuple(value: object) -> tuple[str, ...]:
    if not isinstance(value, list | tuple):
        return tuple()
    return tuple(str(item) for item in value if str(item))


def _string_map(value: object) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    return {str(name): str(path) for name, path in value.items()}


def _artifact_index(routes: Sequence[RouteCertificationReleaseItem]) -> dict[str, str]:
    artifacts: dict[str, str] = {}
    for route in routes:
        if route.path:
            artifacts[route.route_case_id] = route.path
        for name, path in route.artifact_index.items():
            artifacts[f"{route.route_case_id}:{name}"] = path
    return artifacts


__all__ = [
    "OSS_SAFE_PROFILE",
    "RouteCertificationReleaseService",
    "VENDOR_LIVE_PROFILE",
]
