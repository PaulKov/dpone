from __future__ import annotations

import hashlib
import json
import re
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"
PERMISSION_ACCESS = {
    "actions": ["none", "read", "write"],
    "artifact-metadata": ["none", "read", "write"],
    "attestations": ["none", "read", "write"],
    "checks": ["none", "read", "write"],
    "code-quality": ["none", "read", "write"],
    "contents": ["none", "read", "write"],
    "deployments": ["none", "read", "write"],
    "discussions": ["none", "read", "write"],
    "id-token": ["none", "write"],
    "issues": ["none", "read", "write"],
    "models": ["none", "read"],
    "packages": ["none", "read", "write"],
    "pages": ["none", "read", "write"],
    "pull-requests": ["none", "read", "write"],
    "security-events": ["none", "read", "write"],
    "statuses": ["none", "read", "write"],
    "vulnerability-alerts": ["none", "read"],
}


def policy(version: int = 1) -> dict[str, Any]:
    if version == 3:
        parsed = yaml.safe_load(
            (ROOT / ".agents/policy/workflow-security-privileged-v3.yml").read_text(encoding="utf-8")
        )
        assert isinstance(parsed, dict)
        return parsed
    text = SPEC.read_text(encoding="utf-8")
    marked = text.split("<!-- pr3b-policy-v1:begin -->", 1)[1].split("<!-- pr3b-policy-v1:end -->", 1)[0]
    parsed = yaml.safe_load(marked.split("```yaml\n", 1)[1].split("\n```", 1)[0])
    assert isinstance(parsed, dict)
    return parsed


def is_integer(value: object) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def is_sha(value: object) -> bool:
    return isinstance(value, str) and len(value) == 64 and set(value) <= set("0123456789abcdef")


def is_text(value: object, maximum: int, *, detail: bool = False) -> bool:
    if not isinstance(value, str) or not 1 <= len(value.encode("utf-8")) <= maximum:
        return False
    allowed = {"\t", "\n"} if detail else set()
    return all(character >= " " or character in allowed for character in value)


def _canonical_json_sha256(value: dict[str, Any]) -> str:
    raw = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def canonical_route_id_v1(value: dict[str, Any]) -> str:
    return _canonical_json_sha256(value)


@dataclass(frozen=True, slots=True)
class SnapshotReference:
    """Immutable identity of the exact acquisition and policy-parse result."""

    policy_sha256: str | None
    policy_schema_version: int | None
    manifest_sha256: str | None
    complete: bool


def report_routes_match_graph(report: dict[str, Any], graph_route_ids: list[str]) -> bool:
    if not isinstance(report, dict) or not isinstance(report.get("routes"), list):
        return False
    if not all(isinstance(route, dict) and is_sha(route.get("route_id")) for route in report["routes"]):
        return False
    if not all(is_sha(route_id) for route_id in graph_route_ids):
        return False
    observed = [route["route_id"] for route in report["routes"]]
    expected = list(graph_route_ids)
    return expected == sorted(expected) and len(expected) == len(set(expected)) and observed == expected


def report_snapshot_matches(
    report: dict[str, Any],
    *,
    snapshot_reference: SnapshotReference,
) -> bool:
    policy_sha256 = snapshot_reference.policy_sha256
    policy_schema_version = snapshot_reference.policy_schema_version
    manifest_sha256 = snapshot_reference.manifest_sha256
    complete = snapshot_reference.complete
    if not isinstance(complete, bool):
        return False
    if policy_sha256 is not None and not is_sha(policy_sha256):
        return False
    if policy_schema_version is not None and (
        not is_integer(policy_schema_version) or policy_schema_version not in {1, 2, 3}
    ):
        return False
    if policy_sha256 is None and policy_schema_version is not None:
        return False
    if manifest_sha256 is not None and not is_sha(manifest_sha256):
        return False
    if complete is not (manifest_sha256 is not None):
        return False
    try:
        inventory = report["inventory"]
        return (
            report["policy"]["sha256"] == policy_sha256
            and report["policy"]["schema_version"] == policy_schema_version
            and inventory["manifest_sha256"] == manifest_sha256
            and inventory["complete"] is complete
        )
    except (KeyError, TypeError):
        return False


def required_profile_evidence(policy_value: dict[str, Any]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    subjects: list[dict[str, Any]] = []
    coordinates: list[dict[str, Any]] = []
    for profile in policy_value["profiles"].values():
        if _canonical_json_sha256(profile["trigger"]) != profile["trigger_sha256"]:
            raise ValueError("profile trigger digest mismatch")
        subjects.extend(
            {
                "profile_id": profile["id"],
                "workflow": profile["workflow"],
                "job": profile["job"],
                "trigger_sha256": profile["trigger_sha256"],
            }
            for _ in range(profile["required_occurrences"])
        )
        coordinates.extend(
            {
                "profile_id": profile["id"],
                "root_workflow": profile["workflow"],
                "root_event": profile["root_event"],
                "event_variant": variant,
                "edge_chain": profile["required_edge_chain"],
                "workflow": profile["workflow"],
                "job": profile["job"],
            }
            for variant in profile["event_variants"]
        )
    return subjects, coordinates


def mandatory_profile_evidence_matches(
    policy_value: dict[str, Any],
    observed_subjects: list[dict[str, Any]],
    observed_coordinates: list[dict[str, Any]],
) -> bool:
    def encode(item: dict[str, Any]) -> str:
        return json.dumps(
            item,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
            allow_nan=False,
        )

    try:
        expected_subjects, expected_coordinates = required_profile_evidence(policy_value)
        return Counter(map(encode, observed_subjects)) == Counter(map(encode, expected_subjects)) and Counter(
            map(encode, observed_coordinates)
        ) == Counter(map(encode, expected_coordinates))
    except (KeyError, TypeError, ValueError):
        return False


def event_variant(value: object) -> bool:
    if not isinstance(value, str):
        return False
    tokens = value.split(">")
    direct = tokens[0] in {"CLOSED_UNMERGED", "CLOSED_MERGED"} or (
        re.fullmatch(r"ACTIVITY:[a-z][a-z0-9_-]{0,63}", tokens[0]) is not None
    )
    if len(tokens) == 1:
        return direct
    downstream = re.fullmatch(r"WORKFLOW_RUN:[a-z][a-z0-9_-]{0,63}", tokens[1]) is not None
    return len(tokens) == 2 and direct and downstream
