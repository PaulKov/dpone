from __future__ import annotations

from typing import Any

from tests.ci_shadow_pr3b_report_contract_support import PERMISSION_ACCESS
from tests.ci_shadow_pr3b_report_contract_support import is_integer as _is_integer
from tests.ci_shadow_pr3b_report_contract_support import is_sha as _is_sha
from tests.ci_shadow_pr3b_report_contract_support import is_text as _is_text
from tests.ci_shadow_pr3b_report_contract_support import policy as _policy
from tests.ci_shadow_pr3b_report_route_model import validate_routes


def _valid_report_shape_unchecked(report: dict[str, Any]) -> bool:
    if not isinstance(report, dict) or set(report) != {
        "schema_version",
        "status",
        "ok",
        "root",
        "policy",
        "inventory",
        "roots",
        "routes",
        "route_authority",
        "privileged_profiles",
        "findings",
        "limits",
    }:
        return False
    if not _is_integer(report["schema_version"]) or report["schema_version"] != 1:
        return False
    if report["status"] not in {"PASS", "FAIL", "UNVERIFIED"}:
        return False
    if report["ok"] is not (report["status"] == "PASS") or report["root"] != ".":
        return False
    policy = report["policy"]
    if not isinstance(policy, dict) or set(policy) != {"path", "sha256", "schema_version"}:
        return False
    if policy["path"] not in {
        ".agents/policy/workflow-security-privileged.yml",
        ".agents/policy/workflow-security-privileged-v2.yml",
        ".agents/policy/workflow-security-privileged-v3.yml",
    }:
        return False
    if policy["sha256"] is not None and not _is_sha(policy["sha256"]):
        return False
    policy_version = policy["schema_version"]
    if policy_version is not None and (not _is_integer(policy_version) or policy_version not in {1, 2, 3}):
        return False
    if policy_version in {1, 2, 3} and not _is_sha(policy["sha256"]):
        return False
    if policy["sha256"] is None and policy["schema_version"] is not None:
        return False
    inventory = report["inventory"]
    if not isinstance(inventory, dict) or set(inventory) != {
        "complete",
        "manifest_sha256",
        "workflow_count",
        "job_count",
        "edge_count",
        "root_count",
        "route_count",
        "overflow_dimensions",
    }:
        return False
    if not isinstance(inventory["complete"], bool):
        return False
    manifest = inventory["manifest_sha256"]
    if manifest is not None and not _is_sha(manifest):
        return False
    if inventory["complete"] != (manifest is not None):
        return False
    count_limits = {
        "workflow_count": "workflow_files",
        "job_count": "jobs",
        "edge_count": "graph_edges",
        "root_count": "roots",
        "route_count": "routes",
    }
    contract = _policy(policy_version or 1)
    limits = contract["limits"]
    if report["limits"] != limits:
        return False
    if any(
        not _is_integer(inventory[count]) or not 0 <= inventory[count] <= limits[limit] + 1
        for count, limit in count_limits.items()
    ):
        return False
    arrays = ("roots", "routes", "route_authority", "privileged_profiles", "findings")
    overflow = inventory["overflow_dimensions"]
    if not isinstance(overflow, list) or not all(isinstance(report[key], list) for key in arrays):
        return False
    if overflow != sorted(set(overflow)) or any(item not in limits for item in overflow):
        return False
    if any((limit in overflow) is not (inventory[count] == limits[limit] + 1) for count, limit in count_limits.items()):
        return False
    if inventory["complete"] and overflow:
        return False
    if len(report["roots"]) > limits["roots"] or len(report["routes"]) > limits["routes"]:
        return False
    if len(report["route_authority"]) > limits["authority_records"]:
        return False
    if len(report["privileged_profiles"]) > limits["profile_matches"]:
        return False
    if len(report["findings"]) > limits["findings"]:
        return False
    for root in report["roots"]:
        if not isinstance(root, dict) or set(root) != {"workflow", "event"}:
            return False
        if not _is_text(root["workflow"], 1024) or root["event"] not in {"pull_request", "pull_request_target"}:
            return False
    if report["roots"] != sorted(report["roots"], key=lambda item: (item["workflow"].encode(), item["event"])):
        return False
    root_coordinates = {(item["workflow"], item["event"]) for item in report["roots"]}
    if len(root_coordinates) != len(report["roots"]):
        return False
    if inventory["complete"] and len({item["workflow"] for item in report["roots"]}) > inventory["workflow_count"]:
        return False
    if inventory["complete"] and inventory["job_count"] < inventory["workflow_count"]:
        return False
    if inventory["complete"] and inventory["root_count"] != len(report["roots"]):
        return False
    route_validation = validate_routes(report, limits)
    if route_validation is None:
        return False
    routes_by_id = route_validation.routes_by_id
    if inventory["complete"] and inventory["route_count"] != len(report["routes"]):
        return False
    if inventory["complete"] and (
        inventory["workflow_count"] < len(route_validation.workflows)
        or inventory["job_count"] < len(route_validation.jobs)
        or inventory["edge_count"] < len(route_validation.edges)
    ):
        return False
    authority_routes: list[str] = []
    for job in report["route_authority"]:
        required = {
            "route_id",
            "workflow",
            "workflow_name",
            "job_id",
            "classification",
            "declared_permissions",
            "effective_permissions",
            "permission_source",
            "runner",
            "environment",
            "secrets",
            "profile_id",
        }
        if not isinstance(job, dict) or set(job) != required:
            return False
        route = routes_by_id.get(job["route_id"])
        if route is None or any(job[key] != route[key] for key in ("workflow", "job_id", "classification")):
            return False
        if not _is_text(job["workflow"], 1024) or not _is_text(job["workflow_name"], 256):
            return False
        if not _is_text(job["job_id"], 256):
            return False
        if job["permission_source"] not in {"WORKFLOW", "JOB", "CALL_INTERSECTION"}:
            return False
        profiles = {None, *(value["id"] for value in contract["profiles"].values())}
        if job["profile_id"] not in profiles:
            return False
        for field in ("declared_permissions", "effective_permissions"):
            if not isinstance(job[field], dict) or set(job[field]) != set(PERMISSION_ACCESS):
                return False
            if any(value not in PERMISSION_ACCESS[key] for key, value in job[field].items()):
                return False
        if not isinstance(job["runner"], dict) or set(job["runner"]) != {"classification", "labels"}:
            return False
        if job["runner"]["classification"] not in {"GITHUB_HOSTED", "SELF_HOSTED", "UNKNOWN"}:
            return False
        labels = job["runner"]["labels"]
        if not isinstance(labels, list) or not all(_is_text(label, 128) for label in labels):
            return False
        if len(labels) > 16 or labels != sorted(set(labels)):
            return False
        if job["environment"] is not None and not _is_text(job["environment"], 256):
            return False
        if job["secrets"] not in {"NONE", "EXPLICIT", "INHERIT", "UNKNOWN"}:
            return False
        authority_routes.append(job["route_id"])
    if set(authority_routes) != set(routes_by_id) or len(authority_routes) != len(set(authority_routes)):
        return False
    if report["route_authority"] != sorted(
        report["route_authority"], key=lambda item: (item["route_id"], item["workflow"].encode(), item["job_id"])
    ):
        return False
    profile_routes: list[str] = []
    profile_definitions = {}
    for key, value in contract["profiles"].items():
        declared_permissions = {permission: "none" for permission in PERMISSION_ACCESS}
        declared_permissions.update(value["declared_non_none"])
        effective_permissions = {permission: "none" for permission in PERMISSION_ACCESS}
        effective_permissions.update(value["effective_non_none"])
        profile_definitions[value["id"]] = {
            "workflow": value["workflow"],
            "job_id": value["job"],
            "fingerprint": value.get("semantic_sha256", value.get("envelope_sha256")),
            "classification": "POST_MERGE_INTEGRATED_CODE" if key == "merged_closure" else "PR_HEAD",
            "root_event": value["root_event"],
            "event_variants": value["event_variants"],
            "edge_kinds": value["edge_kinds"],
            "required_edge_chain": value["required_edge_chain"],
            "permission_source": value["permission_source"],
            "declared_permissions": declared_permissions,
            "effective_permissions": effective_permissions,
            "runner": value["runner"],
            "environment": value["environment"],
            "secrets": value["secrets"],
        }
    for profile in report["privileged_profiles"]:
        if not isinstance(profile, dict) or set(profile) != {
            "route_id",
            "id",
            "workflow",
            "job_id",
            "fingerprint",
            "classification",
        }:
            return False
        route = routes_by_id.get(profile["route_id"])
        if route is None or any(profile[key] != route[key] for key in ("workflow", "job_id", "classification")):
            return False
        expected_profile = profile_definitions.get(profile["id"])
        if expected_profile is None:
            return False
        if any(
            profile[key] != expected_profile[key] for key in ("workflow", "job_id", "fingerprint", "classification")
        ):
            return False
        root = report["roots"][route["root_index"]]
        if root != {"workflow": expected_profile["workflow"], "event": expected_profile["root_event"]}:
            return False
        if route["event_variant"] not in expected_profile["event_variants"]:
            return False
        if [edge["kind"] for edge in route["edge_chain"]] != expected_profile["edge_kinds"]:
            return False
        if route["edge_chain"] != expected_profile["required_edge_chain"]:
            return False
        authority = next(item for item in report["route_authority"] if item["route_id"] == route["route_id"])
        if any(
            authority[field] != expected_profile[field]
            for field in (
                "permission_source",
                "declared_permissions",
                "effective_permissions",
                "runner",
                "environment",
                "secrets",
            )
        ):
            return False
        profile_routes.append(profile["route_id"])
    if len(profile_routes) != len(set(profile_routes)):
        return False
    expected_profile_ids = {
        job["route_id"]: job["profile_id"] for job in report["route_authority"] if job["profile_id"] is not None
    }
    observed_profile_ids = {profile["route_id"]: profile["id"] for profile in report["privileged_profiles"]}
    if observed_profile_ids != expected_profile_ids:
        return False
    if report["privileged_profiles"] != sorted(
        report["privileged_profiles"], key=lambda item: (item["id"], item["route_id"])
    ):
        return False
    code_to_outcome = contract["recovery"]["code_to_outcome"]
    for finding in report["findings"]:
        if not isinstance(finding, dict) or set(finding) != {
            "code",
            "status",
            "subject",
            "route_id",
            "detail",
            "recovery_command_id",
        }:
            return False
        if finding["status"] not in {"FAIL", "UNVERIFIED"} or not isinstance(finding["code"], str):
            return False
        if code_to_outcome.get(finding["code"]) != {
            "status": finding["status"],
            "recovery_command_id": finding["recovery_command_id"],
        }:
            return False
        if not _is_text(finding["subject"], 1024):
            return False
        if not _is_text(finding["detail"], limits["finding_detail_bytes"], detail=True):
            return False
        if finding["route_id"] is not None and not _is_sha(finding["route_id"]):
            return False

    def finding_order(item: dict[str, Any]) -> tuple[object, ...]:
        return (
            0 if item["status"] == "FAIL" else 1,
            item["code"],
            item["subject"],
            item["route_id"] or "",
            item["detail"],
        )

    if report["findings"] != sorted(report["findings"], key=finding_order):
        return False
    expected_status = (
        "FAIL"
        if any(item["status"] == "FAIL" for item in report["findings"])
        else "UNVERIFIED"
        if report["findings"]
        else "PASS"
    )
    if report["status"] != expected_status:
        return False
    if report["status"] == "PASS" and (
        not inventory["complete"]
        or inventory["workflow_count"] == 0
        or inventory["route_count"] < inventory["root_count"]
        or not _is_sha(policy["sha256"])
        or policy["schema_version"] not in {1, 2, 3}
        or not _is_sha(inventory["manifest_sha256"])
        or overflow
        or any(root["event"] == "pull_request_target" for root in report["roots"])
        or {route["root_index"] for route in report["routes"]} != set(range(len(report["roots"])))
    ):
        return False
    if report["status"] == "PASS":
        for authority in report["route_authority"]:
            if authority["classification"] == "PROVEN_NOT_PR_REACHABLE":
                continue
            privileged = (
                "write" in authority["effective_permissions"].values()
                or authority["secrets"] != "NONE"
                or authority["environment"] is not None
                or authority["runner"]["classification"] != "GITHUB_HOSTED"
            )
            if privileged is not (authority["profile_id"] is not None):
                return False
    if overflow and not any(item["code"] == "PRIVILEGE_RESOURCE_LIMIT" for item in report["findings"]):
        return False
    return True


def valid_report_shape(report: dict[str, Any]) -> bool:
    try:
        return _valid_report_shape_unchecked(report)
    except (KeyError, TypeError, UnicodeError, ValueError):
        return False
