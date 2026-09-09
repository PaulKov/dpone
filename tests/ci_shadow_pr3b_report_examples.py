from __future__ import annotations

import copy
import json
from pathlib import Path
from typing import Any

from tests.ci_shadow_pr3b_report_contract_support import PERMISSION_ACCESS, canonical_route_id_v1

ROOT = Path(__file__).resolve().parents[1]
SPEC = ROOT / "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md"


def report_example() -> dict[str, Any]:
    text = SPEC.read_text(encoding="utf-8")
    begin = "<!-- pr3b-report-v1:begin -->"
    end = "<!-- pr3b-report-v1:end -->"
    assert text.count(begin) == text.count(end) == 1
    block = text.split(begin, 1)[1].split(end, 1)[0].split("```json\n", 1)[1].split("\n```", 1)[0]
    parsed = json.loads(block)
    assert isinstance(parsed, dict)
    return parsed


def report_with_one_codeql_route() -> dict[str, Any]:
    report = copy.deepcopy(report_example())
    permissions = {key: "none" for key in PERMISSION_ACCESS}
    permissions.update({"contents": "read", "security-events": "write"})
    report["roots"] = [{"workflow": ".github/workflows/codeql.yml", "event": "pull_request"}]
    route = {
        "root_index": 0,
        "event_variant": "ACTIVITY:opened",
        "edge_chain": [],
        "workflow": ".github/workflows/codeql.yml",
        "job_id": "analyze",
        "classification": "PR_HEAD",
    }
    digest = canonical_route_id_v1(route)
    report["routes"] = [{"route_id": digest, **route}]
    report["route_authority"] = [
        {
            "route_id": digest,
            "workflow": route["workflow"],
            "workflow_name": "CodeQL",
            "job_id": route["job_id"],
            "classification": route["classification"],
            "declared_permissions": permissions,
            "effective_permissions": permissions,
            "permission_source": "WORKFLOW",
            "runner": {"classification": "GITHUB_HOSTED", "labels": ["ubuntu-latest"]},
            "environment": None,
            "secrets": "NONE",
            "profile_id": "CODEQL_PR_UPLOAD",
        }
    ]
    report["privileged_profiles"] = [
        {
            "route_id": digest,
            "id": "CODEQL_PR_UPLOAD",
            "workflow": route["workflow"],
            "job_id": route["job_id"],
            "fingerprint": "36a5ff903eab36601bd3cfe3636a53497a9d72cd7ca71ce39ef9c19a8aef3c88",
            "classification": route["classification"],
        }
    ]
    report["inventory"].update(workflow_count=1, job_count=1, root_count=1, route_count=1)
    return report


def rebind_route(report: dict[str, Any]) -> None:
    route = report["routes"][0]
    route_id = canonical_route_id_v1({key: value for key, value in route.items() if key != "route_id"})
    route["route_id"] = route_id
    report["route_authority"][0]["route_id"] = route_id
    if report["privileged_profiles"]:
        report["privileged_profiles"][0]["route_id"] = route_id


def make_unprivileged(report: dict[str, Any]) -> None:
    permissions = {key: "none" for key in PERMISSION_ACCESS}
    report["route_authority"][0].update(
        declared_permissions=permissions,
        effective_permissions=permissions,
        permission_source="WORKFLOW",
        profile_id=None,
    )
    report["privileged_profiles"] = []


def profile_report(profile: str) -> dict[str, Any]:
    report = report_with_one_codeql_route()
    if profile == "governance":
        workflow = ".github/workflows/ci.yml"
        job = "governance-attestation"
        profile_id = "ADR0037_GOVERNANCE_SOURCE_ATTESTOR"
        fingerprint = "c5832e92a84ddd4eaa44cbf009d45eb7de2c4a7c3bcfc4258da1702b5aa0b3af"
        variant = "ACTIVITY:opened"
        classification = "PR_HEAD"
        permissions = {"actions": "read", "attestations": "write", "contents": "read", "id-token": "write"}
        edge_chain = [
            {
                "kind": "NEEDS",
                "source_workflow": workflow,
                "source_job": "quality",
                "target_workflow": workflow,
                "target_job": "governance-source",
            },
            {
                "kind": "NEEDS",
                "source_workflow": workflow,
                "source_job": "governance-source",
                "target_workflow": workflow,
                "target_job": job,
            },
        ]
    elif profile == "merged_closure":
        workflow = ".github/workflows/agent-pr-receipt.yml"
        job = "merge-closure"
        profile_id = "ADR0037_MERGED_CLOSURE_CHECK_PUBLISHER"
        fingerprint = "92c783c76af4f23ccbce06892fd297f73c830120a030b739a8b05055dec406f3"
        variant = "CLOSED_MERGED"
        classification = "POST_MERGE_INTEGRATED_CODE"
        permissions = {"actions": "read", "checks": "write", "contents": "read"}
        edge_chain = []
    else:
        raise ValueError(f"unknown closed profile: {profile}")
    complete_permissions = {key: "none" for key in PERMISSION_ACCESS}
    complete_permissions.update(permissions)
    report["roots"] = [{"workflow": workflow, "event": "pull_request"}]
    report["routes"][0].update(
        event_variant=variant,
        edge_chain=edge_chain,
        workflow=workflow,
        job_id=job,
        classification=classification,
    )
    report["route_authority"][0].update(
        workflow=workflow,
        workflow_name="CI" if profile == "governance" else "Agent PR receipt",
        job_id=job,
        classification=classification,
        declared_permissions=complete_permissions,
        effective_permissions=complete_permissions,
        permission_source="JOB",
        profile_id=profile_id,
    )
    report["privileged_profiles"][0].update(
        id=profile_id,
        workflow=workflow,
        job_id=job,
        fingerprint=fingerprint,
        classification=classification,
    )
    report["inventory"].update(job_count=3 if profile == "governance" else 1, edge_count=len(edge_chain))
    rebind_route(report)
    return report
