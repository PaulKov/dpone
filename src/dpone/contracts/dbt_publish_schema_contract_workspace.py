"""Versioned workspace discovery/check report schemas."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

from dpone.contracts.dbt_publish_schema_contract_common import DIGEST, RELATIVE, TOKEN, nullable, object_schema


def workspace_report_schema_contracts(*, compile_report: dict[str, Any]) -> dict[str, dict[str, Any]]:
    issue = deepcopy(compile_report["properties"]["blockers"]["items"])
    for field in ("message", "remediation"):
        issue["properties"][field] = {"type": "string", "minLength": 1, "maxLength": 4096}
    issues = {"type": "array", "items": issue}
    project = object_schema(
        ("project_path", "project_name", "publishing", "profiles_path", "manifest_path", "reason"),
        {
            "project_path": RELATIVE,
            "project_name": nullable(TOKEN),
            "publishing": {"type": "boolean"},
            "profiles_path": nullable(RELATIVE),
            "manifest_path": nullable(RELATIVE),
            "reason": {"enum": ["policy_present", "not_configured", "invalid"]},
        },
    )
    project["allOf"] = [
        {
            "if": {"properties": {"reason": {"const": "policy_present"}}},
            "then": {
                "properties": {
                    "publishing": {"const": True},
                    "project_name": TOKEN,
                    "profiles_path": RELATIVE,
                    "manifest_path": RELATIVE,
                }
            },
            "else": {"properties": {"publishing": {"const": False}}},
        },
    ]
    discovery = object_schema(
        ("schema", "passed", "projects", "blockers"),
        {
            "schema": {"const": "dpone.dbt-workspace-discovery.v1"},
            "passed": {"type": "boolean"},
            "projects": {"type": "array", "maxItems": 64, "items": project},
            "blockers": issues,
        },
    )
    check = object_schema(
        ("schema", "passed", "discovery", "projects", "blockers"),
        {
            "schema": {"const": "dpone.dbt-workspace-check.v1"},
            "passed": {"type": "boolean"},
            "discovery": deepcopy(discovery),
            "projects": {
                "type": "array",
                "maxItems": 64,
                "items": object_schema(
                    ("project_path", "report"), {"project_path": RELATIVE, "report": deepcopy(compile_report)}
                ),
            },
            "blockers": deepcopy(issues),
        },
    )
    compile_result = object_schema(
        (
            "schema",
            "passed",
            "check",
            "output_dir",
            "release_id",
            "source_snapshot_sha256",
            "subject_sha256",
            "blockers",
        ),
        {
            "schema": {"const": "dpone.dbt-workspace-compile.v1"},
            "passed": {"type": "boolean"},
            "check": deepcopy(check),
            "output_dir": {"type": "string", "minLength": 1, "maxLength": 4096},
            "release_id": nullable(DIGEST),
            "source_snapshot_sha256": nullable(DIGEST),
            "subject_sha256": nullable(DIGEST),
            "blockers": deepcopy(issues),
        },
    )
    compile_result["allOf"] = [
        {
            "if": {"properties": {"passed": {"const": True}}},
            "then": {
                "properties": {
                    "release_id": DIGEST,
                    "source_snapshot_sha256": DIGEST,
                    "subject_sha256": DIGEST,
                    "blockers": {"maxItems": 0},
                    "check": {"properties": {"passed": {"const": True}, "projects": {"minItems": 1}}},
                }
            },
            "else": {
                "properties": {
                    name: {"type": "null"} for name in ("release_id", "source_snapshot_sha256", "subject_sha256")
                }
            },
        }
    ]
    return {
        "dpone.dbt-workspace-discovery.v1": discovery,
        "dpone.dbt-workspace-check.v1": check,
        "dpone.dbt-workspace-compile.v1": compile_result,
    }
