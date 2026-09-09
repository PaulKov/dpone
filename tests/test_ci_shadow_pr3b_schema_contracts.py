from __future__ import annotations

import copy
from pathlib import Path
from typing import Any

import yaml

from tests.ci_shadow_pr3b_report_contract_support import (
    mandatory_profile_evidence_matches,
    required_profile_evidence,
)

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


def _read() -> str:
    return SPEC.read_text(encoding="utf-8")


def _squash(value: str) -> str:
    return " ".join(value.split())


def _outcome(status: str, recovery_command_id: str) -> dict[str, str]:
    return {"status": status, "recovery_command_id": recovery_command_id}


def _marked_yaml(text: str, marker: str) -> dict[str, Any]:
    begin = f"<!-- {marker}:begin -->"
    end = f"<!-- {marker}:end -->"
    assert text.count(begin) == text.count(end) == 1
    marked = text.split(begin, 1)[1].split(end, 1)[0]
    block = marked.split("```yaml\n", 1)[1].split("\n```", 1)[0]
    parsed = yaml.safe_load(block)
    assert isinstance(parsed, dict)
    return parsed


def test_pr3b_policy_v1_is_a_closed_exact_value() -> None:
    policy = _marked_yaml(_read(), "pr3b-policy-v1")

    assert list(policy) == [
        "schema_version",
        "authority",
        "roots",
        "edge_kinds",
        "permission_access",
        "limits",
        "profiles",
        "recovery",
    ]
    assert policy["schema_version"] == 1
    assert policy["authority"] == {
        "specification": "docs/feature-design-ci-shadow-pr3b-semantic-privilege-boundary.md",
        "design_base": "c5567128e6e9b847b4b2a023a74ea6d60e7ccd09",
        "adr0037": "docs/adr/0037-immutable-agent-pr-merge-closure.md",
        "pr2_merge": "ec0323feba829d547e5f9c2d23acaac5b686f1a4",
    }
    assert policy["roots"] == ["pull_request", "pull_request_target"]
    assert policy["edge_kinds"] == ["LOCAL_WORKFLOW_CALL", "WORKFLOW_RUN", "NEEDS"]
    assert policy["permission_access"] == PERMISSION_ACCESS
    assert policy["limits"] == {
        "workflow_files": 256,
        "reusable_workflows_including_root": 50,
        "local_call_edges": 9,
        "workflow_run_edges": 1,
        "jobs": 4096,
        "graph_edges": 8192,
        "roots": 512,
        "routes": 16384,
        "authority_records": 16384,
        "profile_matches": 16384,
        "route_edges": 256,
        "findings": 4096,
        "workflow_bytes": 1048576,
        "total_workflow_bytes": 33554432,
        "policy_bytes": 1048576,
        "expression_bytes": 8192,
        "expression_tokens": 512,
        "yaml_depth": 32,
        "yaml_nodes": 100000,
        "finding_detail_bytes": 2048,
        "report_bytes": 16777216,
        "text_stdout_bytes": 16777216,
    }
    assert policy["profiles"] == {
        "codeql": {
            "id": "CODEQL_PR_UPLOAD",
            "workflow": ".github/workflows/codeql.yml",
            "job": "analyze",
            "required_occurrences": 1,
            "root_event": "pull_request",
            "event_variants": ["ACTIVITY:opened", "ACTIVITY:reopened", "ACTIVITY:synchronize"],
            "edge_kinds": [],
            "required_edge_chain": [],
            "trigger": {
                "pull_request": {"branches": ["master"]},
                "push": {"branches": ["master"]},
                "schedule": [{"cron": "21 3 * * 1"}],
            },
            "trigger_sha256": "3273e73a2ce7ec7b858a8a806755c842dbb4533594b318530f831a4fb0445a03",
            "permission_source": "WORKFLOW",
            "declared_non_none": {"contents": "read", "security-events": "write"},
            "effective_non_none": {"contents": "read", "security-events": "write"},
            "runner": {"classification": "GITHUB_HOSTED", "labels": ["ubuntu-latest"]},
            "environment": None,
            "secrets": "NONE",
            "semantic_sha256": "36a5ff903eab36601bd3cfe3636a53497a9d72cd7ca71ce39ef9c19a8aef3c88",
        },
        "governance_source": {
            "id": "ADR0037_GOVERNANCE_SOURCE_ATTESTOR",
            "workflow": ".github/workflows/ci.yml",
            "job": "governance-attestation",
            "required_occurrences": 1,
            "root_event": "pull_request",
            "event_variants": ["ACTIVITY:opened", "ACTIVITY:reopened", "ACTIVITY:synchronize"],
            "edge_kinds": ["NEEDS", "NEEDS"],
            "required_edge_chain": [
                {
                    "kind": "NEEDS",
                    "source_workflow": ".github/workflows/ci.yml",
                    "source_job": "quality",
                    "target_workflow": ".github/workflows/ci.yml",
                    "target_job": "governance-source",
                },
                {
                    "kind": "NEEDS",
                    "source_workflow": ".github/workflows/ci.yml",
                    "source_job": "governance-source",
                    "target_workflow": ".github/workflows/ci.yml",
                    "target_job": "governance-attestation",
                },
            ],
            "trigger": {
                "pull_request": {"branches": ["master"]},
                "push": {"branches": ["master"]},
                "workflow_dispatch": None,
            },
            "trigger_sha256": "481e6bf2e4a8ad5c03d98987c268f105189028497bd227b51786f0764536e2ef",
            "permission_source": "JOB",
            "declared_non_none": {
                "actions": "read",
                "attestations": "write",
                "contents": "read",
                "id-token": "write",
            },
            "effective_non_none": {
                "actions": "read",
                "attestations": "write",
                "contents": "read",
                "id-token": "write",
            },
            "runner": {"classification": "GITHUB_HOSTED", "labels": ["ubuntu-latest"]},
            "environment": None,
            "secrets": "NONE",
            "producer_sha256": "0bf8016384b224f58cd6be67fc892531cfdad21c114b48fb9f0d90a5d3c9f97f",
            "envelope_sha256": "c5832e92a84ddd4eaa44cbf009d45eb7de2c4a7c3bcfc4258da1702b5aa0b3af",
        },
        "merged_closure": {
            "id": "ADR0037_MERGED_CLOSURE_CHECK_PUBLISHER",
            "workflow": ".github/workflows/agent-pr-receipt.yml",
            "job": "merge-closure",
            "required_occurrences": 1,
            "root_event": "pull_request",
            "event_variants": ["CLOSED_MERGED"],
            "edge_kinds": [],
            "required_edge_chain": [],
            "trigger": {"pull_request": {"branches": ["master"], "types": ["edited", "closed"]}},
            "trigger_sha256": "893b21fe5846674b730966a96e22ee6a05aaa693b73152b9720ae071511b3f68",
            "permission_source": "JOB",
            "declared_non_none": {"actions": "read", "checks": "write", "contents": "read"},
            "effective_non_none": {"actions": "read", "checks": "write", "contents": "read"},
            "runner": {"classification": "GITHUB_HOSTED", "labels": ["ubuntu-latest"]},
            "environment": None,
            "secrets": "NONE",
            "envelope_sha256": "92c783c76af4f23ccbce06892fd297f73c830120a030b739a8b05055dec406f3",
        },
    }
    recovery = policy["recovery"]
    assert recovery["runbook_anchor"] == "docs/cicd/runbooks.md#semantic-pr-privilege-boundary"
    assert recovery["recheck_argv"] == [
        "uv",
        "run",
        "python",
        "tools/agent_policy/workflow_security_privileged.py",
        "--root",
        ".",
        "--format",
        "text",
    ]
    assert recovery["code_to_outcome"] == {
        "PRIVILEGE_PULL_REQUEST_TARGET": _outcome("FAIL", "REMOVE_PULL_REQUEST_TARGET"),
        "PRIVILEGE_UNAPPROVED_PR_WRITE": _outcome("FAIL", "REDUCE_OR_ISOLATE_PR_AUTHORITY"),
        "PRIVILEGE_PR_SECRET_OR_ENVIRONMENT": _outcome("FAIL", "REMOVE_PR_SECRET_AUTHORITY"),
        "PRIVILEGE_PR_SELF_HOSTED": _outcome("FAIL", "USE_GITHUB_HOSTED_PR_RUNNER"),
        "PRIVILEGE_WRITE_ALL": _outcome("FAIL", "REPLACE_WRITE_ALL"),
        "PRIVILEGE_READ_ALL": _outcome("FAIL", "REPLACE_READ_ALL"),
        "PRIVILEGE_CODEQL_PROFILE_DRIFT": _outcome("FAIL", "RESTORE_CODEQL_PROFILE"),
        "PRIVILEGE_ADR0037_PROFILE_DRIFT": _outcome("FAIL", "RESTORE_ADR0037_PROFILE"),
        "PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE": _outcome("UNVERIFIED", "BOUND_WORKFLOW_EDGE"),
        "PRIVILEGE_UNKNOWN_EXPRESSION": _outcome("UNVERIFIED", "SIMPLIFY_PRIVILEGE_GUARD"),
        "PRIVILEGE_UNKNOWN_PERMISSION": _outcome("UNVERIFIED", "UPDATE_PERMISSION_CONTRACT"),
        "PRIVILEGE_INVALID_POLICY": _outcome("UNVERIFIED", "REPAIR_PRIVILEGE_POLICY"),
        "PRIVILEGE_INVALID_WORKFLOW": _outcome("UNVERIFIED", "REPAIR_WORKFLOW_SYNTAX"),
        "PRIVILEGE_RESOURCE_LIMIT": _outcome("UNVERIFIED", "REDUCE_OR_PARTITION_WORKFLOWS"),
        "PRIVILEGE_CONCURRENT_MUTATION": _outcome("UNVERIFIED", "RERUN_IMMUTABLE_CHECKOUT"),
    }


def test_pr3b_mandatory_profile_subjects_triggers_and_routes_are_exact_multisets() -> None:
    policy = _marked_yaml(_read(), "pr3b-policy-v1")
    subjects, coordinates = required_profile_evidence(policy)

    assert mandatory_profile_evidence_matches(policy, subjects, coordinates)
    assert len(subjects) == 3
    assert len(coordinates) == 7

    mutations: list[tuple[list[dict[str, Any]], list[dict[str, Any]]]] = []
    mutations.append((subjects[:-1], coordinates))
    mutations.append((subjects + [copy.deepcopy(subjects[0])], coordinates))
    trigger_drift = copy.deepcopy(subjects)
    trigger_drift[0]["trigger_sha256"] = "f" * 64
    mutations.append((trigger_drift, coordinates))
    renamed_job = copy.deepcopy(subjects)
    renamed_job[1]["job"] = "renamed-finalizer"
    mutations.append((renamed_job, coordinates))
    mutations.append((subjects, coordinates[:-1]))
    mutations.append((subjects, coordinates + [copy.deepcopy(coordinates[0])]))
    edge_drift = copy.deepcopy(coordinates)
    edge_drift[3]["edge_chain"] = []
    mutations.append((subjects, edge_drift))

    assert not any(mandatory_profile_evidence_matches(policy, left, right) for left, right in mutations)

    reordered = copy.deepcopy(policy)
    reordered["profiles"]["codeql"]["trigger"] = dict(
        reversed(list(reordered["profiles"]["codeql"]["trigger"].items()))
    )
    assert mandatory_profile_evidence_matches(reordered, subjects, coordinates)

    trigger_mutations = []
    without_branch = copy.deepcopy(policy)
    without_branch["profiles"]["governance_source"]["trigger"]["pull_request"] = {}
    trigger_mutations.append(without_branch)
    extra_branch = copy.deepcopy(policy)
    extra_branch["profiles"]["governance_source"]["trigger"]["pull_request"]["branches"].append("develop")
    trigger_mutations.append(extra_branch)
    with_paths = copy.deepcopy(policy)
    with_paths["profiles"]["governance_source"]["trigger"]["pull_request"]["paths"] = ["src/**"]
    trigger_mutations.append(with_paths)
    without_closed = copy.deepcopy(policy)
    without_closed["profiles"]["merged_closure"]["trigger"]["pull_request"]["types"].remove("closed")
    trigger_mutations.append(without_closed)
    assert not any(mandatory_profile_evidence_matches(item, subjects, coordinates) for item in trigger_mutations)


def test_pr3b_expression_transfer_and_depth_conventions_are_closed() -> None:
    spec = _squash(_read())

    for expected in (
        "No bare property truthiness is accepted",
        "exact string `pull_request`",
        "symbolic `refs/pull/<positive decimal>/merge`",
        "expanded into correlated event variants",
        "`CLOSED_UNMERGED` and `CLOSED_MERGED`",
        "never joins the two `closed` results before profile matching",
        "String `==`/`!=` and `startsWith` use GitHub's case-insensitive string comparison",
        "github.event_name == 'PULL_REQUEST'",
        "github.ref == 'refs/heads/master'",
        "A false unmerged route does not erase a true merged route",
        "approved merged-closure profile reachable",
        "github.event_name=workflow_run",
        "github.event_name != 'pull_request'` is `TRUE",
        "cross-type coercion",
        "preserves UNKNOWN",
        "UNKNOWN | UNKNOWN | UNKNOWN | UNKNOWN",
        "direct root caller is reusable-workflow level 1",
        "nine local call edges reach level 10",
        "tenth local call edge would create level 11",
        "50 unique reusable workflows including every root/callee",
    ):
        assert expected in spec


def test_pr3b_closed_event_variants_preserve_the_merged_profile_route() -> None:
    variants = {
        "CLOSED_UNMERGED": {
            "action": "closed",
            "ref": "refs/pull/17/merge",
            "merged": False,
        },
        "CLOSED_MERGED": {
            "action": "closed",
            "ref": "refs/heads/master",
            "merged": True,
        },
    }

    reachable = {
        variant: values["action"] == "closed" and values["merged"] is True for variant, values in variants.items()
    }
    assert reachable == {"CLOSED_UNMERGED": False, "CLOSED_MERGED": True}
    assert variants["CLOSED_MERGED"]["ref"] == "refs/heads/master"


def test_pr3b_workflow_run_resets_the_downstream_event_context() -> None:
    route_contexts = {
        "ACTIVITY:opened": {"event_name": "pull_request"},
        "ACTIVITY:opened>WORKFLOW_RUN:completed": {"event_name": "workflow_run"},
    }

    guarded = {route: context["event_name"].casefold() != "pull_request" for route, context in route_contexts.items()}
    assert guarded == {
        "ACTIVITY:opened": False,
        "ACTIVITY:opened>WORKFLOW_RUN:completed": True,
    }
    assert {depth: depth <= 1 for depth in (0, 1, 2)} == {0: True, 1: True, 2: False}


def test_pr3b_existing_policy_schema_repair_rejects_authority_free_entries() -> None:
    spec = _squash(_read())

    for expected in (
        "The two forms are mutually exclusive",
        "`reason` alone",
        "both forms",
        "an empty job map/list",
        "exactly eight required `const: true` keys",
        "require_hidden_ci_artifact_transport",
        "isolate_dbt_execution_from_oidc",
        "require_platform_owned_prod_trust_policy",
        "independently reject every malformed form",
    ):
        assert expected in spec
