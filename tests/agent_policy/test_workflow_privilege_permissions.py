from __future__ import annotations

import importlib
from collections.abc import Mapping
from typing import Any

import pytest
from tests.ci_shadow_pr3b_report_contract_support import PERMISSION_ACCESS, canonical_route_id_v1, policy
from tools.agent_policy.workflow_privilege_contracts import valid_public_text

ROOT_WORKFLOW = ".github/workflows/root.yml"
CALLEE_WORKFLOW = ".github/workflows/callee.yml"


def _permission_api() -> Any:
    try:
        module = importlib.import_module("tools.agent_policy.workflow_privilege_permissions")
    except ModuleNotFoundError as exc:
        pytest.fail(f"RED: semantic privilege permission module is not implemented: {exc}", pytrace=False)
    if not hasattr(module, "resolve_authority"):
        pytest.fail("RED: semantic privilege permission API lacks resolve_authority", pytrace=False)
    return module


def _job(
    *,
    permissions: object = None,
    runs_on: object = "ubuntu-latest",
    environment: object = None,
    secrets: object = "NONE",
    uses: str | None = None,
) -> dict[str, object]:
    return {
        "needs": [],
        "if": None,
        "runs-on": runs_on,
        "permissions": permissions,
        "environment": environment,
        "secrets": secrets,
        "uses": uses,
        "with": {},
        "steps": [],
    }


def _workflow(
    path: str,
    *,
    permissions: object,
    jobs: Mapping[str, Mapping[str, object]],
    name: str = "Workflow",
) -> dict[str, object]:
    events: dict[str, object] = (
        {"workflow_call": {}} if path == CALLEE_WORKFLOW else {"pull_request": {"branches": ["master"]}}
    )
    return {
        "path": path,
        "name": name,
        "on": events,
        "permissions": permissions,
        "env": {},
        "defaults": {},
        "jobs": {key: dict(value) for key, value in jobs.items()},
    }


def _route(
    *,
    workflow: str = ROOT_WORKFLOW,
    job_id: str = "inspect",
    edge_chain: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    value: dict[str, object] = {
        "root_index": 0,
        "event_variant": "ACTIVITY:opened",
        "edge_chain": list(edge_chain or []),
        "workflow": workflow,
        "job_id": job_id,
        "classification": "PR_HEAD",
    }
    return {"route_id": canonical_route_id_v1(value), **value}


def _as_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    converter = getattr(value, "to_mapping", None)
    if callable(converter):
        converted = converter()
        assert isinstance(converted, Mapping)
        return dict(converted)
    fields = (
        "declared_permissions",
        "effective_permissions",
        "permission_source",
        "runner",
        "environment",
        "secrets",
        "privileged",
    )
    if all(hasattr(value, field) for field in fields):
        return {field: getattr(value, field) for field in fields}
    pytest.fail(f"authority value is not a deterministic mapping/model: {value!r}")


def _codes(result: object) -> set[str]:
    values: set[str] = set()
    for finding in getattr(result, "findings", ()):
        raw = _as_finding_mapping(finding)["code"]
        code = getattr(raw, "value", raw)
        assert isinstance(code, str)
        values.add(code)
    return values


def _as_finding_mapping(value: object) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    converter = getattr(value, "to_mapping", None)
    if callable(converter):
        converted = converter()
        assert isinstance(converted, Mapping)
        return dict(converted)
    code = getattr(value, "code", None)
    status = getattr(value, "status", None)
    return {"code": code, "status": status}


def _complete(**non_none: str) -> dict[str, str]:
    result = {name: "none" for name in PERMISSION_ACCESS}
    result.update(non_none)
    return result


def _resolve(
    workflows: Mapping[str, Mapping[str, object]],
    route: Mapping[str, object] | None = None,
) -> tuple[object, dict[str, Any]]:
    result = _permission_api().resolve_authority(route or _route(), workflows, policy())
    return result, _as_mapping(result)


def test_workflow_permissions_are_completed_with_omitted_scopes_set_to_none() -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            permissions={"contents": "read", "checks": "write"},
            jobs={"inspect": _job()},
        )
    }

    result, authority = _resolve(workflows)

    expected = _complete(contents="read", checks="write")
    assert authority["declared_permissions"] == expected
    assert authority["effective_permissions"] == expected
    assert authority["permission_source"] == "WORKFLOW"
    assert authority["privileged"] is True
    assert "PRIVILEGE_UNKNOWN_PERMISSION" not in _codes(result)


def test_job_permissions_replace_the_workflow_mapping_instead_of_merging_it() -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            permissions={"contents": "write", "checks": "write"},
            jobs={"inspect": _job(permissions={"contents": "read"})},
        )
    }

    _result, authority = _resolve(workflows)

    assert authority["declared_permissions"] == _complete(contents="read")
    assert authority["effective_permissions"] == _complete(contents="read")
    assert authority["permission_source"] == "JOB"
    assert authority["privileged"] is False


def test_called_workflow_intersection_preserves_caller_oidc_authority() -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            permissions={},
            jobs={
                "delegate": _job(
                    permissions={"contents": "read", "checks": "write", "id-token": "write"},
                    uses="./.github/workflows/callee.yml",
                )
            },
        ),
        CALLEE_WORKFLOW: _workflow(
            CALLEE_WORKFLOW,
            permissions={},
            jobs={"publish": _job()},
            name="Callee",
        ),
    }
    edge: dict[str, object] = {
        "kind": "LOCAL_WORKFLOW_CALL",
        "source_workflow": ROOT_WORKFLOW,
        "source_job": "delegate",
        "target_workflow": CALLEE_WORKFLOW,
        "target_job": None,
    }

    _result, authority = _resolve(
        workflows,
        _route(workflow=CALLEE_WORKFLOW, job_id="publish", edge_chain=[edge]),
    )

    assert authority["declared_permissions"] == authority["effective_permissions"] == _complete()
    assert authority["permission_source"] == "CALL_INTERSECTION"
    assert authority["privileged"] is True


def test_nested_callee_job_mapping_cannot_restore_authority_removed_by_a_caller() -> None:
    middle = ".github/workflows/middle.yml"
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            permissions={},
            jobs={"call-middle": _job(permissions={"contents": "read"}, uses="./.github/workflows/middle.yml")},
        ),
        middle: {
            **_workflow(
                middle,
                permissions={"contents": "write", "checks": "write"},
                jobs={"call-leaf": _job(permissions={"contents": "read"}, uses="./.github/workflows/callee.yml")},
            ),
            "on": {"workflow_call": {}},
        },
        CALLEE_WORKFLOW: _workflow(
            CALLEE_WORKFLOW,
            permissions={"contents": "write", "checks": "write"},
            jobs={"publish": _job(permissions={"contents": "write", "checks": "write"})},
        ),
    }
    edges: list[dict[str, object]] = [
        {
            "kind": "LOCAL_WORKFLOW_CALL",
            "source_workflow": ROOT_WORKFLOW,
            "source_job": "call-middle",
            "target_workflow": middle,
            "target_job": None,
        },
        {
            "kind": "LOCAL_WORKFLOW_CALL",
            "source_workflow": middle,
            "source_job": "call-leaf",
            "target_workflow": CALLEE_WORKFLOW,
            "target_job": None,
        },
    ]

    _result, authority = _resolve(
        workflows,
        _route(workflow=CALLEE_WORKFLOW, job_id="publish", edge_chain=edges),
    )

    assert authority["declared_permissions"] == _complete(contents="write", checks="write")
    assert authority["effective_permissions"] == _complete(contents="read")
    assert authority["permission_source"] == "CALL_INTERSECTION"
    assert authority["privileged"] is False


@pytest.mark.parametrize(
    ("permissions", "expected_code", "expected_privileged"),
    [
        ("write-all", "PRIVILEGE_WRITE_ALL", True),
        ("read-all", "PRIVILEGE_READ_ALL", False),
        ({"future-scope": "read"}, "PRIVILEGE_UNKNOWN_PERMISSION", False),
        ({"future-scope": "write"}, "PRIVILEGE_UNKNOWN_PERMISSION", True),
        ({"id-token": "write"}, None, True),
    ],
)
def test_special_and_unknown_permission_forms_never_default_to_safe(
    permissions: object,
    expected_code: str | None,
    expected_privileged: bool,
) -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            permissions=permissions,
            jobs={"inspect": _job()},
        )
    }

    result, authority = _resolve(workflows)

    assert authority["privileged"] is expected_privileged
    if expected_code is not None:
        assert expected_code in _codes(result)


@pytest.mark.parametrize(
    ("runs_on", "expected_classification", "expected_code"),
    [
        ("ubuntu-latest", "GITHUB_HOSTED", None),
        (["ubuntu-latest", 42], "UNKNOWN", "PRIVILEGE_PR_SELF_HOSTED"),
        ("corp-prod-runner", "UNKNOWN", "PRIVILEGE_PR_SELF_HOSTED"),
        (["linux", "self-hosted", "x64"], "SELF_HOSTED", "PRIVILEGE_PR_SELF_HOSTED"),
        ("${{ matrix.runner }}", "UNKNOWN", "PRIVILEGE_PR_SELF_HOSTED"),
    ],
)
def test_runner_authority_is_normalized_and_unknown_labels_fail_closed(
    runs_on: object,
    expected_classification: str,
    expected_code: str | None,
) -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            permissions={},
            jobs={"inspect": _job(runs_on=runs_on)},
        )
    }

    result, authority = _resolve(workflows)
    runner = _as_mapping(authority["runner"])

    assert runner["classification"] == expected_classification
    assert runner["labels"] == sorted(set(runner["labels"]))
    assert authority["privileged"] is (expected_classification != "GITHUB_HOSTED")
    if expected_code is not None:
        assert expected_code in _codes(result)


@pytest.mark.parametrize(
    ("environment", "secrets", "expected_secret_kind"),
    [
        ("production", "NONE", "NONE"),
        (None, "EXPLICIT", "EXPLICIT"),
        (None, "INHERIT", "INHERIT"),
        ("${{ inputs.environment }}", "UNKNOWN", "UNKNOWN"),
        ("", "NONE", "NONE"),
        ("é" * 129, "NONE", "NONE"),
        (7, "NONE", "NONE"),
    ],
)
def test_environment_and_secret_authority_are_blocking_on_a_pr_route(
    environment: object,
    secrets: str,
    expected_secret_kind: str,
) -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            permissions={},
            jobs={"inspect": _job(environment=environment, secrets=secrets)},
        )
    }

    result, authority = _resolve(workflows)

    assert authority["environment"] == (
        environment if environment is None or valid_public_text(environment, 256) else "UNKNOWN"
    )
    assert authority["secrets"] == expected_secret_kind
    assert authority["privileged"] is True
    assert "PRIVILEGE_PR_SECRET_OR_ENVIRONMENT" in _codes(result)


def test_workflow_env_and_reusable_caller_secrets_are_inherited_authority() -> None:
    workflows = {
        ROOT_WORKFLOW: _workflow(
            ROOT_WORKFLOW,
            permissions={},
            jobs={
                "delegate": _job(
                    uses="./.github/workflows/callee.yml",
                    secrets="INHERIT",
                )
            },
        ),
        CALLEE_WORKFLOW: {
            **_workflow(
                CALLEE_WORKFLOW,
                permissions={},
                jobs={"inspect": _job()},
            ),
            "env": {"AUDIT_TOKEN": "${{ secrets.AUDIT_TOKEN }}"},
        },
    }
    edge: dict[str, object] = {
        "kind": "LOCAL_WORKFLOW_CALL",
        "source_workflow": ROOT_WORKFLOW,
        "source_job": "delegate",
        "target_workflow": CALLEE_WORKFLOW,
        "target_job": None,
    }

    result, authority = _resolve(
        workflows,
        _route(workflow=CALLEE_WORKFLOW, job_id="inspect", edge_chain=[edge]),
    )

    assert authority["secrets"] == "INHERIT"
    assert authority["privileged"] is True
    assert "PRIVILEGE_PR_SECRET_OR_ENVIRONMENT" in _codes(result)
