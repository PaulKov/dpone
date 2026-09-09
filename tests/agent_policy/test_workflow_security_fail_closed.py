from __future__ import annotations

import importlib.util
import sys
from dataclasses import replace
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load_workflow_security() -> ModuleType:
    path = ROOT / "tools" / "agent_policy" / "workflow_security.py"
    spec = importlib.util.spec_from_file_location("dpone_agent_workflow_security_fail_closed", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


workflow_security = _load_workflow_security()


def _target(tmp_path: Path) -> Path:
    from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture

    return copy_repository_fixture(tmp_path, "target")


def test_reusable_contract_is_compiled_once(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.ci_shadow_pr3b_report_contract_support import policy
    from tools.agent_policy import workflow_privilege_graph as graph_api
    from tools.agent_policy import workflow_privilege_parser as parser_api

    caller_count, declaration_count = 37, 113
    inputs = {f"i_{n}": {"type": "string", "default": "x"} for n in range(declaration_count)}
    callee = ".github/workflows/callee.yml"
    workflows: dict[str, dict[str, Any]] = {
        ".github/workflows/root.yml": {
            "name": "Root",
            "on": {"pull_request": {}},
            "jobs": {f"call_{n}": {"uses": f"./{callee}", "with": {"i_0": "x"}} for n in range(caller_count)},
        },
        callee: {
            "name": "Callee",
            "on": {"workflow_call": {"inputs": inputs}},
            "jobs": {"safe": {}},
        },
    }
    original, calls = parser_api._valid_call_input, 0

    def counted(value: object, supplied: object = parser_api._MISSING) -> bool:
        nonlocal calls
        calls += 1
        return original(value, supplied)

    monkeypatch.setattr(parser_api, "_valid_call_input", counted)
    result = graph_api.build_graph(workflows, policy())
    edges = sum(edge.kind == "LOCAL_WORKFLOW_CALL" for edge in result.edges)
    assert (calls, edges) == (declaration_count + caller_count, caller_count)


@pytest.mark.parametrize("producer", ["build_graph", "expand_routes"])
def test_graph_producer_cannot_return_a_coherently_omitted_result(
    producer: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture
    from tools.agent_policy import workflow_privilege_service as service

    original = getattr(service, producer)

    def omit(*args, **kwargs):
        result = original(*args, **kwargs)
        if producer == "build_graph":
            return replace(result, edges=result.edges[:-1], edge_count=result.edge_count - 1)
        return replace(
            result,
            routes=result.routes[:-1],
            canonical_ids=result.canonical_ids[:-1],
            route_count=result.route_count - 1,
        )

    monkeypatch.setattr(service, producer, omit)
    with pytest.raises(service.report_contract.InternalReportError, match="differs from its .* source"):
        service.scan_repository(copy_repository_fixture(tmp_path, "target"))


@pytest.mark.parametrize("mutation", ["route", "condition", "authority"])
def test_resolution_audit_rejects_route_authority_drift(
    mutation: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from tests.agent_policy.workflow_privilege_fixtures import copy_repository_fixture
    from tools.agent_policy import workflow_privilege_service as service
    from tools.agent_policy.workflow_privilege_contracts import TruthValue

    original = service.resolve_routes

    def mutate(candidates, **kwargs):
        if mutation == "condition":
            kwargs["condition_for_route"] = lambda _route: TruthValue.FALSE
        routes, authorities = original(candidates, **kwargs)
        if mutation == "route":
            forged = replace(routes[0], event_variant="ACTIVITY:labeled")
            return (forged, *routes[1:]), (replace(authorities[0], route_id=forged.route_id), *authorities[1:])
        if mutation == "authority":
            index = next(i for i, authority in enumerate(authorities) if authority.privileged)
            forged = replace(
                authorities[index], declared_permissions={}, effective_permissions={}, privileged=False, findings=()
            )
            return routes, (*authorities[:index], forged, *authorities[index + 1 :])
        return routes, authorities

    monkeypatch.setattr(service, "resolve_routes", mutate)
    with pytest.raises((ValueError, service.report_contract.InternalReportError)):
        service.scan_repository(copy_repository_fixture(tmp_path, "target"))


@pytest.mark.parametrize("method", ("add", "finish", "_retain"))
def test_finding_selector_cannot_omit_structural_evidence(
    method: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from tools.agent_policy import workflow_privilege_report as report
    from tools.agent_policy import workflow_privilege_service as service

    root = _target(tmp_path)
    (root / ".github/workflows/missing-needs.yml").write_text(
        "name: Missing needs\non: pull_request\npermissions: {}\njobs:\n"
        "  safe:\n    needs: absent\n    runs-on: ubuntu-latest\n    steps: [{run: echo safe}]\n",
        encoding="utf-8",
    )
    replacement = (lambda *_args: ((), False)) if method == "finish" else (lambda *_args: None)
    monkeypatch.setattr(report.FindingAccumulator, method, replacement)

    with pytest.raises(service.report_contract.InternalReportError, match="finding selection differs"):
        service.scan_repository(root)


@pytest.mark.parametrize(
    "profile",
    "codeql-extra-job codeql-envelope-drift governance-source-copy governance-copy-with-if governance-copy-renamed governance-transport-drift same-job-near-copy governance-envelope-drift".split(),
)
def test_mandatory_profile_job_inventory_is_exact(profile: str, tmp_path: Path) -> None:
    from tools.agent_policy.workflow_privilege_service import scan_repository

    root = _target(tmp_path)
    workflow = root / ".github/workflows" / ("codeql.yml" if profile.startswith("codeql") else "ci.yml")
    document = workflow.read_text(encoding="utf-8")
    if profile == "codeql-extra-job":
        document += "\n  read-only-extra:\n    runs-on: ubuntu-latest\n    permissions: {contents: read}\n    steps: [{run: echo safe}]\n"
    elif profile == "codeql-envelope-drift":
        document = document.replace("    name: Analyze Python\n", "    name: Analyze Python\n    timeout-minutes: 7\n")
    elif profile == "governance-envelope-drift":
        document = document.replace(
            "actions/attest-build-provenance@e8998f949152b193b063cb0ec769d69d929409be",
            "actions/attest-build-provenance@0000000000000000000000000000000000000000",
        )
    else:
        copied = document.split("\n  governance-source:\n", 1)[1].split("\n  governance-attestation:\n", 1)[0]
        if profile.endswith("with-if"):
            copied = "    if: always()\n" + copied
        if profile.endswith("renamed"):
            copied = copied.replace("governance-upload", "governance-upload-copy")
        if profile.endswith("transport-drift"):
            prefix, upload = copied.rsplit("      - name: Upload agent governance evidence\n", 1)
            copied = (
                prefix
                + "      - name: Upload agent governance evidence\n"
                + (
                    upload.replace("governance-upload", "governance-upload-copy")
                    .replace("agent-governance-gate", "copied-governance-gate")
                    .replace("agent_governance_gate.json", "copied_governance_gate.json")
                    .replace(
                        "actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a", "actions/upload-artifact@v4"
                    )
                )
            )
        anchor = "\n  governance-attestation:\n"
        insertion = (
            ("\n" + copied.split("    steps:\n", 1)[1])
            if profile.startswith("same-job")
            else "\n  governance-source-copy:\n" + copied
        )
        document = document.replace(anchor, insertion + anchor)
    workflow.write_text(document, encoding="utf-8")

    report = scan_repository(root)

    assert report == scan_repository(root)
    assert report["status"] == "FAIL"
    expected_code = (
        "PRIVILEGE_CODEQL_PROFILE_DRIFT" if profile.startswith("codeql") else "PRIVILEGE_ADR0037_PROFILE_DRIFT"
    )
    assert expected_code in {item["code"] for item in report["findings"]}
    if profile.endswith("envelope-drift"):
        job = "analyze" if profile.startswith("codeql") else "governance-attestation"
        route_ids = {item["route_id"] for item in report["routes"] if item["job_id"] == job}
        drifts = [
            item for item in report["findings"] if item["detail"] == "closed profile identity or envelope drifted"
        ]
        assert len(route_ids) == len(drifts) == 3
        assert {item["route_id"] for item in drifts} == route_ids


@pytest.mark.parametrize("producer", ("authority", "authority-bool"))
def test_underlying_semantic_producer_cannot_omit_evidence(
    producer: str, monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    from tools.agent_policy import workflow_privilege_permissions as permissions
    from tools.agent_policy import workflow_privilege_service as service

    root = _target(tmp_path)
    (root / ".github/workflows/read-all.yml").write_text(
        "name: Read all\non: pull_request\npermissions: read-all\njobs:\n"
        "  inspect: {runs-on: ubuntu-latest, steps: [{run: echo inspect}]}\n",
        encoding="utf-8",
    )
    original = permissions.resolve_authority

    def omit(*args, **kwargs):
        authority = original(*args, **kwargs)
        return (
            replace(authority, privileged=0)
            if producer.endswith("bool")
            else replace(authority, findings=(), privileged=False)
        )

    monkeypatch.setattr(permissions, "resolve_authority", omit)
    with pytest.raises(ValueError, match="authority decision differs"):
        service.scan_repository(root)


def test_internal_semantic_defect_closes_snapshot_descriptors(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import os

    from tools.agent_policy import workflow_privilege_service as service

    captured: list[tuple[object, tuple[int, ...]]] = []
    from tools.agent_policy.workflow_privilege_policy_selection import VersionedPolicyReader

    def recording_acquire(reader, root, original=VersionedPolicyReader.acquire):
        lease = original(reader, root)
        state = lease._active._state
        assert state is not None
        file_fds = tuple(item.fd for item in (state.policy, *state.workflows))
        captured.append((lease, (*file_fds, *state.directory_fds)))
        return lease

    def fail_graph(*_args):
        raise service.report_contract.InternalReportError("forced defect")

    monkeypatch.setattr(VersionedPolicyReader, "acquire", recording_acquire)
    monkeypatch.setattr(service, "build_graph", fail_graph)

    with pytest.raises(service.report_contract.InternalReportError, match="forced defect"):
        service.scan_repository(_target(tmp_path))

    lease, descriptors = captured[0]
    assert lease._active._closed is True
    for descriptor in descriptors:
        with pytest.raises(OSError):
            os.fstat(descriptor)


def test_workflow_security_rejects_soft_runtime_attestation_gate(
    tmp_path: Path,
) -> None:
    workflow = tmp_path / "unsafe.yml"
    workflow.write_text(
        """
name: Unsafe promotion
on:
  workflow_call:
permissions:
  contents: read
jobs:
  promote:
    runs-on: ubuntu-latest
    steps:
      - name: Required runtime attestation
        continue-on-error: true
        run: |
          echo DPONE_ARTIFACT_ATTESTATION_REQUIRED
          exit 4
      - name: Publish anyway
        if: always()
        run: dpone airflow publish --unsafe
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(
        workflow,
        policy=workflow_security.WorkflowSecurityPolicy.empty(),
    )

    assert any("must not use continue-on-error" in error for error in result.errors)
    assert any("must not bypass the attestation gate" in error for error in result.errors)


def test_workflow_security_rejects_assert_based_parse_gate(
    tmp_path: Path,
) -> None:
    workflow = tmp_path / "unsafe.yml"
    workflow.write_text(
        """
name: Unsafe parse gate
on:
  workflow_call:
permissions:
  contents: read
jobs:
  parse:
    runs-on: ubuntu-latest
    steps:
      - run: |
          python -c "from airflow.providers.dpone import load_dpone_dags; report=load_dpone_dags({}); assert report.loaded"
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(
        workflow,
        policy=workflow_security.WorkflowSecurityPolicy.empty(),
    )

    assert any("runtime gate must use an explicit exit, not assert" in error for error in result.errors)


def test_workflow_security_rejects_hidden_ci_artifact_omission(tmp_path: Path) -> None:
    workflow = tmp_path / "dbt-self-service-dev.yml"
    workflow.write_text(
        """
name: Unsafe hidden artifact transport
on:
  workflow_call:
permissions:
  contents: read
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/upload-artifact@043fb46d1a93c77aae656e7c1c64a875d1fc6a0a
        with:
          name: release
          path: .dpone-ci/release
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(
        workflow,
        policy=workflow_security.WorkflowSecurityPolicy.empty(),
    )

    assert any("include-hidden-files must be true" in error for error in result.errors)


def test_workflow_security_rejects_caller_owned_production_trust(tmp_path: Path) -> None:
    workflow = tmp_path / "dbt-self-service-prod.yml"
    workflow.write_text(
        """
name: Unsafe production trust
on:
  workflow_call:
    inputs:
      trust-policy-path:
        type: string
        required: true
      trust-policy-sha256:
        type: string
        required: true
permissions:
  contents: read
jobs:
  promote:
    runs-on: ubuntu-latest
    env:
      DPONE_TRUST_POLICY_PATH: ${{ inputs.trust-policy-path }}
      DPONE_TRUST_POLICY_SHA256: ${{ inputs.trust-policy-sha256 }}
    steps:
      - run: dpone airflow verify-attestation
""".lstrip(),
        encoding="utf-8",
    )

    result = workflow_security.validate_workflow_file(
        workflow,
        policy=workflow_security.WorkflowSecurityPolicy.empty(),
    )

    assert any("must not be workflow_call inputs" in error for error in result.errors)
    assert any("must be scoped to the production environment" in error for error in result.errors)
    assert any("must use the fixed repository-relative path" in error for error in result.errors)
    assert any("must come from the protected environment variable" in error for error in result.errors)
    assert any("must require the production trust tier" in error for error in result.errors)
