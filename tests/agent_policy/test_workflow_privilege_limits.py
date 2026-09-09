from __future__ import annotations

import hashlib
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any

import pytest
from tests.agent_policy.workflow_privilege_fixtures import (
    LIMITS,
    POLICY_PATH,
    copy_repository_fixture,
    finding_codes,
    write_repository,
)
from tests.ci_shadow_pr3b_report_contract_support import policy
from tools.agent_policy import workflow_privilege_report as report_contract
from tools.agent_policy.workflow_privilege_contracts import RESOURCE_LIMIT_FINDING, V1_LIMITS, finding, finding_key
from tools.agent_policy.workflow_privilege_graph import build_graph
from tools.agent_policy.workflow_privilege_parser import YamlInputError, load_yaml12_mapping
from tools.agent_policy.workflow_privilege_permissions import resolve_authority
from tools.agent_policy.workflow_privilege_service import scan_repository
from tools.agent_policy.workflow_privilege_snapshot import SnapshotLease, SnapshotReader


def _load_yaml(content: bytes, **overrides: int) -> dict[str, Any]:
    limits = dict(max_bytes=LIMITS["workflow_bytes"], max_depth=LIMITS["yaml_depth"], max_nodes=LIMITS["yaml_nodes"])
    return load_yaml12_mapping(content, **(limits | overrides))


def _write_workflow(root: Path, filename: str, name: str, jobs: list[str]) -> None:
    content = "\n".join((f"name: {name}", "on: pull_request", "permissions: {}", "jobs:", *jobs, ""))
    (root / ".github/workflows" / filename).write_text(content, encoding="utf-8")


def test_v1_limits_are_the_exact_closed_policy_value() -> None:
    assert asdict(V1_LIMITS) == LIMITS
    assert all(type(value) is int and value > 0 for value in asdict(V1_LIMITS).values())


def test_snapshot_workflow_count_accepts_n_and_saturates_at_n_plus_one(tmp_path: Path) -> None:
    maximum = LIMITS["workflow_files"]
    content = b"name: Bounded\non: {push: null}\npermissions: {}\njobs: {}\n"
    exact_workflows = {f"workflow-{index:03d}.yml": content for index in range(maximum)}
    exact_root = write_repository(tmp_path / "exact", exact_workflows, policy=b"schema_version: 1\n")
    exact_lease = SnapshotReader(limits=V1_LIMITS).acquire(exact_root)
    exact = exact_lease.finalize(policy_schema_version=1).snapshot
    assert (exact.complete, exact.workflow_count, exact.overflow_dimensions) == (True, maximum, ())
    overflow_workflows = {f"workflow-{index:03d}.yml": content for index in range(maximum + 1)}
    overflow_root = write_repository(tmp_path / "overflow", overflow_workflows, policy=b"schema_version: 1\n")
    overflow_lease = SnapshotReader(limits=V1_LIMITS).acquire(overflow_root)
    overflow = overflow_lease.snapshot
    overflow_lease.close()
    assert overflow.complete is False
    assert overflow.workflow_count == maximum + 1
    assert overflow.manifest_sha256 is None
    assert overflow.overflow_dimensions == ("workflow_files",)
    assert finding_codes(overflow) == ("PRIVILEGE_RESOURCE_LIMIT",)


def test_yaml_byte_limit_accepts_n_and_rejects_n_plus_one() -> None:
    maximum = LIMITS["workflow_bytes"]
    prefix = b"value: ok\n#"
    exact = prefix + (b"x" * (maximum - len(prefix) - 1)) + b"\n"
    assert len(exact) == maximum
    assert _load_yaml(exact, max_bytes=maximum) == {"value": "ok"}
    with pytest.raises(YamlInputError) as raised:
        _load_yaml(exact + b" ", max_bytes=maximum)
    assert raised.value.limit_dimension == "workflow_bytes"


def test_yaml_depth_and_node_limits_accept_n_and_reject_n_plus_one() -> None:
    depth_maximum = LIMITS["yaml_depth"]

    def nested_mapping(depth: int) -> bytes:
        lines = [f"{'  ' * index}level_{index + 1}:" for index in range(depth - 1)]
        lines.append(f"{'  ' * (depth - 1)}value: true")
        return ("\n".join(lines) + "\n").encode()

    exact_depth = _load_yaml(nested_mapping(depth_maximum), max_depth=depth_maximum)
    assert "level_1" in exact_depth
    with pytest.raises(YamlInputError) as depth_error:
        _load_yaml(nested_mapping(depth_maximum + 1), max_depth=depth_maximum)
    assert depth_error.value.limit_dimension == "yaml_depth"
    node_maximum = LIMITS["yaml_nodes"]
    exact_nodes = b"items:\n" + (b"  - x\n" * (node_maximum - 3))
    parsed = _load_yaml(exact_nodes, max_depth=depth_maximum, max_nodes=node_maximum)
    assert len(parsed["items"]) == node_maximum - 3
    with pytest.raises(YamlInputError) as node_error:
        _load_yaml(exact_nodes + b"  - x\n", max_depth=depth_maximum, max_nodes=node_maximum)
    assert node_error.value.limit_dimension == "yaml_nodes"


def test_snapshot_policy_byte_limit_accepts_n_and_rejects_n_plus_one(tmp_path: Path) -> None:
    maximum = LIMITS["policy_bytes"]
    prefix = b"schema_version: 1\n#"
    policy = prefix + (b"p" * (maximum - len(prefix) - 1)) + b"\n"
    workflows = {"safe.yml": b"name: Safe\non: {push: null}\npermissions: {}\njobs: {}\n"}
    root = write_repository(tmp_path / "policy-bytes", workflows, policy=policy)
    exact = SnapshotReader(limits=V1_LIMITS).acquire(root).finalize(policy_schema_version=None).snapshot
    assert exact.policy is not None
    assert (exact.complete, exact.policy.byte_length) == (True, maximum)
    (root / POLICY_PATH).write_bytes(policy + b" ")
    overflow_lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    overflow = overflow_lease.snapshot
    overflow_lease.close()
    assert overflow.complete is False and overflow.policy is None
    assert overflow.overflow_dimensions == ("policy_bytes",)
    assert finding_codes(overflow) == ("PRIVILEGE_INVALID_POLICY", "PRIVILEGE_RESOURCE_LIMIT")


@pytest.mark.parametrize("dimension", ("yaml_depth", "yaml_nodes"))
def test_policy_yaml_limit_is_invalid_policy_and_resource_limited(dimension: str, tmp_path: Path) -> None:
    root = copy_repository_fixture(tmp_path, "target")
    policy_path = root / POLICY_PATH
    if dimension == "yaml_depth":
        levels = "".join(f"{'  ' * level}level_{level}:\n" for level in range(1, LIMITS[dimension] + 1))
        overflow = f"overflow:\n{levels}{'  ' * (LIMITS[dimension] + 1)}value: true\n".encode()
    else:
        overflow = b"overflow:\n" + (b"  - x\n" * LIMITS[dimension])
    policy_path.write_bytes(policy_path.read_bytes() + overflow)
    report = scan_repository(root)
    inventory = report["inventory"]
    assert report["policy"] == {
        "path": POLICY_PATH.as_posix(),
        "sha256": hashlib.sha256(policy_path.read_bytes()).hexdigest(),
        "schema_version": None,
    }
    assert (report["status"], inventory["complete"], inventory["manifest_sha256"]) == ("UNVERIFIED", False, None)
    assert inventory["overflow_dimensions"] == [dimension]
    codes = tuple(item["code"] for item in report["findings"])
    assert codes == ("PRIVILEGE_INVALID_POLICY", "PRIVILEGE_RESOURCE_LIMIT")


def test_snapshot_total_workflow_bytes_accepts_n_and_rejects_n_plus_one(tmp_path: Path) -> None:
    file_maximum = LIMITS["workflow_bytes"]
    total_maximum = LIMITS["total_workflow_bytes"]
    prefix = b"name: Bounded\n#"
    content = prefix + (b"w" * (file_maximum - len(prefix) - 1)) + b"\n"
    workflow_count = total_maximum // file_maximum
    workflows = {f"workflow-{index:02d}.yml": content for index in range(workflow_count)}
    root = write_repository(tmp_path / "total-bytes", workflows, policy=b"schema_version: 1\n")
    exact = SnapshotReader(limits=V1_LIMITS).acquire(root).finalize(policy_schema_version=None).snapshot
    assert exact.complete is True
    assert sum(entry.byte_length for entry in exact.workflows) == total_maximum
    (root / ".github/workflows/overflow.yml").write_bytes(b"x")
    overflow_lease = SnapshotReader(limits=V1_LIMITS).acquire(root)
    overflow = overflow_lease.snapshot
    overflow_lease.close()
    assert overflow.complete is False
    assert overflow.overflow_dimensions == ("total_workflow_bytes",)
    assert finding_codes(overflow) == ("PRIVILEGE_RESOURCE_LIMIT",)


def test_finding_overflow_retains_bounded_fail_evidence_and_resource_sentinel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    observed_fail: list[dict[str, Any]] = []
    observed_values: list[Any] = []
    canonical_findings = report_contract.canonical_findings

    def bounded_findings(values: Any) -> tuple[Any, ...]:
        assert len(values) <= LIMITS["findings"] + 1, f"unbounded finding handoff: {len(values)}"
        observed_values[:] = values
        failure = min((item for item in values if item.status == "FAIL"), key=finding_key, default=None)
        observed_fail[:] = [] if failure is None else [failure.to_mapping()]
        return canonical_findings(values)

    monkeypatch.setattr(report_contract, "canonical_findings", bounded_findings)
    root = copy_repository_fixture(tmp_path, "target")
    jobs: list[str] = []
    for index in range(342):
        jobs.extend(
            (
                f"  privileged-{index:03d}:",
                "    runs-on: corp-prod-runner",
                "    permissions: write-all",
                "    environment: production",
                "    steps:",
                "      - run: echo privileged",
            )
        )
    _write_workflow(root, "finding-overflow.yml", "Finding overflow", jobs)

    report = scan_repository(root)

    assert report["status"] == "FAIL"
    assert report["ok"] is False
    assert report["inventory"]["complete"] is False
    assert report["inventory"]["manifest_sha256"] is None
    assert report["inventory"]["overflow_dimensions"] == ["findings"]
    assert len(report["findings"]) == LIMITS["findings"]
    assert sum(item["code"] == "PRIVILEGE_RESOURCE_LIMIT" for item in report["findings"]) == 1
    assert report["findings"][-1]["code"] == "PRIVILEGE_RESOURCE_LIMIT"
    assert observed_fail and report["findings"][0] == observed_fail[0]
    ordinary = [item for item in observed_values if item != RESOURCE_LIMIT_FINDING]
    ordinary.append(replace(ordinary[-1], subject="sentinel-boundary"))
    selected = canonical_findings((*ordinary, RESOURCE_LIMIT_FINDING))
    assert len(selected) == LIMITS["findings"]
    assert selected.count(RESOURCE_LIMIT_FINDING) == 1
    deduplicated = canonical_findings((*ordinary, ordinary[-1]))
    assert len(deduplicated) == LIMITS["findings"] and RESOURCE_LIMIT_FINDING not in deduplicated
    unknown = tuple(
        replace(RESOURCE_LIMIT_FINDING, code="PRIVILEGE_UNKNOWN_PERMISSION", subject=f"unknown-{index:05d}")
        for index in range(LIMITS["findings"] + 1)
    )
    bounded = canonical_findings(unknown)
    assert canonical_findings(bounded) == bounded and len(bounded) == LIMITS["findings"]
    closed_policy = policy()
    paired = tuple(
        finding(code, f"paired-{index:05d}", "bounded authority", policy=closed_policy)
        for index in range(2_049)
        for code in ("PRIVILEGE_PR_SELF_HOSTED", "PRIVILEGE_UNAPPROVED_PR_WRITE")
    )
    expected = (*sorted(paired, key=finding_key)[: LIMITS["findings"] - 1], RESOURCE_LIMIT_FINDING)
    assert canonical_findings(paired) == canonical_findings(tuple(reversed(paired))) == expected
    monkeypatch.setattr(SnapshotLease, "_revalidates", lambda _self: False)
    concurrent = scan_repository(root)
    assert concurrent["inventory"]["overflow_dimensions"] == ["findings"]
    assert len(concurrent["findings"]) == LIMITS["findings"]
    assert concurrent["findings"][-1]["code"] == "PRIVILEGE_RESOURCE_LIMIT"


def test_unknown_permission_flood_retains_one_route_finding_and_late_write() -> None:
    unknown = {f"future-scope-{index:05d}": "read" for index in range(V1_LIMITS.findings + 1)}
    unknown[f"future-scope-{V1_LIMITS.findings:05d}"] = "write"
    path = ".github/workflows/flood.yml"
    authority = resolve_authority(
        {
            "route_id": "0" * 64,
            "workflow": path,
            "job_id": "inspect",
            "classification": "PR_HEAD",
        },
        {path: {"permissions": unknown, "jobs": {"inspect": {"runs-on": "ubuntu-latest"}}}},
        policy(),
    )

    assert authority.privileged is True
    assert finding_codes(authority) == ("PRIVILEGE_UNKNOWN_PERMISSION",)


def test_graph_canonical_prefix_and_edge_overflow_preserve_prior_fail() -> None:
    path = ".github/workflows/root.yml"
    workflows = {
        path: {
            "path": path,
            "name": "Root",
            "on": {"pull_request_target": None},
            "jobs": {
                "a": {"needs": ["z-missing"]},
                "b": {"needs": ["a-missing"]},
                "c": {"needs": ["m-missing"]},
            },
        }
    }
    bounded = policy()
    bounded["limits"]["findings"] = 3
    graph = build_graph(workflows, bounded)
    assert [(item.code, item.detail) for item in graph.findings] == [
        ("PRIVILEGE_PULL_REQUEST_TARGET", "pull_request_target is forbidden for repository workflows"),
        ("PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE", "missing needs job a-missing"),
        ("PRIVILEGE_RESOURCE_LIMIT", "findings exceeded closed maximum 3"),
    ]
    workflows[path]["jobs"] = {"a": {}, "b": {"needs": ["a"]}, "c": {"needs": ["a"]}}
    bounded["limits"].update(findings=LIMITS["findings"], graph_edges=1)
    graph = build_graph(workflows, bounded)
    assert (graph.edge_count, graph.overflow_dimensions) == (2, ("graph_edges",))
    assert tuple(item.code for item in graph.findings) == (
        "PRIVILEGE_PULL_REQUEST_TARGET",
        "PRIVILEGE_RESOURCE_LIMIT",
    )


def test_graph_finding_expansion_stops_at_n_plus_one() -> None:
    def workflow(path: str) -> dict[str, Any]:
        return {
            "path": path,
            "name": path,
            "on": {"pull_request": None, "workflow_run": {"workflows": ["Missing"]}},
            "jobs": {"scan": {"needs": ["missing"]}},
        }

    paths = (".github/workflows/a.yml", ".github/workflows/z.yml")
    workflows = {path: workflow(path) for path in paths}
    bounded = policy()
    bounded["limits"]["findings"] = 3
    graph = build_graph(workflows, bounded)
    retained = [(item.subject, item.detail) for item in graph.findings if item.code != "PRIVILEGE_RESOURCE_LIMIT"]
    assert retained == [(paths[0], "ambiguous workflow_run producer Missing"), (paths[0], "missing needs job missing")]
    assert graph.overflow_dimensions == ("findings",)
    bounded["limits"].update(findings=1, graph_edges=1)
    assert build_graph(workflows, bounded).overflow_dimensions == ("findings",)


@pytest.mark.parametrize(
    "dimensions",
    (("report_bytes",), ("text_stdout_bytes",), ("report_bytes", "text_stdout_bytes")),
)
def test_output_overflow_returns_the_bounded_minimal_report(
    dimensions: tuple[str, ...], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    root = copy_repository_fixture(tmp_path, "pre-split")
    baseline = scan_repository(root)
    original_finalize = report_contract.finalize_report
    original_render = report_contract.render_text

    def overflow_finalize(report: dict[str, Any], **kwargs: Any) -> dict[str, Any]:
        if report["routes"] and "report_bytes" in dimensions:
            raise report_contract.ReportSizeError("forced JSON overflow")
        return original_finalize(report, **kwargs)

    def overflow_render(report: dict[str, Any]) -> str:
        if report["routes"] and "text_stdout_bytes" in dimensions:
            raise report_contract.ReportSizeError("forced text overflow")
        return original_render(report)

    monkeypatch.setattr(report_contract, "finalize_report", overflow_finalize)
    monkeypatch.setattr(report_contract, "render_text", overflow_render)
    report = scan_repository(root)

    counts = "workflow_count job_count edge_count root_count route_count".split()
    assert {name: report["inventory"][name] for name in counts} == {
        name: baseline["inventory"][name] for name in counts
    }
    assert report["inventory"]["complete"] is False
    assert report["inventory"]["manifest_sha256"] is None
    assert report["inventory"]["overflow_dimensions"] == sorted(dimensions)
    assert report["roots"] == report["routes"] == report["route_authority"] == report["privileged_profiles"] == []
    assert report["findings"] == [baseline["findings"][0], report["findings"][1]]
    assert report["findings"][1]["code"] == "PRIVILEGE_RESOURCE_LIMIT"
    assert report["status"] == "FAIL"
    assert len(report_contract.canonical_json_bytes(report)) < 65_536
    assert len(original_render(report).encode()) < 65_536


def test_output_finalizer_never_downgrades_an_internal_defect(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = copy_repository_fixture(tmp_path, "target")

    def fail_internal(_report: dict[str, Any], **_kwargs: Any) -> dict[str, Any]:
        raise report_contract.InternalReportError("forced internal defect")

    monkeypatch.setattr(report_contract, "finalize_report", fail_internal)
    with pytest.raises(report_contract.InternalReportError, match="forced internal defect"):
        scan_repository(root)


def test_job_count_n_plus_one_is_a_saturated_resource_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    root = copy_repository_fixture(tmp_path, "target")
    jobs: list[str] = []
    for index in range(LIMITS["jobs"] + 1):
        jobs.extend(
            (
                f"  job-{index:04d}:",
                "    runs-on: ubuntu-latest",
                "    steps:",
                "      - run: echo bounded",
            )
        )
    _write_workflow(root, "job-overflow.yml", "Job overflow", jobs)

    report = scan_repository(root)

    assert report["status"] == "UNVERIFIED"
    assert report["inventory"]["complete"] is False
    assert report["inventory"]["job_count"] == LIMITS["jobs"] + 1
    assert report["inventory"]["overflow_dimensions"] == ["jobs"]
    assert tuple(item["code"] for item in report["findings"]) == ("PRIVILEGE_RESOURCE_LIMIT",)
    monkeypatch.setattr(SnapshotLease, "_revalidates", lambda _self: False)
    concurrent = scan_repository(root)
    assert concurrent["inventory"]["job_count"] == LIMITS["jobs"] + 1
    assert tuple(item["code"] for item in concurrent["findings"]) == (
        "PRIVILEGE_CONCURRENT_MUTATION",
        "PRIVILEGE_RESOURCE_LIMIT",
    )


def test_graph_edge_n_plus_one_stops_before_route_expansion(tmp_path: Path) -> None:
    root = copy_repository_fixture(tmp_path, "target")
    jobs = []
    for index in range(136):
        jobs.append(f"  job-{index:03d}:")
        if index:
            predecessors = ", ".join(f"job-{predecessor:03d}" for predecessor in range(index))
            jobs.append(f"    needs: [{predecessors}]")
        jobs.extend(("    runs-on: ubuntu-latest", "    steps:", "      - run: echo bounded"))
    _write_workflow(root, "edge-overflow.yml", "Edge overflow", jobs)

    report = scan_repository(root)

    assert report["status"] == "UNVERIFIED"
    assert report["inventory"]["complete"] is False
    assert report["inventory"]["edge_count"] == LIMITS["graph_edges"] + 1
    assert report["inventory"]["overflow_dimensions"] == ["graph_edges"]
    assert tuple(item["code"] for item in report["findings"]) == ("PRIVILEGE_RESOURCE_LIMIT",)
