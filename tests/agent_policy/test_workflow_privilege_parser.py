from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import replace
from pathlib import Path

import pytest
import yaml
from tests.agent_policy.workflow_privilege_fixtures import (
    LIMITS,
    POLICY_PATH,
    copy_repository_fixture,
    mutated,
    policy_bytes,
    policy_value,
    sha256,
)

ROOT = Path(__file__).resolve().parents[2]
POLICY_SCHEMA = ROOT / "evals/agent/workflow-security-privileged-policy.schema.json"


def _integer_policy_items(
    value: object,
    path: tuple[str | int, ...] = (),
) -> Iterator[tuple[tuple[str | int, ...], int]]:
    if type(value) is int:
        yield path, value
    elif isinstance(value, dict):
        for key, item in value.items():
            yield from _integer_policy_items(item, (*path, key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _integer_policy_items(item, (*path, index))


_INTEGER_POLICY_ITEMS = tuple(_integer_policy_items(policy_value()))


def test_yaml12_loader_preserves_on_and_uses_only_json_scalar_types() -> None:
    from tools.agent_policy.workflow_privilege_parser import load_yaml12_mapping

    document = load_yaml12_mapping(
        b"on: {push: null}\nyes_value: yes\ntruth: true\ncount: 7\nratio: 1.5\n",
        max_bytes=1024,
        max_depth=LIMITS["yaml_depth"],
        max_nodes=LIMITS["yaml_nodes"],
    )

    assert document == {
        "on": {"push": None},
        "yes_value": "yes",
        "truth": True,
        "count": 7,
        "ratio": 1.5,
    }
    assert True not in document


def test_yaml12_loader_rejects_ambiguous_or_non_json_documents() -> None:
    from tools.agent_policy.workflow_privilege_parser import YamlInputError, load_yaml12_mapping

    invalid_documents = {
        "bom": b"\xef\xbb\xbfschema_version: 1\n",
        "invalid-utf8": b"name: \xff\n",
        "nul": b"name: bad\x00value\n",
        "duplicate": b"outer:\n  permissions: {}\n  permissions: {contents: read}\n",
        "converted-duplicate": b"true: one\nTrue: two\n",
        "anchor": b"permissions: &shared {contents: read}\n",
        "alias": b"base: &base {contents: read}\npermissions: *base\n",
        "merge-key": b"permissions: {<<: {contents: read}}\n",
        "custom-tag": b"value: !unsafe payload\n",
        "non-scalar-key": b"? [one, two]\n: value\n",
        "non-finite": b"value: .nan\n",
        "multiple-documents": b"value: one\n---\nvalue: two\n",
        "non-mapping": b"- one\n- two\n",
        "recursive-depth": b"value: " + b"[" * 1200 + b"0" + b"]" * 1200 + b"\n",
        "oversized-integer": b"value: " + b"9" * 5000 + b"\n",
    }

    for content in invalid_documents.values():
        with pytest.raises(YamlInputError):
            load_yaml12_mapping(
                content,
                max_bytes=LIMITS["workflow_bytes"],
                max_depth=LIMITS["yaml_depth"],
                max_nodes=LIMITS["yaml_nodes"],
            )


def test_yaml12_integer_boundary_is_closed_at_640_digits() -> None:
    from tools.agent_policy.workflow_privilege_parser import YamlInputError, load_yaml12_mapping

    options = {"max_bytes": 1024, "max_depth": LIMITS["yaml_depth"], "max_nodes": LIMITS["yaml_nodes"]}
    assert load_yaml12_mapping(b"value: " + b"9" * 640, **options)["value"] > 0
    with pytest.raises(YamlInputError, match="integer scalar"):
        load_yaml12_mapping(b"value: " + b"9" * 641, **options)


def test_policy_parser_accepts_only_the_closed_v1_schema() -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, SnapshotFile
    from tools.agent_policy.workflow_privilege_parser import (
        PolicyValidationError,
        YamlInputError,
        parse_policy,
    )

    schema = json.loads(POLICY_SCHEMA.read_text(encoding="utf-8"))
    content = policy_bytes()
    source = SnapshotFile(POLICY_PATH.as_posix(), "0644", len(content), sha256(content), content)
    parsed = parse_policy(source, schema=schema)
    assert parsed.schema_version == 1
    assert parsed.limits == V1_LIMITS

    mutation = policy_value()
    mutation["limits"]["workflow_files"] = True
    invalid = yaml.safe_dump(mutation, sort_keys=False).encode()
    invalid_source = replace(source, byte_length=len(invalid), sha256=sha256(invalid), content=invalid)
    with pytest.raises(PolicyValidationError):
        parse_policy(invalid_source, schema=schema)

    duplicate = content + b"schema_version: 1\n"
    duplicate_source = replace(source, byte_length=len(duplicate), sha256=sha256(duplicate), content=duplicate)
    with pytest.raises(YamlInputError):
        parse_policy(duplicate_source, schema=schema)


@pytest.mark.parametrize(
    ("path", "exact_value"),
    (pytest.param(path, exact_value, id=".".join(map(str, path))) for path, exact_value in _INTEGER_POLICY_ITEMS),
)
@pytest.mark.parametrize("wrong_type", ("integral-float", "boolean"))
def test_policy_parser_requires_exact_integer_types_at_every_integer_coordinate(
    path: tuple[str | int, ...],
    exact_value: int,
    wrong_type: str,
) -> None:
    from tools.agent_policy.workflow_privilege_contracts import SnapshotFile
    from tools.agent_policy.workflow_privilege_parser import PolicyValidationError, parse_policy

    replacement = float(exact_value) if wrong_type == "integral-float" else bool(exact_value)
    content = yaml.safe_dump(mutated(policy_value(), path, replacement), sort_keys=False).encode()
    source = SnapshotFile(POLICY_PATH.as_posix(), "0644", len(content), sha256(content), content)

    with pytest.raises(PolicyValidationError):
        parse_policy(source, schema=json.loads(POLICY_SCHEMA.read_text(encoding="utf-8")))


def test_workflow_parser_requires_explicit_top_level_permissions() -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, SnapshotFile
    from tools.agent_policy.workflow_privilege_parser import (
        WorkflowValidationError,
        YamlInputError,
        parse_workflow,
    )

    valid = b"name: Safe\non: {pull_request: {branches: [master]}}\npermissions: {}\njobs: {}\n"
    source = SnapshotFile(".github/workflows/safe.yml", "0644", len(valid), sha256(valid), valid)
    parsed = parse_workflow(source, limits=V1_LIMITS)
    assert parsed.path == source.path
    assert parsed.name == "Safe"
    assert parsed.permissions.is_explicit is True
    assert parsed.permissions.non_none == ()
    scalar = valid.replace(b"{pull_request: {branches: [master]}}", b"{pull_request: {types: opened}}")
    parse_workflow(replace(source, byte_length=len(scalar), sha256=sha256(scalar), content=scalar), limits=V1_LIMITS)
    invalid_outputs = (
        valid.replace(b"name: Safe", ("name: " + "é" * 256).encode()),
        valid.replace(b"jobs: {}", ("jobs: {" + "j" * 257 + ": {}}").encode()),
        valid.replace(b"jobs: {}", ("jobs: {scan: {runs-on: [" + ",".join(["host"] * 17) + "]}}").encode()),
        valid.replace(b"jobs: {}", ("jobs: {scan: {environment: " + "é" * 129 + "}}").encode()),
        valid.replace(b"jobs: {}", b"jobs: {scan: {environment: 7}}"),
        valid.replace(b"jobs: {}", b"jobs: {scan: {environment: {name: prod, url: [bad]}}}"),
    )
    for content in invalid_outputs:
        with pytest.raises(WorkflowValidationError):
            parse_workflow(
                replace(source, byte_length=len(content), sha256=sha256(content), content=content), limits=V1_LIMITS
            )

    missing = valid.replace(b"permissions: {}\n", b"")
    missing_source = replace(source, byte_length=len(missing), sha256=sha256(missing), content=missing)
    with pytest.raises(WorkflowValidationError, match="permissions"):
        parse_workflow(missing_source, limits=V1_LIMITS)

    duplicate = valid.replace(b"permissions: {}\n", b"permissions: {}\npermissions: {contents: read}\n")
    duplicate_source = replace(source, byte_length=len(duplicate), sha256=sha256(duplicate), content=duplicate)
    with pytest.raises(YamlInputError):
        parse_workflow(duplicate_source, limits=V1_LIMITS)


@pytest.mark.parametrize(
    ("event", "configuration"),
    (
        ("pull_request", "7"),
        ("pull_request_target", "7"),
        ("workflow_run", "7"),
        ("workflow_run", "null"),
        ("workflow_run", "{}"),
        ("workflow_run", "{types: [completed]}"),
        ("workflow_run", "{workflows: [Producer, 7]}"),
        ("workflow_run", "{workflows: [Producer], types: 7}"),
        ("workflow_call", "bogus"),
        ("pull_request", "{types: [opened, 7]}"),
        ("pull_request", "{types: [" + ", ".join("opened" for _ in range(65)) + "]}"),
        ("workflow_run", "{workflows: [" + ", ".join(f"Producer{index}" for index in range(257)) + "]}"),
        ("pull_request", "{branches: [master, 7]}"),
        ("pull_request", "{unknown: [opened]}"),
        ("pull_request_target", "{paths: []}"),
        ("pull_request", "{types: [Opened]}"),
        ("pull_request", "{types: [" + "a" * 65 + "]}"),
        ("workflow_run", "{workflows: [" + "é" * 129 + "], types: [completed]}"),
    ),
)
def test_security_trigger_configuration_must_be_closed(event: str, configuration: str, tmp_path: Path) -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, SnapshotFile
    from tools.agent_policy.workflow_privilege_parser import WorkflowValidationError, parse_workflow
    from tools.agent_policy.workflow_privilege_service import scan_repository

    content = f"name: Invalid trigger\non:\n  {event}: {configuration}\npermissions: {{}}\njobs: {{}}\n".encode()
    source = SnapshotFile(".github/workflows/invalid-trigger.yml", "0644", len(content), sha256(content), content)
    with pytest.raises(WorkflowValidationError, match="triggers"):
        parse_workflow(source, limits=V1_LIMITS)

    root = copy_repository_fixture(tmp_path, "target")
    (root / source.path).write_bytes(content)
    report = scan_repository(root)
    assert (report["status"], [item["code"] for item in report["findings"]]) == (
        "UNVERIFIED",
        ["PRIVILEGE_INVALID_WORKFLOW"],
    )


@pytest.mark.parametrize("producer_count", (0, 2))
def test_unresolved_workflow_run_producer_is_blocking_evidence(producer_count: int, tmp_path: Path) -> None:
    from tools.agent_policy.workflow_privilege_service import scan_repository

    root = copy_repository_fixture(tmp_path, "target")
    producer_name = "No such workflow" if not producer_count else "Duplicate producer"
    for index in range(producer_count):
        (root / f".github/workflows/duplicate-{index}.yml").write_text(
            f"name: {producer_name}\non: push\npermissions: {{}}\njobs: {{}}\n", encoding="utf-8"
        )
    (root / ".github/workflows/missing-producer.yml").write_text(
        f"name: Missing producer\non:\n  workflow_run:\n    workflows: [{producer_name}]\n"
        "    types: [completed]\npermissions: {}\njobs:\n  publish:\n    runs-on: ubuntu-latest\n"
        "    permissions: {contents: write}\n    steps: []\n",
        encoding="utf-8",
    )

    report = scan_repository(root)

    assert report["status"] == "UNVERIFIED"
    assert [item["code"] for item in report["findings"]] == ["PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE"]


def test_unresolved_workflow_run_without_any_pr_root_is_outside_the_closure() -> None:
    from tests.ci_shadow_pr3b_report_contract_support import policy
    from tools.agent_policy.workflow_privilege_graph import build_graph

    path = ".github/workflows/consumer.yml"
    graph = build_graph(
        {path: {"path": path, "name": "Consumer", "on": {"workflow_run": {"workflows": ["Missing"]}}, "jobs": {}}},
        policy(),
    )
    assert (graph.roots, graph.findings) == ((), ())


def test_empty_pr_workflow_is_controlled_structural_evidence(tmp_path: Path) -> None:
    from tools.agent_policy.workflow_privilege_service import scan_repository

    root = copy_repository_fixture(tmp_path, "target")
    (root / ".github/workflows/empty-pr.yml").write_text(
        "name: Empty PR\non: pull_request\npermissions: {}\njobs: {}\n", encoding="utf-8"
    )
    report = scan_repository(root)
    assert (report["status"], [item["code"] for item in report["findings"]]) == (
        "UNVERIFIED",
        ["PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE"],
    )


@pytest.mark.parametrize(("downstream_length", "expected"), ((41, "PASS"), (42, "UNVERIFIED")))
def test_workflow_run_combined_event_variant_stays_report_safe(
    downstream_length: int, expected: str, tmp_path: Path
) -> None:
    from tools.agent_policy.workflow_privilege_service import scan_repository

    root = copy_repository_fixture(tmp_path, "target")
    directory = root / ".github/workflows"
    (directory / "long-producer.yml").write_text(
        f"name: Long producer\non: {{pull_request: {{types: [{'a' * 64}]}}}}\npermissions: {{}}\n"
        "jobs: {build: {runs-on: ubuntu-latest}}\n",
        encoding="utf-8",
    )
    (directory / "long-consumer.yml").write_text(
        f"name: Long consumer\non: {{workflow_run: {{workflows: [Long producer], "
        f"types: [{'b' * downstream_length}]}}}}\npermissions: {{}}\n"
        "jobs: {inspect: {runs-on: ubuntu-latest}}\n",
        encoding="utf-8",
    )
    report = scan_repository(root)
    consumer_routes = [item for item in report["routes"] if item["workflow"].endswith("long-consumer.yml")]
    assert report["status"] == expected
    assert bool(consumer_routes) is (downstream_length == 41)
    assert all(len(item["event_variant"].encode()) <= 128 for item in report["routes"])
    if downstream_length == 42:
        assert [item["code"] for item in report["findings"]] == ["PRIVILEGE_DYNAMIC_OR_EXTERNAL_EDGE"]


@pytest.mark.parametrize(
    ("job", "expected"),
    [
        ({"needs": "build"}, ("build",)),
        ({"needs": ["test", "build", "test"]}, ("build", "test")),
        ({}, ()),
    ],
)
def test_normalized_needs_is_complete_unique_and_deterministic(
    job: dict[str, object], expected: tuple[str, ...]
) -> None:
    from tools.agent_policy.workflow_privilege_parser import normalized_needs

    assert normalized_needs(job) == expected


@pytest.mark.parametrize(
    ("job", "error"),
    (("needs: [build, 1]", "needs"), ("permissions: null", "permissions"), ("permissions: 7", "permissions")),
)
def test_malformed_job_authority_is_rejected_before_graph_proof(job: str, error: str) -> None:
    from tools.agent_policy.workflow_privilege_contracts import V1_LIMITS, SnapshotFile
    from tools.agent_policy.workflow_privilege_parser import WorkflowValidationError, parse_workflow

    content = f"name: Unsafe\non: {{pull_request: {{}}}}\npermissions: {{}}\njobs: {{bad: {{{job}}}}}\n".encode()
    source = SnapshotFile(".github/workflows/unsafe.yml", "0644", len(content), sha256(content), content)

    with pytest.raises(WorkflowValidationError, match=error):
        parse_workflow(source, limits=V1_LIMITS)


@pytest.mark.parametrize(
    ("declared", "supplied", "secrets", "expected"),
    [
        ({"mode": {"type": "string", "required": True}}, {"mode": "safe"}, "NONE", True),
        ({"parallel": {"type": "number", "default": 2}}, {}, "NONE", True),
        ({"mode": {"type": "string"}}, {"extra": "unsafe"}, "NONE", False),
        ({"mode": {"type": "string", "required": True}}, {}, "NONE", False),
        ({"mode": {"type": "path"}}, {"mode": "safe"}, "NONE", False),
        ({"parallel": {"type": "number", "default": "two"}}, {}, "NONE", False),
        ({"mode": {"type": "string"}}, {"mode": ["unsafe"]}, "NONE", False),
        ({}, {}, "inherit", False),
    ],
)
def test_workflow_call_input_envelope_is_closed(
    declared: dict[str, object],
    supplied: dict[str, object],
    secrets: str,
    expected: bool,
) -> None:
    from tools.agent_policy.workflow_privilege_parser import valid_workflow_call

    caller = {"with": supplied, **({} if secrets == "NONE" else {"secrets": secrets})}
    callee = {"on": {"workflow_call": {"inputs": declared}}}

    assert valid_workflow_call(caller, callee) is expected


@pytest.mark.parametrize(
    "unsafe",
    (
        {key: value}
        for key, value in (
            ("with", []),
            ("with", False),
            ("runs-on", "self-hosted"),
            ("steps", []),
            ("environment", "production"),
            ("name", 7),
            ("strategy", {}),
            ("if", []),
            ("concurrency", []),
            ("container", "image"),
            ("services", {}),
            ("env", {"TOKEN": "unsafe"}),
            ("secrets", "NONE"),
        )
    ),
)
def test_workflow_call_rejects_noncall_job_envelopes(unsafe: dict[str, object]) -> None:
    from tools.agent_policy.workflow_privilege_parser import valid_workflow_call

    assert not valid_workflow_call({"uses": "./.github/workflows/callee.yml", **unsafe}, {"on": {"workflow_call": {}}})
