"""Adversarial contract tests for create-only Binding V2 evidence."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import subprocess
import sys
import threading
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path, PurePosixPath
from types import ModuleType, SimpleNamespace
from typing import Any

import jsonschema
import pytest
import yaml

_PRODUCER = Path("tests/support/postgres_mssql_r1_v3_binding_v2_evidence.py")
_SCHEMA = Path("docs/schemas/evidence/postgres-mssql-r1-v3-binding-v2-layout-4.schema.json")
_ZERO40 = "0" * 40
_ZERO64 = "0" * 64


def _module() -> ModuleType:
    name = "fresh_binding_v2_evidence"
    spec = importlib.util.spec_from_file_location(name, _PRODUCER)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    assert module.__file__ is not None
    assert Path(module.__file__).resolve() == _PRODUCER.resolve()
    return module


def _cases() -> list[dict[str, Any]]:
    return [
        {"case_id": "reader:01:02", "nodeid": "test_x.py::test_case[a]", "expected_class": "typed_rejection"},
        {"case_id": "reader:03:04", "nodeid": "test_x.py::test_case[b]", "expected_class": "invariant_preserved"},
    ]


def _probes(module: ModuleType, phase: str) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    cases = _cases()
    nodeids = [case["nodeid"] for case in cases]
    failed = phase == "red"
    outcomes = ["failed" if failed else "passed", "passed"]
    reports = []
    for nodeid, outcome in zip(nodeids, outcomes, strict=True):
        for when in ("setup", "call", "teardown"):
            observed = outcome if when == "call" else "passed"
            reports.append(
                {
                    "nodeid": nodeid,
                    "phase": when,
                    "outcome": observed,
                    "diagnostic_class": "assertion_failure" if observed == "failed" else "none",
                }
            )
    args = ["test_x.py", "-q"]
    empty = {key: 0 for key in module.OUTCOME_KEYS}
    collection = {
        "probe_kind": "collection",
        "pytest_args": args,
        "exit_code": 0,
        "ordered_nodeids": nodeids,
        "reports": [],
        "semantic_observations": [],
        "input_vectors": [],
        "outcome_counts": empty,
        "import_origins": [{"module_name": "dpone", "checkout_relative_path": "src/dpone/__init__.py"}],
    }
    counts = {**empty, "passed": 1 if failed else 2, "failed": int(failed)}
    behavior = {
        "probe_kind": "behavior",
        "pytest_args": args,
        "exit_code": int(failed),
        "ordered_nodeids": nodeids,
        "reports": reports,
        "semantic_observations": [
            {"case_id": case["case_id"], "nodeid": case["nodeid"], "observed_class": case["expected_class"]}
            for case in cases
        ],
        "input_vectors": [
            {
                "nodeid": case["nodeid"],
                "inputs": [{"name": "payload", "sha256": hashlib.sha256(case["case_id"].encode()).hexdigest()}],
            }
            for case in cases
        ],
        "outcome_counts": counts,
        "import_origins": [{"module_name": "dpone", "checkout_relative_path": "src/dpone/__init__.py"}],
    }
    pins = {
        "COLLECTED_NODE_COUNT": 2,
        "ORDERED_NODEIDS_SHA256": module._digest(module.NODEIDS_DOMAIN, nodeids),
        "FOCUSED_PYTEST_ARGS": args,
        "EXPECTED_RED_OUTCOME_COUNTS": counts,
        "EXPECTED_RED_FAILING_NODEIDS": [nodeids[0]],
        "EXPECTED_RED_DIAGNOSTIC_CLASSES": ["assertion_failure"],
    }
    return pins, collection, behavior


def _error(module: ModuleType, reason: str, call: Callable[[], object]) -> None:
    with pytest.raises(module.EvidenceError) as caught:
        call()
    assert caught.value.reason == reason


def test_registry_is_closed_complete_and_digestible() -> None:
    module = _module()
    raw = Path(module.REGISTRY).read_bytes()
    pins = {
        "CASE_COUNT": 246,
        "CASE_REGISTRY_SHA256": hashlib.sha256(raw).hexdigest(),
        "INCLUDED_PHASE_PAIR_COUNT": 245,
        "EXCLUDED_PHASE_PAIR_COUNT": 1,
    }

    cases, observed = module._registry(Path.cwd(), pins)

    assert observed == raw
    assert len(cases) == 246
    assert [case["case_id"] for case in cases if case["status"] == "excluded"] == ["reader:03:04"]


@pytest.mark.parametrize("mutation", ["digest", "missing", "extra", "duplicate", "status"])
def test_registry_rejects_denominator_mutations(tmp_path: Path, mutation: str) -> None:
    module = _module()
    raw = Path(module.REGISTRY).read_bytes()
    document = json.loads(raw)
    pins = {
        "CASE_COUNT": 246,
        "CASE_REGISTRY_SHA256": hashlib.sha256(raw).hexdigest(),
        "INCLUDED_PHASE_PAIR_COUNT": 245,
        "EXCLUDED_PHASE_PAIR_COUNT": 1,
    }
    if mutation == "digest":
        pins["CASE_REGISTRY_SHA256"] = _ZERO64
    elif mutation == "missing":
        document["cases"].pop()
    elif mutation == "extra":
        added = deepcopy(document["cases"][-1])
        added["case_id"] = "extra"
        document["cases"].append(added)
    elif mutation == "duplicate":
        document["cases"][1]["case_id"] = document["cases"][0]["case_id"]
    else:
        document["cases"][0]["status"] = "excluded"
    path = tmp_path / module.REGISTRY
    path.parent.mkdir(parents=True)
    path.write_bytes(raw if mutation == "digest" else module._canonical(document))

    _error(module, "inventory_invalid", lambda: module._registry(tmp_path, pins))


@pytest.mark.parametrize("phase", ["red", "candidate"])
def test_probe_inventory_accepts_only_exact_phase_closure(phase: str) -> None:
    module = _module()
    pins, collection, behavior = _probes(module, phase)

    results, digest = module._validate_probes(phase, pins, _cases(), collection, behavior)

    assert len(results) == 2
    assert results[1]["status"] == "PASS"
    assert results[0]["status"] == ("FAIL" if phase == "red" else "PASS")
    assert len(digest) == 64


@pytest.mark.parametrize("phase", ["red", "candidate"])
def test_probe_inventory_accepts_canonical_json_roundtrip(phase: str) -> None:
    module = _module()
    pins, collection, behavior = _probes(module, phase)
    collection = module._strict_json(module._canonical(collection))
    behavior = module._strict_json(module._canonical(behavior))

    assert list(behavior["outcome_counts"]) == sorted(module.OUTCOME_KEYS)
    results, digest = module._validate_probes(phase, pins, _cases(), collection, behavior)

    assert len(results) == 2
    assert len(digest) == 64


@pytest.mark.parametrize(
    "mutate",
    [
        lambda _p, c, _b: c["ordered_nodeids"].reverse(),
        lambda _p, _c, b: b["ordered_nodeids"].reverse(),
        lambda _p, _c, b: b["reports"].pop(),
        lambda _p, _c, b: b["reports"].append(deepcopy(b["reports"][0])),
        lambda _p, _c, b: b["outcome_counts"].update(passed=99),
        lambda _p, _c, b: b["semantic_observations"].append(deepcopy(b["semantic_observations"][0])),
        lambda _p, _c, b: b["semantic_observations"].append(
            {"case_id": "foreign", "nodeid": "foreign", "observed_class": "typed_rejection"}
        ),
        lambda _p, _c, b: b["input_vectors"].pop(),
        lambda _p, _c, b: b["input_vectors"].append(deepcopy(b["input_vectors"][0])),
        lambda _p, _c, b: b["input_vectors"][0]["inputs"][0].update(sha256="not-a-digest"),
        lambda _p, _c, b: b.update(exit_code=5),
        lambda _p, c, _b: c.update(reports=[{}]),
    ],
)
def test_probe_inventory_rejects_missing_extra_duplicate_and_forged_data(
    mutate: Callable[[dict[str, Any], dict[str, Any], dict[str, Any]], None],
) -> None:
    module = _module()
    pins, collection, behavior = _probes(module, "candidate")
    mutate(pins, collection, behavior)

    _error(
        module,
        "inventory_invalid",
        lambda: module._validate_probes("candidate", pins, _cases(), collection, behavior),
    )


def test_red_cannot_be_all_pass_even_when_authority_claims_it() -> None:
    module = _module()
    pins, collection, behavior = _probes(module, "candidate")
    pins.update(
        EXPECTED_RED_OUTCOME_COUNTS=behavior["outcome_counts"],
        EXPECTED_RED_FAILING_NODEIDS=[],
        EXPECTED_RED_DIAGNOSTIC_CLASSES=[],
    )
    behavior["exit_code"] = 1

    _error(module, "inventory_invalid", lambda: module._validate_probes("red", pins, _cases(), collection, behavior))


def test_candidate_semantic_mismatch_is_a_required_case_failure() -> None:
    module = _module()
    pins, collection, behavior = _probes(module, "candidate")
    behavior["semantic_observations"][0]["observed_class"] = "wrong"

    _error(
        module,
        "required_case_not_pass",
        lambda: module._validate_probes("candidate", pins, _cases(), collection, behavior),
    )


@pytest.mark.parametrize("outcome", ["skipped", "xfailed", "xpassed", "error"])
@pytest.mark.parametrize("phase", ["red", "candidate"])
def test_probe_inventory_never_treats_nonordinary_outcome_as_pass(phase: str, outcome: str) -> None:
    module = _module()
    pins, collection, behavior = _probes(module, phase)
    row = behavior["reports"][1]
    behavior["outcome_counts"]["passed"] -= 1
    if outcome == "error":
        row.update(phase="setup", outcome="failed", diagnostic_class="unexpected_exception")
        behavior["outcome_counts"]["errors"] = 1
    else:
        row.update(outcome=outcome, diagnostic_class="none")
        behavior["outcome_counts"][outcome] = 1
    if phase == "red":
        pins["EXPECTED_RED_OUTCOME_COUNTS"] = behavior["outcome_counts"]

    _error(
        module,
        "inventory_invalid" if phase == "red" or outcome == "error" else "required_case_not_pass",
        lambda: module._validate_probes(phase, pins, _cases(), collection, behavior),
    )


def _authority_dependencies(module: ModuleType, phase: str) -> list[str]:
    values: dict[str, object] = {key: _ZERO40 for key in module.COMMON_KEYS}
    values.update(
        CASE_REGISTRY_SHA256=_ZERO64,
        CASE_COUNT=246,
        INCLUDED_PHASE_PAIR_COUNT=245,
        EXCLUDED_PHASE_PAIR_COUNT=1,
        ORDERED_NODEIDS_SHA256=_ZERO64,
        COLLECTED_NODE_COUNT=249,
        EXPECTED_RED_OUTCOME_COUNTS={key: 0 for key in module.OUTCOME_KEYS},
        EXPECTED_RED_FAILING_NODEIDS=["x"],
        EXPECTED_RED_DIAGNOSTIC_CLASSES=["assertion_failure"],
        BEHAVIORAL_HARNESS_TREE_SHA256=_ZERO64,
        EVIDENCE_PROTOCOL_TREE_SHA256=_ZERO64,
        FOCUSED_PYTEST_ARGS=["test_x.py", "-q"],
    )
    if phase == "candidate":
        values.update(
            DETERMINISTIC_RED_PIN_COMMIT=_ZERO40,
            RED_EVIDENCE_COMMIT="1" * 40,
            RED_EVIDENCE_ARTIFACT_SHA256=_ZERO64,
            PRODUCTION_PATH_TUPLE=list(module.PRODUCTION_PATHS),
        )
    result = [
        f"BINDING_V2_CONTRACT_VERSION={module.AUTHORITY_VERSION}",
        f"BINDING_V2_PHASE={'red' if phase == 'red' else 'green'}",
    ]
    for key in (*module.COMMON_KEYS, *(module.GREEN_KEYS if phase == "candidate" else ())):
        value = values[key]
        encoded = module._canonical(value).decode().strip() if not isinstance(value, str) else value
        result.append(f"BINDING_V2_{key}={encoded}")
    return result


@pytest.mark.parametrize("phase", ["red", "candidate"])
def test_authority_dependency_tuple_accepts_exact_typed_values(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    phase: str,
) -> None:
    module = _module()
    relative = module.RED_AUTHORITY if phase == "red" else module.GREEN_AUTHORITY
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    path.write_text(yaml.safe_dump({"dependencies": _authority_dependencies(module, phase)}), encoding="utf-8")
    monkeypatch.setattr(module, "_head", lambda _root: _ZERO40)
    monkeypatch.setattr(module, "_blob", lambda _root, _commit, _path: path.read_bytes())
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0))

    pins, raw = module._load_authority(tmp_path, phase, relative)

    assert raw == path.read_bytes()
    assert pins["PHASE"] == ("red" if phase == "red" else "green")
    assert pins["CASE_COUNT"] == 246


@pytest.mark.parametrize(
    "mutation",
    ["duplicate", "reordered", "unknown", "noncanonical", "phase_mismatch"],
)
def test_authority_dependency_tuple_is_closed_and_canonical(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, mutation: str
) -> None:
    module = _module()
    relative = module.RED_AUTHORITY
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    dependencies = _authority_dependencies(module, "red")
    if mutation == "duplicate":
        dependencies.append(dependencies[-1])
    elif mutation == "reordered":
        dependencies[-1], dependencies[-2] = dependencies[-2], dependencies[-1]
    elif mutation == "unknown":
        dependencies.append("BINDING_V2_UNKNOWN=0")
    elif mutation == "noncanonical":
        dependencies[-1] = 'BINDING_V2_FOCUSED_PYTEST_ARGS=["test_x.py", "-q" ]'
    else:
        dependencies[1] = "BINDING_V2_PHASE=candidate"
    path.write_text(yaml.safe_dump({"dependencies": dependencies}), encoding="utf-8")
    monkeypatch.setattr(module, "_head", lambda _root: _ZERO40)
    monkeypatch.setattr(module, "_blob", lambda _root, _commit, _path: path.read_bytes())
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0))

    _error(module, "authority_invalid", lambda: module._load_authority(tmp_path, "red", relative))


@pytest.mark.parametrize("relative", ["/tmp/x", "../x", "x//y", "other.yml"])
def test_authority_path_is_exact_and_safe(tmp_path: Path, relative: str) -> None:
    module = _module()
    _error(module, "arguments_invalid", lambda: module._load_authority(tmp_path, "red", relative))


def test_authority_yaml_duplicate_key_is_rejected(tmp_path: Path) -> None:
    module = _module()
    path = tmp_path / module.RED_AUTHORITY
    path.parent.mkdir(parents=True)
    path.write_text("dependencies: []\ndependencies: []\n", encoding="utf-8")

    _error(module, "authority_invalid", lambda: module._load_authority(tmp_path, "red", module.RED_AUTHORITY))


def test_missing_authority_is_rejected(tmp_path: Path) -> None:
    module = _module()
    _error(module, "authority_invalid", lambda: module._load_authority(tmp_path, "red", module.RED_AUTHORITY))


def _commit(repo: Path, message: str) -> str:
    subprocess.run(("git", "add", "-A"), cwd=repo, check=True, capture_output=True)
    subprocess.run(("git", "commit", "--allow-empty", "-m", message), cwd=repo, check=True, capture_output=True)
    return subprocess.run(
        ("git", "rev-parse", "HEAD"), cwd=repo, check=True, capture_output=True, text=True
    ).stdout.strip()


@pytest.mark.parametrize(
    "mutation",
    [
        "artifact",
        "base",
        "missing_spec",
        "swapped_lineage",
        "missing_protocol",
        "red_pin",
        "red_evidence",
        "owner_order",
        "head",
        "unrelated_base",
        "missing_dependency",
        "wrong_dependency",
        "late_dependency",
    ],
)
def test_candidate_lineage_binds_last_touch_red_artifact_and_exact_production_diff(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    # This repository is authored in tmp_path; it is never a current public authority chain.
    module = _module()
    subprocess.run(("git", "init", "-q"), cwd=tmp_path, check=True)
    subprocess.run(("git", "config", "user.email", "evidence@example.invalid"), cwd=tmp_path, check=True)
    subprocess.run(("git", "config", "user.name", "Evidence Test"), cwd=tmp_path, check=True)
    integration = _commit(tmp_path, "integration")
    spec = tmp_path / "docs/feature-design-postgres-mssql-r1-v3-provider-binding-contract-v2.md"
    spec.parent.mkdir(parents=True)
    spec.write_text("- Status: APPROVED\n", encoding="utf-8")
    approved = _commit(tmp_path, "approved")
    red_authority = tmp_path / module.RED_AUTHORITY
    red_authority.parent.mkdir(parents=True, exist_ok=True)
    red_authority.write_text("red task\n", encoding="utf-8")
    red_task = _commit(tmp_path, "red task")
    for relative in module.BEHAVIOR_TREE_PATHS:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative, encoding="utf-8")
    red_test = _commit(tmp_path, "red tests")
    for relative in module.PROTOCOL_TREE_PATHS:
        if relative == module.PROTOCOL_TREE_PATHS[-1] and mutation in {"missing_dependency", "wrong_dependency"}:
            continue
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative, encoding="utf-8")
    if mutation == "wrong_dependency":
        (tmp_path / "wrong-protocol-dependency.txt").write_text("synthetic wrong dependency\n", encoding="utf-8")
    protocol = _commit(tmp_path, "protocol")
    red_authority.write_text("red pin\n", encoding="utf-8")
    if mutation == "late_dependency":
        (tmp_path / module.PROTOCOL_TREE_PATHS[-1]).write_text("synthetic late dependency\n", encoding="utf-8")
    red_pin = _commit(tmp_path, "red pin")
    artifact_path = tmp_path / f"test_artifacts/postgres-mssql-r1-v3/binding-v2/{red_pin}/red-binding-inventory.json"
    artifact_path.parent.mkdir(parents=True)
    artifact_path.write_bytes(b'{"behavioral_status":"RED"}\n')
    red_evidence = _commit(tmp_path, "red evidence")
    authority = tmp_path / module.GREEN_AUTHORITY
    authority.parent.mkdir(parents=True, exist_ok=True)
    authority.write_text("green authority\n", encoding="utf-8")
    green_task = _commit(tmp_path, "green task")
    for relative in module.PRODUCTION_PATHS:
        path = tmp_path / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(relative, encoding="utf-8")
    implementation = _commit(tmp_path, "implementation")
    monkeypatch.setattr(module, "INTEGRATION_BASE", integration)
    pins = {
        "INTEGRATION_BASE_COMMIT": integration,
        "APPROVED_SPECIFICATION_COMMIT": approved,
        "RED_TASK_CONTRACT_COMMIT": red_task,
        "RED_TEST_AUTHORITY_COMMIT": red_test,
        "EVIDENCE_PROTOCOL_COMMIT": protocol,
        "DETERMINISTIC_RED_PIN_COMMIT": red_pin,
        "RED_EVIDENCE_COMMIT": red_evidence,
        "RED_EVIDENCE_ARTIFACT_SHA256": hashlib.sha256(artifact_path.read_bytes()).hexdigest(),
        "PRODUCTION_PATH_TUPLE": list(module.PRODUCTION_PATHS),
    }

    if mutation in {"missing_dependency", "wrong_dependency", "late_dependency"}:
        _error(
            module,
            "authority_invalid",
            lambda: module._validate_authority(tmp_path, "candidate", pins, implementation, module.GREEN_AUTHORITY),
        )
        return
    observed = module._validate_authority(tmp_path, "candidate", pins, implementation, module.GREEN_AUTHORITY)

    assert observed == {
        "deterministic_red_pin_commit": red_pin,
        "red_evidence_commit": red_evidence,
        "green_task_contract_commit": green_task,
        "implementation_commit": implementation,
    }
    changed = deepcopy(pins)
    candidate = implementation
    reason = "authority_invalid"
    if mutation == "artifact":
        changed["RED_EVIDENCE_ARTIFACT_SHA256"] = _ZERO64
    elif mutation == "base":
        changed["INTEGRATION_BASE_COMMIT"] = approved
    elif mutation == "missing_spec":
        changed["APPROVED_SPECIFICATION_COMMIT"] = _ZERO40
    elif mutation == "swapped_lineage":
        changed["RED_TASK_CONTRACT_COMMIT"] = red_test
        changed["RED_TEST_AUTHORITY_COMMIT"] = red_task
    elif mutation == "missing_protocol":
        changed["EVIDENCE_PROTOCOL_COMMIT"] = _ZERO40
    elif mutation == "red_pin":
        changed["DETERMINISTIC_RED_PIN_COMMIT"] = protocol
    elif mutation == "red_evidence":
        changed["RED_EVIDENCE_COMMIT"] = red_pin
    elif mutation == "owner_order":
        changed["PRODUCTION_PATH_TUPLE"] = list(reversed(module.PRODUCTION_PATHS))
    elif mutation == "head":
        candidate, reason = green_task, "commit_mismatch"
    else:
        # Exact base equality is insufficient: it must also be an ancestor of the specification.
        changed["INTEGRATION_BASE_COMMIT"] = implementation
        monkeypatch.setattr(module, "INTEGRATION_BASE", implementation)
    _error(
        module,
        reason,
        lambda: module._validate_authority(tmp_path, "candidate", changed, candidate, module.GREEN_AUTHORITY),
    )


def test_tree_digest_binds_domain_order_paths_and_bytes(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module()
    blobs = {"a": b"one", "b": b"two"}
    monkeypatch.setattr(module, "_blob", lambda _root, _commit, path: blobs[path])

    observed = module._tree_digest(Path(), "0" * 40, b"domain\0", ("a", "b"))

    rows = [{"path": path, "sha256": hashlib.sha256(blobs[path]).hexdigest()} for path in ("a", "b")]
    assert observed == module._digest(b"domain\0", rows)
    assert observed != module._tree_digest(Path(), "0" * 40, b"domain\0", ("b", "a"))
    assert observed != module._tree_digest(Path(), "0" * 40, b"other\0", ("a", "b"))


@pytest.mark.parametrize("kind", ["collection", "behavior"])
def test_probe_subprocess_is_hermetic_and_machine_silent(kind: str) -> None:
    module = _module()
    nodeid = (
        "tests/test_postgres_mssql_r1_v3_binding_evidence_protocol.py::test_registry_is_closed_complete_and_digestible"
    )

    observed = module._run_probe(Path.cwd(), kind, (nodeid, "-q"))

    assert observed["probe_kind"] == kind
    assert observed["exit_code"] == 0
    assert observed["ordered_nodeids"] == [nodeid]


def test_probe_rejects_foreign_dpone_import_origin(monkeypatch: pytest.MonkeyPatch) -> None:
    module = _module()
    probe = module._Probe(Path.cwd(), "collection", ("x",))
    monkeypatch.setitem(sys.modules, "dpone.foreign_evidence_test", SimpleNamespace(__file__="/tmp/foreign.py"))

    _error(module, "inventory_invalid", probe.document)


def test_publish_is_create_only_durable_and_mode_restricted(tmp_path: Path) -> None:
    module = _module()
    relative = PurePosixPath("artifact/nested/result.json")

    module._publish(tmp_path, relative, b"{}\n")

    final = tmp_path / relative
    assert final.read_bytes() == b"{}\n"
    assert final.stat().st_mode & 0o777 == 0o600
    assert (tmp_path / "artifact").stat().st_mode & 0o777 == 0o700
    assert not list(final.parent.glob(".*.tmp-*"))
    _error(module, "output_conflict", lambda: module._publish(tmp_path, relative, b"changed\n"))
    assert final.read_bytes() == b"{}\n"


@pytest.mark.parametrize("kind", ["parent_symlink", "final_symlink"])
def test_publish_rejects_symlink_traversal(tmp_path: Path, kind: str) -> None:
    module = _module()
    external = tmp_path / "external"
    external.mkdir()
    if kind == "parent_symlink":
        (tmp_path / "artifact").symlink_to(external, target_is_directory=True)
        reason = "unsafe_output_path"
    else:
        (tmp_path / "artifact").mkdir()
        (tmp_path / "artifact/result.json").symlink_to(external / "missing")
        reason = "output_conflict"
    _error(
        module,
        reason,
        lambda: module._publish(tmp_path, PurePosixPath("artifact/result.json"), b"x"),
    )
    assert not list(external.iterdir())


@pytest.mark.parametrize(
    ("operation", "reason"),
    [
        ("open", "filesystem_error"),
        ("write", "filesystem_error"),
        ("file_fsync", "filesystem_error"),
        ("close", "filesystem_error"),
        ("link_exists", "output_conflict"),
        ("link", "filesystem_error"),
        ("first_dir_fsync", "artifact_outcome_unknown"),
        ("unlink_temp", "artifact_outcome_unknown"),
        ("second_dir_fsync", "artifact_outcome_unknown"),
    ],
)
def test_publish_classifies_every_io_failure_and_never_leaves_success(
    tmp_path: Path,
    operation: str,
    reason: str,
) -> None:
    module = _module()
    real = module._Fs()
    fsync_calls = 0

    def fail(name: str, function: Callable[..., Any]) -> Callable[..., Any]:
        def wrapped(*args: Any, **kwargs: Any) -> Any:
            if operation == name:
                if name == "link_exists":
                    raise FileExistsError
                raise OSError(name)
            return function(*args, **kwargs)

        return wrapped

    def fsync(fd: int) -> None:
        nonlocal fsync_calls
        fsync_calls += 1
        names = {1: "file_fsync", 2: "first_dir_fsync", 3: "second_dir_fsync"}
        if operation == names.get(fsync_calls):
            raise OSError(operation)
        os.fsync(fd)

    fake = module._Fs(
        open=fail("open", real.open),
        write=fail("write", real.write),
        fsync=fsync,
        close=fail("close", real.close),
        link=fail("link_exists" if operation == "link_exists" else "link", real.link),
        unlink=fail("unlink_temp", real.unlink),
    )
    relative = PurePosixPath("artifact/result.json")

    _error(module, reason, lambda: module._publish(tmp_path, relative, b"payload\n", fake))

    final = tmp_path / relative
    if operation == "unlink_temp":
        assert final.read_bytes() == b"payload\n"
        assert list(final.parent.glob(".*.tmp-*"))
    else:
        assert not final.exists()
        assert not list(final.parent.glob(".*.tmp-*"))


def test_two_concurrent_publishers_have_one_creator_and_one_conflict(tmp_path: Path) -> None:
    module = _module()
    relative = PurePosixPath("artifact/result.json")
    barrier = threading.Barrier(2)
    outcomes: list[str] = []

    def publish(payload: bytes) -> None:
        barrier.wait()
        try:
            module._publish(tmp_path, relative, payload)
            outcomes.append("created")
        except module.EvidenceError as exc:
            outcomes.append(exc.reason)

    threads = [threading.Thread(target=publish, args=(value,)) for value in (b"one\n", b"two\n")]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    assert sorted(outcomes) == ["created", "output_conflict"]
    assert (tmp_path / relative).read_bytes() in {b"one\n", b"two\n"}


def test_cli_error_is_one_canonical_json_line() -> None:
    result = subprocess.run((sys.executable, str(_PRODUCER)), capture_output=True, check=False)
    assert result.returncode == 2
    assert result.stdout == b""
    assert result.stderr == b'{"reason":"arguments_invalid","status":"error"}\n'


@pytest.mark.parametrize("output", ["/tmp/result.json", "../result.json", "wrong/result.json"])
def test_main_rejects_every_noncanonical_output_before_schema_or_write(
    monkeypatch: pytest.MonkeyPatch,
    output: str,
) -> None:
    module = _module()
    expected = PurePosixPath(f"test_artifacts/postgres-mssql-r1-v3/binding-v2/{_ZERO40}/red-binding-inventory.json")
    monkeypatch.setattr(module, "_document", lambda *_args: ({}, expected))

    _error(
        module,
        "unsafe_output_path",
        lambda: module.main(
            [
                "--phase",
                "red",
                "--authority",
                module.RED_AUTHORITY,
                "--commit",
                _ZERO40,
                "--output",
                output,
            ]
        ),
    )


def test_main_validates_then_publishes_exact_canonical_bytes(
    monkeypatch: pytest.MonkeyPatch,
    capfd: pytest.CaptureFixture[str],
) -> None:
    module = _module()
    expected = PurePosixPath(f"test_artifacts/postgres-mssql-r1-v3/binding-v2/{_ZERO40}/red-binding-inventory.json")
    document = {"z": 1, "a": "value"}
    events: list[object] = []
    monkeypatch.setattr(module, "_document", lambda *_args: (document, expected))
    monkeypatch.setattr(module, "_validate_schema", lambda _root, value: events.append(("schema", value)))
    monkeypatch.setattr(module, "_publish", lambda _root, path, payload: events.append(("publish", path, payload)))

    result = module.main(
        [
            "--phase",
            "red",
            "--authority",
            module.RED_AUTHORITY,
            "--commit",
            _ZERO40,
            "--output",
            expected.as_posix(),
        ]
    )

    captured = capfd.readouterr()
    assert result == 0
    assert captured.out == f'{{"artifact_path":"{expected}","status":"created"}}\n'
    assert captured.err == ""
    assert events == [("schema", document), ("publish", expected, b'{"a":"value","z":1}\n')]


def test_document_stops_before_authority_or_probes_when_worktree_is_dirty(tmp_path: Path) -> None:
    module = _module()
    subprocess.run(("git", "init", "-q"), cwd=tmp_path, check=True)
    (tmp_path / "dirty").write_text("x", encoding="utf-8")

    _error(
        module,
        "worktree_dirty",
        lambda: module._document(tmp_path, "red", module.RED_AUTHORITY, _ZERO40),
    )


def _schema_document(phase: str = "red") -> dict[str, Any]:
    probe = {
        "probe_kind": "collection",
        "pytest_args": ["test_x.py"],
        "exit_code": 0,
        "ordered_nodeids": ["test_x.py::test_x"],
        "reports": [],
        "semantic_observations": [],
        "input_vectors": [],
        "outcome_counts": {
            "passed": 0,
            "failed": 0,
            "skipped": 0,
            "xfailed": 0,
            "xpassed": 0,
            "errors": 0,
        },
        "import_origins": [],
    }
    candidate = phase == "candidate"
    return {
        "record_kind": "binding_contract_evidence",
        "producer_contract_version": "dpone-postgres-mssql-r1-v3-binding-v2-evidence-4",
        "phase": phase,
        "subject_commit": _ZERO40,
        "task_authority_path": (
            "docs/agent-task-contracts/public-binding-evidence-candidate-v1.yml"
            if candidate
            else "docs/agent-task-contracts/public-binding-evidence-red-v1.yml"
        ),
        "task_authority_sha256": _ZERO64,
        "approved_specification_commit": _ZERO40,
        "integration_base_commit": "3977ca2d04ca5dbcf3338d7c31faff8a1199549e",
        "red_task_contract_commit": _ZERO40,
        "red_test_authority_commit": _ZERO40,
        "evidence_protocol_commit": _ZERO40,
        "deterministic_red_pin_commit": _ZERO40,
        "red_evidence_commit": _ZERO40 if candidate else None,
        "green_task_contract_commit": _ZERO40 if candidate else None,
        "implementation_commit": _ZERO40 if candidate else None,
        "final_evidence_commit": None,
        "producer_sha256": _ZERO64,
        "schema_sha256": _ZERO64,
        "registry_sha256": _ZERO64,
        "ordered_nodeids_sha256": _ZERO64,
        "behavioral_harness_tree_sha256": _ZERO64,
        "evidence_protocol_tree_sha256": _ZERO64,
        "input_vector_sha256": _ZERO64,
        "collection_probe": probe,
        "behavior_probe": {**probe, "probe_kind": "behavior"},
        "case_results": [
            {
                "case_id": f"case-{index}",
                "nodeid": f"test_x.py::test_x[{index}]",
                "status": "PASS",
                "observed_class": "typed_rejection",
            }
            for index in range(246)
        ],
        "behavioral_status": "PASS" if candidate else "RED",
        "certification_status": "UNVERIFIED",
        "adapter_layer": "pure",
        "red_evidence_artifact_sha256": _ZERO64 if candidate else None,
        "production_path_tuple": [
            "src/dpone/contracts/mssql_r1_v3_binding_modules.py",
            "src/dpone/contracts/mssql_r1_v3_binding_permissions.py",
            "src/dpone/contracts/mssql_r1_v3_binding_pack.py",
            "src/dpone/contracts/mssql_r1_v3_binding_validation.py",
        ]
        if candidate
        else None,
    }


@pytest.mark.parametrize("phase", ["red", "candidate"])
def test_schema_accepts_only_closed_phase_documents(phase: str) -> None:
    schema = json.loads(_SCHEMA.read_text(encoding="utf-8"))
    jsonschema.Draft202012Validator.check_schema(schema)
    jsonschema.Draft202012Validator(schema).validate(_schema_document(phase))
    assert schema["properties"]["adapter_layer"] == {"enum": ["pure", "mocked", "vendor_live"]}


@pytest.mark.parametrize(
    "mutate",
    [
        lambda value: value.update(extra=True),
        lambda value: value.update(certification_status="PASS"),
        lambda value: value.update(adapter_layer="unknown"),
        lambda value: value.update(red_evidence_commit=_ZERO40),
        lambda value: value["case_results"].pop(),
        lambda value: value["case_results"].append(deepcopy(value["case_results"][0])),
        lambda value: value.update(final_evidence_commit=_ZERO40),
    ],
)
def test_schema_rejects_overclaim_splice_and_denominator_mutations(
    mutate: Callable[[dict[str, Any]], None],
) -> None:
    schema = json.loads(_SCHEMA.read_text(encoding="utf-8"))
    document = _schema_document()
    mutate(document)

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(schema).validate(document)


def test_producer_and_schema_have_no_mutable_pin_or_runtime_import() -> None:
    module = _module()
    source = _PRODUCER.read_text(encoding="utf-8")

    assert module.INTEGRATION_BASE in source
    assert "import dpone" not in source
    assert "requests" not in source
    assert "socket" not in source
    assert "datetime" not in source
    assert "time.time" not in source


@pytest.mark.parametrize("phase", ["red", "candidate"])
@pytest.mark.parametrize("layout", [1, 2, 3])
def test_synthetic_legacy_layout_schema_remains_closed_and_readable(phase: str, layout: int) -> None:
    directory = Path("tests/fixtures/binding_evidence_synthetic_v1")
    historical = json.loads((directory / f"layout-{layout}.schema.json").read_text())
    fixture = json.loads((directory / f"layout-{layout}-{phase}.json").read_text())
    assert fixture["fixture_kind"] == "authored_synthetic_binding_evidence"
    assert fixture["original_wire_bytes_preserved"] is False
    assert fixture["live_certification"] is False
    old = fixture["document"]
    jsonschema.Draft202012Validator(historical).validate(old)
    current = json.loads(_SCHEMA.read_text())
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(current).validate(old)
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(historical).validate(_schema_document(phase))


@pytest.mark.parametrize("layout", [1, 2])
def test_evidence_rejects_mixed_layout_version_and_path_inventory(layout: int) -> None:
    current = json.loads(_SCHEMA.read_text())
    historical = json.loads(
        Path(f"tests/fixtures/binding_evidence_synthetic_v1/layout-{layout}.schema.json").read_text()
    )
    document = _schema_document("candidate")
    document["production_path_tuple"] = [
        item["const"] for item in historical["properties"]["production_path_tuple"]["oneOf"][1]["prefixItems"]
    ]
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.Draft202012Validator(current).validate(document)


def test_current_producer_uses_four_owner_layout_and_new_protocol_domain() -> None:
    module = _module()
    assert module.CONTRACT_VERSION == "dpone-postgres-mssql-r1-v3-binding-v2-evidence-4"
    assert module.SCHEMA == _SCHEMA.as_posix()
    assert module.PROTOCOL_DOMAIN == b"dpone-binding-v2-evidence-protocol-tree-v4\0"
    assert list(module.PRODUCTION_PATHS) == _schema_document("candidate")["production_path_tuple"]
    assert len(module.PRODUCTION_PATHS) == 4


def test_current_public_authority_profile_is_closed() -> None:
    module = _module()
    assert module.CONTRACT_VERSION == "dpone-postgres-mssql-r1-v3-binding-v2-evidence-4"
    assert module.AUTHORITY_VERSION == "dpone-binding-v2-evidence-authority-2"
    assert module.INTEGRATION_BASE == "3977ca2d04ca5dbcf3338d7c31faff8a1199549e"
    assert module.RED_AUTHORITY == "docs/agent-task-contracts/public-binding-evidence-red-v1.yml"
    assert module.GREEN_AUTHORITY == "docs/agent-task-contracts/public-binding-evidence-candidate-v1.yml"
    assert module.SCHEMA == "docs/schemas/evidence/postgres-mssql-r1-v3-binding-v2-layout-4.schema.json"
    assert module.PROTOCOL_DOMAIN == b"dpone-binding-v2-evidence-protocol-tree-v4\0"


@pytest.mark.parametrize("version", ["1", "3", "", "02"])
def test_current_authority_rejects_other_protocol_versions(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, version: str
) -> None:
    module = _module()
    relative = module.RED_AUTHORITY
    path = tmp_path / relative
    path.parent.mkdir(parents=True)
    dependencies = _authority_dependencies(module, "red")
    dependencies[0] = f"BINDING_V2_CONTRACT_VERSION=dpone-binding-v2-evidence-authority-{version}"
    path.write_text(yaml.safe_dump({"dependencies": dependencies}), encoding="utf-8")
    monkeypatch.setattr(module, "_head", lambda _root: _ZERO40)
    monkeypatch.setattr(module, "_blob", lambda _root, _commit, _path: path.read_bytes())
    monkeypatch.setattr(module.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=0))
    _error(module, "authority_invalid", lambda: module._load_authority(tmp_path, "red", relative))


# Independent version4 expected authority closure. Never derive this from the producer or a glob.
_EXPECTED_PROTOCOL_PATHS = (
    "tests/support/postgres_mssql_r1_v3_binding_v2_evidence.py",
    "docs/schemas/evidence/postgres-mssql-r1-v3-binding-v2-layout-4.schema.json",
    "tests/test_postgres_mssql_r1_v3_binding_evidence_protocol.py",
    "tests/support/binding_evidence_synthetic_fixtures_v1.py",
    "tests/test_binding_evidence_synthetic_fixtures_v1.py",
    "tests/fixtures/binding_evidence_synthetic_v1/recipe.json",
    "tests/fixtures/binding_evidence_synthetic_v1/layout-1.schema.json",
    "tests/fixtures/binding_evidence_synthetic_v1/layout-1-red.json",
    "tests/fixtures/binding_evidence_synthetic_v1/layout-1-candidate.json",
    "tests/fixtures/binding_evidence_synthetic_v1/layout-2.schema.json",
    "tests/fixtures/binding_evidence_synthetic_v1/layout-2-red.json",
    "tests/fixtures/binding_evidence_synthetic_v1/layout-2-candidate.json",
    "tests/fixtures/binding_evidence_synthetic_v1/layout-3.schema.json",
    "tests/fixtures/binding_evidence_synthetic_v1/layout-3-red.json",
    "tests/fixtures/binding_evidence_synthetic_v1/layout-3-candidate.json",
)


def test_current_protocol_dependency_tuple_is_complete_and_exact() -> None:
    assert _module().PROTOCOL_TREE_PATHS == _EXPECTED_PROTOCOL_PATHS
    assert len(set(_EXPECTED_PROTOCOL_PATHS)) == 15


@pytest.mark.parametrize("relative", _EXPECTED_PROTOCOL_PATHS)
def test_each_protocol_dependency_binds_bytes_and_missing_input(tmp_path: Path, relative: str) -> None:
    module = _module()
    subprocess.run(("git", "init", "-q"), cwd=tmp_path, check=True)
    subprocess.run(("git", "config", "user.email", "evidence@example.invalid"), cwd=tmp_path, check=True)
    subprocess.run(("git", "config", "user.name", "Evidence Test"), cwd=tmp_path, check=True)
    for name in _EXPECTED_PROTOCOL_PATHS:
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"synthetic protocol input: {name}\n", encoding="utf-8")
    original = _commit(tmp_path, "synthetic protocol fixture")
    before = module._tree_digest(tmp_path, original, module.PROTOCOL_DOMAIN, module.PROTOCOL_TREE_PATHS)
    target = tmp_path / relative
    target.write_bytes(target.read_bytes() + b"synthetic mutation\n")
    changed = _commit(tmp_path, "synthetic input mutation")
    assert module._tree_digest(tmp_path, changed, module.PROTOCOL_DOMAIN, module.PROTOCOL_TREE_PATHS) != before
    target.unlink()
    missing = _commit(tmp_path, "synthetic missing input")
    _error(
        module,
        "authority_invalid",
        lambda: module._tree_digest(tmp_path, missing, module.PROTOCOL_DOMAIN, module.PROTOCOL_TREE_PATHS),
    )
