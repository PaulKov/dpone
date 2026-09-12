"""Create-only hermetic evidence for the restricted Binding V2 contract."""

from __future__ import annotations

import argparse
import contextlib
import hashlib
import json
import os
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from typing import Any, Final, NoReturn

import yaml

CONTRACT_VERSION: Final = "dpone-postgres-mssql-r1-v3-binding-v2-evidence-4"
AUTHORITY_VERSION: Final = "dpone-binding-v2-evidence-authority-2"
INTEGRATION_BASE: Final = "3977ca2d04ca5dbcf3338d7c31faff8a1199549e"
RED_AUTHORITY = "docs/agent-task-contracts/public-binding-evidence-red-v1.yml"
GREEN_AUTHORITY = "docs/agent-task-contracts/public-binding-evidence-candidate-v1.yml"
REGISTRY = "docs/schemas/evidence/postgres-mssql-r1-v3-binding-v2-cases.json"
SCHEMA = "docs/schemas/evidence/postgres-mssql-r1-v3-binding-v2-layout-4.schema.json"
PRODUCER = "tests/support/postgres_mssql_r1_v3_binding_v2_evidence.py"
PROTOCOL_TEST = "tests/test_postgres_mssql_r1_v3_binding_evidence_protocol.py"
BEHAVIOR_PATHS = (
    "tests/test_postgres_mssql_r1_v3_binding_contract.py",
    "tests/test_postgres_mssql_r1_v3_binding_mutation_inventory.py",
    "tests/test_postgres_mssql_r1_v3_binding_semantic_inventory.py",
)
BEHAVIOR_TREE_PATHS = (*BEHAVIOR_PATHS, REGISTRY)
PROTOCOL_TREE_PATHS = (
    PRODUCER,
    SCHEMA,
    PROTOCOL_TEST,
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
PRODUCTION_PATHS = (
    "src/dpone/contracts/mssql_r1_v3_binding_modules.py",
    "src/dpone/contracts/mssql_r1_v3_binding_permissions.py",
    "src/dpone/contracts/mssql_r1_v3_binding_pack.py",
    "src/dpone/contracts/mssql_r1_v3_binding_validation.py",
)
BEHAVIOR_DOMAIN = b"dpone-binding-v2-behavior-tree-v1\0"
PROTOCOL_DOMAIN = b"dpone-binding-v2-evidence-protocol-tree-v4\0"
NODEIDS_DOMAIN = b"dpone-binding-v2-ordered-nodeids-v1\0"
INPUTS_DOMAIN = b"dpone-binding-v2-input-vectors-v1\0"
OUTCOME_KEYS = ("passed", "failed", "skipped", "xfailed", "xpassed", "errors")
COMMON_KEYS = (
    "APPROVED_SPECIFICATION_COMMIT",
    "INTEGRATION_BASE_COMMIT",
    "RED_TASK_CONTRACT_COMMIT",
    "RED_TEST_AUTHORITY_COMMIT",
    "EVIDENCE_PROTOCOL_COMMIT",
    "CASE_REGISTRY_SHA256",
    "CASE_COUNT",
    "INCLUDED_PHASE_PAIR_COUNT",
    "EXCLUDED_PHASE_PAIR_COUNT",
    "ORDERED_NODEIDS_SHA256",
    "COLLECTED_NODE_COUNT",
    "EXPECTED_RED_OUTCOME_COUNTS",
    "EXPECTED_RED_FAILING_NODEIDS",
    "EXPECTED_RED_DIAGNOSTIC_CLASSES",
    "BEHAVIORAL_HARNESS_TREE_SHA256",
    "EVIDENCE_PROTOCOL_TREE_SHA256",
    "FOCUSED_PYTEST_ARGS",
)
GREEN_KEYS = (
    "DETERMINISTIC_RED_PIN_COMMIT",
    "RED_EVIDENCE_COMMIT",
    "RED_EVIDENCE_ARTIFACT_SHA256",
    "PRODUCTION_PATH_TUPLE",
)
HEX = frozenset("0123456789abcdef")
_PRIVATE = "__dpone_binding_v2_probe__"


class EvidenceError(RuntimeError):
    """A closed producer failure with a stable process exit."""

    def __init__(self, reason: str, exit_code: int) -> None:
        super().__init__(reason)
        self.reason, self.exit_code = reason, exit_code


class _Parser(argparse.ArgumentParser):
    def error(self, message: str) -> NoReturn:
        del message
        raise EvidenceError("arguments_invalid", 2)


class _UniqueLoader(yaml.SafeLoader):
    pass


def _mapping(loader: _UniqueLoader, node: yaml.MappingNode, deep: bool = False) -> dict[object, object]:
    result: dict[object, object] = {}
    for key_node, value_node in node.value:
        key = loader.construct_object(key_node, deep=deep)
        if key in result:
            raise EvidenceError("authority_invalid", 2)
        result[key] = loader.construct_object(value_node, deep=deep)
    return result


_UniqueLoader.add_constructor(yaml.resolver.BaseResolver.DEFAULT_MAPPING_TAG, _mapping)


def _canonical(value: object) -> bytes:
    return (json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n").encode()


def _digest(domain: bytes, value: object) -> str:
    return hashlib.sha256(domain + _canonical(value)).hexdigest()


def _sha(value: object) -> bool:
    return isinstance(value, str) and len(value) == 40 and set(value) <= HEX


def _git(root: Path, *args: str, check: bool = True) -> subprocess.CompletedProcess[bytes]:
    result = subprocess.run(("git", *args), cwd=root, capture_output=True, check=False)
    if check and result.returncode:
        raise EvidenceError("authority_invalid", 2)
    return result


def _head(root: Path) -> str:
    return _git(root, "rev-parse", "HEAD").stdout.decode().strip()


def _blob(root: Path, commit: str, path: str) -> bytes:
    result = _git(root, "show", f"{commit}:{path}", check=False)
    if result.returncode:
        raise EvidenceError("authority_invalid", 2)
    return result.stdout


def _parent(root: Path, commit: str) -> str:
    result = _git(root, "rev-parse", f"{commit}^", check=False)
    if result.returncode:
        raise EvidenceError("authority_invalid", 2)
    return result.stdout.decode().strip()


def _ancestor(root: Path, earlier: str, later: str) -> bool:
    return _git(root, "merge-base", "--is-ancestor", earlier, later, check=False).returncode == 0


def _changed_paths(root: Path, earlier: str, later: str) -> list[str]:
    return _git(root, "diff", "--name-only", earlier, later).stdout.decode().splitlines()


def _strict_json(raw: bytes, reason: str = "inventory_invalid") -> Any:
    def pairs(values: list[tuple[str, Any]]) -> dict[str, Any]:
        result: dict[str, Any] = {}
        for key, value in values:
            if key in result:
                raise EvidenceError(reason, 2)
            result[key] = value
        return result

    try:
        return json.loads(raw, object_pairs_hook=pairs)
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise EvidenceError(reason, 2) from exc


def _tree_digest(root: Path, commit: str, domain: bytes, paths: tuple[str, ...]) -> str:
    rows = [{"path": path, "sha256": hashlib.sha256(_blob(root, commit, path)).hexdigest()} for path in paths]
    return _digest(domain, rows)


def _parse_json_value(value: str) -> Any:
    observed = _strict_json(value.encode(), "authority_invalid")
    if _canonical(observed).decode().strip() != value:
        raise EvidenceError("authority_invalid", 2)
    return observed


def _load_authority(root: Path, phase: str, relative: str) -> tuple[dict[str, Any], bytes]:
    expected = RED_AUTHORITY if phase == "red" else GREEN_AUTHORITY
    candidate = PurePosixPath(relative)
    if relative != candidate.as_posix() or candidate.is_absolute() or ".." in candidate.parts or relative != expected:
        raise EvidenceError("arguments_invalid", 2)
    path = root / candidate
    if path.is_symlink() or not path.is_file():
        raise EvidenceError("authority_invalid", 2)
    raw = path.read_bytes()
    try:
        document = yaml.load(raw, Loader=_UniqueLoader)
    except EvidenceError:
        raise
    except (TypeError, ValueError, yaml.YAMLError) as exc:
        raise EvidenceError("authority_invalid", 2) from exc
    if not isinstance(document, dict) or _blob(root, _head(root), relative) != raw:
        raise EvidenceError("authority_invalid", 2)
    validator = root / "tools/agent_policy/task_contract.py"
    checked = subprocess.run((sys.executable, str(validator), relative), cwd=root, capture_output=True, check=False)
    if checked.returncode:
        raise EvidenceError("authority_invalid", 2)
    dependencies = document.get("dependencies")
    if not isinstance(dependencies, list) or any(type(item) is not str for item in dependencies):
        raise EvidenceError("authority_invalid", 2)
    expected_keys = ("CONTRACT_VERSION", "PHASE", *COMMON_KEYS, *(GREEN_KEYS if phase == "candidate" else ()))
    parsed: dict[str, Any] = {}
    order: list[str] = []
    for item in dependencies:
        if not item.startswith("BINDING_V2_") or "=" not in item:
            raise EvidenceError("authority_invalid", 2)
        key, value = item[11:].split("=", 1)
        if key in parsed:
            raise EvidenceError("authority_invalid", 2)
        order.append(key)
        parsed[key] = value
    authority_phase = "red" if phase == "red" else "green"
    if (
        tuple(order) != expected_keys
        or parsed["CONTRACT_VERSION"] != AUTHORITY_VERSION
        or parsed["PHASE"] != authority_phase
    ):
        raise EvidenceError("authority_invalid", 2)
    for key in ("CASE_COUNT", "INCLUDED_PHASE_PAIR_COUNT", "EXCLUDED_PHASE_PAIR_COUNT", "COLLECTED_NODE_COUNT"):
        parsed[key] = _parse_json_value(parsed[key])
        if type(parsed[key]) is not int or parsed[key] < 0:
            raise EvidenceError("authority_invalid", 2)
    if not parsed["CASE_COUNT"] or not parsed["COLLECTED_NODE_COUNT"]:
        raise EvidenceError("authority_invalid", 2)
    json_keys = (
        "EXPECTED_RED_OUTCOME_COUNTS",
        "EXPECTED_RED_FAILING_NODEIDS",
        "EXPECTED_RED_DIAGNOSTIC_CLASSES",
        "FOCUSED_PYTEST_ARGS",
        *(("PRODUCTION_PATH_TUPLE",) if phase == "candidate" else ()),
    )
    for key in json_keys:
        parsed[key] = _parse_json_value(parsed[key])
    counts = parsed["EXPECTED_RED_OUTCOME_COUNTS"]
    failing = parsed["EXPECTED_RED_FAILING_NODEIDS"]
    diagnostics = parsed["EXPECTED_RED_DIAGNOSTIC_CLASSES"]
    focused_args = parsed["FOCUSED_PYTEST_ARGS"]
    if (
        not isinstance(counts, dict)
        or set(counts) != set(OUTCOME_KEYS)
        or any(type(value) is not int or value < 0 for value in counts.values())
        or not isinstance(failing, list)
        or any(type(value) is not str or not value for value in failing)
        or len(failing) != len(set(failing))
        or not isinstance(diagnostics, list)
        or len(diagnostics) != len(failing)
        or any(value not in {"assertion_failure", "unexpected_exception"} for value in diagnostics)
        or not isinstance(focused_args, list)
        or not focused_args
        or any(type(value) is not str or not value for value in focused_args)
    ):
        raise EvidenceError("authority_invalid", 2)
    sha_fields = (
        "APPROVED_SPECIFICATION_COMMIT",
        "INTEGRATION_BASE_COMMIT",
        "RED_TASK_CONTRACT_COMMIT",
        "RED_TEST_AUTHORITY_COMMIT",
        "EVIDENCE_PROTOCOL_COMMIT",
        *(("DETERMINISTIC_RED_PIN_COMMIT", "RED_EVIDENCE_COMMIT") if phase == "candidate" else ()),
    )
    digest_fields = (
        "CASE_REGISTRY_SHA256",
        "ORDERED_NODEIDS_SHA256",
        "BEHAVIORAL_HARNESS_TREE_SHA256",
        "EVIDENCE_PROTOCOL_TREE_SHA256",
        *(("RED_EVIDENCE_ARTIFACT_SHA256",) if phase == "candidate" else ()),
    )
    if not all(_sha(parsed[key]) for key in sha_fields):
        raise EvidenceError("authority_invalid", 2)
    if not all(
        isinstance(parsed[key], str) and len(parsed[key]) == 64 and not (set(parsed[key]) - HEX)
        for key in digest_fields
    ):
        raise EvidenceError("authority_invalid", 2)
    return parsed, raw


def _validate_authority(
    root: Path,
    phase: str,
    pins: dict[str, Any],
    commit: str,
    path: str,
) -> dict[str, str | None]:
    head = _head(root)
    if commit != head:
        raise EvidenceError("commit_mismatch", 2)
    if pins["INTEGRATION_BASE_COMMIT"] != INTEGRATION_BASE:
        raise EvidenceError("authority_invalid", 2)
    spec_path = "docs/feature-design-postgres-mssql-r1-v3-provider-binding-contract-v2.md"
    if b"- Status: APPROVED" not in _blob(root, pins["APPROVED_SPECIFICATION_COMMIT"], spec_path):
        raise EvidenceError("authority_invalid", 2)
    lineage = (
        pins["APPROVED_SPECIFICATION_COMMIT"],
        pins["RED_TASK_CONTRACT_COMMIT"],
        pins["RED_TEST_AUTHORITY_COMMIT"],
        pins["EVIDENCE_PROTOCOL_COMMIT"],
    )
    protocol_touches = [
        _git(root, "log", "-1", "--format=%H", pins["EVIDENCE_PROTOCOL_COMMIT"], "--", item).stdout.decode().strip()
        for item in PROTOCOL_TREE_PATHS
    ]
    behavior_touches = [
        _git(root, "log", "-1", "--format=%H", pins["RED_TEST_AUTHORITY_COMMIT"], "--", item).stdout.decode().strip()
        for item in BEHAVIOR_TREE_PATHS
    ]
    if (
        not _ancestor(root, INTEGRATION_BASE, lineage[0])
        or any(_parent(root, child) != parent for parent, child in zip(lineage, lineage[1:]))
        or _changed_paths(root, lineage[0], lineage[1]) != [RED_AUTHORITY]
        or _changed_paths(root, lineage[1], lineage[2]) != sorted(BEHAVIOR_TREE_PATHS)
        or _changed_paths(root, lineage[2], lineage[3]) != sorted(PROTOCOL_TREE_PATHS)
        or any(value != pins["EVIDENCE_PROTOCOL_COMMIT"] for value in protocol_touches)
        or any(value != pins["RED_TEST_AUTHORITY_COMMIT"] for value in behavior_touches)
    ):
        raise EvidenceError("authority_invalid", 2)
    last_touch = _git(root, "log", "-1", "--format=%H", "--", path).stdout.decode().strip()
    if phase == "red":
        if (
            last_touch != head
            or _parent(root, head) != pins["EVIDENCE_PROTOCOL_COMMIT"]
            or _changed_paths(root, pins["EVIDENCE_PROTOCOL_COMMIT"], head) != [RED_AUTHORITY]
        ):
            raise EvidenceError("authority_invalid", 2)
        return {
            "deterministic_red_pin_commit": head,
            "red_evidence_commit": None,
            "green_task_contract_commit": None,
            "implementation_commit": None,
        }
    red_pin, red_evidence = pins["DETERMINISTIC_RED_PIN_COMMIT"], pins["RED_EVIDENCE_COMMIT"]
    if _parent(root, red_pin) != pins["EVIDENCE_PROTOCOL_COMMIT"] or _parent(root, red_evidence) != red_pin:
        raise EvidenceError("authority_invalid", 2)
    if last_touch != _parent(root, commit) or _parent(root, last_touch) != red_evidence:
        raise EvidenceError("authority_invalid", 2)
    if tuple(pins["PRODUCTION_PATH_TUPLE"]) != PRODUCTION_PATHS:
        raise EvidenceError("authority_invalid", 2)
    changed = _git(root, "diff", "--name-only", last_touch, commit).stdout.decode().splitlines()
    if changed != sorted(PRODUCTION_PATHS):
        raise EvidenceError("authority_invalid", 2)
    red_artifact = f"test_artifacts/postgres-mssql-r1-v3/binding-v2/{red_pin}/red-binding-inventory.json"
    if (
        _changed_paths(root, pins["EVIDENCE_PROTOCOL_COMMIT"], red_pin) != [RED_AUTHORITY]
        or _changed_paths(root, red_pin, red_evidence) != [red_artifact]
        or _changed_paths(root, red_evidence, last_touch) != [GREEN_AUTHORITY]
        or hashlib.sha256(_blob(root, red_evidence, red_artifact)).hexdigest() != pins["RED_EVIDENCE_ARTIFACT_SHA256"]
    ):
        raise EvidenceError("authority_invalid", 2)
    return {
        "deterministic_red_pin_commit": red_pin,
        "red_evidence_commit": red_evidence,
        "green_task_contract_commit": last_touch,
        "implementation_commit": commit,
    }


class _Probe:
    def __init__(self, root: Path, kind: str, args: tuple[str, ...]) -> None:
        self.root, self.kind, self.args = root, kind, args
        self.nodeids: list[str] = []
        self.reports: list[dict[str, Any]] = []
        self.semantic: list[dict[str, str]] = []
        self.inputs: list[dict[str, Any]] = []
        self.exit_code = 0

    def pytest_collection_finish(self, session: Any) -> None:
        self.nodeids = [item.nodeid for item in session.items]

    def pytest_runtest_logreport(self, report: Any) -> None:
        if self.kind == "collection":
            return
        outcome = "skipped" if report.skipped else "passed" if report.passed else "failed"
        if getattr(report, "wasxfail", None):
            outcome = "xpassed" if report.passed else "xfailed"
        row = {
            "nodeid": report.nodeid,
            "phase": report.when,
            "outcome": outcome,
            "diagnostic_class": "none",
        }
        self.reports.append(row)
        if report.when != "call":
            return
        for key, value in report.user_properties:
            if key == "binding_v2_semantic_observation":
                case_id, observed = value
                self.semantic.append({"case_id": case_id, "nodeid": report.nodeid, "observed_class": observed})
            elif key == "binding_v2_input_vector":
                self.inputs.append(
                    {
                        "nodeid": report.nodeid,
                        "inputs": [{"name": name, "sha256": digest} for name, digest in value],
                    }
                )

    def pytest_exception_interact(self, node: Any, call: Any, report: Any) -> None:
        del node
        if self.kind == "collection" or call.excinfo is None:
            return
        import pytest

        diagnostic = (
            "assertion_failure"
            if issubclass(call.excinfo.type, (AssertionError, pytest.fail.Exception))
            else "unexpected_exception"
        )
        for row in reversed(self.reports):
            if row["nodeid"] == report.nodeid and row["phase"] == report.when:
                row["diagnostic_class"] = diagnostic
                break

    def pytest_sessionfinish(self, session: Any, exitstatus: int) -> None:
        del session
        self.exit_code = int(exitstatus)

    def document(self) -> dict[str, Any]:
        counts = dict.fromkeys(OUTCOME_KEYS, 0)
        for row in self.reports:
            if row["phase"] == "call":
                counts[row["outcome"]] += 1
            elif row["outcome"] == "failed":
                counts["errors"] += 1
            elif row["outcome"] in {"skipped", "xfailed", "xpassed"}:
                counts[row["outcome"]] += 1
        roots = tuple(
            (self.root / item).resolve()
            for item in (
                "src",
                "packages/dpone-native-accel/src",
                "packages/dpone-airflow-pack/src",
                "packages/apache-airflow-providers-dpone/src",
            )
        )
        origins = []
        for name, module in sorted(sys.modules.items()):
            if name != "dpone" and not name.startswith("dpone."):
                continue
            raw = getattr(module, "__file__", None)
            paths = (
                (Path(raw).resolve(),)
                if raw
                else tuple(Path(item).resolve() for item in (getattr(module, "__path__", ()) or ()))
            )
            if not paths or not all(
                any(item.is_relative_to(root) for root in roots if root.exists()) for item in paths
            ):
                raise EvidenceError("inventory_invalid", 2)
            origins.append(
                {
                    "module_name": name,
                    "checkout_relative_path": paths[0].relative_to(self.root).as_posix(),
                }
            )
        return {
            "probe_kind": self.kind,
            "pytest_args": list(self.args),
            "exit_code": self.exit_code,
            "ordered_nodeids": self.nodeids,
            "reports": self.reports,
            "semantic_observations": self.semantic,
            "input_vectors": self.inputs,
            "outcome_counts": counts,
            "import_origins": origins,
        }


def _probe_main(root: Path, kind: str, output_fd: int, args: tuple[str, ...]) -> int:
    import pytest

    os.environ.pop("PYTHONPATH", None)
    os.environ.pop("PYTHONHOME", None)
    for relative in reversed(
        (
            "src",
            "packages/dpone-native-accel/src",
            "packages/dpone-airflow-pack/src",
            "packages/apache-airflow-providers-dpone/src",
        )
    ):
        path = root / relative
        if path.exists():
            sys.path.insert(0, str(path))
    probe = _Probe(root, kind, args)
    invocation = [*args, *(("--collect-only",) if kind == "collection" else ())]
    with open(os.devnull, "w", encoding="utf-8") as sink:
        with contextlib.redirect_stdout(sink), contextlib.redirect_stderr(sink):
            pytest.main(invocation, plugins=[probe])
    with os.fdopen(output_fd, "wb", closefd=False) as output:
        output.write(_canonical(probe.document()))
        output.flush()
    return 0


def _run_probe(root: Path, kind: str, args: tuple[str, ...]) -> dict[str, Any]:
    with tempfile.TemporaryDirectory() as directory:
        output = Path(directory) / "probe.json"
        fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        try:
            command = (
                sys.executable,
                str(root / PRODUCER),
                _PRIVATE,
                kind,
                str(fd),
                json.dumps(args),
                str(root),
            )
            result = subprocess.run(
                command,
                cwd=root,
                capture_output=True,
                check=False,
                pass_fds=(fd,),
            )
        finally:
            os.close(fd)
        if result.returncode or result.stdout or result.stderr or not output.is_file():
            raise EvidenceError("inventory_invalid", 2)
        observed = _strict_json(output.read_bytes())
        if not isinstance(observed, dict):
            raise EvidenceError("inventory_invalid", 2)
        return observed


def _registry(root: Path, pins: dict[str, Any]) -> tuple[list[dict[str, Any]], bytes]:
    raw = (root / REGISTRY).read_bytes()
    document = _strict_json(raw)
    if (
        not isinstance(document, dict)
        or set(document) != {"contract_version", "cases"}
        or document["contract_version"] != "dpone-postgres-mssql-r1-v3-binding-v2-case-registry-1"
        or not isinstance(document["cases"], list)
    ):
        raise EvidenceError("inventory_invalid", 2)
    cases = document["cases"]
    case_keys = {
        "case_id",
        "coverage",
        "entrypoint",
        "earlier_phase",
        "later_phase",
        "expected_class",
        "fixture_id",
        "nodeid",
        "status",
    }
    if any(
        not isinstance(item, dict)
        or set(item) != case_keys
        or any(type(item[key]) is not str for key in case_keys - {"coverage"})
        or not isinstance(item["coverage"], list)
        or any(type(value) is not str for value in item["coverage"])
        or item["status"] not in {"constructible", "excluded"}
        or item["expected_class"] not in {"typed_rejection", "invariant_preserved"}
        for item in cases
    ):
        raise EvidenceError("inventory_invalid", 2)
    ids = [item["case_id"] for item in cases]
    if (
        len(ids) != len(cases)
        or len(ids) != len(set(ids))
        or len(cases) != pins["CASE_COUNT"]
        or hashlib.sha256(raw).hexdigest() != pins["CASE_REGISTRY_SHA256"]
    ):
        raise EvidenceError("inventory_invalid", 2)
    included = sum(item.get("status") == "constructible" for item in cases)
    excluded = sum(item.get("status") == "excluded" for item in cases)
    excluded_ids = [item.get("case_id") for item in cases if item.get("status") == "excluded"]
    if (included, excluded) != (
        pins["INCLUDED_PHASE_PAIR_COUNT"],
        pins["EXCLUDED_PHASE_PAIR_COUNT"],
    ) or excluded_ids != ["reader:03:04"]:
        raise EvidenceError("inventory_invalid", 2)
    return cases, raw


def _validate_probes(
    phase: str,
    pins: dict[str, Any],
    cases: list[dict[str, Any]],
    collection: dict[str, Any],
    behavior: dict[str, Any],
) -> tuple[list[dict[str, str]], str]:
    nodeids = collection.get("ordered_nodeids")
    if (
        collection.get("probe_kind") != "collection"
        or behavior.get("probe_kind") != "behavior"
        or collection.get("exit_code") != 0
        or collection.get("reports") != []
        or collection.get("semantic_observations") != []
        or collection.get("input_vectors") != []
        or collection.get("outcome_counts") != dict.fromkeys(OUTCOME_KEYS, 0)
        or not isinstance(nodeids, list)
        or collection.get("pytest_args") != pins["FOCUSED_PYTEST_ARGS"]
        or len(nodeids) != pins["COLLECTED_NODE_COUNT"]
        or len(nodeids) != len(set(nodeids))
        or _digest(NODEIDS_DOMAIN, nodeids) != pins["ORDERED_NODEIDS_SHA256"]
    ):
        raise EvidenceError("inventory_invalid", 2)
    for probe in (collection, behavior):
        origins = probe.get("import_origins")
        if (
            not isinstance(origins, list)
            or not origins
            or len({item.get("module_name") for item in origins if isinstance(item, dict)}) != len(origins)
            or any(
                not isinstance(item, dict)
                or set(item) != {"module_name", "checkout_relative_path"}
                or type(item["module_name"]) is not str
                or not (item["module_name"] == "dpone" or item["module_name"].startswith("dpone."))
                or type(item["checkout_relative_path"]) is not str
                or PurePosixPath(item["checkout_relative_path"]).is_absolute()
                or ".." in PurePosixPath(item["checkout_relative_path"]).parts
                for item in origins
            )
        ):
            raise EvidenceError("inventory_invalid", 2)
    if behavior.get("ordered_nodeids") != nodeids or behavior.get("pytest_args") != pins["FOCUSED_PYTEST_ARGS"]:
        raise EvidenceError("inventory_invalid", 2)
    counts = behavior.get("outcome_counts")
    reports = behavior.get("reports")
    if not isinstance(counts, dict) or set(counts) != set(OUTCOME_KEYS) or not isinstance(reports, list):
        raise EvidenceError("inventory_invalid", 2)
    if behavior.get("exit_code") != (1 if phase == "red" else 0):
        raise EvidenceError("inventory_invalid", 2)
    observed_counts = dict.fromkeys(OUTCOME_KEYS, 0)
    for row in reports:
        if (
            not isinstance(row, dict)
            or set(row) != {"nodeid", "phase", "outcome", "diagnostic_class"}
            or row["phase"] not in {"setup", "call", "teardown"}
            or row["outcome"] not in {"passed", "failed", "skipped", "xfailed", "xpassed"}
            or row["diagnostic_class"] not in {"none", "assertion_failure", "unexpected_exception"}
        ):
            raise EvidenceError("inventory_invalid", 2)
        if row["phase"] == "call":
            observed_counts[row["outcome"]] += 1
        elif row["outcome"] == "failed":
            observed_counts["errors"] += 1
        elif row["outcome"] in {"skipped", "xfailed", "xpassed"}:
            observed_counts[row["outcome"]] += 1
    if counts != observed_counts:
        raise EvidenceError("inventory_invalid", 2)
    report_keys = [(row["nodeid"], row["phase"]) for row in reports]
    if (
        len(report_keys) != len(nodeids) * 3
        or len(report_keys) != len(set(report_keys))
        or set(report_keys) != {(nodeid, when) for nodeid in nodeids for when in ("setup", "call", "teardown")}
    ):
        raise EvidenceError("inventory_invalid", 2)
    call_rows = [row for row in reports if isinstance(row, dict) and row.get("phase") == "call"]
    calls = {row.get("nodeid"): row for row in call_rows}
    if len(calls) != len(call_rows) or set(calls) != set(nodeids):
        raise EvidenceError("inventory_invalid", 2)
    failing = [nodeid for nodeid in nodeids if calls.get(nodeid, {}).get("outcome") == "failed"]
    diagnostics = [calls[nodeid]["diagnostic_class"] for nodeid in failing]
    if phase == "red":
        if (
            counts != pins["EXPECTED_RED_OUTCOME_COUNTS"]
            or failing != pins["EXPECTED_RED_FAILING_NODEIDS"]
            or diagnostics != pins["EXPECTED_RED_DIAGNOSTIC_CLASSES"]
            or not failing
            or any(counts[key] for key in ("skipped", "xfailed", "xpassed", "errors"))
        ):
            raise EvidenceError("inventory_invalid", 2)
    elif any(counts[key] for key in ("failed", "skipped", "xfailed", "xpassed", "errors")) or counts["passed"] != len(
        nodeids
    ):
        raise EvidenceError("required_case_not_pass", 1)
    observation_rows = behavior.get("semantic_observations")
    vector_items = behavior.get("input_vectors")
    if not isinstance(observation_rows, list) or not isinstance(vector_items, list):
        raise EvidenceError("inventory_invalid", 2)
    if any(
        not isinstance(item, dict)
        or set(item) != {"case_id", "nodeid", "observed_class"}
        or any(type(item[key]) is not str for key in item)
        for item in observation_rows
    ):
        raise EvidenceError("inventory_invalid", 2)
    observations = {item["case_id"]: item for item in observation_rows}
    if len(observations) != len(observation_rows):
        raise EvidenceError("inventory_invalid", 2)
    vectors: dict[str, list[dict[str, str]]] = {}
    for item in vector_items:
        if (
            not isinstance(item, dict)
            or set(item) != {"nodeid", "inputs"}
            or type(item["nodeid"]) is not str
            or not isinstance(item["inputs"], list)
            or item["nodeid"] in vectors
            or not item["inputs"]
        ):
            raise EvidenceError("inventory_invalid", 2)
        names: set[str] = set()
        for value in item["inputs"]:
            if (
                not isinstance(value, dict)
                or set(value) != {"name", "sha256"}
                or type(value["name"]) is not str
                or not value["name"]
                or value["name"] in names
                or type(value["sha256"]) is not str
                or len(value["sha256"]) != 64
                or set(value["sha256"]) - HEX
            ):
                raise EvidenceError("inventory_invalid", 2)
            names.add(value["name"])
        vectors[item["nodeid"]] = item["inputs"]
    case_ids = {case["case_id"] for case in cases}
    if set(observations) - case_ids or set(vectors) != {case["nodeid"] for case in cases}:
        raise EvidenceError("inventory_invalid", 2)
    results: list[dict[str, str]] = []
    vector_rows = []
    for case in cases:
        nodeid, case_id = case["nodeid"], case["case_id"]
        call = calls.get(nodeid)
        if call is None or nodeid not in vectors:
            raise EvidenceError("inventory_invalid", 2)
        observed = observations.get(case_id, {}).get("observed_class", "not_observed")
        if case_id in observations and observations[case_id]["nodeid"] != nodeid:
            raise EvidenceError("inventory_invalid", 2)
        status = "PASS" if call["outcome"] == "passed" and observed == case["expected_class"] else "FAIL"
        if phase == "candidate" and status != "PASS":
            raise EvidenceError("required_case_not_pass", 1)
        results.append(
            {
                "case_id": case_id,
                "nodeid": nodeid,
                "status": status,
                "observed_class": observed,
            }
        )
        vector_rows.append({"case_id": case_id, "inputs": vectors[nodeid]})
    return results, _digest(INPUTS_DOMAIN, vector_rows)


@dataclass(frozen=True)
class _Fs:
    open: Any = os.open
    write: Any = os.write
    fsync: Any = os.fsync
    close: Any = os.close
    link: Any = os.link
    unlink: Any = os.unlink


def _safe_parent(root: Path, relative: PurePosixPath) -> Path:
    cursor = root
    for part in relative.parent.parts:
        cursor /= part
        if cursor.is_symlink() or (cursor.exists() and not cursor.is_dir()):
            raise EvidenceError("unsafe_output_path", 3)
        if not cursor.exists():
            try:
                cursor.mkdir(mode=0o700)
            except FileExistsError:
                if cursor.is_symlink() or not cursor.is_dir():
                    raise EvidenceError("unsafe_output_path", 3) from None
            except OSError as exc:
                raise EvidenceError("filesystem_error", 3) from exc
    return cursor


def _publish(root: Path, relative: PurePosixPath, payload: bytes, fs: _Fs = _Fs()) -> None:
    parent = _safe_parent(root, relative)
    final = root / relative
    if final.exists() or final.is_symlink():
        raise EvidenceError("output_conflict", 3)
    temp = parent / f".{relative.name}.tmp-{os.getpid()}-{os.urandom(8).hex()}"
    fd: int | None = None
    linked = False
    try:
        fd = fs.open(temp, os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0), 0o600)
        view = memoryview(payload)
        while view:
            written = fs.write(fd, view)
            if written <= 0:
                raise OSError("short write")
            view = view[written:]
        fs.fsync(fd)
        fs.close(fd)
        fd = None
        fs.link(temp, final, follow_symlinks=False)
        linked = True
        for remove_temp in (False, True):
            directory_fd = os.open(parent, os.O_RDONLY)
            try:
                fs.fsync(directory_fd)
            finally:
                os.close(directory_fd)
            if not remove_temp:
                fs.unlink(temp)
    except FileExistsError as exc:
        raise EvidenceError("output_conflict", 3) from exc
    except EvidenceError:
        raise
    except OSError as exc:
        if linked:
            try:
                fs.unlink(final)
                directory_fd = os.open(parent, os.O_RDONLY)
                try:
                    fs.fsync(directory_fd)
                finally:
                    os.close(directory_fd)
            except OSError:
                pass
            raise EvidenceError("artifact_outcome_unknown", 3) from exc
        raise EvidenceError("filesystem_error", 3) from exc
    finally:
        if fd is not None:
            try:
                fs.close(fd)
            except OSError:
                pass
        if temp.exists() or temp.is_symlink():
            try:
                fs.unlink(temp)
            except OSError:
                pass


def _document(
    root: Path,
    phase: str,
    authority: str,
    commit: str,
) -> tuple[dict[str, Any], PurePosixPath]:
    if _git(root, "status", "--porcelain=v1", "--untracked-files=all").stdout:
        raise EvidenceError("worktree_dirty", 2)
    pins, authority_raw = _load_authority(root, phase, authority)
    commits = _validate_authority(root, phase, pins, commit, authority)
    cases, registry_raw = _registry(root, pins)
    expected_trees = (
        (
            pins["BEHAVIORAL_HARNESS_TREE_SHA256"],
            _tree_digest(
                root,
                commit,
                BEHAVIOR_DOMAIN,
                BEHAVIOR_TREE_PATHS,
            ),
        ),
        (
            pins["EVIDENCE_PROTOCOL_TREE_SHA256"],
            _tree_digest(
                root,
                pins["EVIDENCE_PROTOCOL_COMMIT"],
                PROTOCOL_DOMAIN,
                PROTOCOL_TREE_PATHS,
            ),
        ),
    )
    if any(expected != observed for expected, observed in expected_trees):
        raise EvidenceError("authority_invalid", 2)
    args = tuple(pins["FOCUSED_PYTEST_ARGS"])
    collection = _run_probe(root, "collection", args)
    behavior = _run_probe(root, "behavior", args)
    results, input_digest = _validate_probes(phase, pins, cases, collection, behavior)
    filename = "red-binding-inventory.json" if phase == "red" else "binding-inventory.json"
    relative = PurePosixPath(
        f"test_artifacts/postgres-mssql-r1-v3/binding-v2/{commit}/{filename}",
    )
    document = {
        "record_kind": "binding_contract_evidence",
        "producer_contract_version": CONTRACT_VERSION,
        "phase": phase,
        "subject_commit": commit,
        "task_authority_path": authority,
        "task_authority_sha256": hashlib.sha256(authority_raw).hexdigest(),
        "approved_specification_commit": pins["APPROVED_SPECIFICATION_COMMIT"],
        "integration_base_commit": pins["INTEGRATION_BASE_COMMIT"],
        "red_task_contract_commit": pins["RED_TASK_CONTRACT_COMMIT"],
        "red_test_authority_commit": pins["RED_TEST_AUTHORITY_COMMIT"],
        "evidence_protocol_commit": pins["EVIDENCE_PROTOCOL_COMMIT"],
        **commits,
        "final_evidence_commit": None,
        "producer_sha256": hashlib.sha256(
            _blob(root, pins["EVIDENCE_PROTOCOL_COMMIT"], PRODUCER),
        ).hexdigest(),
        "schema_sha256": hashlib.sha256(
            _blob(root, pins["EVIDENCE_PROTOCOL_COMMIT"], SCHEMA),
        ).hexdigest(),
        "registry_sha256": hashlib.sha256(registry_raw).hexdigest(),
        "ordered_nodeids_sha256": pins["ORDERED_NODEIDS_SHA256"],
        "behavioral_harness_tree_sha256": pins["BEHAVIORAL_HARNESS_TREE_SHA256"],
        "evidence_protocol_tree_sha256": pins["EVIDENCE_PROTOCOL_TREE_SHA256"],
        "input_vector_sha256": input_digest,
        "collection_probe": collection,
        "behavior_probe": behavior,
        "case_results": results,
        "behavioral_status": "RED" if phase == "red" else "PASS",
        "certification_status": "UNVERIFIED",
        "adapter_layer": "pure",
        "red_evidence_artifact_sha256": (pins["RED_EVIDENCE_ARTIFACT_SHA256"] if phase == "candidate" else None),
        "production_path_tuple": (pins["PRODUCTION_PATH_TUPLE"] if phase == "candidate" else None),
    }
    return document, relative


def _validate_schema(root: Path, document: dict[str, Any]) -> None:
    try:
        import jsonschema

        schema = _strict_json((root / SCHEMA).read_bytes(), "schema_invalid")
        jsonschema.Draft202012Validator.check_schema(schema)
        jsonschema.Draft202012Validator(schema).validate(document)
    except EvidenceError:
        raise
    except Exception as exc:
        raise EvidenceError("schema_invalid", 2) from exc


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if args and args[0] == _PRIVATE:
        if len(args) != 5:
            raise EvidenceError("inventory_invalid", 2)
        _, kind, output_fd, encoded_args, root_arg = args
        observed = _strict_json(encoded_args.encode())
        if (
            kind not in {"collection", "behavior"}
            or not isinstance(observed, list)
            or any(type(item) is not str for item in observed)
        ):
            raise EvidenceError("inventory_invalid", 2)
        try:
            descriptor = int(output_fd)
        except ValueError as exc:
            raise EvidenceError("inventory_invalid", 2) from exc
        return _probe_main(Path(root_arg), kind, descriptor, tuple(observed))
    parser = _Parser()
    parser.add_argument("--phase", choices=("red", "candidate"), required=True)
    parser.add_argument("--authority", required=True)
    parser.add_argument("--commit", required=True)
    parser.add_argument("--output", required=True)
    parsed = parser.parse_args(args)
    root = Path.cwd().resolve()
    document, expected = _document(root, parsed.phase, parsed.authority, parsed.commit)
    supplied = PurePosixPath(parsed.output)
    if parsed.output != supplied.as_posix() or supplied.is_absolute() or ".." in supplied.parts or supplied != expected:
        raise EvidenceError("unsafe_output_path", 3)
    _validate_schema(root, document)
    _publish(root, supplied, _canonical(document))
    sys.stdout.buffer.write(_canonical({"artifact_path": supplied.as_posix(), "status": "created"}))
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except EvidenceError as exc:
        sys.stderr.buffer.write(_canonical({"reason": exc.reason, "status": "error"}))
        raise SystemExit(exc.exit_code) from None
