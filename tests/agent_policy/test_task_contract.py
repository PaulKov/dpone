from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path
from types import ModuleType

import yaml

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


task_contract = _load("dpone_agent_task_contract", "tools/agent_policy/task_contract.py")


def _valid_contract() -> dict:
    return {
        "schema_version": 1,
        "task_id": "DPONE-AGENT-001",
        "title": "Add task contract validation",
        "goal": "Validate agent task boundaries before implementation.",
        "specification": "docs/feature-design-agent-task-contract-gate.md",
        "base_commit": "86baa376",
        "integrator": "main_codex",
        "acceptance_criteria": ["Malformed ownership contracts fail validation."],
        "public_contract_impact": {
            "level": "compatible",
            "surfaces": ["agent governance receipt"],
            "migration_required": False,
        },
        "owned_paths": ["tools/agent_policy/task_contract.py", "tests/agent_policy/test_task_contract.py"],
        "read_only_paths": ["docs/agent-development.md"],
        "forbidden_paths": ["pyproject.toml", "uv.lock", "CHANGELOG.md", "mkdocs.yml", ".github/workflows"],
        "shared_file_owner": "integrator",
        "dependencies": [],
        "required_checks": {
            "focused": ["uv run pytest tests/agent_policy/test_task_contract.py -q"],
            "broad": ["uv run pytest tests/agent_policy tests/test_module_size_gate.py -q"],
            "live": [],
        },
        "required_outputs": ["implementation", "tests", "documentation_impact", "completion_report"],
        "completion_statuses": {"allowed": ["PASS", "FAIL", "SKIP", "N/A", "UNVERIFIED"]},
        "stop_conditions": [
            "Required edit falls outside owned_paths.",
            "Approved specification is missing or contradicted.",
            "Public-contract impact is larger than declared.",
            "A live check requires unapproved credentials or environment.",
            "Another writer owns the same semantic contract.",
        ],
    }


def test_task_contract_template_is_valid_in_template_mode() -> None:
    result = task_contract.validate_file(ROOT / "docs/agent-templates/agent-task-contract.yml", template=True)

    assert result.errors == []


def test_valid_concrete_task_contract_passes() -> None:
    result = task_contract.validate_payload(_valid_contract(), label="contract.yml")

    assert result.errors == []


def test_root_integrator_may_own_explicit_forbidden_shared_paths() -> None:
    payload = _valid_contract()
    payload["integrator"] = payload["shared_file_owner"] = "/root"
    payload["integrator_owned_paths"] = ["mkdocs.yml", ".github/workflows/ci.yml"]

    result = task_contract.validate_payload(payload, label="contract.yml")

    assert result.errors == []


def test_integrator_owned_paths_require_matching_owner_and_forbidden_coverage() -> None:
    mismatched_owner = _valid_contract()
    mismatched_owner["integrator_owned_paths"] = ["mkdocs.yml"]
    unprotected = _valid_contract()
    unprotected["integrator"] = unprotected["shared_file_owner"] = "/root"
    unprotected["integrator_owned_paths"] = ["docs/quality-metrics.md"]

    owner_result = task_contract.validate_payload(mismatched_owner, label="owner.yml")
    coverage_result = task_contract.validate_payload(unprotected, label="coverage.yml")

    assert any("integrator == shared_file_owner" in error for error in owner_result.errors)
    assert any("must remain covered by forbidden_paths" in error for error in coverage_result.errors)


def test_task_contract_validator_rejects_fields_outside_closed_schema() -> None:
    payload = _valid_contract()
    payload["invented_ownership_escape"] = []

    result = task_contract.validate_payload(payload, label="contract.yml")

    assert any("schema <root>" in error and "Additional properties" in error for error in result.errors)


def test_concrete_task_contract_rejects_empty_placeholders() -> None:
    payload = _valid_contract()
    payload["goal"] = ""

    result = task_contract.validate_payload(payload, label="contract.yml")

    assert any("goal must not be empty" in error for error in result.errors)


def test_task_contract_rejects_owned_forbidden_overlap() -> None:
    payload = _valid_contract()
    payload["owned_paths"].append(".github/workflows/ci.yml")

    result = task_contract.validate_payload(payload, label="contract.yml")

    assert any("owned_paths overlaps forbidden_paths" in error for error in result.errors)


def test_task_contract_requires_standard_forbidden_paths() -> None:
    payload = _valid_contract()
    payload["forbidden_paths"].remove("uv.lock")

    result = task_contract.validate_payload(payload, label="contract.yml")

    assert any("forbidden_paths must include" in error for error in result.errors)


def test_agent_task_contract_schema_is_closed_json_contract() -> None:
    data = json.loads((ROOT / "evals/agent/agent-task-contract.schema.json").read_text(encoding="utf-8"))

    assert data["type"] == "object"
    assert data["additionalProperties"] is False
    assert "owned_paths" in data["required"]
    assert data["properties"]["integrator_owned_paths"]["uniqueItems"] is True


def test_task_contract_cli_reports_json(tmp_path: Path, capsys) -> None:
    contract = tmp_path / "contract.yml"
    contract.write_text(yaml.safe_dump(_valid_contract()), encoding="utf-8")

    code = task_contract.main([str(contract), "--format", "json"])
    payload = json.loads(capsys.readouterr().out)

    assert code == 0
    assert payload["status"] == "passed"


def test_task_contract_cli_reports_malformed_canonical_schema_without_traceback(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    contract = tmp_path / "contract.yml"
    contract.write_text(yaml.safe_dump(_valid_contract()), encoding="utf-8")
    malformed_schema = tmp_path / "task-contract.schema.json"
    malformed_schema.write_text(json.dumps({"type": 7}), encoding="utf-8")
    monkeypatch.setattr(task_contract, "TASK_CONTRACT_SCHEMA", malformed_schema)

    code = task_contract.main([str(contract), "--format", "json"])
    captured = capsys.readouterr()
    payload = json.loads(captured.out)

    assert code == 1
    assert captured.err == ""
    assert payload["status"] == "failed"
    assert len(payload["errors"]) == 1
    assert "cannot load task contract schema" in payload["errors"][0]
    assert "Traceback" not in captured.out


def test_task_contract_cli_reports_unresolvable_schema_in_both_output_modes(
    tmp_path: Path,
    monkeypatch,
    capsys,
) -> None:
    contract = tmp_path / "contract.yml"
    contract.write_text(yaml.safe_dump(_valid_contract()), encoding="utf-8")
    unresolved_schema = tmp_path / "task-contract.schema.json"
    unresolved_schema.write_text(
        json.dumps(
            {
                "$schema": "https://json-schema.org/draft/2020-12/schema",
                "$ref": "urn:dpone:missing",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setattr(task_contract, "TASK_CONTRACT_SCHEMA", unresolved_schema)

    json_code = task_contract.main([str(contract), "--format", "json"])
    json_output = capsys.readouterr()
    json_payload = json.loads(json_output.out)
    text_code = task_contract.main([str(contract), "--format", "text"])
    text_output = capsys.readouterr()

    assert json_code == text_code == 1
    assert json_output.err == text_output.err == ""
    assert json_payload["status"] == "failed"
    assert len(json_payload["errors"]) == 1
    assert "cannot execute task contract schema" in json_payload["errors"][0]
    assert "ERROR:" in text_output.out
    assert "cannot execute task contract schema" in text_output.out
    assert "Agent task contract validation: FAILED (1 errors, 0 warnings)" in text_output.out
    assert "Traceback" not in json_output.out + text_output.out
