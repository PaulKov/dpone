from __future__ import annotations

import importlib.util
import json
import sys
from copy import deepcopy
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from jsonschema import Draft202012Validator
from tests.agent_policy.test_release_merge_receipt_gate import (
    Adapter,
    _archive,
    _artifact,
    _check,
    _receipt,
    _run,
    _verify,
    gate,
)

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


contract = _load(
    "dpone_agent_pr_merge_receipt_contract_test",
    "tools/agent_policy/pr_merge_receipt_contract.py",
)
schema = json.loads((ROOT / "evals/agent/pr-merge-receipt.schema.json").read_text(encoding="utf-8"))
schema_validator = Draft202012Validator(schema)


def _contract_accepts(payload: dict[str, Any]) -> bool:
    try:
        contract.validate_receipt_payload(payload)
    except ValueError:
        return False
    return True


def _failure_receipt() -> dict[str, Any]:
    payload = _receipt(b"source")
    payload.update(
        {
            "status": "FAIL",
            "binding_id": None,
            "protected_base_ref": None,
            "pr_number": None,
            "merged_at": None,
            "integration_method": None,
            "reviewed_head_sha": None,
            "reviewed_head_tree": None,
            "base_parent_sha": None,
            "integration_commit_sha": None,
            "integration_tree": None,
            "changed_paths": [],
            "pr_body_sha256": None,
            "source_receipt": None,
            "errors": ["closed failure"],
        }
    )
    return payload


def _contract_matrix() -> list[tuple[str, dict[str, Any]]]:
    valid = _receipt(b"source")
    cases = [("valid-pass", valid), ("valid-fail", _failure_receipt())]
    for field in contract.RECEIPT_FIELDS:
        candidate = deepcopy(valid)
        candidate.pop(field)
        cases.append((f"missing-receipt-{field}", candidate))
    extra = deepcopy(valid)
    extra["unreviewed_authority"] = True
    cases.append(("extra-receipt-field", extra))

    source = valid["source_receipt"]
    assert isinstance(source, dict)
    for field in contract.SOURCE_RECEIPT_FIELDS:
        candidate = deepcopy(valid)
        candidate["source_receipt"].pop(field)
        cases.append((f"missing-source-{field}", candidate))
    extra_source = deepcopy(valid)
    extra_source["source_receipt"]["unreviewed_authority"] = True
    cases.append(("extra-source-field", extra_source))

    for field in contract.PRODUCER_FIELDS:
        candidate = deepcopy(valid)
        candidate["producer"].pop(field)
        cases.append((f"missing-producer-{field}", candidate))
    extra_producer = deepcopy(valid)
    extra_producer["producer"]["actor"] = "unreviewed"
    cases.append(("extra-producer-field", extra_producer))

    mutations = {
        "status-type": ("status", {}),
        "integration-method-type": ("integration_method", []),
        "pr-number-bool": ("pr_number", True),
        "duplicate-path": ("changed_paths", ["a", "a"]),
        "pass-errors": ("errors", ["unexpected"]),
        "invalid-binding": ("binding_id", "sha256:bad"),
    }
    for name, (field, value) in mutations.items():
        candidate = deepcopy(valid)
        candidate[field] = value
        cases.append((name, candidate))
    empty_failure = _failure_receipt()
    empty_failure["errors"] = []
    cases.append(("fail-without-error", empty_failure))
    return cases


def test_dependency_free_contract_matches_published_json_schema_matrix() -> None:
    for case, payload in _contract_matrix():
        expected = schema_validator.is_valid(payload)
        assert _contract_accepts(payload) is expected, case


@pytest.mark.parametrize(
    "missing_field",
    [
        "schema_version",
        "protected_base_ref",
        "pr_number",
        "merged_at",
        "integration_method",
        "reviewed_head_tree",
        "base_parent_sha",
        "integration_tree",
        "changed_paths",
        "pr_body_sha256",
        "errors",
        "warnings",
    ],
)
def test_release_consumer_rejects_missing_fields_even_with_rebound_digest(
    missing_field: str,
) -> None:
    source = b"source"
    receipt = _receipt(source)
    receipt.pop(missing_field)
    receipt["binding_id"] = gate.merge_binding.compute_binding_id(receipt)
    raw = _archive(receipt=receipt, source=source)
    adapter = Adapter([_check()], _run(), [_artifact(raw)], raw)

    with pytest.raises(ValueError, match="closed schema"):
        _verify(adapter)


def test_release_consumer_rejects_unknown_fields() -> None:
    source = b"source"
    receipt = _receipt(source)
    receipt["unreviewed_authority"] = "accepted"
    raw = _archive(receipt=receipt, source=source)
    adapter = Adapter([_check()], _run(), [_artifact(raw)], raw)

    with pytest.raises(ValueError, match="closed schema"):
        _verify(adapter)
