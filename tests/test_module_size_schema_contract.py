from __future__ import annotations

import json
from collections.abc import Callable
from copy import deepcopy
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator

from dpone.metrics.module_size_baseline import ModuleSizeBaselineError, decode_module_size_baseline


def test_module_size_baseline_v2_matches_published_schema() -> None:
    schema = json.loads(Path("docs/schemas/quality/module-size-baseline-v2.schema.json").read_text(encoding="utf-8"))
    payload = json.loads(Path("docs/module_size_baseline.json").read_text(encoding="utf-8"))

    Draft202012Validator.check_schema(schema)
    errors = tuple(
        Draft202012Validator(schema, format_checker=Draft202012Validator.FORMAT_CHECKER).iter_errors(payload)
    )

    assert errors == ()


def test_module_size_baseline_v2_schema_rejects_unknown_fields() -> None:
    schema = json.loads(Path("docs/schemas/quality/module-size-baseline-v2.schema.json").read_text(encoding="utf-8"))
    payload = json.loads(Path("docs/module_size_baseline.json").read_text(encoding="utf-8"))
    first_path = next(iter(payload["debt"]))
    payload["debt"][first_path]["headroom"] = 1

    errors = tuple(Draft202012Validator(schema).iter_errors(payload))

    assert any("Additional properties are not allowed" in error.message for error in errors)


@pytest.mark.parametrize(
    ("mutate", "runtime_match"),
    [
        (lambda payload, path: payload["debt"].__setitem__(f"./{path}", payload["debt"].pop(path)), "canonical"),
        (
            lambda payload, path: payload["debt"].__setitem__("src/dpone/./unsafe.py", payload["debt"].pop(path)),
            "canonical",
        ),
        (
            lambda payload, path: payload["debt"].__setitem__("src/dpone//unsafe.py", payload["debt"].pop(path)),
            "canonical",
        ),
        (
            lambda payload, path: payload["debt"][path].__setitem__("accepted_adr", "docs/adr/./unsafe.md"),
            "confined docs/adr",
        ),
        (lambda payload, path: payload["debt"].__setitem__(f"{path}\n", payload["debt"].pop(path)), "control"),
        (
            lambda payload, path: payload["debt"][path].__setitem__(
                "accepted_adr", "docs/adr/0047-module-size-debt-ratchet.md\n"
            ),
            "control",
        ),
        (lambda payload, path: payload["debt"][path].__setitem__("owner", "core\nplatform"), "control"),
        (lambda payload, path: payload["debt"][path].__setitem__("reason", "legacy\tdebt"), "control"),
        (lambda payload, path: payload["debt"][path].__setitem__("reason", "legacy\u0000debt"), "control"),
    ],
)
def test_published_schema_and_runtime_reject_same_invalid_baselines(
    mutate: Callable[[dict[str, Any], str], object], runtime_match: str
) -> None:
    schema = json.loads(Path("docs/schemas/quality/module-size-baseline-v2.schema.json").read_text(encoding="utf-8"))
    payload = deepcopy(json.loads(Path("docs/module_size_baseline.json").read_text(encoding="utf-8")))
    first_path = next(iter(payload["debt"]))
    mutate(payload, first_path)

    assert tuple(Draft202012Validator(schema).iter_errors(payload))
    with pytest.raises(ModuleSizeBaselineError, match=runtime_match):
        decode_module_size_baseline(json.dumps(payload).encode(), source="test")
