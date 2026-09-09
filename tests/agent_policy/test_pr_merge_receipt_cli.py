from __future__ import annotations

import importlib.util
import sys
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[2]


def _load(name: str, relative: str) -> ModuleType:
    spec = importlib.util.spec_from_file_location(name, ROOT / relative)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


merge_receipt = _load(
    "dpone_agent_pr_merge_receipt_help_test",
    "tools/agent_policy/pr_merge_receipt.py",
)


def test_help_explains_authority_outputs_defaults_and_exit_codes(
    capsys: Any,
) -> None:
    with pytest.raises(SystemExit) as raised:
        merge_receipt.main(["--help"])

    help_text = capsys.readouterr().out
    assert raised.value.code == 0
    assert "Immutable pull_request closed-event JSON" in help_text
    assert "default: GITHUB_TOKEN" in help_text
    assert "Exit 0 writes a PASS receipt" in help_text
    assert "Exit 1" in help_text
    assert "--integration-sha" not in help_text
