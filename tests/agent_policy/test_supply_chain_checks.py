from __future__ import annotations

import importlib.util
import sys
from pathlib import Path


def _load_module() -> object:
    root = Path(__file__).resolve().parents[2]
    path = root / "tools/agent_policy/supply_chain_checks.py"
    spec = importlib.util.spec_from_file_location("dpone_supply_chain_checks_test", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _workflow() -> str:
    return """
jobs:
  attest:
    steps:
      - uses: actions/attest@0123456789abcdef
        with:
          subject-path: |
            dist/*.whl
            dist/*.tar.gz
      - run: |
          receipt="test_artifacts/supply-chain/github-attestations/package.github-attestation.json"
          gh attestation verify "$artifact" \
            --source-ref "$GITHUB_REF" \
            --source-digest "${RELEASE_COMMIT}" > "$receipt"
  publish:
    name: Authorize controller publication handoff
    needs:
      - attest
"""


def test_release_attestation_check_accepts_current_exact_commit_contract(tmp_path: Path) -> None:
    module = _load_module()
    workflow = tmp_path / "release.yml"
    workflow.write_text(_workflow(), encoding="utf-8")

    result = module.check_release_attestations(tmp_path, release_workflow="release.yml")

    assert result.status == "PASS"


def test_release_attestation_check_rejects_missing_exact_commit_binding(tmp_path: Path) -> None:
    module = _load_module()
    workflow = tmp_path / "release.yml"
    workflow.write_text(
        _workflow().replace('--source-digest "${RELEASE_COMMIT}"', '--source-digest "$GITHUB_SHA"'),
        encoding="utf-8",
    )

    result = module.check_release_attestations(tmp_path, release_workflow="release.yml")

    assert result.status == "FAIL"
    assert '--source-digest "${RELEASE_COMMIT}"' in result.details


def test_release_attestation_check_rejects_publish_without_attest_dependency(tmp_path: Path) -> None:
    module = _load_module()
    workflow = tmp_path / "release.yml"
    workflow.write_text(_workflow().replace("      - attest", "      - build"), encoding="utf-8")

    result = module.check_release_attestations(tmp_path, release_workflow="release.yml")

    assert result.status == "FAIL"
    assert "- attest" in result.details
