from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path

import yaml
from jsonschema import Draft202012Validator

ROOT = Path(__file__).resolve().parents[1]
MANIFEST = ROOT / ".agents/policy/ci-shadow-bundle-v1.yml"
SCHEMA = ROOT / "docs/schemas/cicd/ci-shadow-bundle-v1.schema.json"


def test_bundle_manifest_is_closed_and_lists_the_complete_trusted_audit_core() -> None:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    Draft202012Validator(json.loads(SCHEMA.read_text(encoding="utf-8"))).validate(manifest)

    entries = {entry["path"]: entry["role"] for entry in manifest["files"]}
    assert entries[".github/workflows/pr-gate-shadow.yml"] == "EXECUTION_WORKFLOW"
    assert entries[".github/workflows/pr-gate-shadow-audit.yml"] == "TRUST_CORE"
    assert entries["src/dpone/services/ci/shadow_audit.py"] == "TRUST_CORE"
    assert all((ROOT / path).is_file() for path in entries)
    assert all(
        hashlib.sha256((ROOT / path).read_bytes()).hexdigest() == manifest_entry["sha256"]
        for manifest_entry in manifest["files"]
        for path in (manifest_entry["path"],)
    )


def test_trusted_python_core_has_no_dynamic_import_or_execution_escape_hatch() -> None:
    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    trusted_python = [
        ROOT / item["path"]
        for item in manifest["files"]
        if item["role"] == "TRUST_CORE" and item["path"].endswith(".py")
    ]
    forbidden_calls = {"__import__", "eval", "exec"}

    for path in trusted_python:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call) and isinstance(node.func, ast.Name):
                assert node.func.id not in forbidden_calls, path


def test_bundle_declares_every_local_dpone_import_of_its_trusted_python_core() -> None:
    """Keep the approved control-plane closure explicit and reviewable."""

    manifest = yaml.safe_load(MANIFEST.read_text(encoding="utf-8"))
    declared = {item["path"] for item in manifest["files"]}
    trusted_python = [
        ROOT / item["path"]
        for item in manifest["files"]
        if item["role"] == "TRUST_CORE" and item["path"].endswith(".py")
    ]

    for path in trusted_python:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if not isinstance(node, ast.ImportFrom) or node.level != 0 or not node.module:
                continue
            if not node.module.startswith("dpone."):
                continue
            module_path = Path("src", *node.module.split(".")).with_suffix(".py")
            assert module_path.as_posix() in declared, (path, module_path)
