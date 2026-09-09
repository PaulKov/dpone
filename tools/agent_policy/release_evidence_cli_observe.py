"""Observe-only CLI runners for Shape B publication inventory commands."""

from __future__ import annotations

import importlib.util
import json
import sys
from argparse import Namespace
from pathlib import Path
from typing import Any


def _load_sibling(module_name: str, filename: str) -> Any:
    if module_name in sys.modules:
        return sys.modules[module_name]
    path = Path(__file__).with_name(filename)
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load {filename}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    return module


support = _load_sibling("dpone_agent_release_evidence_cli_support", "release_evidence_cli_support.py")
github = _load_sibling("dpone_agent_release_github_api", "release_github_api.py")
pypi_inv = _load_sibling("dpone_agent_release_pypi_inventory", "release_pypi_inventory.py")
immutable_inv = _load_sibling("dpone_agent_release_immutable_inventory", "release_immutable_inventory.py")
tp_inv = _load_sibling(
    "dpone_agent_release_trusted_publisher_inventory",
    "release_trusted_publisher_inventory.py",
)
draft_inv = _load_sibling("dpone_agent_release_draft_inventory", "release_draft_inventory.py")


def run_pypi_inventory_observe(store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str) -> int:
    expected: list[dict[str, Any]] = []
    if getattr(args, "expected_json", None):
        expected = json.loads(Path(args.expected_json).read_text(encoding="utf-8"))
        if not isinstance(expected, list):
            print(json.dumps({"status": "USAGE", "error": "expected_json must be a list"}, sort_keys=True))
            return 2
    try:
        result = pypi_inv.run_pypi_inventory_observe(
            store,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            producer=prod,
            now_utc=now,
            expected_distributions=expected,
            retention_days=args.retention_days,
            index_url=args.index_url,
        )
    except pypi_inv.InventoryError as exc:
        print(json.dumps({"status": "INVENTORY_ERROR", "error": str(exc)}, sort_keys=True))
        return 6
    except pypi_inv.StreamPrerequisiteError as exc:
        print(json.dumps({"status": "PREREQUISITE", "error": str(exc)}, sort_keys=True))
        return 4
    except RuntimeError as exc:
        print(json.dumps({"status": "STORE_ERROR", "error": str(exc)}, sort_keys=True))
        return 7
    print(json.dumps(result, sort_keys=True))
    return 0


def run_immutable_inventory_observe(
    store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str
) -> int:
    api = github.GitHubApi(token=support.require_env(args.github_token_env))
    try:
        result = immutable_inv.run_immutable_inventory(
            store,
            api,
            owner=args.owner,
            repo=args.repo,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            producer=prod,
            now_utc=now,
            retention_days=args.retention_days,
        )
    except immutable_inv.InventoryError as exc:
        print(json.dumps({"status": "INVENTORY_ERROR", "error": str(exc)}, sort_keys=True))
        return 6
    except immutable_inv.StreamPrerequisiteError as exc:
        print(json.dumps({"status": "PREREQUISITE", "error": str(exc)}, sort_keys=True))
        return 4
    except RuntimeError as exc:
        print(json.dumps({"status": "STORE_ERROR", "error": str(exc)}, sort_keys=True))
        return 7
    print(json.dumps(result, sort_keys=True))
    return 0


def run_trusted_publisher_inventory_observe(
    store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str
) -> int:
    try:
        result = tp_inv.run_trusted_publisher_inventory(
            store,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            producer=prod,
            now_utc=now,
            retention_days=args.retention_days,
            index_url=args.index_url,
        )
    except tp_inv.InventoryError as exc:
        print(json.dumps({"status": "INVENTORY_ERROR", "error": str(exc)}, sort_keys=True))
        return 6
    except tp_inv.StreamPrerequisiteError as exc:
        print(json.dumps({"status": "PREREQUISITE", "error": str(exc)}, sort_keys=True))
        return 4
    except RuntimeError as exc:
        print(json.dumps({"status": "STORE_ERROR", "error": str(exc)}, sort_keys=True))
        return 7
    print(json.dumps(result, sort_keys=True))
    return 0


def run_draft_inventory_observe(
    store: Any, ids: dict[str, Any], args: Namespace, prod: dict[str, Any], now: str
) -> int:
    api = github.GitHubApi(token=support.require_env(args.github_token_env))
    try:
        result = draft_inv.run_draft_inventory(
            store,
            api,
            owner=args.owner,
            repo=args.repo,
            release_identity_id=ids["release_identity_id"],
            release_authority_id=ids["release_authority_id"],
            repository_id=args.repository_id,
            tag_ref=ids["tag_ref"],
            producer=prod,
            now_utc=now,
            retention_days=args.retention_days,
        )
    except draft_inv.InventoryError as exc:
        print(json.dumps({"status": "INVENTORY_ERROR", "error": str(exc)}, sort_keys=True))
        return 6
    except draft_inv.StreamPrerequisiteError as exc:
        print(json.dumps({"status": "PREREQUISITE", "error": str(exc)}, sort_keys=True))
        return 4
    except RuntimeError as exc:
        print(json.dumps({"status": "STORE_ERROR", "error": str(exc)}, sort_keys=True))
        return 7
    print(json.dumps(result, sort_keys=True))
    return 0
