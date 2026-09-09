from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from importlib import import_module
from pathlib import Path
from typing import Any

from dpone.contracts.gitops_schema_semantics import validate_gitops_semantics
from dpone.gitops.schema_contract_primitives import GitOpsSchemaContract
from dpone.gitops.schema_contracts import get_gitops_schema_contract, gitops_schema_contracts


@dataclass(frozen=True, slots=True)
class GitOpsSchemaCatalogService:
    """Read-only facade over public GitOps JSON Schema contracts."""

    def list_contracts(self, *, prefix: str | None = None) -> dict[str, Any]:
        contracts = tuple(_filter_contracts(gitops_schema_contracts(), prefix=prefix))
        return {
            "kind": "gitops.schema_catalog",
            "count": len(contracts),
            "contracts": [_contract_summary(contract) for contract in contracts],
        }

    def show_contract(self, raw_kind: str) -> dict[str, Any]:
        contract = get_gitops_schema_contract(raw_kind)
        if contract is None:
            return _blocked(
                "gitops.schema_contract",
                "schema_unknown_kind",
                f"No GitOps schema contract is registered for {raw_kind}",
                path=str(raw_kind),
            )
        return {
            "kind": "gitops.schema_contract",
            "contract": {
                **_contract_summary(contract),
                "schema": contract.schema,
            },
            "passed": True,
            "issues": [],
        }

    def validate_payload(self, *, raw_kind: str, payload_path: str | Path) -> dict[str, Any]:
        path = Path(payload_path)
        contract = get_gitops_schema_contract(raw_kind)
        if contract is None:
            return _blocked(
                "gitops.schema_validation",
                "schema_unknown_kind",
                f"No GitOps schema contract is registered for {raw_kind}",
                path=str(raw_kind),
                payload=str(path),
            )
        payload, load_issue = _load_json_object(path)
        if load_issue is not None:
            return {
                "kind": "gitops.schema_validation",
                "contract": _contract_summary(contract),
                "payload": str(path),
                "passed": False,
                "issues": [load_issue],
            }
        issues = _validate_with_jsonschema(payload, contract)
        return {
            "kind": "gitops.schema_validation",
            "contract": _contract_summary(contract),
            "payload": str(path),
            "passed": not issues,
            "issues": issues,
        }


def _filter_contracts(
    contracts: tuple[GitOpsSchemaContract, ...],
    *,
    prefix: str | None,
) -> tuple[GitOpsSchemaContract, ...]:
    needle = (prefix or "").strip()
    if not needle:
        return contracts
    return tuple(
        contract for contract in contracts if contract.kind.startswith(needle) or contract.name.startswith(needle)
    )


def _contract_summary(contract: GitOpsSchemaContract) -> dict[str, str]:
    return {
        "name": contract.name,
        "kind": contract.kind,
        "schema_id": str(contract.schema.get("$id") or ""),
    }


def _load_json_object(path: Path) -> tuple[dict[str, Any], dict[str, str] | None]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {}, _issue("schema_payload_not_found", f"Payload file does not exist: {path}", path=str(path))
    except json.JSONDecodeError as exc:
        return {}, _issue("schema_payload_json_invalid", f"Payload file is not valid JSON: {exc}", path=str(path))
    except OSError as exc:
        return {}, _issue("schema_payload_read_failed", f"Payload file could not be read: {exc}", path=str(path))
    if not isinstance(payload, Mapping):
        return {}, _issue("schema_type_mismatch", "Payload must be a JSON object", path="$")
    return dict(payload), None


def _validate_with_jsonschema(payload: dict[str, Any], contract: GitOpsSchemaContract) -> list[dict[str, str]]:
    jsonschema = import_module("jsonschema")
    validator = jsonschema.Draft202012Validator(contract.schema)
    issues = []
    for error in sorted(validator.iter_errors(payload), key=lambda item: list(item.path)):
        path = "$"
        if error.path:
            path = "$." + ".".join(str(part) for part in error.path)
        issues.append(
            _issue(
                "schema_validation_failed",
                str(error.message),
                path=path,
                source=contract.kind,
            )
        )
    if not issues:
        issues.extend(
            _issue(code, message, path=path, source=contract.kind)
            for code, message, path in validate_gitops_semantics(payload, kind=contract.kind)
        )
    return issues


def _blocked(kind: str, code: str, message: str, *, path: str, payload: str | None = None) -> dict[str, Any]:
    result: dict[str, Any] = {
        "kind": kind,
        "passed": False,
        "issues": [_issue(code, message, path=path)],
    }
    if payload is not None:
        result["payload"] = payload
    return result


def _issue(code: str, message: str, *, path: str, source: str = "gitops.schema") -> dict[str, str]:
    return {
        "code": code,
        "message": message,
        "path": path,
        "source": source,
    }


__all__ = ["GitOpsSchemaCatalogService"]
