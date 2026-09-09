"""Application service for the governed REST delivery design contract gate."""

from __future__ import annotations

import argparse
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from dpone.services.docs.errors import DocsConfigurationError

from .context import DocsServiceContext
from .rest_delivery_design_contract import (
    format_design_contract_report,
    validate_design_contract,
)

_DEFAULT_CONTRACT = "docs/rest-bulk-delivery-design-contract-v1.yaml"
_DEFAULT_SCHEMA = "docs/schema/rest-bulk-delivery-design-contract-v1.schema.json"


class CheckRestDeliveryDesignContractService:
    """Validate the reviewed REST delivery authority without mutating the repo."""

    def __init__(self, *, ctx: DocsServiceContext):
        self.ctx = ctx
        self.log = ctx.logger

    def run(self, args: argparse.Namespace | Mapping[str, Any]) -> tuple[int, dict[str, object] | str]:
        output_format = str(_arg(args, "format", "text") or "text").strip().lower()
        if output_format not in {"text", "json"}:
            raise DocsConfigurationError("--format must be text or json")

        contract_path = self._resolve_path(_arg(args, "contract", _DEFAULT_CONTRACT), label="Contract")
        schema_path = self._resolve_path(_arg(args, "schema", _DEFAULT_SCHEMA), label="Schema")
        contract_content = self.ctx.fs.read_bytes(contract_path)
        schema_text = self.ctx.fs.read_text(schema_path, encoding="utf-8")
        report = validate_design_contract(
            contract_content=contract_content,
            schema_text=schema_text,
        )
        payload: dict[str, object] | str
        if output_format == "json":
            payload = report.to_dict()
        else:
            payload = format_design_contract_report(report)

        if report.ok:
            self.log.info("REST delivery design contract is valid")
            return 0, payload
        self.log.error("REST delivery design contract failed: %s issue(s)", len(report.issues))
        return 2, payload

    def _resolve_path(self, raw: object, *, label: str) -> Path:
        text = str(raw or "").strip()
        if not text:
            raise DocsConfigurationError(f"{label} path must not be empty")
        path = Path(text)
        if not path.is_absolute():
            path = (self.ctx.settings.repo_root / path).resolve()
        if not self.ctx.fs.exists(path):
            raise DocsConfigurationError(f"{label} file not found: {path}")
        return path


def _arg(args: argparse.Namespace | Mapping[str, Any], key: str, default: object) -> object:
    if isinstance(args, Mapping):
        return args.get(key, default)
    return getattr(args, key, default)


__all__ = ["CheckRestDeliveryDesignContractService"]
