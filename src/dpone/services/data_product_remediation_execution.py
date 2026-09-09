"""File-IO facade for remediation execution commands."""

from __future__ import annotations

import json
import subprocess
from collections.abc import Mapping
from importlib import import_module
from pathlib import Path
from typing import Any

import yaml


class DataProductRemediationExecutionFacade:
    """Thin CLI facade; execution decisions live in readiness modules."""

    def plan(
        self,
        *,
        manifest_path: str,
        remediation_plan_path: str,
        remediation_gate_path: str | None = None,
        authority_gate_path: str | None = None,
        parameters_path: str | None = None,
    ) -> dict[str, Any]:
        return (
            _execution()
            .RemediationExecutionPlanner()
            .plan(
                manifest=_read_mapping(manifest_path),
                remediation_plan=_read_mapping(remediation_plan_path),
                remediation_gate=_read_optional(remediation_gate_path),
                authority_gate=_read_optional(authority_gate_path),
                parameters=_read_optional(parameters_path),
            )
        )

    def run(
        self,
        *,
        plan_path: str,
        idempotency_key: str | None = None,
        lock_path: str | None = None,
        execute: bool = False,
    ) -> dict[str, Any]:
        return (
            _execution()
            .RemediationExecutionRunner()
            .run(
                execution_plan=_read_mapping(plan_path),
                executor=_LocalDponeCommandExecutor() if execute else None,
                execute=execute,
                idempotency_key=idempotency_key,
                lock=_read_optional(lock_path),
            )
        )

    def certify(
        self, *, run_path: str, evidence_dir: str | None = None, profile: str = "prod_strict"
    ) -> dict[str, Any]:
        return (
            _execution()
            .RemediationExecutionCertifier()
            .certify(
                run=_read_mapping(run_path),
                evidence_payloads=_evidence_payloads(evidence_dir),
                profile=profile,
            )
        )

    def report(self, *, certificate_path: str) -> dict[str, Any]:
        return _rendering().RemediationExecutionRenderer().report(certificate=_read_mapping(certificate_path))


class _LocalDponeCommandExecutor:
    """Execute prevalidated local commands without shell interpolation."""

    def execute(self, argv: tuple[str, ...], *, timeout_seconds: int) -> dict[str, Any]:
        try:
            result = subprocess.run(
                argv,
                check=False,
                capture_output=True,
                shell=False,
                text=True,
                timeout=timeout_seconds,
            )
        except subprocess.TimeoutExpired as exc:
            return {
                "status": "failed",
                "exit_code": None,
                "duration_ms": None,
                "stdout": exc.stdout or "",
                "stderr": exc.stderr or "timeout expired",
                "timeout_seconds": timeout_seconds,
            }
        return {
            "status": "succeeded" if result.returncode == 0 else "failed",
            "exit_code": result.returncode,
            "duration_ms": None,
            "stdout": result.stdout,
            "stderr": result.stderr,
            "timeout_seconds": timeout_seconds,
        }


def _evidence_payloads(path: str | None) -> tuple[Mapping[str, Any], ...]:
    if not path:
        return ()
    root = Path(path)
    if not root.exists():
        return ()
    sources = sorted([*root.rglob("*.json"), *root.rglob("*.yaml"), *root.rglob("*.yml")])
    return tuple(_read_mapping(str(source)) for source in sources)


def _read_optional(path: str | None) -> dict[str, Any]:
    return _read_mapping(path) if path else {}


def _read_mapping(path: str | None) -> dict[str, Any]:
    if not path:
        return {}
    source = Path(path)
    text = source.read_text(encoding="utf-8")
    raw = json.loads(text) if source.suffix.lower() == ".json" else yaml.safe_load(text)
    if not isinstance(raw, Mapping):
        raise ValueError(f"{path} must contain an object")
    return dict(raw)


def _execution() -> Any:
    return import_module("dpone.readiness.data_product_remediation_execution")


def _rendering() -> Any:
    return import_module("dpone.readiness.data_product_remediation_execution_rendering")


__all__ = ["DataProductRemediationExecutionFacade"]
