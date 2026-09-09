"""Universal authoring validation for governed generic MSSQL routes."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.contracts.connector_declarations import canonical_endpoint_type
from dpone.manifest.models import ProcessSpec
from dpone.manifest.validation_models import Severity, ValidationIssue


def validate_generic_mssql_transaction_state(
    spec: ProcessSpec,
    *,
    manifest_path: Path,
) -> list[ValidationIssue]:
    """Require the external target-atomic state authority before runtime hydration."""

    raw_config = spec.raw_config if isinstance(spec.raw_config, Mapping) else {}
    sink = _mapping(raw_config.get("sink"))
    if canonical_endpoint_type(str(sink.get("type") or "")) != "mssql":
        return []

    load_cfg = getattr(spec.config, "load_config", None)
    options = _mapping(getattr(load_cfg, "options", None))
    reconciliation = _mapping(options.get("reconciliation"))
    if (
        reconciliation
        and bool(reconciliation.get("enabled", True))
        and str(reconciliation.get("mode") or "key_snapshot").strip().lower() == "key_snapshot"
    ):
        # The snapshot route retains the six-object XMin contract and owns its
        # more specific authoring diagnostics.
        return []

    state = _mapping(options.get("state")) or _mapping(raw_config.get("state"))
    selector = spec.selector or spec.name
    if not state:
        return [
            _issue(
                "MSSQL_TRANSACTION_STATE_REQUIRED",
                "Governed MSSQL routes require an explicit state block backed by the external generic transaction catalog.",
                manifest_path,
                selector,
            )
        ]
    if canonical_endpoint_type(str(state.get("type") or "")) != "mssql":
        return [
            _issue(
                "MSSQL_TRANSACTION_REQUIRES_MSSQL_STATE",
                "Governed MSSQL routes require state.type=mssql so state and target can share one SQL Server transaction.",
                manifest_path,
                selector,
            )
        ]
    if str(state.get("atomicity") or "after_target").strip().lower() != "target_atomic":
        return [
            _issue(
                "MSSQL_TRANSACTION_REQUIRES_TARGET_ATOMIC_STATE",
                "Governed MSSQL routes require state.atomicity=target_atomic.",
                manifest_path,
                selector,
            )
        ]
    if str(state.get("provisioning") or "external").strip().lower() != "external":
        return [
            _issue(
                "MSSQL_TRANSACTION_REQUIRES_EXTERNAL_STATE",
                "Governed MSSQL routes require state.provisioning=external; runtime DDL is forbidden.",
                manifest_path,
                selector,
            )
        ]
    return []


def _issue(code: str, message: str, path: Path, selector: str) -> ValidationIssue:
    return ValidationIssue(
        severity=Severity.ERROR,
        code=code,
        message=message,
        manifest_path=path,
        selector=selector,
    )


def _mapping(value: object) -> Mapping[str, object]:
    return value if isinstance(value, Mapping) else {}


__all__ = ["validate_generic_mssql_transaction_state"]
