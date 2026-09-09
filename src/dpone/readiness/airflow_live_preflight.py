"""Fail-closed live preflight planning for the beginner Airflow facade."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Protocol

from dpone.readiness.airflow_secret_redaction import is_secret_key
from dpone.readiness.error_contract import dpone_error, error_docs_url, manual_fix
from dpone.security_redaction import redact_text

_PROBES = ("credential_resolution", "bounded_source_probe", "bounded_sink_probe")


@dataclass(frozen=True)
class LivePreflightContext:
    environment: str
    source_path: Path
    connection_refs: tuple[str, ...]
    resolved_connection_refs: tuple[str, ...]


class AirflowLivePreflightRunner(Protocol):
    def run(self, context: LivePreflightContext) -> dict[str, Any]:
        """Execute bounded live probes through an explicitly configured runtime runner."""


def build_live_preflight_report(
    *,
    connection_details: dict[str, Any],
    environment: str,
    source_path: Path,
    source_label: str | None = None,
    runner: AirflowLivePreflightRunner | None = None,
) -> dict[str, Any]:
    """Return live-preflight probes, failing closed when no runner is configured."""

    connection_refs = _string_list(connection_details.get("connection_refs"))
    resolved_connection_refs = _string_list(connection_details.get("resolved_connection_refs"))
    public_source = source_label or source_path.name or "pipeline.yaml"
    if runner is not None:
        return _configured_runner_report(
            runner=runner,
            context=LivePreflightContext(
                environment=environment,
                source_path=source_path,
                connection_refs=connection_refs,
                resolved_connection_refs=resolved_connection_refs,
            ),
            source_label=public_source,
        )
    errors = (
        dpone_error(
            "DPONE_LIVE_CHECK_RUNNER_NOT_CONFIGURED",
            (
                "Live bounded preflight requires an explicitly configured runtime runner; "
                "static and connection configuration checks passed."
            ),
            stage="check_live",
            path=public_source,
            entity={"kind": "pipeline_source", "id": public_source},
            docs_url=error_docs_url("DPONE_LIVE_CHECK_RUNNER_NOT_CONFIGURED"),
            fixes=[manual_fix("configure_live_preflight_runner")],
        ),
    )
    return {
        "schema": "dpone.live-preflight.v1",
        "passed": False,
        "runner": "not_configured",
        "network": False,
        "secrets": False,
        "source_queries": False,
        "planned_network": True,
        "planned_secrets": True,
        "planned_source_queries": "bounded_probes",
        "environment": environment,
        "connection_refs": list(connection_refs),
        "resolved_connection_refs": list(resolved_connection_refs),
        "probes": _blocked_probes(resolved_connection_refs),
        "errors": list(errors),
    }


def _configured_runner_report(
    *,
    runner: AirflowLivePreflightRunner,
    context: LivePreflightContext,
    source_label: str,
) -> dict[str, Any]:
    try:
        result = runner.run(context)
    except Exception as exc:
        return _runner_failed_report(context, exc, source_label=source_label)
    probes = _configured_probes(result.get("probes"))
    errors = _error_list(result.get("errors"))
    if not probes:
        errors.append(
            dpone_error(
                "DPONE_LIVE_CHECK_RUNNER_RESULT_INVALID",
                "Live preflight runner returned no probe results.",
                stage="check_live",
                path=source_label,
                entity={"kind": "pipeline_source", "id": source_label},
                docs_url=error_docs_url("DPONE_LIVE_CHECK_RUNNER_RESULT_INVALID"),
                fixes=[manual_fix("inspect_live_preflight_runner")],
            )
        )
    failed_probe_refs = [probe["connection_ref"] for probe in probes if probe["status"] == "failed"]
    if failed_probe_refs and not errors:
        errors.append(
            dpone_error(
                "DPONE_LIVE_CHECK_PROBE_FAILED",
                "One or more bounded live preflight probes failed.",
                stage="check_live",
                path=source_label,
                entity={"kind": "connection_ref", "id": failed_probe_refs[0]},
                docs_url=error_docs_url("DPONE_LIVE_CHECK_PROBE_FAILED"),
                fixes=[manual_fix("inspect_live_preflight_probe")],
            )
        )
    return {
        "schema": "dpone.live-preflight.v1",
        "passed": not errors and all(probe["status"] == "passed" for probe in probes),
        "runner": "configured",
        "network": any(bool(probe["network"]) for probe in probes),
        "secrets": any(bool(probe["secrets"]) for probe in probes),
        "source_queries": any(bool(probe["source_queries"]) for probe in probes),
        "planned_network": True,
        "planned_secrets": True,
        "planned_source_queries": "bounded_probes",
        "environment": context.environment,
        "connection_refs": list(context.connection_refs),
        "resolved_connection_refs": list(context.resolved_connection_refs),
        "probes": probes,
        "errors": errors,
    }


def _runner_failed_report(
    context: LivePreflightContext,
    exc: Exception,
    *,
    source_label: str,
) -> dict[str, Any]:
    return {
        "schema": "dpone.live-preflight.v1",
        "passed": False,
        "runner": "configured",
        "network": False,
        "secrets": False,
        "source_queries": False,
        "planned_network": True,
        "planned_secrets": True,
        "planned_source_queries": "bounded_probes",
        "environment": context.environment,
        "connection_refs": list(context.connection_refs),
        "resolved_connection_refs": list(context.resolved_connection_refs),
        "probes": [],
        "errors": [
            dpone_error(
                "DPONE_LIVE_CHECK_RUNNER_FAILED",
                f"Live preflight runner failed: {redact_text(str(exc))}",
                stage="check_live",
                path=source_label,
                entity={"kind": "pipeline_source", "id": source_label},
                docs_url=error_docs_url("DPONE_LIVE_CHECK_RUNNER_FAILED"),
                fixes=[manual_fix("inspect_live_preflight_runner")],
            )
        ],
    }


def _configured_probes(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    probes: list[dict[str, Any]] = []
    for probe in value:
        if not isinstance(probe, dict):
            continue
        connection_ref = probe.get("connection_ref")
        probe_name = probe.get("probe")
        status = probe.get("status")
        if not isinstance(connection_ref, str) or not isinstance(probe_name, str) or not isinstance(status, str):
            continue
        if probe_name not in _PROBES or status not in {"passed", "failed"}:
            continue
        probes.append(
            {
                "connection_ref": connection_ref,
                "probe": probe_name,
                "status": status,
                "runner": "configured",
                "network": bool(probe.get("network", True)),
                "secrets": bool(probe.get("secrets", True)),
                "source_queries": bool(probe.get("source_queries", probe_name != "credential_resolution")),
            }
        )
    return probes


def _error_list(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [_redacted_error(error) for error in value if isinstance(error, dict)]


def _redacted_error(error: dict[str, Any]) -> dict[str, Any]:
    redacted = _redacted_value(error)
    return redacted if isinstance(redacted, dict) else {}


def _redacted_value(value: Any, *, key: str | None = None) -> Any:
    if key and is_secret_key(key):
        return "[REDACTED]"
    if isinstance(value, dict):
        return {str(item_key): _redacted_value(item, key=str(item_key)) for item_key, item in value.items()}
    if isinstance(value, list):
        return [_redacted_value(item) for item in value]
    if isinstance(value, str):
        return redact_text(value)
    return value


def _blocked_probes(connection_refs: tuple[str, ...]) -> list[dict[str, Any]]:
    return [
        {
            "connection_ref": connection_ref,
            "probe": probe,
            "status": "blocked",
            "runner": "not_configured",
            "network": True,
            "secrets": True,
            "source_queries": probe != "credential_resolution",
        }
        for connection_ref in connection_refs
        for probe in _PROBES
    ]


def _string_list(value: Any) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple, set)):
        return ()
    return tuple(sorted({item for item in value if isinstance(item, str) and item}))


__all__ = ["AirflowLivePreflightRunner", "LivePreflightContext", "build_live_preflight_report"]
