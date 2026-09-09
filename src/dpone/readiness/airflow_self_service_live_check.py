"""Live preflight result assembly for Airflow self-service checks."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from dpone.readiness.airflow_live_preflight import AirflowLivePreflightRunner


from pathlib import Path
from typing import Any

from dpone.readiness.airflow_live_preflight import build_live_preflight_report
from dpone.readiness.airflow_self_service_models import SelfServiceResult


def live_preflight_result(
    *,
    details: dict[str, Any],
    errors: list[dict[str, Any]],
    source_path: Path,
    source_label: str,
    environment: str,
    runner: AirflowLivePreflightRunner | None,
) -> SelfServiceResult:
    live_preflight = build_live_preflight_report(
        connection_details=details,
        environment=environment,
        source_path=source_path,
        source_label=source_label,
        runner=runner,
    )
    details["live_preflight"] = f"runner_{live_preflight['runner']}"
    details["live_preflight_report"] = live_preflight
    details["network"] = live_preflight["network"]
    details["secrets"] = live_preflight["secrets"]
    details["source_queries"] = live_preflight["source_queries"]
    errors.extend(live_preflight["errors"])
    passed = not errors and bool(live_preflight["passed"])
    return SelfServiceResult(
        passed=passed,
        errors=tuple(errors),
        details=details,
        exit_code=None if passed else 3,
    )


__all__ = ["live_preflight_result"]
