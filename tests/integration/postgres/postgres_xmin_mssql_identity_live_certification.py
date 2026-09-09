"""Orchestrator and evidence writer for target identity/repair vendor-live proof."""

from __future__ import annotations

import json
from collections.abc import Callable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from tools.route_live_certification.recorder import RouteLiveObservationRecorder

from dpone.security_redaction import redact_public_text
from tests.integration.postgres.postgres_live_support import (
    mssql_connector,
    postgres_connector,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_registry import (
    run_target_identity_registry_live,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_repairs import (
    run_config_coordinate_drift,
    run_empty_snapshot_repairs,
    run_target_authority_transfer,
)
from tests.integration.postgres.postgres_xmin_mssql_identity_live_unsafe_repairs import (
    run_unsafe_window_repairs,
)

EVIDENCE_PATH = Path("test_artifacts/live_certification/postgres_mssql_target_identity_authority.json")
JUNIT_PATH = Path("test_artifacts/live_certification/junit/postgres_mssql_target_identity_authority.xml")

REQUIRED_CASES = frozenset(
    {
        "target_identity_ci_alias_convergence",
        "target_identity_cs_case_distinct_mixed_collation",
        "target_identity_missing_decoy_pre_source",
        "target_identity_concurrent_distinct_owners",
        "target_config_coordinate_drift",
        "repair_empty_snapshot_all_delete",
        "repair_wraparound_and_freeze_full_baseline",
        "repair_target_authority_transfer",
    }
)
_UTC = timezone.utc  # noqa: UP017 - mypy uses the supported Python 3.10 stubs.


def run_identity_authority_certification(
    root: Path,
    *,
    route_live_recorder: RouteLiveObservationRecorder,
) -> frozenset[str]:
    """Execute every required case, persist evidence, and fail on any gap."""

    root.mkdir(parents=True, exist_ok=True)
    cases: dict[str, dict[str, Any]] = {}
    failures: dict[str, dict[str, str]] = {}

    try:
        registry = run_target_identity_registry_live(
            root / "registry",
            route_live_recorder=route_live_recorder,
        )
        registry_cases = registry["cases"]
        cases.update(
            {
                "target_identity_ci_alias_convergence": _registry_case(registry_cases["ci_alias_convergence"]),
                "target_identity_cs_case_distinct_mixed_collation": _registry_case(
                    registry_cases["cs_case_distinct_mixed_collation"]
                ),
                "target_identity_missing_decoy_pre_source": _registry_case(
                    registry_cases["missing_and_decoy_pre_source"]
                ),
                "target_identity_concurrent_distinct_owners": _registry_case(
                    registry_cases["concurrent_distinct_owners"]
                ),
            }
        )
    except Exception as exc:  # noqa: BLE001 - all remaining independent cases must still run.
        for case_id in REQUIRED_CASES:
            if case_id.startswith("target_identity_"):
                failures[case_id] = _failure(exc)

    runners: tuple[tuple[str, Callable[..., dict[str, Any]]], ...] = (
        ("target_config_coordinate_drift", run_config_coordinate_drift),
        ("repair_empty_snapshot_all_delete", run_empty_snapshot_repairs),
        ("repair_wraparound_and_freeze_full_baseline", run_unsafe_window_repairs),
        ("repair_target_authority_transfer", run_target_authority_transfer),
    )
    for case_id, runner in runners:
        try:
            result = runner(
                root / case_id,
                route_live_recorder=route_live_recorder,
            )
            result["execution_path"] = "standard_etlprocessor_route"
            cases[case_id] = result
        except Exception as exc:  # noqa: BLE001 - aggregate complete matrix diagnostics.
            failures[case_id] = _failure(exc)

    completed = frozenset(cases)
    missing = REQUIRED_CASES - completed
    status = "passed" if not missing and not failures else "failed"
    evidence = {
        "schema_version": "dpone.postgres_mssql.target_identity_authority_live.v1",
        "status": status,
        "release_ready": status == "passed",
        "generated_at_utc": datetime.now(_UTC).isoformat(),
        "execution": {
            "kind": "real_vendor",
            "standard_route": "ETLProcessor -> PostgresSource -> MSSQLSink",
            "business_route_bypass_used": False,
            "connectors_are_test_doubles": False,
            "junit_path": str(JUNIT_PATH),
        },
        "coverage": {
            "standard_etl_route_cases": [
                "target_config_coordinate_drift",
                "repair_empty_snapshot_all_delete",
                "repair_wraparound_and_freeze_full_baseline",
                "repair_target_authority_transfer",
            ],
            "registry_transactional_state_cases": [
                "target_identity_ci_alias_convergence",
                "target_identity_cs_case_distinct_mixed_collation",
                "target_identity_missing_decoy_pre_source",
                "target_identity_concurrent_distinct_owners",
            ],
            "fault_injection": {
                "natural_xid_wraparound": "not feasible in a bounded disposable run",
                "executed_substitute": "standard route with XMin manager wraparound result injected",
            },
            "supplemental_not_executed": [
                {
                    "cell": "concurrent_full_etl_transfer_vs_stale_old_full_etl",
                    "reason": (
                        "Not required by the frozen matrix; concurrent owner serialization is live-certified "
                        "at the production applock/state transaction boundary, while transfer and stale-old "
                        "full ETL paths are certified sequentially through the standard route."
                    ),
                }
            ],
        },
        "vendors": _vendor_versions(),
        "required_cases": sorted(REQUIRED_CASES),
        "completed_cases": sorted(completed),
        "missing_cases": sorted(missing),
        "failed_cases": failures,
        "cases": cases,
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE_PATH.write_text(
        json.dumps(evidence, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    if failures or missing:
        raise AssertionError(
            f"identity/authority live matrix incomplete: failed={sorted(failures)} missing={sorted(missing)}"
        )
    return completed


def _registry_case(value: dict[str, Any]) -> dict[str, Any]:
    result = dict(value)
    result["status"] = "passed"
    result["execution_path"] = "production_registry_and_transactional_state"
    return result


def _failure(exc: Exception) -> dict[str, str]:
    message = redact_public_text(
        exc,
        fallback="vendor-live case failed; details unavailable",
        strip_traceback=True,
    )
    return {"exception_type": type(exc).__name__, "diagnostic": message[:2048]}


def _vendor_versions() -> dict[str, Any]:
    postgres = postgres_connector()
    mssql = mssql_connector(database="master")
    try:
        pg = postgres.get_records(
            "SELECT current_setting('server_version') AS server_version, "
            "current_setting('server_version_num') AS server_version_num, "
            "version() AS version_string",
            as_dict=True,
        )[0]
        ms = mssql.get_records(
            "SELECT CONVERT(nvarchar(128), SERVERPROPERTY('ProductVersion')) AS product_version, "
            "CONVERT(nvarchar(128), SERVERPROPERTY('ProductMajorVersion')) AS product_major_version, "
            "CONVERT(nvarchar(128), SERVERPROPERTY('ProductLevel')) AS product_level, "
            "CONVERT(nvarchar(256), SERVERPROPERTY('Edition')) AS edition, "
            "CONVERT(nvarchar(max), @@VERSION) AS version_string",
            as_dict=True,
        )[0]
        driver = _odbc_version(mssql)
        return {
            "postgresql": {name: str(value) for name, value in pg.items()},
            "sql_server": {name: str(value) for name, value in ms.items()},
            "odbc": driver,
        }
    finally:
        postgres.close()
        mssql.close()


def _odbc_version(connector: Any) -> dict[str, str]:
    import pyodbc

    connection = connector.connection
    return {
        "driver_name": str(connection.getinfo(pyodbc.SQL_DRIVER_NAME)),
        "driver_version": str(connection.getinfo(pyodbc.SQL_DRIVER_VER)),
    }


__all__ = [
    "EVIDENCE_PATH",
    "JUNIT_PATH",
    "REQUIRED_CASES",
    "run_identity_authority_certification",
]
