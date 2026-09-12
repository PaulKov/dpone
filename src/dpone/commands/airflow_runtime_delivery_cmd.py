"""Internal runtime-image CLI entry points for pinned Airflow KPO delivery."""

from __future__ import annotations

import argparse
import json
import logging

from dpone.app.composition_verified_pack_dispatcher import compose_verified_pack_dispatcher
from dpone.readiness.airflow_runtime_init_fetch import (
    AirflowRuntimeDeliveryError,
    AirflowRuntimeInitFetchService,
)
from dpone.readiness.airflow_runtime_pack_exec import (
    execute_verified_pack_command,
    report_pack_os_error,
)


def register_runtime_init_fetch_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    return subparsers.add_parser(
        "runtime-init-fetch",
        help="Fetch and verify one pinned runtime workload inside a KPO init container",
        description=(
            "Internal KPO init-container command. Reads the canonical "
            "versioned dpone.airflow-runtime-init-fetch-plan.v1-v3 contract from "
            "DPONE_INIT_FETCH_PLAN_B64 and DPONE_INIT_FETCH_PLAN_SHA256, "
            "then publishes runtime-fetch-ready.json only after verification."
        ),
    )


def register_runtime_pack_exec_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    return subparsers.add_parser(
        "runtime-pack-exec",
        help="Execute only the workload command selected from a verified fetched pack",
        description=(
            "Internal KPO base-container command. Revalidates the pinned init-fetch "
            "plan and runtime-fetch-ready.json, then runs the structured command "
            "selected from the verified workload pack. Captures output and summary "
            "under /var/lib/dpone/run; runtime publishes /airflow/xcom/return.json "
            "for the KPO xcom sidecar, while separate hooks use no XCom path."
        ),
    )


def cmd_airflow_runtime_init_fetch(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del args, ctx
    try:
        result = AirflowRuntimeInitFetchService().init_fetch()
    except AirflowRuntimeDeliveryError as exc:
        logger.error("%s: %s", exc.code, exc)
        return _exit_code(exc)
    print(json.dumps(dict(result), ensure_ascii=False, separators=(",", ":"), sort_keys=True))
    return 0


def cmd_airflow_runtime_pack_exec(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del args, ctx
    try:
        command = AirflowRuntimeInitFetchService().prepare_pack_exec()
        return execute_verified_pack_command(
            command,
            logger=logger,
            composition_dispatcher=compose_verified_pack_dispatcher(command),
        )
    except AirflowRuntimeDeliveryError as exc:
        logger.error("%s: %s", exc.code, exc)
        return _exit_code(exc)
    except OSError as exc:
        report_pack_os_error(exc, logger=logger)
        return 5


def _exit_code(exc: AirflowRuntimeDeliveryError) -> int:
    if exc.code == "DPONE_ARTIFACT_REGISTRY_UNAVAILABLE":
        return 3
    if exc.code in {
        "DPONE_INIT_FETCH_PLAN_INVALID",
        "DPONE_INIT_FETCH_PLAN_TOO_LARGE",
        "DPONE_INIT_FETCH_PLAN_HASH_MISMATCH",
        "DPONE_INIT_FETCH_PLAN_NON_CANONICAL",
    }:
        return 2
    return 4


__all__ = [
    "cmd_airflow_runtime_init_fetch",
    "cmd_airflow_runtime_pack_exec",
    "register_runtime_init_fetch_parser",
    "register_runtime_pack_exec_parser",
]
