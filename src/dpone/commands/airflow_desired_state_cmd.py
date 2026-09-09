"""CLI composition for Airflow desired-state publish and fetch."""

from __future__ import annotations

import argparse
import json
import logging
from pathlib import Path

from dpone.adapters.airflow_desired_state_checkpoint import (
    AtomicAirflowDesiredStateSnapshotWriter,
)
from dpone.commands.airflow_desired_state_publish_cmd import (
    cmd_prepare,
    cmd_publish,
    register_prepare_parser,
    register_publish_parser,
)
from dpone.commands.func_command import CommandGroup, FuncCommand
from dpone.contracts.airflow_desired_state import DesiredStateRevision
from dpone.readiness.airflow_artifact_delivery import (
    DEFAULT_MAX_OBJECT_BYTES,
    DEFAULT_MAX_TOTAL_BYTES,
    ArtifactRegistryOptions,
)
from dpone.readiness.airflow_desired_state_authority import (
    AirflowDesiredStateAuthority,
    load_airflow_desired_state_authority,
)
from dpone.readiness.airflow_desired_state_reconcile import reconcile_desired_state
from dpone.readiness.airflow_desired_state_store import (
    DesiredStateStoreOptions,
)
from dpone.services.airflow_desired_state_fetch import (
    AirflowDesiredStateFetcher,
    DesiredStateFetchError,
    DesiredStateFetchRequest,
)
from dpone.services.airflow_desired_state_reconcile import (
    DesiredStateReconcileError,
)


def desired_state_group() -> CommandGroup:
    def build(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
        return subparsers.add_parser(
            "desired-state",
            help="Publish or fetch one exact Airflow desired deployment",
        )

    return CommandGroup(
        name="desired-state",
        help="Publish or fetch one exact Airflow desired deployment",
        build_parser=build,
        subcommands=[
            FuncCommand(
                "prepare",
                register_prepare_parser,
                cmd_prepare,
                _requires_app_context=False,
            ),
            FuncCommand(
                "publish",
                register_publish_parser,
                cmd_publish,
                _requires_app_context=False,
            ),
            FuncCommand("fetch", _register_fetch_parser, _cmd_fetch, _requires_app_context=False),
            FuncCommand(
                "reconcile",
                _register_reconcile_parser,
                _cmd_reconcile,
                _requires_app_context=False,
            ),
        ],
        subdest="airflow_desired_state_cmd",
    )


def _register_fetch_parser(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "fetch",
        help="Fetch and atomically persist one verified desired-state snapshot",
    )
    _add_store_arguments(parser)
    parser.add_argument("--previous-revision")
    parser.add_argument("--output", required=True, help="Verified local desired-state snapshot")
    parser.add_argument("--status-output", required=True, help="Fetch evidence JSON")
    return parser


def _register_reconcile_parser(
    subparsers: argparse._SubParsersAction,
) -> argparse.ArgumentParser:
    parser = subparsers.add_parser(
        "reconcile",
        help="Materialize and activate the exact remote desired deployment",
    )
    _add_store_arguments(parser)
    artifact_access = parser.add_mutually_exclusive_group(required=True)
    artifact_access.add_argument(
        "--artifact-identity-mode",
        choices=["workload_identity"],
    )
    artifact_access.add_argument("--artifact-connection-id")
    parser.add_argument(
        "--artifact-connection-type",
        choices=["airflow", "env", "vault"],
    )
    parser.add_argument("--cache-root", required=True)
    parser.add_argument(
        "--max-object-bytes",
        type=int,
        default=DEFAULT_MAX_OBJECT_BYTES,
    )
    parser.add_argument(
        "--max-total-bytes",
        type=int,
        default=DEFAULT_MAX_TOTAL_BYTES,
    )
    return parser


def _add_store_arguments(parser: argparse.ArgumentParser) -> None:
    access = parser.add_mutually_exclusive_group(required=True)
    access.add_argument("--identity-mode", choices=["workload_identity"])
    access.add_argument("--connection-id")
    parser.add_argument("--connection-type", choices=["airflow", "env", "vault"])


def _cmd_fetch(args: argparse.Namespace, *, ctx: object, logger: logging.Logger) -> int:
    del ctx
    status_output = Path(args.status_output)
    try:
        authority = load_airflow_desired_state_authority()
        _require_distinct_paths(Path(args.output), status_output)
        store = _store_options(args, authority).build()
        evidence = AirflowDesiredStateFetcher(
            reader=store,
            snapshot_writer=AtomicAirflowDesiredStateSnapshotWriter(Path(args.output)),
        ).fetch(
            DesiredStateFetchRequest(
                environment=authority.environment,
                previous_revision=(
                    None if args.previous_revision is None else DesiredStateRevision(args.previous_revision)
                ),
            )
        )
        payload = {**evidence.to_dict(), "output_path": str(Path(args.output))}
    except (DesiredStateFetchError, OSError, ValueError) as exc:
        logger.warning("Airflow desired-state fetch failed: %s", type(exc).__name__)
        code = exc.code if isinstance(exc, DesiredStateFetchError) else "DPONE_AIRFLOW_DESIRED_STATE_INPUT_INVALID"
        payload = _failure(code)
        _write_json(status_output, payload)
        return 3 if isinstance(exc, DesiredStateFetchError) else 2
    _write_json(status_output, payload)
    return 0


def _cmd_reconcile(
    args: argparse.Namespace,
    *,
    ctx: object,
    logger: logging.Logger,
) -> int:
    del ctx
    cache_root = Path(args.cache_root)
    try:
        authority = load_airflow_desired_state_authority()
        reconcile_desired_state(
            authority=authority,
            desired_store_options=_store_options(args, authority),
            artifact_registry_options=ArtifactRegistryOptions(
                registry_uri=authority.artifact_registry_uri,
                identity_mode=args.artifact_identity_mode,
                connection_type=args.artifact_connection_type,
                connection_id=args.artifact_connection_id,
            ),
            cache_root=cache_root,
            max_object_bytes=args.max_object_bytes,
            max_total_bytes=args.max_total_bytes,
        )
    except DesiredStateReconcileError as exc:
        logger.warning("Airflow desired-state reconcile failed: %s", type(exc).__name__)
        return _reconcile_exit_code(exc)
    except (OSError, ValueError) as exc:
        logger.warning("Airflow desired-state reconcile failed: %s", type(exc).__name__)
        return 2
    except Exception as exc:  # noqa: BLE001 - public boundary redacts dependency details.
        logger.error("Airflow desired-state reconcile failed: %s", type(exc).__name__)
        return 5
    return 0


def _store_options(
    args: argparse.Namespace,
    authority: AirflowDesiredStateAuthority,
) -> DesiredStateStoreOptions:
    return DesiredStateStoreOptions(
        desired_uri=authority.desired_state_uri,
        certified_endpoint_url=authority.certified_s3_endpoint_url,
        identity_mode=args.identity_mode,
        connection_type=args.connection_type,
        connection_id=args.connection_id,
    )


def _require_distinct_paths(first: Path, second: Path) -> None:
    if first.resolve(strict=False) == second.resolve(strict=False):
        raise ValueError("Airflow desired-state control paths must be distinct")


def _failure(
    code: str,
    *,
    state_may_have_changed: bool | None = None,
) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema": "dpone.error.v1",
        "passed": False,
        "errors": [{"code": code, "message": "Airflow desired-state operation failed."}],
    }
    if state_may_have_changed is not None:
        payload["state_may_have_changed"] = state_may_have_changed
    return payload


def _reconcile_exit_code(exc: DesiredStateReconcileError) -> int:
    if exc.state_may_have_changed:
        return 4
    if exc.code in {
        "DPONE_AIRFLOW_DESIRED_STATE_NOT_FOUND",
        "DPONE_AIRFLOW_DESIRED_STATE_PRECOMMIT_UNAVAILABLE",
        "DPONE_AIRFLOW_DESIRED_STATE_UNAVAILABLE",
    }:
        return 3
    return 4


def _write_json(
    path: Path,
    payload: dict[str, object],
    *,
    root: Path | None = None,
) -> None:
    body = (json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n").encode()
    AtomicAirflowDesiredStateSnapshotWriter(path, root=root).commit(body)


__all__ = ["desired_state_group"]
