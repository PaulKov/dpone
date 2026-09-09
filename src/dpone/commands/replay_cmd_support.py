from __future__ import annotations

import argparse
from typing import Any

from dpone.contracts import RuntimeConfigurationError
from dpone.strategy_intelligence.replay import ReplayExecutionService
from dpone.strategy_intelligence.replay_adapters import ReplayAdapterRegistry
from dpone.strategy_intelligence.replay_clients import ReplayBackendConnection, RuntimeReplayBackendFactory

_CREDENTIAL_SOURCES = ("env", "vault", "airflow", "params")


def add_live_replay_arguments(parser: argparse.ArgumentParser) -> None:
    """Add common live replay backend arguments to resync/resume commands."""

    parser.add_argument(
        "--live-backend",
        action="store_true",
        help="Execute through runtime SQL/Kafka replay backend when combined with --yes",
    )
    parser.add_argument("--connection-id", help="Target sink connection id used by the runtime replay backend")
    parser.add_argument(
        "--connection-type",
        choices=list(_CREDENTIAL_SOURCES),
        default="env",
        help="Credential provider for --connection-id",
    )
    parser.add_argument("--target-schema", default="public", help="Target schema/database for DB replay backends")
    parser.add_argument("--target-table", help="Target table or Kafka topic for live replay")
    parser.add_argument("--staging-schema", default="staging", help="Staging schema/database for DB replay backends")
    parser.add_argument("--mount-point", help="Vault mount point when --connection-type vault is used")
    parser.add_argument("--connection-path", help="Vault path or provider-specific credential path")


def build_replay_execution_service(args: argparse.Namespace | Any) -> ReplayExecutionService:
    """Build replay execution service from CLI args.

    Default mode stays artifact-only. Live backend mode is explicit and only
    resolves runtime connectors for approved execution (`--yes`).
    """

    artifact_dir = getattr(args, "artifact_dir", ".dpone/replay")
    if not bool(getattr(args, "live_backend", False)):
        return ReplayExecutionService(artifact_dir=artifact_dir)

    _validate_live_backend_args(args)
    if not bool(getattr(args, "yes", False)):
        return ReplayExecutionService(artifact_dir=artifact_dir)

    backend = RuntimeReplayBackendFactory().build(
        ReplayBackendConnection(
            sink_type=str(getattr(args, "sink_type")),
            connection_id=str(getattr(args, "connection_id")),
            credentials_source=_credentials_source(getattr(args, "connection_type", "env")),
            target_schema=str(getattr(args, "target_schema", "public")),
            target_table=str(getattr(args, "target_table")),
            staging_schema=str(getattr(args, "staging_schema", "staging")),
            mount_point=getattr(args, "mount_point", None),
            path=getattr(args, "connection_path", None),
        )
    )
    return ReplayExecutionService(
        artifact_dir=artifact_dir,
        adapter_registry=ReplayAdapterRegistry(backend=backend),
    )


def _validate_live_backend_args(args: argparse.Namespace | Any) -> None:
    missing: list[str] = []
    if not getattr(args, "connection_id", None):
        missing.append("--connection-id")
    if not getattr(args, "target_table", None):
        missing.append("--target-table")
    if missing:
        joined = ", ".join(missing)
        raise RuntimeConfigurationError(f"Live replay backend requires {joined}.")


def _credentials_source(value: str) -> str:
    normalized = str(value).strip().lower()
    if normalized in _CREDENTIAL_SOURCES:
        return normalized
    choices = ", ".join(_CREDENTIAL_SOURCES)
    raise RuntimeConfigurationError(f"Unsupported replay connection type: {value}. Expected one of: {choices}.")
