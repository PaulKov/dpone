"""Compose guarded ClickHouse recovery from logical connection references."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.prepared_recovery_plan_store import read_plan, write_plan
from dpone.runtime.credentials.binding_resolver import BindingCredentialResolver
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from dpone.runtime.sinks.clickhouse_prepared_recovery_facade import (
    PreparedRecoveryBackend,
    build_prepared_recovery_backend,
)


@dataclass(slots=True)
class PreparedRecoveryRuntime:
    """One admitted authority, catalog and connector for a single invocation."""

    backend: PreparedRecoveryBackend

    def plan(self, args: Any) -> Any:
        return self.backend.plan(args)

    def execute(self, plan: Any, *, confirmation_digest: str) -> Any:
        return self.backend.execute(plan, confirmation_digest=confirmation_digest)

    def save_plan(self, plan: Any, path: str) -> None:
        write_plan(plan, Path(path))

    def load_plan(self, args: Any, path: str) -> Any:
        plan = read_plan(Path(path))
        if (
            plan.cluster != args.cluster
            or plan.record.database != args.database
            or plan.record.target != args.target
            or plan.operation_id != args.operation_id
            or plan.authority_version != args.authority_version
            or plan.operation_started_at != datetime.fromisoformat(args.operation_started_at)
        ):
            raise ValueError("recovery plan does not match requested operation")
        return plan


def build_prepared_recovery_runtime(args: Any) -> PreparedRecoveryRuntime:
    """Resolve an alias and admit only an externally provisioned strict authority.

    This function never bootstraps, migrates or edits authority storage. Vault-
    backed references require the normal pinned runtime credential context;
    this standalone command fails closed when that context is unavailable.
    """
    codec = PyYamlCodec()
    binding_set = _read_mapping(Path(args.binding_set), codec)
    registry = _read_mapping(Path(args.connection_registry), codec)
    resolved = BindingCredentialResolver(binding_set=binding_set, connection_registry=registry).resolve(
        args.connection_ref
    )
    connector = ResolvedConnectorFactory.create(resolved)
    return PreparedRecoveryRuntime(
        build_prepared_recovery_backend(connector, cluster=args.cluster, database=args.database)
    )


def _read_mapping(path: Path, codec: PyYamlCodec) -> dict[str, Any]:
    if not path.is_file() or path.stat().st_size > 1024 * 1024:
        raise ValueError("binding document is absent or too large")
    payload = codec.load(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("binding document must be a mapping")
    return payload
