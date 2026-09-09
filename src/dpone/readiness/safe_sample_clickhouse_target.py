"""Credential-aware ClickHouse target adapter at the runtime composition boundary."""

from __future__ import annotations

from typing import Any, Protocol

from dpone.services.safe_sample_clickhouse_target import ClickHouseTemporaryTargetAdapter
from dpone.services.safe_sample_execution_common import credential_resolution_error, redact_mapping
from dpone.services.safe_sample_policy import TemporaryTargetPlan


class RuntimeCredentialResolver(Protocol):
    def resolve(self, connection_ref: str) -> Any:
        """Return an object with credentials and safe_metadata attributes."""


class ConnectionProviderFactory(Protocol):
    def create(self, credentials: Any) -> Any:
        """Return a callable that provides one ClickHouse connection."""


class CredentialResolvingClickHouseTemporaryTargetAdapter:
    """Bind ClickHouse target DDL to one workload-scoped credential resolver."""

    def __init__(
        self,
        *,
        credential_resolver: RuntimeCredentialResolver,
        connection_ref: str,
        connection_provider_factory: ConnectionProviderFactory,
    ) -> None:
        self._credential_resolver = credential_resolver
        self._connection_ref = str(connection_ref or "").strip()
        self._connection_provider_factory = connection_provider_factory
        self._delegate: ClickHouseTemporaryTargetAdapter | None = None
        self._safe_metadata: dict[str, object] = {}

    def create(self, plan: TemporaryTargetPlan) -> dict[str, object]:
        delegate = self._delegate_for(plan)
        return {
            **delegate.create(plan),
            "applied": True,
            "credential_resolution": dict(self._safe_metadata),
        }

    def drop(self, plan: TemporaryTargetPlan) -> dict[str, object]:
        delegate = self._delegate_for(plan)
        return {
            **delegate.drop(plan),
            "applied": True,
            "credential_resolution": dict(self._safe_metadata),
        }

    def _delegate_for(self, plan: TemporaryTargetPlan) -> ClickHouseTemporaryTargetAdapter:
        self._validate_plan(plan)
        if self._delegate is not None:
            return self._delegate
        try:
            resolved = self._credential_resolver.resolve(self._connection_ref)
        except Exception as exc:
            if credential_resolution_error(exc) is not None:
                raise
            raise RuntimeError("ClickHouse temporary target credential resolution failed") from exc
        credentials = getattr(resolved, "credentials", None)
        if credentials is None:
            raise RuntimeError("ClickHouse temporary target credentials are unavailable")
        provider = self._connection_provider_factory.create(credentials)
        self._safe_metadata = redact_mapping(getattr(resolved, "safe_metadata", {}))
        self._delegate = ClickHouseTemporaryTargetAdapter(execute=lambda statement: provider().execute(statement))
        return self._delegate

    def _validate_plan(self, plan: TemporaryTargetPlan) -> None:
        if str(plan.sink_type or "").strip().lower() != "clickhouse":
            raise ValueError("ClickHouse temporary target requires sink_type=clickhouse")
        if not self._connection_ref or plan.connection_ref != self._connection_ref:
            raise ValueError("ClickHouse temporary target connection_ref does not match runtime binding")


__all__ = [
    "ConnectionProviderFactory",
    "CredentialResolvingClickHouseTemporaryTargetAdapter",
    "RuntimeCredentialResolver",
]
