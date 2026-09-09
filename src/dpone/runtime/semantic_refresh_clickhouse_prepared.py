"""Authenticated durable PREPARED loader for a distinct ClickHouse COMMIT task."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dpone.runtime.semantic_refresh_clickhouse_authorization import (
    ClickHousePublicationAuthorization,
)
from dpone.runtime.semantic_refresh_clickhouse_prepared_codec import (
    LoadedClickHousePreparedPublication,
    load_prepared_publication,
)

if TYPE_CHECKING:
    from dpone.ports.semantic_refresh_artifact_seal import SemanticRefreshSealAuthorizationPort
    from dpone.ports.semantic_refresh_clickhouse_authority import (
        SemanticRefreshClickHousePublicationAuthorityPort,
    )
    from dpone.ports.semantic_refresh_clickhouse_prepared import (
        DurableClickHousePreparedPublication,
        SemanticRefreshClickHousePreparedPublicationPort,
    )


class ClickHousePreparedPublicationLoadError(RuntimeError):
    """Raised when durable PREPARED input is absent, swapped, or unauthenticated."""


class AuthenticatedClickHousePreparedPublicationLoader:
    """Rebuild typed plan/receipt and recheck protected operation and seal identity."""

    def __init__(
        self,
        *,
        durable: SemanticRefreshClickHousePreparedPublicationPort,
        authority: SemanticRefreshClickHousePublicationAuthorityPort,
        seal_authorization: SemanticRefreshSealAuthorizationPort,
    ) -> None:
        self._durable = durable
        self._authorization = ClickHousePublicationAuthorization(authority)
        self._seal_authorization = seal_authorization

    def load(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> LoadedClickHousePreparedPublication:
        """Return one authenticated cross-task COMMIT input or fail before ClickHouse I/O."""

        try:
            documents = self._durable.load_prepared(
                workflow_execution_binding_sha256,
                operation_id,
            )
        except Exception as exc:
            raise ClickHousePreparedPublicationLoadError("durable PREPARED authority is unavailable") from exc
        return self._authenticate(
            documents,
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )

    def load_if_prepared(
        self,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> LoadedClickHousePreparedPublication | None:
        """Return authenticated durable PREPARED, or None only for locked PREPARING."""

        try:
            documents = self._durable.find_prepared(
                workflow_execution_binding_sha256,
                operation_id,
            )
        except Exception as exc:
            raise ClickHousePreparedPublicationLoadError("durable PREPARED authority is unavailable") from exc
        if documents is None:
            return None
        return self._authenticate(
            documents,
            workflow_execution_binding_sha256=workflow_execution_binding_sha256,
            operation_id=operation_id,
        )

    def _authenticate(
        self,
        documents: DurableClickHousePreparedPublication,
        *,
        workflow_execution_binding_sha256: str,
        operation_id: str,
    ) -> LoadedClickHousePreparedPublication:
        try:
            loaded = load_prepared_publication(documents)
            self._authorization.prepare(loaded.plan)
            seal = self._seal_authorization.load(
                workflow_execution_binding_sha256=workflow_execution_binding_sha256,
                operation_id=operation_id,
            )
        except Exception as exc:
            raise ClickHousePreparedPublicationLoadError("durable PREPARED authority is unavailable") from exc
        expected = (
            loaded.plan.operation_id,
            loaded.plan.operation_plan_sha256,
            loaded.plan.workflow_plan_sha256,
            loaded.plan.workflow_execution_id,
            loaded.plan.workflow_execution_binding_sha256,
            loaded.plan.attempt_binding_sha256,
            loaded.plan.fence_epoch,
        )
        observed = (
            seal.operation_id,
            seal.operation_plan_sha256,
            seal.workflow_plan_sha256,
            seal.workflow_execution_id,
            seal.workflow_execution_binding_sha256,
            seal.attempt_binding_sha256,
            seal.fencing_epoch,
        )
        if observed != expected:
            raise ClickHousePreparedPublicationLoadError("durable PREPARED seal identity differs")
        return loaded


__all__ = [
    "AuthenticatedClickHousePreparedPublicationLoader",
    "ClickHousePreparedPublicationLoadError",
]
