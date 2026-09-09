"""ETL facade for decision audit context and governed audit storage."""

from __future__ import annotations

import contextlib
from collections.abc import Iterator
from typing import TYPE_CHECKING, Any

from dpone.runtime.decision_audit import (
    CompositeDecisionPublisher,
    DecisionAuditPolicy,
    RuntimeDecisionContext,
    RuntimeDecisionSummary,
)
from dpone.runtime.etl.audit_policy import (
    audit_policy,
    is_clickhouse_connector,
    is_mssql_connector,
)

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class RuntimeDecisionLifecycle:
    """Wires runtime decision audit without expanding ``ETLProcessor`` fanout."""

    def __init__(
        self,
        *,
        load_governance_service: Any,
        load_identity_service: Any,
        logger: Any | None,
    ) -> None:
        self._load_governance_service = load_governance_service
        self._load_identity_service = load_identity_service
        self._logger = logger
        self._summary = RuntimeDecisionSummary()

    def configure_audit_storage(self, *, sink: Any, load_config: LoadConfig) -> None:
        policy = audit_policy(load_config)
        if not policy.enabled:
            return
        connector = getattr(sink, "connector", None)
        if is_mssql_connector(connector):
            self._configure_mssql_audit_storage(connector=connector, policy=policy)
            return
        if not is_clickhouse_connector(connector):
            return
        from dpone.runtime.state.clickhouse import ClickHouseLoadAuditStorage, ClickHouseLoadStepAuditStorage
        from dpone.runtime.state.clickhouse_state_design import ClickHouseStateTableDesign

        table_design = ClickHouseStateTableDesign.from_load_config(load_config)
        if getattr(self._load_identity_service, "audit_storage", None) is None:
            self._load_identity_service.audit_storage = ClickHouseLoadAuditStorage(
                connector,
                schema=policy.state_schema,
                table=policy.loads_table,
                table_design=table_design,
            )
        self._load_governance_service.set_default_audit_storage(
            ClickHouseLoadStepAuditStorage(
                connector,
                schema=policy.state_schema,
                table=policy.steps_table,
                table_design=table_design,
            )
        )

    def _configure_mssql_audit_storage(self, *, connector: Any, policy: Any) -> None:
        """Wire canonical SQL Server load lifecycle and step audit stores."""

        from dpone.runtime.state.mssql import MSSQLLoadAuditStorage
        from dpone.runtime.state.mssql_load_step_audit import MSSQLLoadStepAuditStorage

        if getattr(self._load_identity_service, "audit_storage", None) is None:
            self._load_identity_service.audit_storage = MSSQLLoadAuditStorage(
                connector,
                schema=policy.state_schema,
                table=policy.loads_table,
            )
        self._load_governance_service.set_default_audit_storage(
            MSSQLLoadStepAuditStorage(
                connector,
                schema=policy.state_schema,
                table=policy.steps_table,
            )
        )

    @contextlib.contextmanager
    def activate(self, *, load_config: LoadConfig, load_record: Any) -> Iterator[None]:
        publisher = CompositeDecisionPublisher(
            policy=DecisionAuditPolicy.from_load_config(load_config),
            governance_service=self._load_governance_service,
            load_record=load_record,
            logger=self._logger,
            summary=self._summary,
        )
        with RuntimeDecisionContext.activate(publisher):
            yield

    def summary_json(self) -> dict[str, Any]:
        return self._summary.to_jsonable()


__all__ = ["RuntimeDecisionLifecycle"]
