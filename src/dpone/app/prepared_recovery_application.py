"""Explicit source-free recovery composition; saved plans are not admission."""

from __future__ import annotations

from collections.abc import Callable, Iterator, Mapping
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.adapters.publication_plan_file import read_private_plan, write_private_plan
from dpone.app.publication_operator_context import (
    OperatorScopeBlocked,
    context_subject,
    owned_recovery_target,
    resolve_authority,
    verified_context,
)
from dpone.contracts.prepared_recovery_plan import PreparedRecoveryOperatorPlan, decode_recovery_operator_plan
from dpone.contracts.publication_authority_binding import publication_binding_digest
from dpone.runtime.publication_authority_composition import (
    BoundPublicationAuthorityProvider,
    build_publication_authority,
)
from dpone.runtime.sinks.clickhouse_cluster_publication_catalog import ClickHouseClusterPublicationCatalog
from dpone.runtime.sinks.clickhouse_cluster_publication_ddl import ClickHouseClusterPublicationDdl
from dpone.runtime.sinks.clickhouse_prepared_recovery import PreparedRecoveryService

if TYPE_CHECKING:
    from dpone.contracts.publication_authority_binding import PublicationAuthorityBinding
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.ports.prepared_recovery import PreparedRecoverySafetyObserver


class PreparedRecoveryApplication:
    """One operation under an injected trusted hold; no retry or source creation."""

    def __init__(
        self,
        *,
        environ: Mapping[str, str] | None = None,
        safety: PreparedRecoverySafetyObserver | None = None,
        connector_factory: Callable[[ResolvedBindingConnection], Any] | None = None,
        clock: Callable[[], int] | None = None,
    ) -> None:
        self._environ, self._safety = environ, safety
        self._factory, self._clock = connector_factory, clock

    def plan(
        self,
        *,
        connection_ref: str,
        sink_connection_ref: str,
        environment: str,
        cluster: str,
        database: str,
        target: str,
        operation_id: str,
        expected_version: int,
        path: Path,
    ) -> dict[str, str]:
        """Observe exact native PREPARED, close resources, exclusively save scope."""
        digest = ""
        try:
            with self._service(connection_ref, sink_connection_ref, environment, cluster, database) as scope:
                service, binding, pin, subject, endpoint = scope
                recovery = service.plan(target=target, operation_id=operation_id, expected_version=expected_version)
                plan = PreparedRecoveryOperatorPlan(binding, pin, subject, sink_connection_ref, endpoint, recovery)
                payload, digest = plan.payload.encode(), plan.digest
            write_private_plan(path, payload)
            return _result("ready", "native_recovery_planned", digest)
        except OperatorScopeBlocked as error:
            return _result("blocked", str(error), digest)
        except Exception:
            return _result("outcome_unknown", "recovery_plan_unavailable", digest)

    def execute(self, *, path: Path, environment: str, confirmation_digest: str) -> dict[str, str]:
        """Confirm before credentials; re-admit and reconcile, never re-plan."""
        digest = ""
        try:
            plan = decode_recovery_operator_plan(read_private_plan(path))
            digest = plan.digest
            if not plan.confirms(confirmation_digest):
                raise OperatorScopeBlocked("plan_confirmation_required")
            if environment != plan.binding.environment:
                raise OperatorScopeBlocked("plan_environment_differs")
            with self._service(
                plan.binding.connection_ref,
                plan.sink_connection_ref,
                environment,
                plan.recovery.safety.inventory.cluster,
                plan.recovery.preparation.prepared.record.database,
                expected=plan,
            ) as scope:
                receipt = scope[0].execute(plan.recovery, confirmation_digest=plan.recovery.digest)
            result = _result("completed", "native_recovery_completed", digest)
            result["authority_version"] = str(receipt.authority_version)
            return result
        except OperatorScopeBlocked as error:
            return _result("blocked", str(error), digest)
        except Exception:
            return _result("outcome_unknown", "recovery_requires_inspection", digest)

    @contextmanager
    def _service(
        self,
        connection_ref: str,
        sink_connection_ref: str,
        environment: str,
        cluster: str,
        database: str,
        expected: PreparedRecoveryOperatorPlan | None = None,
    ) -> Iterator[tuple[PreparedRecoveryService, PublicationAuthorityBinding, str, str, str]]:
        if self._safety is None:
            raise OperatorScopeBlocked("held_recovery_observer_required")
        context = verified_context(self._environ, environment)
        subject = context_subject(context)
        if expected is not None and expected.context_subject != subject:
            raise OperatorScopeBlocked("plan_context_differs")
        sql, binding, pin = resolve_authority(context, connection_ref)
        if expected is not None and (expected.binding != binding or expected.endpoint_identity != pin):
            raise OperatorScopeBlocked("plan_authority_scope_differs")
        sink = context.resolver.resolve(sink_connection_ref)
        provider = BoundPublicationAuthorityProvider(
            lambda: build_publication_authority(
                connection=sql, binding=binding, environment=environment, connector_factory=self._factory
            )
        )
        with owned_recovery_target(sink, database, self._factory) as (connector, endpoint):
            if expected is not None and expected.sink_endpoint_identity != endpoint:
                raise OperatorScopeBlocked("plan_sink_endpoint_differs")
            catalog = ClickHouseClusterPublicationCatalog(connector)
            service = PreparedRecoveryService(
                catalog=catalog,
                provider=provider,
                ddl=ClickHouseClusterPublicationDdl(connector, catalog),
                safety=self._safety,
                binding_digest=publication_binding_digest(binding, endpoint_identity=pin),
                cluster=cluster,
                database=database,
                clock=self._clock,
            )
            yield service, binding, pin, subject, endpoint


def _result(status: str, reason: str, digest: str) -> dict[str, str]:
    return {
        "contract": "dpone.native-recovery-result.v1",
        "status": status,
        "reason_code": reason,
        "plan_digest": digest,
    }
