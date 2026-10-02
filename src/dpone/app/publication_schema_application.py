"""Source-free catalog setup from an init-fetch-verified connection context.

The context must be supplied by the trusted deployment runner, as for normal
runtime execution. A local plan/digest is scope confirmation, not a credential,
deployment signature, writer freeze, or permission to retire a legacy operation.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.adapters.publication_plan_file import read_private_plan, write_private_plan
from dpone.app.publication_operator_context import OperatorScopeBlocked as _ScopeBlocked
from dpone.app.publication_operator_context import resolve_authority, verified_context
from dpone.contracts.publication_schema import decode_schema_plan
from dpone.runtime.publication_authority_composition import build_publication_schema

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection
    from dpone.runtime.state.mssql_publication_schema import MssqlPublicationSchema


class PublicationSchemaApplication:
    """One explicit operation; no source/sink construction or automatic retry."""

    def __init__(
        self,
        *,
        environ: Mapping[str, str] | None = None,
        connector_factory: Callable[[ResolvedBindingConnection], Any] | None = None,
    ) -> None:
        self._environ = environ
        self._connector_factory = connector_factory

    def plan(self, *, connection_ref: str, environment: str, path: Path) -> dict[str, str]:
        """Inspect without DDL, then exclusively publish an owner-private plan.

        Credential resolution and read-only SQL admission are required. An exact
        existing catalog is allowed; partial catalogs and unknown observations
        produce no new plan file. A failed file durability ACK retains evidence.
        """
        digest = ""
        try:
            service = self._service(connection_ref, environment)
            plan = service.plan()
            digest = plan.digest
            result = service.inspect(plan)
            if result.status in {"ready", "completed"}:
                write_private_plan(path, plan.payload.encode())
            return _result(result.status, result.reason_code, digest)
        except _ScopeBlocked as error:
            return _result("blocked", str(error), digest)
        except Exception:
            return _result("outcome_unknown", "schema_plan_unavailable", digest)

    def inspect(self, *, path: Path, environment: str) -> dict[str, str]:
        """Read back the saved scope without DDL, including after a lost ACK."""
        return self._execute(path=path, environment=environment, confirmation_digest=None, apply=False)

    def apply(self, *, path: Path, environment: str, confirmation_digest: str) -> dict[str, str]:
        """Validate confirmation before context/credentials; provision at most once."""
        return self._execute(path=path, environment=environment, confirmation_digest=confirmation_digest, apply=True)

    def _execute(self, *, path: Path, environment: str, confirmation_digest: str | None, apply: bool) -> dict[str, str]:
        digest = ""
        try:
            plan = decode_schema_plan(read_private_plan(path))
            digest = plan.digest
            if apply and not plan.confirms(confirmation_digest):
                raise _ScopeBlocked("plan_confirmation_required")
            if environment != plan.binding.environment:
                raise _ScopeBlocked("plan_environment_differs")
            service = self._service(plan.binding.connection_ref, environment)
            if service.plan() != plan:
                raise _ScopeBlocked("plan_scope_or_ddl_differs")
            result = service.apply(plan, confirmation_digest=digest) if apply else service.inspect(plan)
            return _result(result.status, result.reason_code, digest)
        except _ScopeBlocked as error:
            return _result("blocked", str(error), digest)
        except Exception:
            return _result("outcome_unknown", "schema_operation_requires_inspection", digest)

    def _service(self, connection_ref: str, environment: str) -> MssqlPublicationSchema:
        context = verified_context(self._environ, environment)
        resolved, binding, _ = resolve_authority(context, connection_ref)
        return build_publication_schema(
            connection=resolved,
            binding=binding,
            environment=context.environment,
            connector_factory=self._connector_factory,
        )


def _result(status: str, reason: str, digest: str) -> dict[str, str]:
    return {
        "contract": "dpone.publication-schema-result.v1",
        "status": status,
        "reason_code": reason,
        "plan_digest": digest,
    }
