"""Fail-closed orchestration for protected development runtime entrypoints."""

from __future__ import annotations

from collections.abc import Callable
from typing import TYPE_CHECKING

from dpone.contracts.development_delivery_authority import (
    DevelopmentAuthorityError,
    DevelopmentExecutionSubject,
    development_release_authority_projection,
    require_development_runtime_authority,
)
from dpone.contracts.strict_json import strict_json_object
from dpone.ports.development_runtime_authority import (
    DevelopmentRuntimeAuthority,
    DevelopmentRuntimeAuthorization,
    DevelopmentRuntimeAuthorizationRequest,
)
from dpone.runtime.init_fetch_contract import (
    InitFetchError,
    development_runtime_authority_error,
)

if TYPE_CHECKING:
    from dpone.runtime.runtime_init_fetch_plan import RuntimeInitFetchPlan


def authorize_development_runtime(
    plan: RuntimeInitFetchPlan,
    *,
    authority: DevelopmentRuntimeAuthority | None = None,
    authority_loader: Callable[[], DevelopmentRuntimeAuthority] | None = None,
) -> DevelopmentRuntimeAuthorization | None:
    """Reopen current authority before any protected runtime operation."""

    if not plan.development_authority_required:
        return None
    try:
        verifier = authority or (authority_loader() if authority_loader is not None else None)
        if verifier is None:
            raise DevelopmentAuthorityError("runtime_authorization")
        result = verifier.authorize(_request(plan))
        if not isinstance(result, DevelopmentRuntimeAuthorization):
            raise DevelopmentAuthorityError("runtime_authorization")
        receipt = result.authority
        if receipt.environment != plan.environment:
            raise DevelopmentAuthorityError("environment")
        receipt.require_current(
            now=result.checked_at,
            current_revocation_epoch=result.current_revocation_epoch,
        )
        if not receipt.allows_execution(_subject(plan)):
            raise DevelopmentAuthorityError("workload_execution")
        return result
    except Exception as exc:
        raise _authority_error() from exc


def require_fetched_development_authority(
    release_payload: bytes,
    *,
    plan: RuntimeInitFetchPlan,
    authorization: DevelopmentRuntimeAuthorization | None,
) -> None:
    """Bind the fetched release projection to the current process result."""

    if not plan.development_authority_required:
        return
    try:
        if authorization is None:
            raise DevelopmentAuthorityError("current_external_authority")
        projection = development_release_authority_projection(strict_json_object(release_payload))
        if projection is None:
            raise DevelopmentAuthorityError("release_projection")
        if plan.execution.kind == "runtime":
            require_development_runtime_authority(
                projection,
                authority=authorization.authority,
                workload_id=plan.workload_pack.id,
                now=authorization.checked_at,
                current_revocation_epoch=authorization.current_revocation_epoch,
            )
        else:
            if authorization.authority.release_projection() != projection:
                raise DevelopmentAuthorityError("current_external_authority")
            authorization.authority.require_current(
                now=authorization.checked_at,
                current_revocation_epoch=authorization.current_revocation_epoch,
            )
            if not authorization.authority.allows_execution(_subject(plan)):
                raise DevelopmentAuthorityError("workload_execution")
    except Exception as exc:
        raise _authority_error() from exc


def _request(plan: RuntimeInitFetchPlan) -> DevelopmentRuntimeAuthorizationRequest:
    return DevelopmentRuntimeAuthorizationRequest(
        environment=plan.environment,
        release_id=plan.release_id,
        deployment_id=plan.deployment_id,
        runtime_image_digest=plan.runtime_image_digest,
        workload_id=plan.workload_pack.id,
        execution_kind=plan.execution.kind,
        hook_id=plan.execution.hook_name,
    )


def _subject(plan: RuntimeInitFetchPlan) -> DevelopmentExecutionSubject:
    return DevelopmentExecutionSubject(
        workload_id=plan.workload_pack.id,
        kind=plan.execution.kind,
        hook_id=plan.execution.hook_name,
    )


def _authority_error() -> InitFetchError:
    return development_runtime_authority_error()


__all__ = [
    "authorize_development_runtime",
    "require_fetched_development_authority",
]
