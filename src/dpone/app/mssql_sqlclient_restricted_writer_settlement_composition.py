"""Composition root for exact P9b departure and remote settlement."""

from collections.abc import Callable, Iterator
from contextlib import contextmanager
from pathlib import Path
from threading import Lock
from time import monotonic
from typing import Any, Protocol, cast
from uuid import uuid4

from dpone.adapters.filesystem_evidence import DescriptorPinnedCreateOnlyEvidenceWriter
from dpone.adapters.mssql_sqlclient_restricted_writer_settlement_evidence_actor import (
    RestrictedWriterSettlementEvidenceActor,
    TdsJournalActorUnknown,
)
from dpone.app.mssql_sqlclient_departure_infrastructure_composition import admitted_departure_launcher as _admission
from dpone.app.mssql_sqlclient_restricted_writer_departure import make_verifier_factory
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1
from dpone.services.mssql_tds_restricted_writer_settlement import (
    _claim_restricted_writer,
    settle_restricted_writer,
)
from dpone.services.mssql_tds_restricted_writer_verify_coordinator import RestrictedWriterVerifyRetained

ERROR = "mssql_native.sqlclient_restricted_writer_settlement_composition_invalid"


class CompositionOperations(Protocol):
    plan_type: type

    def plan(self, **values: object) -> object: ...
    def attempt_digest(self, attempt: object) -> str: ...
    def operation_digest(self, identity: object) -> str: ...


class _CleanupCustody:
    """One-shot shared cleanup authority for composition and service."""

    _ORDER = ("orphan", "verifier", "coordinator", "evidence")

    def __init__(self) -> None:
        self._lock = Lock()
        self._closed = False
        self._callbacks: dict[str, Callable[[], None]] = {}

    def add(self, kind: str, callback: Callable[[], None]) -> None:
        with self._lock:
            if self._closed or kind not in self._ORDER or kind in self._callbacks:
                raise ValueError(ERROR)
            self._callbacks[kind] = callback

    def close(self) -> None:
        with self._lock:
            if self._closed:
                return
            self._closed = True
            callbacks = tuple(self._callbacks[kind] for kind in self._ORDER if kind in self._callbacks)
        failure = None
        for callback in callbacks:
            try:
                callback()
            except BaseException as error:
                if failure is None:
                    failure = error
        if failure is not None:
            raise failure


def make_restricted_writer_departure_plan(
    retained: RestrictedWriterVerifyRetained,
    grant_evidence: object,
    management_admission: object,
    writer_admission: object,
    *,
    implementation_sha256: str,
    package_root: str,
    admission_sha256: str,
    startup_deadline: float,
    operation_deadline: float,
    max_address_space_bytes: int,
    operations: CompositionOperations,
) -> object:
    """Bind the P9b helper plan to the exact retained P9a request and result."""
    if type(retained) is not RestrictedWriterVerifyRetained:
        raise ValueError(ERROR)
    retained.assert_retained()
    owner = retained._owner
    association = cast(Any, owner._association)
    if (
        owner._request is None
        or owner._result is None
        or getattr(association, "_verify_grant_ref", None) is not grant_evidence
    ):
        raise ValueError(ERROR)
    return operations.plan(
        helper_id=uuid4(),
        grant_evidence=grant_evidence,
        verify_request=owner._request,
        verify_result=owner._result,
        management_admission=management_admission,
        writer_admission=writer_admission,
        implementation_sha256=implementation_sha256,
        package_root=package_root,
        admission_sha256=admission_sha256,
        startup_deadline=startup_deadline,
        operation_deadline=operation_deadline,
        max_address_space_bytes=max_address_space_bytes,
    )


def settle_mssql_sqlclient_restricted_writer(
    pool,
    retained: RestrictedWriterVerifyRetained,
    origin,
    coordinator_factory: Callable[[], object],
    plan: object,
    verifier_factory: Callable[[object], tuple[Callable[[], object], Callable[[float], None]]],
    evidence_root: Path,
    *,
    deadline: float,
    cleanup_deadline: float,
    operations,
) -> object:
    """Open one settlement actor and consume the exact retained VERIFY once."""
    if (
        type(retained) is not RestrictedWriterVerifyRetained
        or type(plan) is not operations.plan_type
        or not isinstance(evidence_root, Path)
        or not evidence_root.is_absolute()
    ):
        raise ValueError(ERROR)
    retained.assert_retained()
    if not callable(coordinator_factory):
        raise ValueError(ERROR)
    owner = retained._owner
    association = cast(Any, owner._association)
    if (
        plan.verify_request is not owner._request
        or plan.verify_result is not owner._result
        or getattr(association, "_verify_grant_ref", None) is not plan.grant_evidence
    ):
        raise ValueError(ERROR)
    claim = _claim_restricted_writer(retained, operations)
    claim.bind_plan(plan, operations)
    return _settle_claimed_mssql_sqlclient_restricted_writer(
        claim,
        pool,
        origin,
        coordinator_factory,
        plan,
        verifier_factory,
        evidence_root,
        deadline=deadline,
        cleanup_deadline=cleanup_deadline,
        operations=operations,
    )


def _settle_claimed_mssql_sqlclient_restricted_writer(
    claim,
    pool,
    origin,
    coordinator_factory,
    plan,
    verifier_factory,
    evidence_root: Path,
    *,
    deadline: float,
    cleanup_deadline: float,
    operations,
) -> object:
    """Allocate and settle only after the exact retained authority is claimed."""
    retained = claim.retained
    owner = retained._owner
    association = cast(Any, owner._association)
    claim.assert_owned(operations)
    if not callable(coordinator_factory):
        raise ValueError(ERROR)
    if claim.plan is not plan:
        raise ValueError(ERROR)
    operation_sha = operations.operation_digest(association._verify_identity)
    attempt_sha = operations.attempt_digest(plan.attempt)

    @contextmanager
    def writer() -> Iterator[CreateOnlyEvidenceWriterV1]:
        yield DescriptorPinnedCreateOnlyEvidenceWriter(evidence_root)

    custody = _CleanupCustody()
    try:
        coordinator = claim.coordinator
        custody.add("coordinator", lambda: claim.close_coordinator(deadline=cleanup_deadline))
        run, close = verifier_factory(plan)
        custody.add("verifier", lambda: close(cleanup_deadline))
        try:
            evidence = pool.open(
                lambda actor_deadline, actor_clock: RestrictedWriterSettlementEvidenceActor(
                    writer,
                    attempt_sha,
                    operation_sha,
                    operations,
                    actor_deadline,
                    actor_clock,
                ),
                deadline=deadline,
            )
        except TdsJournalActorUnknown as error:
            if error.gateway is not None:
                gateway = error.gateway
                custody.add("orphan", lambda: gateway.close(deadline=cleanup_deadline))
            raise
        custody.add("evidence", lambda: evidence.close(deadline=cleanup_deadline))
        return settle_restricted_writer(
            claim,
            origin,
            coordinator,
            evidence,
            run,
            close,
            deadline=deadline,
            cleanup_deadline=cleanup_deadline,
            clock=monotonic,
            operations=operations,
            cleanup_custody=custody.close,
        )
    finally:
        try:
            custody.close()
        except BaseException:
            pass


def settle_mssql_sqlclient_restricted_writer_managed(
    pool,
    store_factory,
    retained: RestrictedWriterVerifyRetained,
    origin,
    limits,
    lease,
    departure_launcher,
    management_admission,
    writer_admission,
    management_credentials,
    evidence_root: Path,
    *,
    supervisor_token: str,
    deadline: float,
    helper_startup_timeout: float,
    cleanup_deadline: float,
    operations,
):
    """Compose the exact VERIFY coordinator, helper, evidence and settlement."""
    if type(retained) is not RestrictedWriterVerifyRetained or retained._owner._association is not origin:
        raise ValueError(ERROR)
    retained.assert_retained()
    claim = _claim_restricted_writer(retained, operations)
    association = cast(Any, origin)
    grant_evidence = association._verify_grant_ref
    _, admission_sha, implementation, package_root, address_space = _admission(departure_launcher)
    startup_deadline = min(deadline, monotonic() + helper_startup_timeout)
    plan = make_restricted_writer_departure_plan(
        retained,
        grant_evidence,
        management_admission,
        writer_admission,
        implementation_sha256=implementation,
        package_root=package_root,
        admission_sha256=admission_sha,
        startup_deadline=startup_deadline,
        operation_deadline=deadline,
        max_address_space_bytes=address_space,
        operations=operations,
    )
    claim.bind_plan(plan, operations)
    verifier_factory = make_verifier_factory(pool, evidence_root, departure_launcher, management_credentials, retained)

    return _settle_claimed_mssql_sqlclient_restricted_writer(
        claim,
        pool,
        origin,
        lambda: claim.coordinator,
        plan,
        verifier_factory,
        evidence_root,
        deadline=deadline,
        cleanup_deadline=cleanup_deadline,
        operations=operations,
    )
