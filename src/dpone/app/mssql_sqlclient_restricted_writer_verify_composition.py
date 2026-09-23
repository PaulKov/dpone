"""Concrete create-only evidence composition for one P9a execution."""

from collections.abc import Callable, Iterator
from contextlib import AbstractContextManager, contextmanager
from pathlib import Path
from time import monotonic
from typing import Any, cast

from dpone.adapters.mssql_sqlclient_restricted_writer_verify_evidence_actor import (
    RestrictedWriterVerifyCreateOnlyEvidenceWriter,
    RestrictedWriterVerifyEvidenceActor,
)
from dpone.app.mssql_sqlclient_restricted_writer_verify_request import (
    RestrictedWriterVerificationOperations,
    RestrictedWriterVerifyLaunchRequest,
)
from dpone.app.mssql_tds_coordinator_composition import TdsActorPoolCapability
from dpone.ports.bounded_window import WindowStore
from dpone.services.mssql_tds_restricted_writer_verification import verify_restricted_writer
from dpone.services.mssql_tds_restricted_writer_verify_coordinator import RestrictedWriterVerifyRetained

StoreFactory = Callable[[], AbstractContextManager[WindowStore]]


def bind_mssql_sqlclient_restricted_writer_verify(attempt, settled, *, deadline: float):
    """Return the original P8 association as the sole P9 reservation authority."""
    association = getattr(attempt, "_permission_grant_owner", None)
    if association is None or getattr(association, "_host", None) is not attempt:
        raise ValueError("mssql_native.sqlclient_restricted_writer_verify_origin_invalid")
    return association.bind_restricted_writer_verify(
        settled,
        RestrictedWriterVerificationOperations(),
        deadline=deadline,
    )


def run_mssql_sqlclient_restricted_writer_verify(
    pool: TdsActorPoolCapability,
    association,
    launcher,
    launch_request: RestrictedWriterVerifyLaunchRequest,
    credential_supplier,
    evidence_root: Path,
    *,
    deadline: float,
    coordinator_factory: Callable[[], object],
) -> RestrictedWriterVerifyRetained:
    """Open the one evidence actor, then keep exact owners through LOCAL_EXIT."""
    if not isinstance(evidence_root, Path) or not evidence_root.is_absolute():
        raise ValueError("mssql_native.sqlclient_restricted_writer_verify_composition_invalid")

    @contextmanager
    def writer() -> Iterator[RestrictedWriterVerifyCreateOnlyEvidenceWriter]:
        yield RestrictedWriterVerifyCreateOnlyEvidenceWriter(evidence_root)

    operations = RestrictedWriterVerificationOperations()
    evidence = pool.open(
        lambda actor_deadline, actor_clock: RestrictedWriterVerifyEvidenceActor(
            writer,
            launch_request.request.operation_id,
            actor_deadline,
            actor_clock,
            operations,
        ),
        deadline=deadline,
    )
    try:
        return verify_restricted_writer(
            association,
            cast(Any, evidence),
            launcher,
            cast(Any, launch_request),
            credential_supplier,
            cast(Any, coordinator_factory),
            operations,
            deadline=deadline,
            clock=monotonic,
        )
    except RuntimeError as error:
        retained = getattr(error, "retained", None)
        contain_unknown = getattr(retained, "contain_unknown", None)
        if error.args != ("mssql_native.sqlclient_restricted_writer_verify_unknown",) or not callable(contain_unknown):
            raise
        contain_unknown(deadline=monotonic() + launch_request.termination_timeout)
        raise
    finally:
        del credential_supplier
        evidence.close(deadline=deadline)
