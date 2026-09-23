"""Private P10b composition for one managed writer launch and registration."""

from collections.abc import Callable
from contextlib import AbstractContextManager
from time import monotonic
from typing import Any, cast

from dpone.adapters.mssql_sqlclient_evidence_actor import SqlClientEvidenceActor
from dpone.adapters.mssql_sqlclient_launch import SqlClientLauncher
from dpone.adapters.mssql_tds_actor_core import TdsActorPool, TdsJournalActorUnknown
from dpone.ports.evidence import CreateOnlyEvidenceWriterV1
from dpone.services.mssql_tds_writer_launch import (
    EvidenceGateway,
    SqlClientEvidenceOpenUnknown,
    SqlClientWriterRegistered,
    launch_and_register_sqlclient_writer,
)


def launch_mssql_sqlclient_writer(
    admitted: object,
    *,
    pool: TdsActorPool,
    evidence_writer_factory: Callable[[], AbstractContextManager[CreateOnlyEvidenceWriterV1]],
    clock: Callable[[], float] = monotonic,
) -> SqlClientWriterRegistered:
    """Inject fixed adapters while the service owns all ordering and custody."""

    def open_evidence(attempt_sha256: str, deadline: float) -> EvidenceGateway:
        try:
            return pool.open(
                lambda actor_deadline, actor_clock: SqlClientEvidenceActor(
                    evidence_writer_factory,
                    attempt_sha256,
                    actor_deadline,
                    actor_clock,
                ),
                deadline=deadline,
            )
        except TdsJournalActorUnknown as error:
            raise SqlClientEvidenceOpenUnknown(cast(EvidenceGateway | None, error.gateway)) from None

    return launch_and_register_sqlclient_writer(
        cast(Any, admitted),
        clock=clock,
        open_evidence=open_evidence,
        build_launcher=SqlClientLauncher.for_input_descriptor,
    )


__all__ = ("launch_mssql_sqlclient_writer",)
