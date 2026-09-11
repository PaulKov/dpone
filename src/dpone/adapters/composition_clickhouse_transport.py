"""One closed synchronous ClickHouse HTTP request after a durable gateway claim.

No proxies, redirects, retries, sessions or asynchronous inserts. HTTP 200 alone
is insufficient (https://clickhouse.com/docs/interfaces/http): only a complete,
framed, bounded empty response with no error header acknowledges these commands.
Acknowledgement is transport evidence, not catalog outcome or writer quiescence.
"""

from __future__ import annotations

from dataclasses import dataclass
from hashlib import sha256
from typing import Protocol

from dpone.adapters.composition_clickhouse_http import BoundedClickHouseHttp, ClickHouseHttpError, clickhouse_http_path
from dpone.adapters.composition_clickhouse_http import ClickHouseTransportCredentials as ClickHouseTransportCredentials
from dpone.contracts.composition_clickhouse_dispatch import (
    MAX_DISPATCH_PAYLOAD_BYTES,
    ClickHouseDispatch,
    CreateGenerationDispatch,
    ExchangeSnapshotDispatch,
    InsertGenerationDispatch,
)
from dpone.contracts.composition_identity import CompositionAdmissionError


class ClickHouseDispatchJournal(Protocol):
    """Commit the unique global-SQL claim before socket creation.

    The trusted gateway reopens exact ACTIVE/RUNNING ownership, epochs, physical
    enrollment, scoped credentials and immutable operation originals here.
    Existing claims or lost commit acknowledgements MUST raise; durable readback
    is not a new executor permit. Closure retains every pending claim until its
    complete response and outcome are durably recorded by the gateway.
    """

    def claim_once(self, dispatch: ClickHouseDispatch) -> None: ...


@dataclass(frozen=True, slots=True)
class ClickHouseDispatchObservation:
    """Complete transport observation; root persists it before worker ACK.

    request_body_bytes counts payload octets actually accepted by socket.send,
    excluding HTTP headers, URI SQL, TLS records and TCP framing. It is not
    source-export bytes. No failed/partial observation is a terminal receipt.
    """

    dispatch_sha256: str
    claim_key: str
    query_id: str
    request_body_bytes: int
    response_body_bytes: int
    response_body_sha256: str
    response_framing: str


class ClickHouseDispatchTransportError(RuntimeError):
    """Unacknowledged request; retain its claim and never resend automatically.

    The byte count is a lower bound on failures: a failed send may have accepted
    additional bytes without returning a count. Do not refund its reservation.
    Response/server/driver text and credential values are deliberately omitted.
    """

    def __init__(self, *, dispatch_sha256: str, request_body_bytes: int) -> None:
        super().__init__("ClickHouse dispatch is unacknowledged; protected reconciliation is required")
        self.dispatch_sha256 = dispatch_sha256
        self.request_body_bytes = request_body_bytes


def _statement(dispatch: ClickHouseDispatch) -> str:
    target = dispatch.target
    generation = f"`{target.database}`.`{target.generation_table}`"
    if type(dispatch) is CreateGenerationDispatch:
        fields = ", ".join(f"`{column.name}` {column.type_name}" for column in dispatch.columns)
        return f"CREATE TABLE {generation} UUID '{dispatch.generation_uuid}' ({fields}) ENGINE = MergeTree ORDER BY tuple()"
    if type(dispatch) is InsertGenerationDispatch:
        fields = ", ".join(f"`{column.name}`" for column in dispatch.columns)
        return f"INSERT INTO {generation} ({fields}) FORMAT Native"
    if type(dispatch) is ExchangeSnapshotDispatch:
        return f"EXCHANGE TABLES `{target.database}`.`{target.target_table}` AND {generation}"
    raise CompositionAdmissionError("clickhouse_dispatch_operation")


class ClickHouseDispatchTransport:
    """Use only inside the trusted gateway; DTO possession grants no authority."""

    def __init__(
        self,
        *,
        endpoint: str,
        credentials: ClickHouseTransportCredentials,
        journal: ClickHouseDispatchJournal,
        timeout_seconds: float,
        max_payload_bytes: int = MAX_DISPATCH_PAYLOAD_BYTES,
        max_response_bytes: int = 64 * 1024,
        ca_file: str | None = None,
    ) -> None:
        self._http = BoundedClickHouseHttp(
            endpoint=endpoint,
            credentials=credentials,
            timeout_seconds=timeout_seconds,
            max_payload_bytes=max_payload_bytes,
            max_response_bytes=max_response_bytes,
            ca_file=ca_file,
        )
        self._journal, self._max_payload = journal, max_payload_bytes

    def execute(self, dispatch: ClickHouseDispatch, *, payload: bytes = b"") -> ClickHouseDispatchObservation:
        """Validate originals, claim once, send once, drain a complete response.

        Callers must persist the returned observation before acknowledging work.
        A crash between response and persistence leaves the durable claim pending.
        This method never reconciles/reopens a claim and never changes ownership.
        """
        if type(dispatch) not in {CreateGenerationDispatch, InsertGenerationDispatch, ExchangeSnapshotDispatch}:
            raise CompositionAdmissionError("clickhouse_dispatch_operation")
        dispatch.__post_init__()
        dispatch_hash = dispatch.dispatch_sha256
        if type(payload) is not bytes or len(payload) > self._max_payload:
            raise CompositionAdmissionError("clickhouse_dispatch_payload_budget")
        if isinstance(dispatch, InsertGenerationDispatch):
            if (
                len(payload) != dispatch.payload_bytes
                or "sha256:" + sha256(payload).hexdigest() != dispatch.payload_sha256
            ):
                raise CompositionAdmissionError("clickhouse_dispatch_payload_identity")
        elif payload:
            raise CompositionAdmissionError("clickhouse_dispatch_unexpected_payload")
        path = clickhouse_http_path(query_id=dispatch.query_id, statement=_statement(dispatch))
        self._journal.claim_once(dispatch)
        try:
            observed = self._http.request(path=path, payload=payload, query_id=dispatch.query_id)
            if observed.body:
                raise ClickHouseHttpError(observed.request_body_bytes)
        except ClickHouseHttpError as error:
            raise ClickHouseDispatchTransportError(
                dispatch_sha256=dispatch_hash, request_body_bytes=error.request_body_bytes
            ) from None
        return ClickHouseDispatchObservation(
            dispatch_hash,
            dispatch.claim_key,
            dispatch.query_id,
            observed.request_body_bytes,
            len(observed.body),
            "sha256:" + sha256(observed.body).hexdigest(),
            observed.framing,
        )
