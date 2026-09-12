"""Admitted query and mutation execution on one bound physical transaction."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass
from threading import Lock
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID

from dpone.contracts.mssql_r1_v3_effect import MssqlR1EffectAttemptEnvelopeV3
from dpone.contracts.mssql_r1_v3_registration import MssqlTargetRegistrationPayloadV1
from dpone.contracts.mssql_r1_v3_rendered import (
    MssqlR1RenderedStatementV1,
    MssqlR1VerifiedRenderedMutationV1,
)

if TYPE_CHECKING:
    from dpone.ports.mssql_r1_v3 import (
        MssqlR1RenderedMutationVerifierPort,
        MssqlR1RendererAuthorityResolverPort,
        MssqlR1TransactionV3,
    )


class MssqlR1V3ProviderAdmissionError(RuntimeError):
    """The retained effect or physical session failed independent admission."""


class MssqlR1V3RenderedAdmissionAdapter:
    """Rebuild trust in retained SQL using the closed environment-owned verifier.

    The injected verifier, rather than request-carried evidence or statement
    digests, owns exact certified-template comparison.
    """

    def __init__(
        self,
        authority_resolver: MssqlR1RendererAuthorityResolverPort,
        verifier: MssqlR1RenderedMutationVerifierPort,
    ) -> None:
        self._authority_resolver = authority_resolver
        self._verifier = verifier

    def admit(self, attempt: MssqlR1EffectAttemptEnvelopeV3) -> tuple[MssqlR1RenderedStatementV1, ...]:
        if not isinstance(attempt, MssqlR1EffectAttemptEnvelopeV3):
            raise MssqlR1V3ProviderAdmissionError("postgres_mssql_r1.exact_v3_attempt_required")
        request = attempt.request
        registration = MssqlTargetRegistrationPayloadV1.from_canonical_bytes(request.registration_payload_bytes)
        if registration.payload_digest != request.registration_payload_digest:
            raise MssqlR1V3ProviderAdmissionError("postgres_mssql_r1.registration_payload_mismatch")
        authority = self._authority_resolver.resolve(registration.resolved_profile_digest)
        verified = self._verifier.verify(request.mutation_plan, request.rendered_bundle, authority)
        if not isinstance(verified, MssqlR1VerifiedRenderedMutationV1) or (
            verified.plan != request.mutation_plan
            or verified.bundle != request.rendered_bundle
            or verified.authority != authority
            or verified.resolved_profile_digest != registration.resolved_profile_digest
        ):
            raise MssqlR1V3ProviderAdmissionError("postgres_mssql_r1.rendered_mutation_reproof_mismatch")
        return verified.bundle.statements


@dataclass(frozen=True, slots=True)
class _HandleBinding:
    handle: object
    session_identity: object


class MssqlR1V3TransactionHandleBinder:
    """Bind one V3 transaction ID to one exact handle and physical session.

    Construct one binder for the whole target UoW and share it between every
    mutation and quality adapter participating in that transaction.
    """

    def __init__(
        self,
        handle_for: Callable[[MssqlR1TransactionV3], object],
        session_identity_for: Callable[[object], object],
    ) -> None:
        self._handle_for = handle_for
        self._session_identity_for = session_identity_for
        self._bindings: dict[UUID, _HandleBinding] = {}
        self._lock = Lock()

    def resolve(self, transaction: MssqlR1TransactionV3) -> object:
        transaction_id = transaction.transaction_id
        if not isinstance(transaction_id, UUID):
            raise MssqlR1V3ProviderAdmissionError("postgres_mssql_r1.transaction_identity_invalid")
        handle = self._handle_for(transaction)
        session_identity = self._session_identity_for(handle)
        if session_identity is None:
            raise MssqlR1V3ProviderAdmissionError("postgres_mssql_r1.physical_session_identity_missing")
        with self._lock:
            expected = self._bindings.get(transaction_id)
            if expected is None:
                self._bindings[transaction_id] = _HandleBinding(handle, session_identity)
            elif expected.handle is not handle or expected.session_identity != session_identity:
                raise MssqlR1V3ProviderAdmissionError("postgres_mssql_r1.transaction_handle_changed")
        return handle


class MssqlR1V3OdbcQualityQueryAdapter:
    """Execute exact admitted SQL and return at most two rows for cardinality proof."""

    def __init__(self, binder: MssqlR1V3TransactionHandleBinder) -> None:
        self._binder = binder

    def query(
        self,
        transaction: MssqlR1TransactionV3,
        statement: object,
    ) -> tuple[tuple[object, ...], ...]:
        if not isinstance(statement, MssqlR1RenderedStatementV1):
            raise TypeError("postgres_mssql_r1.retained_statement_required")
        handle = self._binder.resolve(transaction)
        result = cast(Any, handle).execute(statement.statement_utf8_bytes.decode("utf-8"))
        reader = result if hasattr(result, "fetchone") else handle
        rows: list[tuple[object, ...]] = []
        for _ in range(2):
            row = cast(Any, reader).fetchone()
            if row is None:
                break
            rows.append(tuple(row))
        return tuple(rows)


class MssqlR1V3OdbcMutationCommandAdapter:
    """Execute an already admitted statement on the supplied transaction handle."""

    def __init__(self, binder: MssqlR1V3TransactionHandleBinder) -> None:
        self._binder = binder

    def execute(
        self,
        transaction: MssqlR1TransactionV3,
        statement: object,
    ) -> None:
        if not isinstance(statement, MssqlR1RenderedStatementV1):
            raise TypeError("postgres_mssql_r1.retained_statement_required")
        handle = self._binder.resolve(transaction)
        cast(Any, handle).execute(statement.statement_utf8_bytes.decode("utf-8"))


__all__ = [
    "MssqlR1V3OdbcMutationCommandAdapter",
    "MssqlR1V3OdbcQualityQueryAdapter",
    "MssqlR1V3ProviderAdmissionError",
    "MssqlR1V3RenderedAdmissionAdapter",
    "MssqlR1V3TransactionHandleBinder",
]
