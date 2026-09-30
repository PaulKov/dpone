"""Admitted SQL retirement store. No provisioning, dispatch or retry capability.

The application service must hold authenticated deployment observations and a
durable attempt claim. This adapter admits the catalog/binding and verifies the
exact SQL receipt. Synthetic observations cannot certify a deployed migration.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from dpone.ports.mssql_publication import publication_binding_digest, publication_slot_key
from dpone.ports.publication_retirement import plan_retirement, require_retirement_history, retirement_record
from dpone.runtime.state.mssql_publication_admission import require_publication_catalog
from dpone.runtime.state.mssql_publication_envelope import decode_envelope
from dpone.runtime.state.mssql_publication_transaction import (
    PublicationTransactionUnknown,
    execute_publication_transaction,
)
from dpone.runtime.state.mssql_retirement_queries import (
    retirement_params,
    retirement_read_statement,
    retirement_statement,
)

if TYPE_CHECKING:
    from dpone.ports.mssql_publication import (
        PublicationAuthorityBinding,
        PublicationCatalogReader,
        PublicationSessionFactory,
    )
    from dpone.ports.publication_retirement import PublicationRetirementPlan, RetirementInspection, RetirementWrite


class MssqlPublicationRetirement:
    """A separate absent-only capability, deliberately not a native authority."""

    def __init__(
        self,
        *,
        catalog_connector: PublicationCatalogReader,
        session_factory: PublicationSessionFactory,
        binding: PublicationAuthorityBinding,
        endpoint_identity: str,
        clock: Callable[[], int],
        write_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._binding = binding
        self._digest = publication_binding_digest(binding, endpoint_identity=endpoint_identity)
        require_publication_catalog(catalog_connector, binding=binding)
        self._sessions, self._clock, self._write_id = session_factory, clock, write_id_factory

    def _require_plan(self, plan: PublicationRetirementPlan, *, writing: bool) -> None:
        if writing:
            plan_retirement(plan.observation, now=self._clock())
        else:
            require_retirement_history(plan)
        if plan.observation.binding_digest != self._digest:
            raise ValueError("retirement destination binding differs")
        if len(plan.payload.encode()) > 1024 * 1024:
            raise ValueError("retirement plan exceeds admitted artifact size")

    def inspect(self, plan: PublicationRetirementPlan) -> RetirementInspection:
        """Read only; ambiguous or corrupt history is not absence or success."""
        self._require_plan(plan, writing=False)

        def validate(rows: list[tuple[Any, ...]]) -> RetirementInspection:
            if not rows:
                return "absent"
            _, exact = self._verify(rows, plan)
            return "exact" if exact else "conflict"

        try:
            return execute_publication_transaction(
                self._sessions,
                "DECLARE @slot char(64)=?;\n" + retirement_read_statement(self._binding),
                (publication_slot_key(self._binding, plan.original.record.target_key),),
                validate=validate,
            )
        except PublicationTransactionUnknown:
            return "unknown"

    def retire_if_absent(self, plan: PublicationRetirementPlan) -> RetirementWrite:
        """One transaction, never UPDATE, never a recovered dispatch permit."""
        self._require_plan(plan, writing=True)
        written = retirement_record(plan, write_id=self._write_id())

        def validate(rows: list[tuple[Any, ...]]) -> RetirementWrite:
            won, exact = self._verify(rows, plan)
            if won and (not exact or rows[0][2] != written.payload.encode()):
                raise ValueError("retirement receipt does not match this invocation")
            return "acknowledged" if won else "conflict"

        try:
            return execute_publication_transaction(
                self._sessions,
                retirement_statement(self._binding),
                retirement_params(self._binding, self._digest, written, plan.payload.encode()),
                validate=validate,
            )
        except PublicationTransactionUnknown:
            return "unknown"

    def _verify(self, rows: list[tuple[Any, ...]], plan: PublicationRetirementPlan) -> tuple[bool, bool]:
        if len(rows) != 1 or len(rows[0]) != 13:
            raise ValueError("ambiguous retirement receipt")
        (
            won,
            revision,
            payload,
            digest,
            write_id,
            binding,
            event,
            chain,
            origin,
            provenance,
            provenance_hash,
            operation,
            phase,
        ) = rows[0]
        if won not in (0, 1) or type(revision) is not int or not 0 < revision < 2**63 or event != 1 or chain != 1:
            raise ValueError("retirement revision/history differs")
        record = decode_envelope(payload)
        if (
            binding != bytes.fromhex(self._digest)
            or hashlib.sha256(payload).digest() != digest
            or hashlib.sha256(provenance).digest() != provenance_hash
            or record.operation_id != operation
            or record.phase.value != phase
            or record.authority_write_id != UUID(str(write_id)).hex
        ):
            raise ValueError("retirement envelope integrity differs")
        expected = retirement_record(plan, write_id=UUID(str(write_id)))
        return bool(won), (
            revision == 1 and record == expected and origin == plan.origin and provenance == plan.payload.encode()
        )
