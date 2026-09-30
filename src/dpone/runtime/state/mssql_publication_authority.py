"""MSSQL authority adapter with exact CAS receipts and commit-ACK-only permits.

Not enabled by the public runtime factory until deployment admission, adoption
and live fault acceptance are implemented. No table provisioning or fallback.
"""

from __future__ import annotations

import hashlib
from collections.abc import Callable
from dataclasses import replace
from typing import Any
from uuid import UUID, uuid4

from dpone.ports.clickhouse_cluster_publication import contracts as c
from dpone.ports.mssql_publication import (
    PublicationAuthorityBinding,
    PublicationCatalogReader,
    PublicationSessionFactory,
    native_publication_provenance,
    publication_binding_digest,
    publication_slot_key,
)
from dpone.ports.publication_retirement import decode_retirement_plan, retirement_record
from dpone.runtime.state.mssql_publication_admission import require_publication_catalog
from dpone.runtime.state.mssql_publication_envelope import decode_envelope, require_transition
from dpone.runtime.state.mssql_publication_queries import (
    mutation_params,
    mutation_statement,
    operation_read_statement,
    read_statement,
)
from dpone.runtime.state.mssql_publication_transaction import (
    PublicationTransactionUnknown,
    execute_publication_transaction,
)


class MssqlPublicationAuthority:
    """Short dedicated transactions; this class never owns a business session.

    Composition supplies independently admitted SQL endpoint identity and a
    factory of fresh DBAPI sessions to that same endpoint/location. Structural
    admission is mandatory. Native writes cannot import historical progress.
    """

    def __init__(
        self,
        *,
        catalog_connector: PublicationCatalogReader,
        session_factory: PublicationSessionFactory,
        binding: PublicationAuthorityBinding,
        endpoint_identity: str,
        write_id_factory: Callable[[], UUID] = uuid4,
    ) -> None:
        self._binding = binding
        self._digest = publication_binding_digest(binding, endpoint_identity=endpoint_identity)
        require_publication_catalog(catalog_connector, binding=binding)
        self._session_factory = session_factory
        self._write_id = write_id_factory

    def read_versioned(self, target_key: str) -> c.VersionedAuthorityRecord | None:
        """Return an exact envelope or absence; never recover a dispatch permit."""
        slot = publication_slot_key(self._binding, target_key)

        def validate(rows: list[tuple[Any, ...]]) -> c.VersionedAuthorityRecord | None:
            return None if not rows else self._decode_receipt(rows, target_key)[1]

        try:
            return execute_publication_transaction(
                self._session_factory,
                "DECLARE @slot char(64)=?;\n" + read_statement(self._binding),
                (slot,),
                validate=validate,
            )
        except PublicationTransactionUnknown:
            raise c.ClusterPublicationError(
                "DPONE_MSSQL_PUBLICATION_READ_UNKNOWN", "authority could not be verified"
            ) from None

    def create_if_absent(self, record: c.AuthorityRecord) -> c.AuthorityMutationResult:
        return self._mutate(None, record)

    def read_for_operation(self, target_key: str, operation_id: str) -> c.VersionedAuthorityRecord | None:
        """Validate the immutable root before admitting any work for this ID.

        The first retirement event remains authoritative after later native
        publications. This read is not a reservation or a dispatch permission;
        mutation must repeat its history guard under the exclusive slot lock.
        """
        if not isinstance(operation_id, str) or not 0 < len(operation_id) <= 128:
            raise ValueError("invalid publication operation identity")

        def validate(
            rows: list[tuple[Any, ...]],
        ) -> tuple[c.VersionedAuthorityRecord, c.VersionedAuthorityRecord] | None:
            if not rows:
                return None
            _, current, root = self._decode_history(rows, target_key)
            return current, root

        try:
            observed = execute_publication_transaction(
                self._session_factory,
                operation_read_statement(self._binding),
                (publication_slot_key(self._binding, target_key),),
                validate=validate,
            )
        except PublicationTransactionUnknown:
            raise c.ClusterPublicationError(
                "DPONE_MSSQL_PUBLICATION_READ_UNKNOWN", "operation history could not be verified"
            ) from None
        if observed is None:
            return None
        current, root = observed
        if root.record.phase is c.AuthorityPhase.RETIRED_UNPUBLISHED and root.record.operation_id == operation_id:
            raise c.ClusterPublicationError(
                "DPONE_MSSQL_PUBLICATION_RETIRED_OPERATION", "retired operation cannot be admitted again"
            )
        return current

    def compare_and_swap(
        self, current: c.VersionedAuthorityRecord, desired: c.AuthorityRecord
    ) -> c.AuthorityMutationResult:
        return self._mutate(current, desired)

    def _mutate(
        self, current: c.VersionedAuthorityRecord | None, desired: c.AuthorityRecord
    ) -> c.AuthorityMutationResult:
        dispatch = require_transition(current, desired)
        written = replace(desired, authority_write_id=self._write_id().hex)
        params = mutation_params(self._binding, self._digest, current, written)
        expected_revision = current.version + 1 if current else 1

        def validate(rows: list[tuple[Any, ...]]) -> tuple[bool, c.VersionedAuthorityRecord]:
            won, observed, root = self._decode_history(rows, written.target_key)
            if root.record.phase is c.AuthorityPhase.RETIRED_UNPUBLISHED:
                if root.record.operation_id == written.operation_id:
                    raise ValueError("retired operation cannot be mutated again")
                if won and current is not None and current.record.phase is c.AuthorityPhase.RETIRED_UNPUBLISHED:
                    if root != current:
                        raise ValueError("fresh publication retirement provenance differs")
            if won and (observed.record != written or observed.version != expected_revision):
                raise ValueError("publication write receipt does not match this invocation")
            return won, observed

        try:
            won, observed = execute_publication_transaction(
                self._session_factory, mutation_statement(self._binding), params, validate=validate
            )
        except PublicationTransactionUnknown:
            return c.AuthorityMutationResult(c.AuthorityMutationStatus.OUTCOME_UNKNOWN)
        if not won:
            return c.AuthorityMutationResult(c.AuthorityMutationStatus.CONFLICT, observed=observed)
        permit = c.DispatchPermit.for_record(written) if dispatch else None
        return c.AuthorityMutationResult(c.AuthorityMutationStatus.VERIFIED, observed=observed, permit=permit)

    def _decode_history(
        self, rows: list[tuple[Any, ...]], target_key: str
    ) -> tuple[bool, c.VersionedAuthorityRecord, c.VersionedAuthorityRecord]:
        """Authenticate current state and fixed-key origin before transaction ACK."""
        if len(rows) != 1 or len(rows[0]) != 26:
            raise ValueError("ambiguous publication root receipt")
        won, current = self._decode_receipt([rows[0][:13]], target_key)
        root = self._decode_receipt([rows[0][13:]], target_key)[1]
        if root.version != 1 or (current.version == 1 and current != root):
            raise ValueError("publication root identity differs")
        if root.record.phase is not c.AuthorityPhase.RETIRED_UNPUBLISHED:
            require_transition(None, root.record)
        return won, current, root

    def _decode_receipt(self, rows: list[tuple[Any, ...]], target_key: str) -> tuple[bool, c.VersionedAuthorityRecord]:
        if len(rows) != 1 or len(rows[0]) != 13:
            raise ValueError("ambiguous publication receipt")
        (
            won,
            revision,
            payload,
            digest,
            write_id,
            binding,
            event_ok,
            chain_ok,
            origin,
            provenance,
            provenance_hash,
            operation,
            phase,
        ) = rows[0]
        if won not in (0, 1) or type(revision) is not int or not 0 < revision < 2**63:
            raise ValueError("invalid publication receipt revision")
        if binding != bytes.fromhex(self._digest) or event_ok != 1 or chain_ok != 1:
            raise ValueError("publication binding/history mismatch")
        record = decode_envelope(payload)
        if (
            record.target_key != target_key
            or record.operation_id != operation
            or record.phase.value != phase
            or hashlib.sha256(payload).digest() != digest
            or record.authority_write_id != UUID(str(write_id)).hex
            or hashlib.sha256(provenance).digest() != provenance_hash
        ):
            raise ValueError("publication envelope identity differs")
        self._require_origin(origin, provenance, revision, record)
        return bool(won), c.VersionedAuthorityRecord(record, revision)

    def _require_origin(self, origin: str, provenance: bytes, revision: int, record: c.AuthorityRecord) -> None:
        if origin == "legacy_retired":
            plan = decode_retirement_plan(provenance)
            if (
                revision != 1
                or plan.observation.binding_digest != self._digest
                or retirement_record(plan, write_id=UUID(hex=record.authority_write_id or "")) != record
            ):
                raise ValueError("retirement origin differs from publication slot")
            return
        expected = native_publication_provenance(self._digest)
        if origin != "native" or provenance != expected or record.phase is c.AuthorityPhase.RETIRED_UNPUBLISHED:
            raise ValueError("publication origin requires explicit adoption")
