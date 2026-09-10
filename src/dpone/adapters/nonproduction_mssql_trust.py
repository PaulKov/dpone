"""Read independently provisioned SQL trust; no grant consumption or enrollment.

The injected protected connection and service/environment pins come from the
composition root, never artifacts. A frozen revision is comparison data, not a
permit. Authentication still verifies original signatures, clock and revocation;
later issuance must compare this revision inside its own admission transaction.
"""

from __future__ import annotations

from dataclasses import dataclass, field

from dpone.adapters.composition_mssql_attempts import ConnectionFactory, composition_control_transaction
from dpone.adapters.composition_mssql_schema import (
    COMPOSITION_MSSQL_LEDGER_LOCK,
    COMPOSITION_MSSQL_SCHEMA_VERSION,
    require_control_schema,
)
from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.nonproduction_mssql_schema import (
    NONPRODUCTION_MSSQL_SCHEMA_VERSION,
    require_nonproduction_mssql_schema,
)
from dpone.contracts.nonproduction_authority import NonproductionAuthorityPolicy
from dpone.contracts.nonproduction_scope import MAX_DOCUMENT_BYTES, NonproductionAuthorityError, uuid_text
from dpone.ports.nonproduction_authentication import NonproductionTrustSnapshot


@dataclass(frozen=True, slots=True)
class NonproductionTrustRevision:
    """Documentary version from protected storage, never caller authority.

    Revision changes detect A→B→A even when the latest policy bytes match an
    earlier version. The existing snapshot tuple API intentionally stays intact;
    callers must bracket authentication with this independent revision and a
    same-transaction comparison before consumption/admission writes.
    """

    service_id: str
    environment_id: str
    revision: int
    snapshot: NonproductionTrustSnapshot = field(repr=False)

    def __post_init__(self) -> None:
        uuid_text(self.service_id)
        uuid_text(self.environment_id)
        if type(self.revision) is not int or not 1 <= self.revision <= 2**63 - 1:
            raise NonproductionAuthorityError("trust_revision")
        if type(self.snapshot) is not NonproductionTrustSnapshot:
            raise NonproductionAuthorityError("trust_snapshot")
        self.snapshot.require_integrity()
        policy = NonproductionAuthorityPolicy.from_bytes(
            self.snapshot.policy_bytes, expected_sha256=self.snapshot.policy_sha256
        )
        if policy.environment_id != self.environment_id:
            raise NonproductionAuthorityError("trust_environment")
        if policy.revocation_epoch != self.snapshot.current_revocation_epoch:
            raise NonproductionAuthorityError("trust_revocation_epoch")


class MssqlNonproductionTrustProvider:
    """Fresh read-only SQL observations from an externally protected authority.

    Provisioners append under the same global transaction lock as admission.
    No runtime write, installation, repair, grant-selected lookup, signature
    verification, implicit retry or mutable head pointer is provided here.
    Verifier-policy semantics/root freshness remain the authenticator's job.
    """

    def __init__(
        self,
        connection_factory: ConnectionFactory,
        *,
        expected_service_id: str,
        expected_environment_id: str,
        control_schema: str = "dpone_control",
    ) -> None:
        uuid_text(expected_service_id)
        uuid_text(expected_environment_id)
        self._factory = connection_factory
        self._service_id = expected_service_id
        self._environment_id = expected_environment_id
        self._schema = require_control_schema(control_schema)

    def read(self) -> NonproductionTrustSnapshot:
        """Preserve the existing authenticator API; this is not an admission grant."""
        return self.read_revision().snapshot

    def read_revision(self) -> NonproductionTrustRevision:
        """Read once on a fresh connection; uncertain commit returns no observation.

        This transaction performs no DML. Best-effort cleanup never certifies
        connection closure; subsequent admission requires a fresh locked compare.
        """
        try:
            with composition_control_transaction(self._factory, self._schema, self._service_id) as ledger:
                observed = self.read_revision_in(ledger)
            return observed
        except NonproductionAuthorityError:
            raise
        except Exception:
            raise NonproductionAuthorityError("trust_read_unavailable") from None

    def read_revision_in(self, ledger: CompositionMssqlLedger) -> NonproductionTrustRevision:
        """Read inside an existing protected transaction/lock, without committing.

        Recheck externally pinned control identity and complete schema. Caller
        must retain this transaction through its later consumption/RUNNING writes;
        merely constructing a ledger object cannot establish the precondition.
        """
        try:
            self._require_ledger(ledger)
            require_nonproduction_mssql_schema(ledger.cursor, self._schema)
            ledger.cursor.execute(
                "SELECT TOP (1) LOWER(CONVERT(char(36), environment_id)), revision, schema_version, "
                f"CASE WHEN DATALENGTH(policy_document) BETWEEN 1 AND {MAX_DOCUMENT_BYTES} "
                "THEN policy_document END, policy_sha256, "
                f"CASE WHEN DATALENGTH(verifier_policy_document) BETWEEN 1 AND {MAX_DOCUMENT_BYTES} "
                "THEN verifier_policy_document END, verifier_policy_sha256, current_revocation_epoch "
                f"FROM {ledger.table('nonproduction_trust')} WITH (HOLDLOCK) "
                "WHERE environment_id = ? ORDER BY revision DESC;",
                self._environment_id,
            )
            records = tuple(tuple(value) for value in ledger.cursor.fetchall())
            if len(records) != 1 or len(records[0]) != 8:
                raise NonproductionAuthorityError("trust_enrollment_missing")
            environment, revision, version, policy, policy_hash, verifier, verifier_hash, epoch = records[0]
            if type(version) is not int or version != NONPRODUCTION_MSSQL_SCHEMA_VERSION:
                raise NonproductionAuthorityError("trust_schema_version")
            if environment != self._environment_id:
                raise NonproductionAuthorityError("trust_environment")
            snapshot = NonproductionTrustSnapshot(policy, policy_hash, verifier, verifier_hash, epoch)
            return NonproductionTrustRevision(self._service_id, environment, revision, snapshot)
        except NonproductionAuthorityError:
            raise
        except Exception:
            raise NonproductionAuthorityError("trust_read_unavailable") from None

    def require_revision_in(
        self, ledger: CompositionMssqlLedger, expected: NonproductionTrustRevision
    ) -> NonproductionTrustRevision:
        """Compare latest full bytes and monotonic revision in the caller's ledger.

        Expected data must originate from a trusted read preceding actual grant
        authentication. This comparison cannot turn a caller DTO into authority.
        No separate transaction or commit is introduced before future writes.
        """
        if type(expected) is not NonproductionTrustRevision:
            raise NonproductionAuthorityError("trust_revision_subject")
        expected.__post_init__()
        observed = self.read_revision_in(ledger)
        if observed != expected:
            raise NonproductionAuthorityError("trust_revision_changed")
        return observed

    def _require_ledger(self, ledger: CompositionMssqlLedger) -> None:
        if type(ledger) is not CompositionMssqlLedger or ledger.schema != self._schema:
            raise NonproductionAuthorityError("trust_ledger")
        ledger.cursor.execute(
            "SELECT @@TRANCOUNT, XACT_STATE(), APPLOCK_MODE(N'public', ?, N'Transaction');",
            COMPOSITION_MSSQL_LEDGER_LOCK,
        )
        records = tuple(tuple(value) for value in ledger.cursor.fetchall())
        if (
            len(records) != 1
            or len(records[0]) != 3
            or type(records[0][0]) is not int
            or records[0][0] < 1
            or type(records[0][1]) is not int
            or records[0][1] != 1
            or records[0][2] != "Exclusive"
        ):
            raise NonproductionAuthorityError("trust_ledger_lock")
        ledger.cursor.execute(
            "SELECT singleton, schema_version, LOWER(CONVERT(char(36), service_id)) "
            f"FROM {ledger.table('authority')} WITH (HOLDLOCK);"
        )
        if tuple(tuple(value) for value in ledger.cursor.fetchall()) != (
            (1, COMPOSITION_MSSQL_SCHEMA_VERSION, self._service_id),
        ):
            raise NonproductionAuthorityError("trust_control_authority")
