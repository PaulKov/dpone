"""Same-ledger grant originals and complete membership, without worker authority.

The trusted transaction owner must authenticate original signatures beforehand,
independently reconstruct scope, retain the existing exclusive ledger lock,
rollback the whole transaction on any failure, then commit once and independently
read back. These methods never connect, commit, rollback, install or issue. A
successful return inside a transaction is not an acknowledged durable result.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from datetime import datetime, timezone
from typing import Any

from dpone.adapters.composition_mssql_store_queries import CompositionMssqlLedger
from dpone.adapters.nonproduction_mssql_registration_schema import (
    REGISTRATION_COLUMNS,
    registration_projection_sql,
    require_nonproduction_registration_insert_options,
    require_nonproduction_registration_schema,
)
from dpone.adapters.nonproduction_mssql_trust import MssqlNonproductionTrustProvider, NonproductionTrustRevision
from dpone.contracts.nonproduction_authority import utc_timestamp
from dpone.contracts.nonproduction_grants import (
    NonproductionExecutionGrant,
)
from dpone.contracts.nonproduction_registration import (
    NonproductionGrantRegistration,
    NonproductionRegistrationOriginals,
    original_sha256,
    require_execution_memberships,
    require_membership_history,
    workload_document,
)
from dpone.contracts.nonproduction_scope import NonproductionAuthorityError, digest
from dpone.ports.nonproduction_authentication import NonproductionTrustSnapshot


def _values(record: NonproductionGrantRegistration) -> tuple[Any, ...]:
    originals, grant = record.originals, record.originals.grant
    subject_id = grant.activation_id if isinstance(grant, NonproductionExecutionGrant) else grant.qualification_run_id
    return (
        grant.consumption_subject_sha256,
        grant.grant_id,
        grant.phase,
        grant.scope.environment_id,
        grant.scope.campaign_id,
        subject_id,
        1,
        originals.grant_bytes,
        originals.grant_sha256,
        originals.bundle_bytes,
        originals.bundle_sha256,
        originals.signature_subject_bytes,
        original_sha256(originals.signature_subject_bytes),
        originals.request_bytes,
        originals.request_sha256,
        record.trust_revision,
        record.registered_at,
    )


class MssqlNonproductionRegistrationStore:
    """Protected storage only; the current externally pinned trust is mandatory.

    Existing exact execution registration is a historical read, even when its
    grant has since expired. Qualification consumption always rejects repetition;
    read_in remains available for audit. Neither path repeats signature checks or
    authorizes a worker. The later private coordinator must invoke real crypto.
    """

    def __init__(self, trust_provider: MssqlNonproductionTrustProvider, *, clock: Callable[[], datetime]) -> None:
        if type(trust_provider) is not MssqlNonproductionTrustProvider or not callable(clock):
            raise NonproductionAuthorityError("registration_dependencies")
        self._trust, self._clock = trust_provider, clock

    def read_in(
        self,
        ledger: CompositionMssqlLedger,
        consumption_subject_sha256: str,
        *,
        expected_revision: NonproductionTrustRevision,
    ) -> NonproductionGrantRegistration | None:
        """Historical exact read and complete pool audit; no fresh permission.

        Current revision/lock are checked, while old originals are validated at
        their original immutable trust revision and registration time. Revocation
        cannot erase historical evidence. Caller controls the transaction lifetime.
        """
        try:
            current, now = self._begin(ledger, expected_revision)
            digest(consumption_subject_sha256)
            return self._read(ledger, consumption_subject_sha256, current, now)
        except NonproductionAuthorityError:
            raise
        except Exception:
            raise NonproductionAuthorityError("registration_unavailable") from None

    def register_execution_in(
        self,
        ledger: CompositionMssqlLedger,
        *,
        request_bytes: bytes,
        grant_bytes: bytes,
        signature_bundle: bytes,
        signature_subject_bytes: bytes,
        expected_revision: NonproductionTrustRevision,
    ) -> NonproductionGrantRegistration:
        """Bind execution originals and every complete-parent member atomically.

        Exact repeat returns documentary history without charging twice. Changed
        original grant, request, bundle or subject under the same key rejects.
        The caller must rollback the whole transaction if any insert/read fails.
        """
        return self._register(
            ledger,
            grant_bytes,
            signature_bundle,
            signature_subject_bytes,
            request_bytes,
            expected_revision,
            "execution",
        )

    def consume_qualification_in(
        self,
        ledger: CompositionMssqlLedger,
        *,
        grant_bytes: bytes,
        signature_bundle: bytes,
        signature_subject_bytes: bytes,
        expected_revision: NonproductionTrustRevision,
    ) -> NonproductionGrantRegistration:
        """Consume the exact qualification run once; never invent work-item IDs.

        No workload/attempt/export charge or seed/read permission is produced.
        Deterministic compilation may subsequently use the separate historical read.
        """
        return self._register(
            ledger, grant_bytes, signature_bundle, signature_subject_bytes, None, expected_revision, "qualification"
        )

    def _begin(
        self, ledger: CompositionMssqlLedger, expected: NonproductionTrustRevision
    ) -> tuple[NonproductionTrustRevision, datetime]:
        current = self._trust.require_revision_in(ledger, expected)
        require_nonproduction_registration_schema(ledger.cursor, ledger.schema)
        return current, self._now()

    def _now(self) -> datetime:
        try:
            now = self._clock()
            if type(now) is not datetime or now.tzinfo is None or now.utcoffset() is None:
                raise ValueError
            return now.astimezone(timezone.utc)  # noqa: UP017
        except Exception:
            raise NonproductionAuthorityError("registration_clock") from None

    def _register(
        self,
        ledger: CompositionMssqlLedger,
        grant_bytes: bytes,
        bundle: bytes,
        subject: bytes,
        request: bytes | None,
        expected: NonproductionTrustRevision,
        phase: str,
    ) -> NonproductionGrantRegistration:
        try:
            current, now = self._begin(ledger, expected)
            originals = NonproductionRegistrationOriginals(grant_bytes, bundle, subject, request)
            grant = originals.grant
            if grant.phase != phase or grant.scope.environment_id != current.environment_id:
                raise NonproductionAuthorityError("registration_phase_environment")
            prior = self._read(ledger, grant.consumption_subject_sha256, current, now)
            if prior is not None:
                if prior.originals != originals:
                    raise NonproductionAuthorityError("registration_rebind")
                if phase == "qualification":
                    raise NonproductionAuthorityError("qualification_consumed")
                return prior
            policy = originals.require_policy(
                current.snapshot.policy_bytes,
                current.snapshot.policy_sha256,
                current.snapshot.current_revocation_epoch,
                now,
            )
            members = self._pool(ledger, grant.scope.campaign_id, phase, current, now)
            if type(grant) is NonproductionExecutionGrant:
                require_execution_memberships(grant, policy, tuple(sorted(members)))
            record = NonproductionGrantRegistration(originals, current.revision, now.strftime("%Y-%m-%dT%H:%M:%SZ"))
            subject_id = _values(record)[5]
            if phase == "qualification" and self._rows(ledger, "phase = ? AND phase_subject_id = ?", phase, subject_id):
                raise NonproductionAuthorityError("registration_subject_reuse")
            finished = self._now()
            if finished < now:
                raise NonproductionAuthorityError("registration_clock")
            originals.require_policy(
                current.snapshot.policy_bytes,
                current.snapshot.policy_sha256,
                current.snapshot.current_revocation_epoch,
                finished,
            )
            require_nonproduction_registration_insert_options(ledger.cursor)
            ledger.cursor.execute(
                f"INSERT INTO {ledger.table('nonproduction_grants')} "
                f"({', '.join(REGISTRATION_COLUMNS)}) VALUES ({', '.join('?' for _ in REGISTRATION_COLUMNS)});",
                *_values(record),
            )
            if type(grant) is NonproductionExecutionGrant:
                for workload in grant.workloads:
                    if workload.workload_id not in members:
                        document = workload_document(workload.workload_id)
                        ledger.cursor.execute(
                            f"INSERT INTO {ledger.table('nonproduction_memberships')} "
                            "(environment_id, campaign_id, phase, workload_sha256, workload_document, "
                            "first_grant_sha256) VALUES (?, ?, ?, ?, ?, ?);",
                            current.environment_id,
                            grant.scope.campaign_id,
                            phase,
                            original_sha256(document),
                            document,
                            grant.consumption_subject_sha256,
                        )
            readback = self._read(ledger, grant.consumption_subject_sha256, current, finished)
            if readback != record:
                raise NonproductionAuthorityError("registration_readback")
            return record
        except NonproductionAuthorityError:
            raise
        except Exception:
            raise NonproductionAuthorityError("registration_unavailable") from None

    @staticmethod
    def _rows(ledger: CompositionMssqlLedger, where: str, *parameters: object) -> tuple[tuple[Any, ...], ...]:
        ledger.cursor.execute(
            f"SELECT TOP (2) {registration_projection_sql()} FROM {ledger.table('nonproduction_grants')} WITH (HOLDLOCK) "
            f"WHERE {where} ORDER BY consumption_subject_sha256;",
            *parameters,
        )
        return tuple(tuple(value) for value in ledger.cursor.fetchall())

    def _read(
        self, ledger: CompositionMssqlLedger, key: str, current: NonproductionTrustRevision, now: datetime
    ) -> NonproductionGrantRegistration | None:
        rows = self._rows(ledger, "consumption_subject_sha256 = ?", key)
        if not rows:
            return None
        if len(rows) != 1:
            raise NonproductionAuthorityError("registration_row")
        record = self._decode(ledger, rows[0], current, now)
        if record.originals.grant.consumption_subject_sha256 != key:
            raise NonproductionAuthorityError("registration_identity")
        self._pool(ledger, record.originals.grant.scope.campaign_id, record.originals.grant.phase, current, now)
        return record

    def _decode(
        self, ledger: CompositionMssqlLedger, row: tuple[Any, ...], current: NonproductionTrustRevision, now: datetime
    ) -> NonproductionGrantRegistration:
        if len(row) != len(REGISTRATION_COLUMNS) or type(row[6]) is not int or row[6] != 1:
            raise NonproductionAuthorityError("registration_row")
        originals = NonproductionRegistrationOriginals(row[7], row[9], row[11], row[13])
        record = NonproductionGrantRegistration(originals, row[15], row[16])
        if _values(record) != row or originals.grant.scope.environment_id != current.environment_id:
            raise NonproductionAuthorityError("registration_identity")
        instant = utc_timestamp(record.registered_at)
        if instant > now or record.trust_revision > current.revision:
            raise NonproductionAuthorityError("registration_history")
        historical = self._historical(ledger, current, record.trust_revision)
        originals.require_policy(
            historical.snapshot.policy_bytes,
            historical.snapshot.policy_sha256,
            historical.snapshot.current_revocation_epoch,
            instant,
        )
        return record

    @staticmethod
    def _historical(
        ledger: CompositionMssqlLedger, current: NonproductionTrustRevision, revision: int
    ) -> NonproductionTrustRevision:
        if revision == current.revision:
            return current
        ledger.cursor.execute(
            "SELECT LOWER(CONVERT(char(36), environment_id)), revision, schema_version, "
            "CASE WHEN DATALENGTH(policy_document) BETWEEN 1 AND 1048576 THEN policy_document END, "
            "policy_sha256, CASE WHEN DATALENGTH(verifier_policy_document) BETWEEN 1 AND 1048576 "
            "THEN verifier_policy_document END, verifier_policy_sha256, current_revocation_epoch "
            f"FROM {ledger.table('nonproduction_trust')} WITH (HOLDLOCK) WHERE environment_id = ? AND revision = ?;",
            current.environment_id,
            revision,
        )
        rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
        if (
            len(rows) != 1
            or len(rows[0]) != 8
            or rows[0][:3] != (current.environment_id, revision, 1)
            or type(rows[0][1]) is not int
            or type(rows[0][2]) is not int
        ):
            raise NonproductionAuthorityError("registration_history")
        return NonproductionTrustRevision(
            current.service_id, current.environment_id, revision, NonproductionTrustSnapshot(*rows[0][3:])
        )

    def _pool_records(
        self,
        ledger: CompositionMssqlLedger,
        campaign: str,
        phase: str,
        current: NonproductionTrustRevision,
        now: datetime,
    ) -> Iterator[NonproductionGrantRegistration]:
        """One full original at a time; nested trust reads cannot invalidate a page."""
        previous: str | None = None
        while True:
            after = "" if previous is None else " AND consumption_subject_sha256 > ?"
            ledger.cursor.execute(
                f"SELECT TOP (1) consumption_subject_sha256 FROM {ledger.table('nonproduction_grants')} WITH (HOLDLOCK) "
                f"WHERE environment_id = ? AND campaign_id = ? AND phase = ?{after} "
                "ORDER BY consumption_subject_sha256;",
                current.environment_id,
                campaign,
                phase,
                *((previous,) if previous is not None else ()),
            )
            page = ledger.cursor.fetchone()
            if page is None:
                return
            if len(page) != 1 or digest(page[0]) <= (previous or ""):
                raise NonproductionAuthorityError("registration_pool_order")
            previous = page[0]
            rows = self._rows(ledger, "consumption_subject_sha256 = ?", previous)
            if len(rows) != 1:
                raise NonproductionAuthorityError("registration_row")
            record = self._decode(ledger, rows[0], current, now)
            grant = record.originals.grant
            if (grant.scope.campaign_id, grant.phase, grant.consumption_subject_sha256) != (campaign, phase, previous):
                raise NonproductionAuthorityError("registration_pool")
            yield record

    def _pool(
        self,
        ledger: CompositionMssqlLedger,
        campaign: str,
        phase: str,
        current: NonproductionTrustRevision,
        now: datetime,
    ) -> frozenset[str]:
        """Bound memory, not lock duration: all immutable history is still audited."""
        ledger.cursor.execute(
            "SELECT TOP (65) LOWER(CONVERT(char(36), environment_id)), LOWER(CONVERT(char(36), campaign_id)), phase, "
            "workload_sha256, CASE WHEN DATALENGTH(workload_document) BETWEEN 1 AND 1048576 THEN workload_document END, "
            f"first_grant_sha256 FROM {ledger.table('nonproduction_memberships')} WITH (HOLDLOCK) "
            "WHERE environment_id = ? AND campaign_id = ? AND phase = ? ORDER BY workload_sha256;",
            current.environment_id,
            campaign,
            phase,
        )
        rows = tuple(tuple(row) for row in ledger.cursor.fetchall())
        if len(rows) > 64:
            raise NonproductionAuthorityError("registration_membership_overflow")
        for row in rows:
            if len(row) != 6 or tuple(row[:3]) != (current.environment_id, campaign, phase):
                raise NonproductionAuthorityError("registration_membership")
        return require_membership_history(
            tuple((row[3], row[4], row[5]) for row in rows), self._pool_records(ledger, campaign, phase, current, now)
        )
