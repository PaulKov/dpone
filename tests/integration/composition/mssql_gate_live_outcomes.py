"""Test-only OUTCOME producer from actual client events and reopened SQL rows.

This is not a dpone worker/route producer. It never constructs CLOSED_GATES or
QUIESCENCE: those must already exist from the real gate. Each OUTCOME selects
the protected issuance journal, binds the attempt/epochs, and retains canonical
observation bytes in SQL plus safe JUnit properties before container destruction.
"""

from __future__ import annotations

import json
from contextlib import closing

from tests.integration.composition.mssql_gate_live_provisioning import execute

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_control import (
    CompositionAdmissionError,
    CompositionAttemptProof,
    CompositionProofAuthority,
    composition_attempt_epoch_subject,
    encode_attempt_proof,
)


class AcknowledgementLost(RuntimeError):
    """The injected client failure happened after a real SQL commit completed."""


class OutcomeProducer:
    def __init__(self, case):
        self.case = case
        self.ack_lost = False

    def lose_commit_ack(self, connection):
        """Complete the real DBAPI commit, then lose its acknowledgement once."""
        try:
            connection.commit()
        except Exception:
            raise RuntimeError("synthetic_commit_failed") from None
        self.ack_lost = True
        raise AcknowledgementLost("injected_loss_after_real_commit")

    def record_unknown(self):
        if not self.ack_lost:
            raise CompositionAdmissionError("synthetic_unknown_event_missing")
        return self._persist(
            "COMMIT_UNKNOWN", {"client_event": "injected_loss_after_real_commit", "rows_reconciled": False}
        )

    def reconcile(self, expected):
        """A new SQL connection observes rows; expected values never become facts."""
        rows = self.case.target_sql("SELECT row_id,value FROM [managed].[rows] ORDER BY row_id;")
        state = "SUCCEEDED" if rows == tuple(expected) else "FAILED"
        return self._persist(
            state,
            {
                "client_event": "independent_sql_reconciliation",
                "rows_reconciled": True,
                "observed_rows": rows,
                "expected_rows": tuple(expected),
                "observed_row_count": len(rows),
                "observed_rows_sha256": canonical_fingerprint({"rows": rows}),
            },
        )

    def _persist(self, state, observation):
        case = self.case
        attempt = case.attempt
        # Reopen control observations independently of the worker and gate
        # producer. These scope records are read from the actual protected DB.
        current = case.attempts.read_exact(attempt)
        assert current.state in {"RUNNING", "COMMIT_UNKNOWN"}
        records = case.sql(
            f"SELECT connector,LOWER(CONVERT(char(36),service_id)),principal_id FROM {case.table('issued_authorities')} "
            "WHERE attempt_sha256=? ORDER BY connector,service_id,principal_id;",
            attempt.attempt_sha256,
        )
        authorities = tuple(CompositionProofAuthority(*row) for row in records)
        row = case.gate_row()
        if (
            row is None
            or row[2] != "CLOSED"
            or authorities
            != (CompositionProofAuthority("mssql", case.environment.service_id, "mssql-sid:" + row[0].hex()),)
        ):
            raise CompositionAdmissionError("synthetic_outcome_issued_scope")
        kinds = {
            item[0]
            for item in case.sql(
                f"SELECT kind FROM {case.table('proofs')} WHERE attempt_sha256=?;", attempt.attempt_sha256
            )
        }
        if not {"CLOSED_GATES", "QUIESCENCE"}.issubset(kinds):
            raise CompositionAdmissionError("synthetic_outcome_gate_proofs_missing")
        evidence = {
            "schema": "dpone.synthetic-mssql-outcome-observation.v1",
            "attempt_sha256": attempt.attempt_sha256,
            "activation_request_sha256": attempt.activation_request_sha256,
            "guard_epochs_sha256": composition_attempt_epoch_subject(attempt),
            "database_id": case.pins[0],
            "database_guid": case.pins[1],
            "state": state,
            **observation,
        }
        proof = CompositionAttemptProof(
            "OUTCOME",
            attempt.attempt_sha256,
            attempt.activation_request_sha256,
            composition_attempt_epoch_subject(attempt),
            authorities,
            canonical_fingerprint(evidence),
            state,
        )
        document = json.dumps(evidence, sort_keys=True, separators=(",", ":")).encode()
        with closing(case.environment.connect()) as connection:
            connection.autocommit = False
            try:
                execute(
                    connection,
                    f"INSERT INTO [{case.environment.schema}].[synthetic_outcomes] VALUES (?,?,?);",
                    proof.evidence_sha256,
                    attempt.attempt_sha256,
                    document,
                )
                execute(
                    connection,
                    f"INSERT INTO {case.table('proofs')} (attempt_sha256,kind,proof_sha256,activation_request_sha256,guard_epochs_sha256,proof_document) VALUES (?,'OUTCOME',?,?,?,?);",
                    attempt.attempt_sha256,
                    proof.proof_sha256,
                    proof.activation_request_sha256,
                    proof.guard_epochs_sha256,
                    encode_attempt_proof(proof),
                )
                connection.commit()
            except Exception:
                connection.rollback()
                raise RuntimeError("synthetic_outcome_persistence_failed") from None
        assert case.sql(
            f"SELECT evidence_document FROM [{case.environment.schema}].[synthetic_outcomes] WHERE evidence_sha256=?;",
            proof.evidence_sha256,
        ) == ((document,),)
        case.record("outcome_" + state.lower(), {"proof": proof.to_dict(), "evidence": evidence})
        return proof
