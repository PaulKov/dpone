"""Original MSSQL gate evidence tests; policy doubles are not catalog certification."""

from __future__ import annotations

import json
from dataclasses import replace
from typing import Any, cast

import pytest

from dpone.adapters import composition_mssql_historical_gate as historical
from dpone.adapters.composition_mssql_gate_proofs import gate_proof
from dpone.contracts.composition_activation import CompositionAdmissionError
from dpone.contracts.composition_proof import CompositionProofAuthority
from dpone.contracts.strict_json import canonical_json_bytes
from dpone.ports.composition_sql import CompositionSqlContext
from tests.composition_mssql_store_helpers import SERVICE_ID
from tests.test_composition_mssql_terminal import ProofSql, proof_setup, require_selected, store_proof


class GateSql(ProofSql):
    """Retained original rows and current login only; business DMV reads fail."""

    def __init__(self) -> None:
        super().__init__()
        self.gates: list[tuple[Any, ...]] = []
        self.logins: list[tuple[Any, ...]] = []
        self.enrollments: dict[str, tuple[Any, ...]] = {}
        self.schemas: dict[str, list[tuple[Any, ...]]] = {}
        self.evidence: dict[str, bytes] = {}
        self.database_rows: list[tuple[Any, ...]] = [("control",)]

    def _select(self, sql: str, parameters: tuple[Any, ...]) -> list[tuple[Any, ...]]:
        assert "sys.dm_" not in sql, "Historical closure must not observe successor business activity"
        if sql == "SELECT DB_NAME();":
            return self.database_rows
        if "FROM sys.server_principals" in sql:
            assert "TOP (2)" in sql
            return self.logins
        if "composition_login_gates]" in sql:
            assert "TOP (2)" in sql
            return self.gates
        if "composition_mssql_enrollments]" in sql:
            assert "TOP (2)" in sql
            return [self.enrollments[parameters[0]]] if parameters[0] in self.enrollments else []
        if "composition_mssql_managed_schemas]" in sql:
            assert "TOP (8193)" in sql
            return self.schemas.get(parameters[0], [])[:8193]
        if "composition_mssql_gate_evidence]" in sql:
            assert "TOP (2)" in sql and "DATALENGTH(evidence_document) BETWEEN 1 AND 8388608" in sql
            if parameters[1] not in self.evidence:
                return []
            document = self.evidence[parameters[1]]
            return [(document if 1 <= len(document) <= 8388608 else None,)]
        return super()._select(sql, parameters)


def gate_setup(monkeypatch: pytest.MonkeyPatch) -> tuple[GateSql, Any, tuple[Any, ...]]:
    database = GateSql()
    _, identity, proofs = proof_setup("mssql", database)
    sid = bytes.fromhex("ab" * 16)
    name = "dpone_v3_" + identity.attempt_sha256[7:]
    values = []
    for kind in ("CLOSED_GATES", "QUIESCENCE"):
        body: dict[str, Any] = {
            "schema": "dpone.composition-mssql-gate-observation.v1",
            "attempt_sha256": identity.attempt_sha256,
            "service_id": SERVICE_ID,
            "login_sid": sid.hex(),
            "login_name": name,
            "kind": kind,
            "login_disabled": True,
            "authentication_barrier": "CLOSING",
        }
        if kind == "QUIESCENCE":
            body.update(database_ids=[17], sessions=0, transactions=0)
        proof = gate_proof(identity, service_id=SERVICE_ID, sid=sid, kind=kind, evidence=body)
        database.evidence[proof.evidence_sha256] = canonical_json_bytes(body)
        values.append(proof)
    values.append(proofs[2])
    database.proofs.clear()
    for proof in values:
        store_proof(database, proof)
    database.gates = [("execution", sid, name, "CLOSED", values[0].evidence_sha256)]
    database.logins = [(name, sid, "S", True, 0, 0)]
    guard = identity.guard_epochs[0][0]
    database.enrollments[guard] = (
        "mssql",
        SERVICE_ID,
        "business",
        17,
        "10000000-0000-4000-8000-000000000001",
        "2026-01-01T00:00:00.000",
        "writer_role",
    )
    database.schemas[guard] = [("managed",)]
    monkeypatch.setattr(historical, "require_gate_policy", lambda context, name: None)
    return database, identity, tuple(values)


def check_gate(database: GateSql, identity: Any, proofs: tuple[Any, ...]) -> None:
    issued = tuple(CompositionProofAuthority(*row) for row in database.issued)
    historical.require_historical_mssql_gate(
        cast(CompositionSqlContext, database), identity, issued, proofs[0], proofs[1], expected_service_id=SERVICE_ID
    )


def test_gate_policy_transaction_replacement_stops_before_gate_read(monkeypatch: pytest.MonkeyPatch) -> None:
    database, identity, proofs = gate_setup(monkeypatch)

    def replace_transaction(context: Any, name: str) -> None:
        database.transaction = (1, 1, "Exclusive", 8)

    monkeypatch.setattr(historical, "require_gate_policy", replace_transaction)
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        check_gate(database, identity, proofs)
    assert not any("composition_login_gates]" in sql for sql, _ in database.statements)


def test_historical_enrollment_rejects_malformed_managed_schema_with_fixed_reason(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    database.schemas[identity.guard_epochs[0][0]] = [([],)]
    with pytest.raises(CompositionAdmissionError, match="terminal_mssql_enrollment"):
        check_gate(database, identity, proofs)


def test_retained_originals_validate_with_current_disabled_sid_without_business_queries(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    before = database.gates[:], database.logins[:], database.evidence.copy()
    require_selected(database, identity, proofs)
    assert before == (database.gates, database.logins, database.evidence)
    assert not any("sys.dm_" in sql for sql, _ in database.statements)
    for proof in proofs[:2]:
        assert (
            database.evidence[proof.evidence_sha256]
            == json.dumps(
                json.loads(database.evidence[proof.evidence_sha256]),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode()
        )


@pytest.mark.parametrize("deployment_present", [False, True])
def test_mssql_issuance_always_requires_gate_policy(monkeypatch: pytest.MonkeyPatch, deployment_present: bool) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    if not deployment_present:
        database.gates.clear()
        database.schemas.clear()
        database.enrollments.clear()
        database.evidence.clear()
    calls = []

    def reject_policy(context: Any, name: str) -> None:
        calls.append(name)
        raise CompositionAdmissionError("login_gate_policy")

    monkeypatch.setattr(historical, "require_gate_policy", reject_policy)
    with pytest.raises(CompositionAdmissionError, match="login_gate_policy"):
        require_selected(database, identity, proofs)
    assert calls == ["control"]


@pytest.mark.parametrize(
    "index,value",
    [
        (0, "qualification"),
        (1, b"x" * 16),
        (1, bytearray.fromhex("ab" * 16)),
        (2, "renamed"),
        (3, "CLOSING"),
        (4, "sha256:" + "0" * 64),
    ],
)
def test_gate_identity_cannot_change(monkeypatch: pytest.MonkeyPatch, index: int, value: Any) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    row = list(database.gates[0])
    row[index] = value
    database.gates = [tuple(row)]
    with pytest.raises(CompositionAdmissionError, match="login_gate_identity"):
        check_gate(database, identity, proofs)


@pytest.mark.parametrize("index,value", [(0, "renamed"), (1, b"x" * 16), (2, "U"), (3, False), (4, 1), (5, 1)])
def test_current_login_requires_original_name_sid_disabled_state_and_privileges(
    monkeypatch: pytest.MonkeyPatch, index: int, value: Any
) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    row = list(database.logins[0])
    row[index] = value
    database.logins = [tuple(row)]
    with pytest.raises(CompositionAdmissionError, match="login_principal_readback"):
        check_gate(database, identity, proofs)


@pytest.mark.parametrize(
    "target,reason",
    [
        ("gates", "login_gate_missing"),
        ("logins", "login_principal_readback"),
        ("enrollments", "terminal_mssql_enrollment"),
        ("schemas", "terminal_mssql_enrollment"),
        ("evidence", "terminal_mssql_evidence"),
        ("database_rows", "login_gate_policy"),
    ],
)
def test_missing_historical_dependency_rejects(monkeypatch: pytest.MonkeyPatch, target: str, reason: str) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    getattr(database, target).clear()
    with pytest.raises(CompositionAdmissionError, match=reason):
        check_gate(database, identity, proofs)


@pytest.mark.parametrize(
    "fragment",
    [
        "composition_login_gates]",
        "FROM sys.server_principals",
        "composition_mssql_enrollments]",
        "composition_mssql_gate_evidence]",
        "SELECT DB_NAME();",
    ],
)
def test_duplicate_dependency_rows_reject(monkeypatch: pytest.MonkeyPatch, fragment: str) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    database.after_execute = lambda db, sql, p: db.results.extend(db.results[:]) if fragment in sql else None
    with pytest.raises(CompositionAdmissionError):
        check_gate(database, identity, proofs)


@pytest.mark.parametrize(
    "index,value",
    [
        (0, "clickhouse"),
        (1, "00000000-0000-0000-0000-000000000000"),
        (2, "db]"),
        (3, True),
        (3, 4),
        (3, 2**31),
        (4, "bad"),
        (5, ""),
        (5, "x" * 34),
        (6, "bad role"),
    ],
)
def test_retained_enrollment_requires_exact_types_and_valid_identity(
    monkeypatch: pytest.MonkeyPatch, index: int, value: Any
) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    guard = identity.guard_epochs[0][0]
    row = list(database.enrollments[guard])
    row[index] = value
    database.enrollments[guard] = tuple(row)
    with pytest.raises(CompositionAdmissionError, match="terminal_mssql_enrollment"):
        check_gate(database, identity, proofs)


@pytest.mark.parametrize(
    "rows", [[("schema",), ("schema",)], [("bad]",)], [(1,)], [("b",), ("a",)], [("schema",)] * 8193]
)
def test_managed_schema_partition_requires_complete_bounded_canonical_rows(
    monkeypatch: pytest.MonkeyPatch, rows: Any
) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    database.schemas[identity.guard_epochs[0][0]] = rows
    with pytest.raises(CompositionAdmissionError, match="terminal_mssql_enrollment"):
        check_gate(database, identity, proofs)


@pytest.mark.parametrize(
    "kind,field,value",
    [
        (0, "login_disabled", 1),
        (0, "authentication_barrier", "CLOSED"),
        (0, "login_sid", "00" * 16),
        (1, "sessions", False),
        (1, "transactions", False),
        (1, "sessions", 1),
        (1, "database_ids", [18]),
    ],
)
def test_original_evidence_is_bytes_not_python_equality(
    monkeypatch: pytest.MonkeyPatch, kind: int, field: str, value: Any
) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    evidence_hash = proofs[kind].evidence_sha256
    evidence = json.loads(database.evidence[evidence_hash])
    evidence[field] = value
    database.evidence[evidence_hash] = canonical_json_bytes(evidence)
    with pytest.raises(CompositionAdmissionError, match="terminal_mssql_evidence"):
        check_gate(database, identity, proofs)


@pytest.mark.parametrize("damage", ["trailing", "oversized", "different_hash"])
def test_gate_evidence_original_requires_canonical_bytes_and_matching_hash(
    monkeypatch: pytest.MonkeyPatch, damage: str
) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    key = proofs[1].evidence_sha256
    if damage == "different_hash":
        new_key = "sha256:" + "0" * 64
        database.evidence[new_key] = database.evidence[key]
        proofs = (proofs[0], replace(proofs[1], evidence_sha256=new_key), proofs[2])
    else:
        database.evidence[key] += b" " if damage == "trailing" else b"x" * 8388609
    with pytest.raises(CompositionAdmissionError, match="terminal_mssql_evidence"):
        check_gate(database, identity, proofs)


@pytest.mark.parametrize(
    "fragment",
    [
        "SELECT DB_NAME();",
        "composition_login_gates]",
        "FROM sys.server_principals",
        "composition_mssql_enrollments]",
        "composition_mssql_managed_schemas]",
        "composition_mssql_gate_evidence]",
    ],
)
def test_gate_observations_recheck_transaction_immediately(monkeypatch: pytest.MonkeyPatch, fragment: str) -> None:
    database, identity, proofs = gate_setup(monkeypatch)

    def change(db: GateSql, sql: str, parameters: Any) -> None:
        if fragment in sql:
            db.transaction = (1, 1, "Exclusive", 8)

    database.after_execute = change
    with pytest.raises(CompositionAdmissionError, match="shared_transaction_identity"):
        check_gate(database, identity, proofs)
    changed_at = next(i for i, (sql, _) in enumerate(database.statements) if fragment in sql)
    assert len(database.statements) == changed_at + 2


@pytest.mark.parametrize("aggregate", [False, True])
def test_single_mssql_producer_cannot_certify_mixed_issuance(monkeypatch: pytest.MonkeyPatch, aggregate: bool) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    database.issued.append(("clickhouse", SERVICE_ID, "clickhouse-user:00000000-0000-0000-0000-000000000000"))
    if aggregate:
        authorities = tuple(sorted(CompositionProofAuthority(*row) for row in database.issued))
        proofs = tuple(replace(proof, authorities=authorities) for proof in proofs)
        for proof in proofs:
            store_proof(database, proof)
    with pytest.raises(
        CompositionAdmissionError, match="terminal_mssql_authorities" if aggregate else "terminal_proof_authorities"
    ):
        require_selected(database, identity, proofs)


def test_legacy_zero_database_guid_and_maximum_managed_schema_partition(monkeypatch: pytest.MonkeyPatch) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    guard = identity.guard_epochs[0][0]
    row = list(database.enrollments[guard])
    row[4] = "00000000-0000-0000-0000-000000000000"
    database.enrollments[guard] = tuple(row)
    database.schemas[guard] = [(f"schema_{number:04d}",) for number in range(8192)]
    check_gate(database, identity, proofs)


def test_historical_gate_rejects_redirected_namespace_before_policy(monkeypatch: pytest.MonkeyPatch) -> None:
    database, identity, proofs = gate_setup(monkeypatch)
    database.redirect_tables = True
    monkeypatch.setattr(historical, "require_gate_policy", lambda *args: pytest.fail("Redirected policy invoked"))
    with pytest.raises(CompositionAdmissionError, match="control_schema"):
        check_gate(database, identity, proofs)
