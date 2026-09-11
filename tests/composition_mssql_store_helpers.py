"""Transactional DB-API fault model; this is explicitly not SQL Server proof."""

from dataclasses import replace

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import CompositionPhysicalResource, CompositionWorkloadAdmission
from dpone.contracts.composition_ownership import CompositionOwnerReference
from tests.composition_mssql_store_fault_model import FaultDatabase
from tests.test_composition_activation_contract import digest, request

SERVICE_ID = "20000000-0000-4000-8000-000000000001"
CH_SERVICE_ID = "20000000-0000-4000-8000-000000000002"


def mixed_request():
    """A native SQL workload, generated CH transfer and independent SQL transfer."""
    base = request()
    sql = replace(
        base.resources[0],
        service_id=SERVICE_ID,
        guard_id=domain_guard("mssql", SERVICE_ID, base.resources[0].physical_subject_sha256),
    )
    physical = digest("clickhouse database continuity")
    ch = CompositionPhysicalResource(
        domain_guard("clickhouse", CH_SERVICE_ID, physical),
        "clickhouse",
        CH_SERVICE_ID,
        physical,
        digest("CH original catalog"),
        (digest("CH target"),),
    )
    workloads = (
        *base.workloads,
        CompositionWorkloadAdmission(
            "c_generated_данные",
            "native",
            digest("CH pack"),
            "mssql_clickhouse_full_refresh_v1",
            ch.write_subjects,
        ),
    )
    return replace(base, workloads=workloads, resources=tuple(sorted((sql, ch), key=lambda value: value.guard_id)))


def domain_guard(connector, service_id, physical):
    return canonical_fingerprint(
        {
            "schema": "dpone.composition-physical-domain.v1",
            "connector": connector,
            "service_id": service_id,
            "physical_subject_sha256": physical,
        }
    )


def owner_key(value):
    """The execution's derived common owner key, never its activation UUID."""
    return CompositionOwnerReference("execution", value.activation_id).owner_key


class Database(FaultDatabase):
    """Supply the stable mixed request to the separately owned SQL fault model."""

    def __init__(self, value=None):
        super().__init__(value or mixed_request(), SERVICE_ID)


def journal_attempt(database, *, workload_id="a_native", state="SUCCEEDED", extra_principal=False):
    """Inject protected producer output into the double, never into a live store."""
    from dpone.contracts.composition_attempt import CompositionAttemptIdentity
    from dpone.contracts.composition_persistence import encode_attempt_identity, encode_attempt_proof
    from dpone.contracts.composition_proof import (
        CompositionAttemptProof,
        CompositionProofAuthority,
        composition_attempt_epoch_subject,
    )

    request_value = database.request
    workload = next(row for row in request_value.workloads if row.workload_id == workload_id)
    resources = tuple(
        row for row in request_value.resources if set(row.write_subjects).intersection(workload.write_subjects)
    )
    epochs = tuple((row.guard_id, database.data["domains"][row.guard_id][3]) for row in resources)
    attempt = CompositionAttemptIdentity(
        request_value.request_sha256,
        workload_id,
        workload.constituent_id,
        workload.pack_sha256,
        digest("plan"),
        "synthetic-run",
        "execute",
        1,
        -1,
        epochs,
    )
    epoch_digest = composition_attempt_epoch_subject(attempt)
    authorities = tuple(
        sorted(
            CompositionProofAuthority(
                row.connector,
                row.service_id,
                "mssql-sid:" + "a" * 32
                if row.connector == "mssql"
                else "clickhouse-user:30000000-0000-4000-8000-000000000001",
            )
            for row in resources
        )
    )
    if extra_principal:
        authorities = tuple(
            sorted(
                (
                    *authorities,
                    CompositionProofAuthority(
                        "clickhouse", CH_SERVICE_ID, "clickhouse-user:30000000-0000-4000-8000-000000000002"
                    ),
                )
            )
        )
    database.data["issued_authorities"][attempt.attempt_sha256] = tuple(
        (row.connector, row.service_id, row.principal_id) for row in authorities
    )
    hashes = {}
    for kind in ("CLOSED_GATES", "QUIESCENCE", "OUTCOME"):
        proof = CompositionAttemptProof(
            kind,
            attempt.attempt_sha256,
            request_value.request_sha256,
            epoch_digest,
            authorities,
            digest(kind),
            ("COMMIT_UNKNOWN" if state == "RUNNING" else state) if kind == "OUTCOME" else None,
        )
        hashes[kind] = proof.proof_sha256
        database.data["proofs"][attempt.attempt_sha256, kind, proof.proof_sha256] = (
            "execution",
            encode_attempt_proof(proof),
        )
    database.data["operations"][attempt.attempt_sha256] = (
        attempt.attempt_sha256,
        "execution",
        owner_key(request_value),
        request_value.request_sha256,
        attempt.attempt_sha256,
        encode_attempt_identity(attempt),
        state,
        hashes["CLOSED_GATES"],
        hashes["QUIESCENCE"],
        hashes["OUTCOME"],
    )
    database.data["operation_domains"].extend(
        (attempt.attempt_sha256, owner_key(request_value), guard, epoch) for guard, epoch in epochs
    )
    return attempt


def alter_proof_epoch(database, proof_key):
    """Rehash canonical forged metadata so actual attempt binding must reject it."""
    from dpone.contracts.composition_persistence import decode_attempt_proof, encode_attempt_proof

    family, document = database.data["proofs"].pop(proof_key)
    proof = replace(decode_attempt_proof(document, proof_key[2]), guard_epochs_sha256=digest("wrong"))
    database.data["proofs"][*proof_key[:2], proof.proof_sha256] = (family, encode_attempt_proof(proof))
    record = list(database.data["operations"][proof_key[0]])
    record[7 + ("CLOSED_GATES", "QUIESCENCE", "OUTCOME").index(proof_key[1])] = proof.proof_sha256
    database.data["operations"][proof_key[0]] = tuple(record)


def damage_terminal_record(database, attempt, damage):
    """Corrupt one explicit v2 field while preserving other stored originals."""
    key = attempt.attempt_sha256
    proof_key = next(iter(database.data["proofs"]))
    if damage == "missing_proof":
        del database.data["proofs"][proof_key]
    elif damage == "proof_bytes":
        family, document = database.data["proofs"][proof_key]
        database.data["proofs"][proof_key] = (family, document + b" ")
    elif damage == "proof_epoch":
        alter_proof_epoch(database, proof_key)
    elif damage == "missing_issuer":
        del database.data["issued_authorities"][key]
    elif damage == "foreign_issuer":
        connector, service_id, principal = database.data["issued_authorities"][key][0]
        database.data["issued_authorities"][key] = ((connector, service_id, principal[:-1] + "b"),)
    elif damage == "partition":
        database.data["operation_domains"] = [row for row in database.data["operation_domains"] if row[0] != key]
    elif damage == "epoch":
        row = database.data["operation_domains"][0]
        database.data["operation_domains"][0] = (*row[:3], row[3] + 1)
    else:
        record = list(database.data["operations"][key])
        record[{"parent": 3, "terminal_hash": 7}[damage]] = digest("wrong")
        database.data["operations"][key] = tuple(record)


def damage_authority(database, damage):
    """Change exactly the catalog/enrollment precondition selected by the case."""
    guard = database.request.resources[0].guard_id
    if damage == "missing_domain":
        guard = next(row.guard_id for row in database.request.resources if row.connector == "clickhouse")
        del database.data["domains"][guard]
    elif damage == "foreign_owner":
        row = database.data["domains"][guard]
        database.data["domains"][guard] = (*row[:3], 9, digest("foreign owner"))
    elif damage == "authority":
        database.data["authority"] = [(1, 2, "10000000-0000-4000-8000-000000000099")]
    elif damage == "schema":
        database.data["catalog"]["[dpone_control].[composition_owners]", "table"] = ()
    else:
        database.data["authority"] = [(1, 1, SERVICE_ID)]
