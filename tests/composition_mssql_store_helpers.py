"""Transactional DB-API fault model; this is explicitly not SQL Server proof."""

from copy import deepcopy
from dataclasses import replace

from dpone.contracts.airflow_deployment import canonical_fingerprint
from dpone.contracts.composition_activation import CompositionPhysicalResource, CompositionWorkloadAdmission
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


class Database:
    """Committed data and injected failures independent of each connection's copy."""

    def __init__(self, value=None):
        self.request = value or mixed_request()
        self.data = {
            "authority": [(1, 1, SERVICE_ID)],
            "tables": 8,
            "activations": {},
            "domains": {},
            "activation_domains": {},
            "attempts": {},
            "attempt_domains": {},
            "proofs": {},
            "issued_authorities": {},
        }
        for resource in self.request.resources:
            self.data["domains"][resource.guard_id] = (
                resource.connector,
                resource.service_id,
                resource.physical_subject_sha256,
                0,
                None,
            )
        self.connections = []
        self.statements = []
        self.fail_commit = False
        self.commit_applies = True
        self.before_connect = None
        self.fail_connect = None
        self.fail_sql = None
        self.lock_result = 0

    def connect(self):
        if self.before_connect:
            self.before_connect(self)
        if self.fail_connect:
            raise RuntimeError(self.fail_connect)
        connection = Connection(self)
        self.connections.append(connection)
        return connection


class Connection:
    def __init__(self, database):
        self.database = database
        self.data = deepcopy(database.data)
        self.autocommit = True
        self.closed = False
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        assert not self.closed
        return Cursor(self)

    def commit(self):
        self.commits += 1
        if self.database.commit_applies:
            self.database.data = deepcopy(self.data)
        if self.database.fail_commit:
            raise RuntimeError("driver password=synthetic-secret")

    def rollback(self):
        self.rollbacks += 1
        self.data = deepcopy(self.database.data)

    def close(self):
        self.closed = True


class Cursor:
    def __init__(self, connection):
        self.connection = connection
        self.results = []
        self.closed = False

    def execute(self, sql, *parameters):
        assert not self.closed and not self.connection.closed
        assert self.connection.autocommit is False
        normalized = " ".join(sql.split())
        database = self.connection.database
        database.statements.append((normalized, parameters))
        if database.fail_sql and database.fail_sql in normalized:
            raise RuntimeError("driver password=synthetic-secret")
        data = self.connection.data
        self.results = []
        if normalized.startswith("SET XACT_ABORT"):
            assert "SERIALIZABLE" in sql
            assert "IF @@TRANCOUNT = 0 BEGIN TRANSACTION" in sql
        elif "sp_getapplock" in sql:
            assert "@LockOwner = N'Transaction'" in sql and parameters == ("dpone:composition-control:v1",)
            self.results = [(database.lock_result,)]
        elif "FROM [dpone_control].[composition_authority]" in sql:
            self.results = data["authority"][:]
        elif "FROM sys.tables" in sql:
            self.results = [(data["tables"],)]
        elif "SELECT request_sha256, request_document, state" in sql:
            found = data["activations"].get(parameters[0])
            self.results = [] if found is None else [found]
        elif "SELECT guard_id, resource_document, fencing_epoch" in sql:
            self.results = [
                (guard, *value)
                for (activation, guard), value in sorted(data["activation_domains"].items())
                if activation == parameters[0]
            ]
        elif "SELECT connector, LOWER(CONVERT(char(36), service_id)), physical_subject_sha256" in sql:
            found = data["domains"].get(parameters[0])
            self.results = [] if found is None else [found]
        elif normalized.startswith("INSERT INTO [dpone_control].[composition_activations]"):
            activation, digest_value, document = parameters
            assert activation not in data["activations"]
            data["activations"][activation] = (digest_value, document, "PREPARED")
        elif normalized.startswith("INSERT INTO [dpone_control].[composition_activation_domains]"):
            activation, guard, document, epoch = parameters
            assert (activation, guard) not in data["activation_domains"]
            data["activation_domains"][activation, guard] = (document, epoch)
        elif normalized.startswith("UPDATE [dpone_control].[composition_domains] SET fencing_epoch"):
            next_epoch, activation, guard, epoch = parameters
            assert "WHERE guard_id = ? AND fencing_epoch = ? AND owner_activation_id IS NULL" in sql
            record = data["domains"][guard]
            if record[3:] == (epoch, None):
                data["domains"][guard] = (*record[:3], next_epoch, activation)
                self.results = [(guard,)]
        elif normalized.startswith("UPDATE [dpone_control].[composition_domains] SET owner_activation_id = NULL"):
            guard, epoch, activation = parameters
            assert "WHERE guard_id = ? AND fencing_epoch = ? AND owner_activation_id = ?" in sql
            record = data["domains"][guard]
            if record[3:] == (epoch, activation):
                data["domains"][guard] = (*record[:3], epoch, None)
                self.results = [(guard,)]
        elif normalized.startswith("UPDATE [dpone_control].[composition_activations]"):
            state, activation, digest_value, document, size, before = parameters
            assert "request_document = ?" in sql and "DATALENGTH(request_document) = ?" in sql
            if data["activations"][activation] == (digest_value, document, before) and size == len(document):
                data["activations"][activation] = (digest_value, document, state)
                self.results = [(state,)]
        elif normalized.startswith("SELECT LOWER(CONVERT(char(36), activation_id))"):
            self.results = [
                (activation,) for activation, guard in sorted(data["activation_domains"]) if guard == parameters[0]
            ]
        elif normalized.startswith("SELECT TOP (1) a.attempt_sha256"):
            for key, attempt in data["attempts"].items():
                if any(guard == parameters[0] for guard, _ in data["attempt_domains"].get(key, ())):
                    if attempt[4] not in {"SUCCEEDED", "FAILED"} or None in attempt[5:]:
                        self.results = [(key,)]
                        break
        elif normalized.startswith("SELECT attempt_sha256, LOWER(CONVERT(char(36), activation_id))"):
            self.results = [
                (key, *attempt)
                for key, attempt in sorted(data["attempts"].items())
                if attempt[0] == parameters[0] or attempt[1] == parameters[1]
            ]
        elif normalized.startswith("SELECT guard_id, fencing_epoch FROM [dpone_control].[composition_attempt_domains]"):
            self.results = list(data["attempt_domains"].get(parameters[0], ()))
        elif normalized.startswith("SELECT connector, LOWER(CONVERT(char(36), service_id)), principal_id"):
            self.results = list(data["issued_authorities"].get(parameters[0], ()))
        elif normalized.startswith("SELECT activation_request_sha256, guard_epochs_sha256, proof_document"):
            found = data["proofs"].get(parameters)
            self.results = [] if found is None else [found]
        else:
            raise AssertionError(f"Unmodeled SQL: {normalized}")
        return self

    def fetchone(self):
        return self.results.pop(0) if self.results else None

    def fetchall(self):
        rows, self.results = self.results, []
        return rows

    def close(self):
        self.closed = True


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
            request_value.request_sha256,
            epoch_digest,
            encode_attempt_proof(proof),
        )
    database.data["attempts"][attempt.attempt_sha256] = (
        request_value.activation_id,
        request_value.request_sha256,
        epoch_digest,
        encode_attempt_identity(attempt),
        state,
        hashes["CLOSED_GATES"],
        hashes["QUIESCENCE"],
        hashes["OUTCOME"],
    )
    database.data["attempt_domains"][attempt.attempt_sha256] = epochs
    return attempt
