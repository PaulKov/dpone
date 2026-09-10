"""Transactional v2 SQL fault model; it never certifies a real SQL installation.

The original-row SELECT model is reused unchanged, while this module owns
connection isolation, commit faults and exact store mutations. Only catalog
observations and the external historical gate observer are explicit doubles;
the real catalog, transaction, shared-history and original proof policies run.
"""

import re
from copy import deepcopy

import pytest

from dpone.adapters import composition_mssql_terminal
from dpone.adapters.composition_mssql_schema import COMPOSITION_MSSQL_SCHEMA_VERSION
from tests.test_composition_mssql_catalog import expected_rows


@pytest.fixture
def offline_gate(monkeypatch):
    """Model only the separate historical gate observer, never proof decoding."""

    def observed(context, attempt, issued, closed, quiescent, *, expected_service_id):
        database = context.cursor.connection.database
        assert expected_service_id == database.service_id
        assert any(authority.connector == "mssql" for authority in issued)
        database.gate_observations.append((attempt, issued, closed, quiescent))

    monkeypatch.setattr(composition_mssql_terminal, "require_historical_mssql_gate", observed)


class FaultDatabase:
    """Committed rows and fault controls, independent of each connection copy."""

    def __init__(self, request, service_id):
        self.request, self.service_id = request, service_id
        self.data = {
            "authority": [(1, COMPOSITION_MSSQL_SCHEMA_VERSION, service_id)],
            "catalog": expected_rows("dpone_control"),
            "owners": {},
            "domains": {},
            "owner_domains": [],
            "operations": {},
            "operation_domains": [],
            "proofs": {},
            "issued_authorities": {},
        }
        for resource in request.resources:
            self.data["domains"][resource.guard_id] = (
                resource.connector,
                resource.service_id,
                resource.physical_subject_sha256,
                0,
                None,
            )
        self.connections, self.statements, self.gate_observations = [], [], []
        self.fail_commit, self.commit_applies = False, True
        self.before_connect = self.fail_connect = self.fail_sql = None
        self.lock_result, self.next_transaction, self.lock_owner = 0, 1, None

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
        # SharedSql imports mixed_request from the helper: delay this import
        # until fixture modules have loaded, with no alternate reader policy.
        from tests.test_composition_mssql_ownership import SharedSql

        self.model = SharedSql()
        self.database, self.data = database, deepcopy(database.data)
        self.autocommit, self.closed = True, False
        self.commits = self.rollbacks = 0
        self.transaction_id = None
        self.transaction_ids = []

    def cursor(self):
        assert not self.closed
        return Cursor(self)

    def finish(self):
        if self.database.lock_owner == self.transaction_id:
            self.database.lock_owner = None
        self.transaction_id = None

    def commit(self):
        self.commits += 1
        assert self.transaction_id is not None
        if self.database.commit_applies:
            self.database.data = deepcopy(self.data)
            self.finish()
        if self.database.fail_commit:
            raise RuntimeError("driver password=synthetic-secret")

    def rollback(self):
        self.rollbacks += 1
        self.data = deepcopy(self.database.data)
        self.finish()

    def close(self):
        self.finish()
        self.closed = True


class Cursor:
    def __init__(self, connection):
        self.connection, self.results, self.closed = connection, [], False

    def execute(self, sql, *parameters):
        connection = self.connection
        assert not self.closed and not connection.closed and connection.autocommit is False
        sql = " ".join(sql.split())
        database = connection.database
        database.statements.append((sql, parameters))
        if database.fail_sql and database.fail_sql in sql:
            raise RuntimeError("driver password=synthetic-secret")
        self.results = self._select_or_mutate(sql, parameters)
        return self

    def _select_or_mutate(self, sql, parameters):
        c, d = self.connection, self.connection.database
        if sql.startswith("SET XACT_ABORT"):
            assert "SERIALIZABLE" in sql and "IF @@TRANCOUNT = 0 BEGIN TRANSACTION" in sql
            if c.transaction_id is None:
                c.transaction_id, d.next_transaction = d.next_transaction, d.next_transaction + 1
                c.transaction_ids.append(c.transaction_id)
                c.data = deepcopy(d.data)
            return []
        if "sp_getapplock" in sql:
            assert "@LockOwner = N'Transaction'" in sql and parameters == ("dpone:composition-control:v1",)
            assert c.transaction_id is not None
            result = d.lock_result if d.lock_owner in {None, c.transaction_id} else -1
            if result >= 0:
                d.lock_owner = c.transaction_id
            return [(result,)]
        match = re.match(r"SELECT TOP \((\d+)\) /\* composition_schema:(\w+) \*/ ", sql)
        if match:
            return list(c.data["catalog"][parameters[-1] if parameters else "database", match[2]][: int(match[1])])
        if sql.startswith("INSERT ") or "UPDATE [dpone_control]" in sql:
            assert c.transaction_id is not None and d.lock_owner == c.transaction_id
            return mutate(c.data, sql, parameters)
        if "FROM [dpone_control].[composition_proofs]" in sql:
            return proof_rows(c.data, sql, parameters)
        if "FROM [dpone_control].[composition_issued_authorities]" in sql:
            assert "TOP (8193)" in sql and "WHERE operation_key = ?" in sql
            return list(c.data["issued_authorities"].get(parameters[0], ()))[:8193]
        for name in ("owners", "domains", "owner_domains", "operations", "operation_domains", "authority"):
            setattr(c.model, name, c.data[name])
        c.model.transaction = (
            (1, 1, "Exclusive" if d.lock_owner == c.transaction_id else "NoLock", c.transaction_id)
            if c.transaction_id is not None
            else (0, 0, "NoLock", None)
        )
        return c.model._select(sql, parameters)

    def fetchone(self):
        return self.results.pop(0) if self.results else None

    def fetchall(self):
        rows, self.results = self.results, []
        return rows

    def close(self):
        self.closed = True


def mutate(data, sql, p):
    """Apply only the exact v2 store DML/CAS shapes; all other writes reject."""
    if sql.startswith("INSERT INTO [dpone_control].[composition_owners]"):
        key, identifier, digest, document = p
        assert key not in data["owners"]
        assert not any(
            row[1] == "execution" and (row[2] == identifier or row[3] == digest) for row in data["owners"].values()
        )
        data["owners"][key] = (key, "execution", identifier, digest, document, "PREPARED")
    elif sql.startswith("INSERT INTO [dpone_control].[composition_owner_domains]"):
        key, guard, document, epoch = p
        assert not any(
            (row[0], row[1]) == (key, guard) or (row[1], row[3]) == (guard, epoch) for row in data["owner_domains"]
        )
        assert key in data["owners"] and data["domains"][guard][3:] == (epoch, key)
        data["owner_domains"].append((key, guard, document, epoch))
    elif "UPDATE [dpone_control].[composition_domains] SET fencing_epoch" in sql:
        next_epoch, key, guard, epoch = p
        assert "WHERE guard_id = ? AND fencing_epoch = ? AND owner_key IS NULL" in sql
        assert "OUTPUT inserted.guard_id INTO @changed" in sql and next_epoch == epoch + 1
        record = data["domains"][guard]
        if record[3:] == (epoch, None):
            data["domains"][guard] = (*record[:3], next_epoch, key)
            return [(guard,)]
    elif "UPDATE [dpone_control].[composition_domains] SET owner_key = NULL" in sql:
        guard, epoch, key = p
        assert "WHERE guard_id = ? AND fencing_epoch = ? AND owner_key = ?" in sql
        assert "OUTPUT inserted.guard_id INTO @changed" in sql
        record = data["domains"][guard]
        if record[3:] == (epoch, key):
            data["domains"][guard] = (*record[:3], epoch, None)
            return [(guard,)]
    elif "UPDATE [dpone_control].[composition_owners] SET state" in sql:
        state, key, identifier, digest, document, size, before = p
        assert "subject_document = ? AND DATALENGTH(subject_document) = ? AND state = ?" in sql
        assert "OUTPUT inserted.state INTO @changed" in sql
        record = data["owners"].get(key)
        if record == (key, "execution", identifier, digest, document, before) and len(document) == size:
            data["owners"][key] = (*record[:5], state)
            return [(state,)]
    elif sql.startswith("INSERT INTO [dpone_control].[composition_operations]"):
        assert sql == (
            "INSERT INTO [dpone_control].[composition_operations] "
            "(operation_key, operation_family, owner_key, owner_subject_sha256, replay_key, operation_document, state) "
            "VALUES (?, 'execution', ?, ?, ?, ?, 'RUNNING');"
        )
        key, owner, subject, replay, document = p
        assert key not in data["operations"] and key == replay
        assert data["owners"][owner][1] == "execution" and data["owners"][owner][3] == subject
        assert not any(row[1] == "execution" and row[4] == replay for row in data["operations"].values())
        data["operations"][key] = (key, "execution", owner, subject, replay, document, "RUNNING", None, None, None)
    elif sql.startswith("INSERT INTO [dpone_control].[composition_operation_domains]"):
        assert sql == (
            "INSERT INTO [dpone_control].[composition_operation_domains] "
            "(operation_key, owner_key, guard_id, fencing_epoch) VALUES (?, ?, ?, ?);"
        )
        key, owner, guard, epoch = p
        assert data["operations"][key][2] == owner
        assert any((row[0], row[1], row[3]) == (owner, guard, epoch) for row in data["owner_domains"])
        assert not any((row[0], row[2]) == (key, guard) for row in data["operation_domains"])
        data["operation_domains"].append((key, owner, guard, epoch))
    elif "UPDATE [dpone_control].[composition_operations] SET state" in sql:
        state, closed, quiescent, outcome, key, before = p
        assert "OUTPUT inserted.state INTO @changed" in sql
        assert "WHERE operation_key = ? AND operation_family = 'execution' AND state = ?" in sql
        record = data["operations"].get(key)
        if record is not None and record[1] == "execution" and record[6] == before:
            data["operations"][key] = (*record[:6], state, closed, quiescent, outcome)
            return [(state,)]
    else:
        raise AssertionError("Unmodeled mutation")
    return []


def proof_rows(data, sql, p):
    """Bound exact and keyset original proof projections without validating them."""

    def bounded(record):
        family, document = record
        return family, document if type(document) is bytes and 1 <= len(document) <= 8388608 else None

    if "SELECT TOP (2) operation_family" in sql:
        found = data["proofs"].get(p)
        return [] if found is None else [bounded(found)]
    assert "SELECT TOP (1) proof_sha256" in sql and "ORDER BY proof_sha256;" in sql
    rows = sorted(
        (key[2], *bounded(value))
        for key, value in data["proofs"].items()
        if key[:2] == p[:2] and (len(p) == 2 or key[2] > p[2])
    )
    return rows[:1]


def mutation_statements(database):
    """Detect DML/DDL even inside the store's DECLARE/OUTPUT-into batches."""
    return [
        sql for sql, _ in database.statements if re.search(r"\b(INSERT|UPDATE|DELETE|CREATE|ALTER|DROP|MERGE)\b", sql)
    ]
