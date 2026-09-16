"""Composition ordering with real original authentication and isolated SQL ports."""

from dataclasses import replace
from types import SimpleNamespace

import pytest

from dpone.adapters.dbt_mssql_physical_catalog_lifecycle import MssqlPhysicalCatalogRegistrationLifecycle
from tests.test_dbt_mssql_physical_catalog_policy import _claim, _fixture, _reader
from tests.test_native_original_verification import _native_verifier


class SqlBoundary:
    def __init__(self):
        self.events = []
        self.binding = None
        self.final_drift = False

    def resolve(self, registration, binding=None):
        self.events.append("binding-read" if binding else "registration-read")
        return binding or registration

    def apply(self, registration, **schema):
        self.events.append("module-install" if schema else "binding-schema")
        return SimpleNamespace(module_sha256="a" * 64)

    def verify(self, registration, **schema):
        self.events.append("module-read")
        return SimpleNamespace(module_sha256=("b" if self.final_drift else "a") * 64)

    def register(self, registration, binding):
        self.events.append("binding-write")
        self.binding = binding
        return binding

    def connection(self):
        self.events.append("schema-observe")
        return SchemaConnection()


class SchemaConnection:
    autocommit = True

    def cursor(self):
        return self

    def execute(self, sql, *params):
        self.rows = [(8, 1, "base")] if "SELECT schema_id" in sql else []
        return self

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def commit(self):
        pass

    def rollback(self):
        pass

    def close(self):
        pass


def lifecycle(reader, sql):
    return MssqlPhysicalCatalogRegistrationLifecycle(
        policy_reader=reader,
        registration_store=sql,
        binding_schema=sql,
        module_provisioner=sql,
        binding_store=sql,
        connection_factory=sql.connection,
    )


def test_real_policy_precedes_all_sql_and_final_reads_precede_success(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    sql = SqlBoundary()
    with _native_verifier(fixture, []) as verifier:
        value = lifecycle(_reader(verifier), sql).apply(fixture.refs, registration=_claim(verifier, fixture))
    assert value.model_schema_id == 8
    assert value.catalog_module_sha256 == "sha256:" + "a" * 64
    assert sql.events == [
        "registration-read",
        "schema-observe",
        "binding-schema",
        "module-install",
        "binding-write",
        "module-read",
        "binding-read",
    ]


def test_invalid_actual_policy_never_touches_sql(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    sql = SqlBoundary()
    with _native_verifier(fixture, []) as verifier:
        claim = _claim(verifier, fixture)
        claim = replace(claim, limits=replace(claim.limits, max_catalog_rows=claim.limits.max_catalog_rows + 1))
        with pytest.raises(ValueError, match="limits differ"):
            lifecycle(_reader(verifier), sql).apply(fixture.refs, registration=claim)
    assert sql.events == []


def test_changed_final_module_yields_no_accepted_binding(tmp_path, monkeypatch):
    fixture = _fixture(tmp_path, monkeypatch)
    sql = SqlBoundary()
    sql.final_drift = True
    with _native_verifier(fixture, []) as verifier:
        with pytest.raises(RuntimeError, match="module changed"):
            lifecycle(_reader(verifier), sql).apply(fixture.refs, registration=_claim(verifier, fixture))
    assert "module-read" in sql.events
