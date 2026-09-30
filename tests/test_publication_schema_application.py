"""Operator scope uses real pinned context/files; only SQL is fault-scripted."""

import json
from importlib import import_module

import pytest

from dpone.adapters.publication_plan_file import read_private_plan, write_private_plan
from dpone.contracts.publication_schema import decode_schema_plan
from dpone.runtime.credentials.resolved_connector_factory import ResolvedConnectorFactory
from tests.test_mssql_publication_schema import SchemaSession
from tests.test_publication_authority_composition import _OBSERVED, _PIN
from tests.test_runtime_connection_context_loader import _replace_plan_descriptor, _runtime_context


class OperatorSession(SchemaSession):
    @property
    def connection(self):
        return self

    def get_records(self, sql, params=()):
        if "SERVERPROPERTY" in sql:
            return [_OBSERVED]
        return super().get_records(sql, params)


def operator_context(tmp_path):
    environ, root = _runtime_context(tmp_path)
    path = root / "connection-registry.json"
    registry = json.loads(path.read_bytes())
    registry["connections"]["source-registry"] = {
        "type": "mssql",
        "connection": {
            "host": "synthetic-sql-server",
            "database": "Example_System",
            "schema": "dbo",
            "publication_authority": {
                "service_id": "example",
                "environment": "prod",
                "endpoint_identity_sha256": _PIN,
            },
        },
        "credentials": {
            "resolver": "airflow_env",
            "connection_id": "synthetic-metadata",
            "payload_format": "airflow_connection_uri",
        },
    }
    content = json.dumps(registry, sort_keys=True, separators=(",", ":")).encode()
    path.write_bytes(content)
    return _replace_plan_descriptor(environ, name="connection_registry", content=content), root


@pytest.fixture(autouse=True)
def projected_credentials(monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW_CONN_SYNTHETIC_METADATA",
        "mssql://synthetic-user:synthetic-private-password@synthetic-sql-server/Example_System",
    )


def application(environ, sessions):
    module = import_module("dpone.app.publication_schema_application")
    assert hasattr(module, "PublicationSchemaApplication"), "operator application is missing"

    def connect(resolved):
        assert resolved.credentials.password == "synthetic-private-password"
        session = OperatorSession()
        sessions.append(session)
        return session

    return module.PublicationSchemaApplication(environ=environ, connector_factory=connect)


def test_plan_admits_pinned_registry_and_writes_private_scope_without_ddl(tmp_path):
    environ, _ = operator_context(tmp_path)
    sessions = []
    path = tmp_path / "plan.json"
    result = application(environ, sessions).plan(connection_ref="source-main", environment="prod", path=path)
    assert result["status"] == "ready"
    assert result["reason_code"] == "catalog_absent"
    plan = decode_schema_plan(read_private_plan(path))
    assert plan.binding.database == "Example_System"
    assert plan.binding.connection_ref == "source-main"
    assert plan.binding.environment == "prod"
    assert result["plan_digest"] == plan.digest
    assert len(sessions) == 1 and sessions[0].ddl == []
    assert sessions[0].events[-1] == "close"
    assert "synthetic-private-password" not in path.read_text() + json.dumps(result)


@pytest.mark.parametrize("digest", ["0" * 64, "é" * 64, "A" * 64, "a" * 63, "", None, 123])
def test_apply_requires_exact_digest_before_context_or_credentials(tmp_path, monkeypatch, digest):
    environ, _ = operator_context(tmp_path)
    path = tmp_path / "plan.json"
    sessions = []
    app = application(environ, sessions)
    app.plan(connection_ref="source-main", environment="prod", path=path)
    # Remove credential material: bad confirmation must still be a proven block.
    monkeypatch.delenv("AIRFLOW_CONN_SYNTHETIC_METADATA")
    result = app.apply(path=path, environment="prod", confirmation_digest=digest)
    assert result["status"] == "blocked"
    assert result["reason_code"] == "plan_confirmation_required"
    assert len(sessions) == 1


def test_apply_and_inspect_use_same_closed_plan_and_endpoint(tmp_path):
    environ, _ = operator_context(tmp_path)
    sessions = []
    app = application(environ, sessions)
    path = tmp_path / "plan.json"
    planned = app.plan(connection_ref="source-main", environment="prod", path=path)
    applied = app.apply(path=path, environment="prod", confirmation_digest=planned["plan_digest"])
    assert applied["status"] == "completed"
    assert applied["reason_code"] == "catalog_created"
    assert len(sessions[1].ddl) == 3
    inspected = app.inspect(path=path, environment="prod")
    assert inspected["status"] == "ready"  # independent scripted session is absent
    assert sessions[2].ddl == []


@pytest.mark.parametrize("environment", ["test", "", "PROD"])
def test_wrong_environment_never_opens_sql_or_writes_plan(tmp_path, environment):
    environ, _ = operator_context(tmp_path)
    sessions = []
    path = tmp_path / "plan.json"
    result = application(environ, sessions).plan(connection_ref="source-main", environment=environment, path=path)
    assert result["status"] == "blocked"
    assert sessions == [] and not path.exists()


@pytest.mark.parametrize("damage", ["missing", "tampered", "unbound"])
def test_unverified_or_unbound_context_never_falls_back(tmp_path, damage):
    environ, root = operator_context(tmp_path)
    if damage == "missing":
        environ = {}
    if damage == "tampered":
        with (root / "binding-set.json").open("ab") as out:
            out.write(b" ")
    path = tmp_path / "plan.json"
    sessions = []
    result = application(environ, sessions).plan(
        connection_ref="unbound" if damage == "unbound" else "source-main", environment="prod", path=path
    )
    assert result["status"] != "ready"
    assert sessions == [] and not path.exists()
    assert "Traceback" not in json.dumps(result)


def test_existing_plan_is_never_replaced(tmp_path):
    environ, _ = operator_context(tmp_path)
    path = tmp_path / "plan.json"
    write_private_plan(path, b"retained-evidence")
    result = application(environ, []).plan(connection_ref="source-main", environment="prod", path=path)
    assert result["status"] != "ready"
    assert read_private_plan(path) == b"retained-evidence"


@pytest.mark.parametrize("change", ["pin", "service", "database", "alias"])
def test_changed_admitted_scope_cannot_apply_old_plan(tmp_path, change):
    environ, root = operator_context(tmp_path)
    sessions = []
    path = tmp_path / "plan.json"
    result = application(environ, sessions).plan(connection_ref="source-main", environment="prod", path=path)
    registry_path = root / "connection-registry.json"
    registry = json.loads(registry_path.read_bytes())
    properties = registry["connections"]["source-registry"]["connection"]
    if change == "pin":
        properties["publication_authority"]["endpoint_identity_sha256"] = "f" * 64
    elif change == "service":
        properties["publication_authority"]["service_id"] = "other-service"
    elif change == "database":
        properties["database"] = "Other_System"
    else:
        del registry["connections"]["source-registry"]
    content = json.dumps(registry, sort_keys=True, separators=(",", ":")).encode()
    registry_path.write_bytes(content)
    changed = _replace_plan_descriptor(environ, name="connection_registry", content=content)
    applied = application(changed, sessions).apply(
        path=path, environment="prod", confirmation_digest=result["plan_digest"]
    )
    assert applied["status"] not in {"ready", "completed"}
    assert len(sessions) == 1  # before connector creation, not after mutation


def test_driver_ack_loss_is_redacted_unknown_without_second_session(tmp_path):
    environ, _ = operator_context(tmp_path)
    path = tmp_path / "plan.json"
    planned = application(environ, []).plan(connection_ref="source-main", environment="prod", path=path)
    opened = []

    def connect(_):
        session = OperatorSession(fail="commit")
        opened.append(session)
        return session

    app = import_module("dpone.app.publication_schema_application").PublicationSchemaApplication(
        environ=environ, connector_factory=connect
    )
    result = app.apply(path=path, environment="prod", confirmation_digest=planned["plan_digest"])
    assert result["status"] == "outcome_unknown"
    assert result["reason_code"] == "catalog_requires_readback"
    assert len(opened) == 1 and len(opened[0].ddl) == 3
    assert "synthetic-private" not in json.dumps(result)


def test_registered_cli_plan_apply_and_inspect_are_real_application(tmp_path, monkeypatch, capsys):
    from tests.test_publication_schema_command import run

    environ, _ = operator_context(tmp_path)
    for key, value in environ.items():
        monkeypatch.setenv(key, value)
    session = OperatorSession()
    monkeypatch.setattr(ResolvedConnectorFactory, "create", lambda _: session)
    path = tmp_path / "plan.json"
    common = ["--environment", "prod", "--plan-file", str(path)]
    assert run(["plan", "--connection-ref", "source-main", *common]) == 0
    plan = json.loads(capsys.readouterr().out)
    assert run(["apply", "--confirm-digest", plan["plan_digest"], *common]) == 0
    assert json.loads(capsys.readouterr().out)["reason_code"] == "catalog_created"
    assert len(session.ddl) == 3
    assert run(["inspect", *common]) == 0
    assert json.loads(capsys.readouterr().out)["reason_code"] == "catalog_exact"
    assert len(session.ddl) == 3
