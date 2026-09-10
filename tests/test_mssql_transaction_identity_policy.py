"""Pure identity ownership preserves runtime hashes and source admission ordering."""

import ast
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.contracts import mssql_transaction_identity as policy
from dpone.runtime.etl import mssql_transaction_identity as runtime


def test_canonical_identity_policy_has_no_runtime_dependencies():
    tree = ast.parse(Path(policy.__file__).read_text())
    assert all(
        not node.module.startswith(("dpone.runtime", "dpone.backfill"))
        for node in ast.walk(tree)
        if isinstance(node, ast.ImportFrom) and node.module
    )
    assert runtime.require_source_physical_identity_binding is policy.require_source_physical_identity_binding


def test_runtime_identity_preserves_frozen_v1_scope_hashes():
    config = SimpleNamespace(options={}, target_table="target")
    context = SimpleNamespace(run_id="run", config={"pipeline": "pipe", "task": "task"})
    identity = runtime.invocation_identity(context, config, dag_id="process")
    assert identity.invocation_digest.hex() == "e252f2abc5ab113aedc30e5efe9a76be8e2dfee32d75eff230cc219d73fa0e17"
    operation = runtime.operation_request(config, identity)
    assert operation.scope_hash.hex() == "e19b22328f8be64fadb979ed7abfc348170f2d5bc49130fe39f36cb26a3e7c07"
    assert operation.owner_digest.hex() == "1ea2e4874943034136040e8b63abfc55357bd7663592170db3dfd6ce9ea02364"


def test_source_binding_rejects_before_connector_callback():
    calls = []
    source = SimpleNamespace(mssql_transaction_source_physical_identity=lambda config: calls.append(config))
    with pytest.raises(policy.MssqlTransactionContractError, match="postgres_source_authority_digest_required"):
        runtime.resolve_source_physical_identity(source, SimpleNamespace(options={"source_type": "postgres"}))
    assert calls == []


def test_canonical_attempt_uses_injected_fingerprint_and_exact_coordinates():
    calls = []
    source = object()
    config = SimpleNamespace(load_strategy=SimpleNamespace(value="append"))
    invocation = policy.InvocationIdentity(run_id="run", process="process", task_partition="pipe:task")

    def fingerprint(load_config, **bindings):
        calls.append((load_config, bindings))
        return b"r" * 32

    result = policy.build_mssql_attempt_request(
        config,
        invocation=invocation,
        target_identity=b"t" * 32,
        source_identity=source,
        load_id="load",
        request_coordinates=("database", "schema", "table"),
        route_fingerprint=fingerprint,
    )
    assert result.route_fingerprint == b"r" * 32
    assert (result.target_database, result.target_schema, result.target_table) == ("database", "schema", "table")
    assert calls == [(config, {"target_identity": b"t" * 32, "source_identity": source})]


def test_runtime_backfill_provider_errors_keep_public_translation(monkeypatch):
    config = SimpleNamespace(
        options={"backfill": {"chunk_context": {"run_key": "key"}}},
        load_strategy=SimpleNamespace(value="backfill"),
    )
    cause = ValueError("invalid runtime authority")

    def reject(load_config):
        assert load_config is config
        raise cause

    monkeypatch.setattr(runtime, "execution_policy_from_load_config", reject)
    with pytest.raises(policy.MssqlTransactionContractError, match="backfill_execution_policy_invalid") as error:
        runtime.invocation_identity(SimpleNamespace(run_id="run"), config, dag_id="process")
    assert error.value.__cause__ is cause
