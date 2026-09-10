"""Canonical policy ownership preserves runtime entry points and persisted identity."""

import ast
from pathlib import Path


def test_signed_authority_runtime_exports_canonical_policy():
    from dpone.contracts import governed_database_authority as canonical
    from dpone.runtime.credentials import governed_database_authority as runtime

    assert runtime.require_governed_mssql_database_authority is canonical.require_governed_mssql_database_authority
    assert runtime.require_governed_postgres_source_authority is canonical.require_governed_postgres_source_authority


def test_canonical_route_policy_has_no_runtime_imports():
    from dpone.contracts import mssql_transaction_route_identity as canonical

    tree = ast.parse(Path(canonical.__file__).read_text())
    imports = [n.module for n in ast.walk(tree) if isinstance(n, ast.ImportFrom)]
    assert not any(name and name.startswith("dpone.runtime") for name in imports)


def test_pre_extraction_version_one_fingerprints_remain_identical():
    """Golden values captured from base332da02 before the policy relocation."""
    from dpone.config.load_config import ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION, LoadConfig
    from dpone.config.load_strategy import LoadStrategy
    from dpone.contracts.source_physical_identity import SourcePhysicalIdentity
    from dpone.runtime.etl.mssql_transaction_route_identity import invocation_route_fingerprint

    expected = {
        "clickhouse": "667f67cf0554ff86b0de8f43cbf6e95414987ae845ed22d84436fb9af388739c",
        "postgres": "31201c3b1c2afd126ce02de2127eb60e3bff453e4841a155349a326ba2c9a336",
        "aliases": "53b86487b5c92d8b2c98a574e860e337f45ec1289b8cc89b422675ed727dc88c",
    }
    for source, fingerprint in expected.items():
        dialect = "clickhouse" if source == "clickhouse" else "postgres"
        options = {"source_type": dialect, "sink_type": "mssql"}
        if source == "aliases":
            options[ROUTE_IDENTITY_V1_ENDPOINT_TYPES_OPTION] = {"source_type": "PostgreSQL", "sink_type": "odbc"}
        config = LoadConfig(
            source_conn_id="synthetic-source",
            target_conn_id="synthetic-target",
            source_schema="src",
            source_table="events",
            target_schema="dbo",
            target_table="events",
            target_database="synthetic",
            load_strategy=LoadStrategy.FULL_REFRESH,
            options=options,
        )
        identity = SourcePhysicalIdentity(
            dialect=dialect,
            cluster_identifier="synthetic-cluster",
            database="synthetic-source",
            effective_principal="reader",
            session_principal="reader",
        )
        assert (
            invocation_route_fingerprint(config, target_identity=b"t" * 32, source_identity=identity).hex()
            == fingerprint
        )
        # Runtime-only identity inputs never enter the persisted route hash.
        config.options["run_id"] = object()
        config.options["__dpone_mssql_transaction_lease"] = object()
        assert (
            invocation_route_fingerprint(config, target_identity=b"t" * 32, source_identity=identity).hex()
            == fingerprint
        )
