"""Isolated infrastructure for production-hydrated PostgreSQL→MSSQL live proof.

This module intentionally owns only disposable integration resources.  The
state catalog is rendered by the public ``dpone state`` CLI, while runtime
connections are delivered through the same pinned init-fetch context consumed
by production.  No state storage or connector is injected into the hydrator.
"""

from __future__ import annotations

import base64
import hashlib
import json
import re
import uuid
from collections.abc import Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from dpone.runtime.runtime_init_fetch_plan import (
    RuntimeArtifactDescriptor,
    RuntimeExecutionSelection,
    RuntimeInitFetchPlan,
    RuntimeWorkloadPackRef,
    canonical_runtime_init_fetch_plan_bytes,
)
from tests.integration.postgres.postgres_live_support import (
    mssql_connector,
    postgres_connector,
    wait_until_ready,
)
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    factual_postgres_source_authority,
)
from tests.integration.postgres.postgres_mssql_production_hydration_cleanup import (
    MSSQL_ADMIN_CONNECT_TIMEOUT_SECONDS,
    MSSQL_ADMIN_QUERY_TIMEOUT_SECONDS,
    cleanup_production_hydration_resources,
    close_runtime_bindings,
)
from tests.integration.postgres.postgres_mssql_production_hydration_cli import (
    render_state_ddl_with_environment_cli,
)

STATE_SCHEMA = "governance"
SOURCE_SCHEMA = "dpone_hydration"
TARGET_SCHEMA = "dpone_hydration"
STAGING_SCHEMA = "staging"

_GO_SEPARATOR = re.compile(r"^\s*GO\s*$", re.IGNORECASE | re.MULTILINE)
_TARGET_REGISTRY_STATEMENT = "-- dpone:statement"
_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z][A-Za-z0-9_]{0,127}$")

_SOURCE_BINDING = "production-hydration-source"
_TARGET_BINDING = "production-hydration-target"
_STATE_BINDING = "production-hydration-state"
_SOURCE_REGISTRY_REF = "integration-postgres-source"
_TARGET_REGISTRY_REF = "integration-mssql-target"
_STATE_REGISTRY_REF = "integration-mssql-state"

_PG_USERNAME_ENV = "DPONE_HYDRATION_PG_USERNAME"
_PG_PASSWORD_ENV = "DPONE_HYDRATION_PG_PASSWORD"
_MSSQL_USERNAME_ENV = "DPONE_HYDRATION_MSSQL_USERNAME"
_MSSQL_PASSWORD_ENV = "DPONE_HYDRATION_MSSQL_PASSWORD"


@dataclass(frozen=True, slots=True)
class VendorCoordinates:
    """Connection coordinates projected into the verified integration registry."""

    postgres_host: str
    postgres_port: int
    postgres_database: str
    postgres_username: str
    postgres_password: str = field(repr=False)
    mssql_host: str = "127.0.0.1"
    mssql_port: int = 1433
    mssql_username: str = "sa"
    mssql_password: str = field(default="", repr=False)
    mssql_driver: str = "ODBC Driver 18 for SQL Server"
    mssql_bcp_path: str = "bcp"

    @classmethod
    def from_connectors(cls, postgres: Any, mssql: Any) -> VendorCoordinates:
        """Reuse already-resolved test sessions without embedding credentials."""

        values = {
            "postgres_username": str(getattr(postgres, "user", "") or ""),
            "postgres_password": str(getattr(postgres, "password", "") or ""),
            "mssql_username": str(getattr(mssql, "user", "") or ""),
            "mssql_password": str(getattr(mssql, "password", "") or ""),
        }
        if any(not value for value in values.values()):
            raise RuntimeError("production hydration live proof requires explicit Docker credentials")
        return cls(
            postgres_host=str(postgres.host),
            postgres_port=int(postgres.port),
            postgres_database=str(postgres.database),
            postgres_username=values["postgres_username"],
            postgres_password=values["postgres_password"],
            mssql_host=str(mssql.host),
            mssql_port=int(mssql.port),
            mssql_username=values["mssql_username"],
            mssql_password=values["mssql_password"],
            mssql_driver=str(mssql.driver),
            mssql_bcp_path=str(mssql.bcp_path),
        )


@dataclass(frozen=True, slots=True)
class ProductionHydrationLiveFixture:
    """All disposable resources needed by one real production hydration run."""

    coordinates: VendorCoordinates
    postgres: Any
    target: Any
    state: Any
    source_table: str
    target_table: str
    target_database: str
    state_database: str
    target_binding_id: uuid.UUID
    postgres_source_authority: Mapping[str, Any]
    target_database_authorities: Mapping[str, Any]
    state_database_authorities: Mapping[str, Any]
    transfer_root: Path
    runtime_environment: Mapping[str, str]
    rendered_state_ddl: str = field(repr=False)
    rendered_state_ddl_sha256: str

    @property
    def runtime_config(self) -> dict[str, Any]:
        """Return canonical connection-ref authoring with external MSSQL state."""

        return {
            "name": "production_hydration_live",
            "source": {
                "type": "postgres",
                "connection_ref": _SOURCE_BINDING,
            },
            "sink": {
                "type": "mssql",
                "connection_ref": _TARGET_BINDING,
            },
            "state": {
                "type": "mssql",
                "connection_ref": _STATE_BINDING,
                "atomicity": "target_atomic",
                "provisioning": "external",
                # Generic hydration consumes only database/schema authority.
                # A deliberately unused legacy XMin name proves that it does
                # not provision or require the six-object XMin catalog.
                "table": {"name": "unused_xmin_state"},
            },
        }

    def load_config(
        self,
        *,
        source_database: str | None = None,
        source_schema: str | None = None,
        source_table: str | None = None,
        target_database: str | None = None,
        staging_database: str | None = None,
    ) -> Any:
        """Build the full-refresh relation consumed by production ETLProcessor."""

        from dpone.config import LoadConfig, LoadStrategy

        resolved_target_database = target_database or self.target_database
        return LoadConfig(
            source_conn_id=_SOURCE_BINDING,
            target_conn_id=_TARGET_BINDING,
            source_database=source_database or self.coordinates.postgres_database,
            target_database=resolved_target_database,
            source_schema=source_schema or SOURCE_SCHEMA,
            source_table=source_table or self.source_table,
            target_schema=TARGET_SCHEMA,
            target_table=self.target_table,
            staging_schema=STAGING_SCHEMA,
            staging_database=staging_database or resolved_target_database,
            load_strategy=LoadStrategy.FULL_REFRESH,
            export_format="csv",
            compress_export=False,
            options={
                "source_type": "postgres",
                "sink_type": "mssql",
                "batch_commit_mode": "whole",
                "work_dir": str(self.transfer_root),
                "technical_columns": "forbidden",
                "lineage": False,
                "schema_contract": {
                    "enforcement": "strict",
                    "columns": {
                        "id": {"type": "integer", "nullable": False},
                        "metric_code": {"type": "string", "nullable": False},
                        "metric_value": {"type": "float", "nullable": False},
                        "note": {"type": "string", "nullable": True},
                    },
                },
                "bulk": {
                    "mode": "bcp",
                    "bcp": {"batch_size": 1000, "packet_size": 16384},
                },
            },
        )

    def runtime_environment_for(
        self,
        root: Path,
        *,
        coordinates: VendorCoordinates | None = None,
        target_database: str | None = None,
        state_database: str | None = None,
        target_database_authorities: Mapping[str, Any] | None = None,
        state_database_authorities: Mapping[str, Any] | None = None,
        postgres_source_authority: Mapping[str, Any] | None = None,
    ) -> dict[str, str]:
        """Write a fresh signed context for one reviewed database-authority case."""

        return _write_verified_runtime_context(
            root,
            coordinates=coordinates or self.coordinates,
            target_database=target_database or self.target_database,
            state_database=state_database or self.state_database,
            target_database_authorities=(
                self.target_database_authorities if target_database_authorities is None else target_database_authorities
            ),
            state_database_authorities=(
                self.state_database_authorities if state_database_authorities is None else state_database_authorities
            ),
            postgres_source_authority=(
                self.postgres_source_authority if postgres_source_authority is None else postgres_source_authority
            ),
        )

    def install_target_authority(self, connector: Any) -> None:
        """Reinstall the exact target schemas and preserved registry binding."""

        connector.execute_query(f"CREATE SCHEMA [{TARGET_SCHEMA}] AUTHORIZATION [dbo]")
        connector.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
        _install_target_identity_registry(
            connector,
            database=self.target_database,
            schema=TARGET_SCHEMA,
            table=self.target_table,
            binding_id=self.target_binding_id,
        )

    def install_state_authority(self, connector: Any) -> None:
        """Reinstall the exact CLI-rendered generic catalog after a DB recreate."""

        connector.execute_query(f"CREATE SCHEMA [{STATE_SCHEMA}] AUTHORIZATION [dbo]")
        _execute_cli_ddl(connector, self.rendered_state_ddl)


@contextmanager
def production_hydration_live_fixture(tmp_path: Path) -> Iterator[ProductionHydrationLiveFixture]:
    """Create fresh PG source and isolated target/state SQL Server databases."""

    suffix = uuid.uuid4().hex[:12]
    source_table = _safe(f"hydration_source_{suffix}")
    target_table = _safe(f"hydration_target_{suffix}")
    target_database = _safe(f"dpone_ph_target_{suffix}")
    state_database = _safe(f"dpone_ph_state_{suffix}")
    transfer_root = tmp_path / "transfer"
    context_root = tmp_path / "runtime"
    rendered_ddl = render_state_ddl_with_environment_cli(
        database=state_database,
        schema=STATE_SCHEMA,
    )

    master = mssql_connector(
        database="master",
        connect_timeout=MSSQL_ADMIN_CONNECT_TIMEOUT_SECONDS,
        query_timeout=MSSQL_ADMIN_QUERY_TIMEOUT_SECONDS,
    )
    postgres = postgres_connector()
    coordinates = VendorCoordinates.from_connectors(postgres, master)
    target = None
    state = None
    wait_until_ready("postgres", lambda: postgres.get_records("SELECT 1"))
    wait_until_ready("mssql", lambda: master.get_records("SELECT 1"))
    try:
        master.execute_query(f"CREATE DATABASE [{target_database}]")
        master.execute_query(f"CREATE DATABASE [{state_database}]")
        target = mssql_connector(database=target_database)
        state = mssql_connector(database=state_database)
        target.execute_query(f"CREATE SCHEMA [{TARGET_SCHEMA}] AUTHORIZATION [dbo]")
        target.execute_query(f"CREATE SCHEMA [{STAGING_SCHEMA}] AUTHORIZATION [dbo]")
        state.execute_query(f"CREATE SCHEMA [{STATE_SCHEMA}] AUTHORIZATION [dbo]")

        _execute_cli_ddl(state, rendered_ddl)
        target_binding_id = _install_target_identity_registry(
            target,
            database=target_database,
            schema=TARGET_SCHEMA,
            table=target_table,
        )
        _install_postgres_source(postgres, table=source_table)
        postgres_source_authority = factual_postgres_source_authority(
            postgres,
            load_config=SimpleNamespace(
                source_database=coordinates.postgres_database,
                source_schema=SOURCE_SCHEMA,
                source_table=source_table,
            ),
        )
        target_database_authorities = {
            target_database: _database_authority_payload(database_identity(target, target_database))
        }
        state_database_authorities = {
            state_database: _database_authority_payload(database_identity(state, state_database))
        }
        runtime_environment = _write_verified_runtime_context(
            context_root,
            coordinates=coordinates,
            target_database=target_database,
            state_database=state_database,
            target_database_authorities=target_database_authorities,
            state_database_authorities=state_database_authorities,
            postgres_source_authority=postgres_source_authority,
        )
        yield ProductionHydrationLiveFixture(
            coordinates=coordinates,
            postgres=postgres,
            target=target,
            state=state,
            source_table=source_table,
            target_table=target_table,
            target_database=target_database,
            state_database=state_database,
            target_binding_id=target_binding_id,
            postgres_source_authority=postgres_source_authority,
            target_database_authorities=target_database_authorities,
            state_database_authorities=state_database_authorities,
            transfer_root=transfer_root,
            runtime_environment=runtime_environment,
            rendered_state_ddl=rendered_ddl,
            rendered_state_ddl_sha256=hashlib.sha256(rendered_ddl.encode("utf-8")).hexdigest(),
        )
    finally:
        cleanup_production_hydration_resources(
            postgres=postgres,
            target=target,
            state=state,
            master=master,
            source_schema=SOURCE_SCHEMA,
            source_table=source_table,
            target_database=target_database,
            state_database=state_database,
        )


def state_user_tables(state: Any) -> tuple[dict[str, Any], ...]:
    """Read every user table in the isolated state database."""

    return tuple(
        state.get_records(
            "SELECT s.name AS schema_name, t.name AS table_name "
            "FROM sys.tables AS t "
            "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
            "WHERE t.is_ms_shipped = 0 ORDER BY s.name, t.name",
            as_dict=True,
        )
    )


def generic_state_row_counts(state: Any) -> dict[str, int]:
    """Read exact row counts from the four generic governance tables."""

    from dpone.runtime.state.mssql_generic_transaction_names import GENERIC_TRANSACTION_TABLES

    return {
        table: int(
            state.get_records(
                f"SELECT COUNT_BIG(*) AS row_count FROM [{STATE_SCHEMA}].[{table}]",
                as_dict=True,
            )[0]["row_count"]
        )
        for table in GENERIC_TRANSACTION_TABLES
    }


def generic_receipt_readback(state: Any) -> dict[str, Any]:
    """Read one committed receipt together with its target/fence authority."""

    rows = state.get_records(
        f"""
        SELECT r.receipt_id, r.operation_key, r.attempt_key, r.scope_hash,
               r.operation_epoch, r.owner_digest, r.load_id,
               r.payload_manifest_sha256, r.declared_rows, r.actual_raw_rows,
               r.actual_native_rows, r.native_contract_sha256,
               r.mutation_plan_sha256, r.target_before_sha256,
               r.target_after_sha256, r.extraction_started_at_utc,
               r.extraction_completed_at_utc, r.extraction_clock_authority,
               r.loaded_at_utc, r.inserted_rows, r.updated_rows, r.total_rows,
               r.staging_rows, r.replaced_rows, r.committed_at_utc,
               a.target_identity, a.generation, a.route_fingerprint,
               a.target_database, a.target_schema, a.target_table, a.strategy,
               f.current_generation, f.current_attempt_key,
               f.current_route_fingerprint
        FROM [{STATE_SCHEMA}].[dpone_load_receipt] AS r
        INNER JOIN [{STATE_SCHEMA}].[dpone_load_attempt] AS a
            ON a.attempt_key = r.attempt_key
        INNER JOIN [{STATE_SCHEMA}].[dpone_target_fence] AS f
            ON f.target_identity = a.target_identity
        """,
        as_dict=True,
    )
    if len(rows) != 1:
        raise AssertionError("production hydration must create exactly one generic receipt")
    return dict(rows[0])


def database_identity(connector: Any, database: str) -> dict[str, Any]:
    """Read database identity from SQL Server instead of trusting config text."""

    rows = connector.get_records(
        "SELECT d.database_id, d.name AS database_name, d.collation_name, "
        "CONVERT(nvarchar(33), d.create_date, 126) AS create_token, "
        "CONVERT(nvarchar(36), r.database_guid) AS database_guid "
        "FROM sys.databases AS d "
        "INNER JOIN sys.database_recovery_status AS r ON r.database_id = d.database_id "
        "WHERE d.database_id = DB_ID(?)",
        (database,),
        as_dict=True,
    )
    if len(rows) != 1:
        raise AssertionError(f"database identity is unavailable: {database}")
    return dict(rows[0])


def _execute_cli_ddl(state: Any, ddl: str) -> None:
    for batch in _GO_SEPARATOR.split(ddl):
        if batch.strip():
            state.execute_query(batch)


def _install_target_identity_registry(
    target: Any,
    *,
    database: str,
    schema: str,
    table: str,
    binding_id: uuid.UUID | None = None,
) -> uuid.UUID:
    fixture = Path(__file__).with_name("sql") / "postgres_xmin_mssql_target.sql"
    statements = fixture.read_text(encoding="utf-8").replace("__DATABASE__", database).split(_TARGET_REGISTRY_STATEMENT)
    for statement in statements[:2]:
        if statement := statement.strip():
            target.execute_query(statement)
    resolved_binding_id = binding_id or uuid.uuid4()
    target.execute_query(
        f"INSERT INTO [{database}].[dbo].[dpone_target_identity] "
        "([binding_id], [schema_name], [table_name]) VALUES (?, ?, ?)",
        (str(resolved_binding_id), schema, table),
    )
    return resolved_binding_id


def _install_postgres_source(postgres: Any, *, table: str) -> None:
    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{SOURCE_SCHEMA}"')
    postgres.execute_query(f'DROP TABLE IF EXISTS "{SOURCE_SCHEMA}"."{table}" CASCADE')
    postgres.execute_query(
        f'CREATE TABLE "{SOURCE_SCHEMA}"."{table}" ('
        "id integer NOT NULL, metric_code character varying(64) NOT NULL, "
        "metric_value double precision NOT NULL, note text NULL)"
    )
    for row in (
        (1, "requests.total", 12.5, "first"),
        (2, "errors.total", 0.0, None),
        (3, "latency.p99", 0.125, "Привет Ω"),
    ):
        postgres.execute_query(
            f'INSERT INTO "{SOURCE_SCHEMA}"."{table}" (id, metric_code, metric_value, note) VALUES (%s, %s, %s, %s)',
            row,
        )


def _write_verified_runtime_context(
    root: Path,
    *,
    coordinates: VendorCoordinates,
    target_database: str,
    state_database: str,
    target_database_authorities: Mapping[str, Any],
    state_database_authorities: Mapping[str, Any],
    postgres_source_authority: Mapping[str, Any],
) -> dict[str, str]:
    """Write an exact pinned init-fetch context and return its process env."""

    environment = "integration"
    context_identity = _canonical_json_bytes(
        {
            "source": {
                "host": coordinates.postgres_host,
                "port": coordinates.postgres_port,
                "database": coordinates.postgres_database,
                "authority": postgres_source_authority,
            },
            "target_database": target_database,
            "state_database": state_database,
            "target_database_authorities": target_database_authorities,
            "state_database_authorities": state_database_authorities,
        }
    )
    context_token = hashlib.sha256(context_identity).hexdigest()
    context_dir = f"sha256-{context_token}"
    context_root = root / "payload" / "runtime-connection-contexts" / context_dir
    context_root.mkdir(parents=True)
    payloads = {
        "binding_set": {
            "schema": "dpone.binding-set.v1",
            "environment": environment,
            "bindings": {
                _SOURCE_BINDING: {"connection_ref": _SOURCE_REGISTRY_REF},
                _TARGET_BINDING: {"connection_ref": _TARGET_REGISTRY_REF},
                _STATE_BINDING: {"connection_ref": _STATE_REGISTRY_REF},
            },
            "runtime": {},
        },
        "connection_registry": {
            "schema": "dpone.connection-registry.v1",
            "environment": environment,
            "connections": {
                _SOURCE_REGISTRY_REF: {
                    "type": "postgres",
                    "connection": {
                        "host": coordinates.postgres_host,
                        "port": coordinates.postgres_port,
                        "database": coordinates.postgres_database,
                        "schema": SOURCE_SCHEMA,
                        "postgres_source_authority": dict(postgres_source_authority),
                    },
                    "credentials": _env_credentials(
                        username=_PG_USERNAME_ENV,
                        password=_PG_PASSWORD_ENV,
                    ),
                },
                _TARGET_REGISTRY_REF: {
                    "type": "mssql",
                    "connection": _mssql_connection(
                        coordinates,
                        database=target_database,
                        schema=TARGET_SCHEMA,
                        database_authorities=target_database_authorities,
                    ),
                    "credentials": _env_credentials(
                        username=_MSSQL_USERNAME_ENV,
                        password=_MSSQL_PASSWORD_ENV,
                    ),
                },
                _STATE_REGISTRY_REF: {
                    "type": "mssql",
                    "connection": _mssql_connection(
                        coordinates,
                        database=state_database,
                        schema=STATE_SCHEMA,
                        database_authorities=state_database_authorities,
                    ),
                    "credentials": _env_credentials(
                        username=_MSSQL_USERNAME_ENV,
                        password=_MSSQL_PASSWORD_ENV,
                    ),
                },
            },
        },
        "credential_runtime": {
            "schema": "dpone.credential-runtime.v1",
            "environment": environment,
        },
    }
    descriptors: dict[str, RuntimeArtifactDescriptor] = {}
    for name, payload in payloads.items():
        filename = name.replace("_", "-") + ".json"
        content = _canonical_json_bytes(payload)
        (context_root / filename).write_bytes(content)
        descriptors[name] = RuntimeArtifactDescriptor(
            artifact_ref=f"cache://runtime-connection-contexts/{context_dir}/{filename}",
            sha256=_digest(content),
            bytes=len(content),
        )

    release_id = _literal_digest("production-hydration-release")
    deployment_id = _literal_digest("production-hydration-deployment")
    image_digest = _literal_digest("production-hydration-runtime-image")
    plan = RuntimeInitFetchPlan(
        environment=environment,
        trust_tier="non_production",
        release_id=release_id,
        deployment_id=deployment_id,
        runtime_image_ref=f"registry.example/dpone/runtime@{image_digest}",
        runtime_image_digest=image_digest,
        artifact_registry_ref="dpone-artifacts",
        registry_config_ref={
            "kind": "kubernetes_config_map",
            "name": "dpone-artifacts",
            "key": "registry.json",
            "sha256": _literal_digest("registry-config"),
        },
        trust_policy_ref=None,
        identity={
            "method": "kubernetes_workload_identity",
            "service_account": "dpone-runtime",
            "namespace": "data-platform",
        },
        release=_artifact(
            f"cache://releases/{release_id.replace(':', '-')}/release-set.json",
            "release-artifact",
        ),
        deployment=_artifact(
            f"cache://deployments/{environment}/{deployment_id.replace(':', '-')}/deployment.json",
            "deployment-artifact",
        ),
        binding_set=descriptors["binding_set"],
        connection_registry=descriptors["connection_registry"],
        credential_runtime=descriptors["credential_runtime"],
        workload_pack=RuntimeWorkloadPackRef(
            id="production-hydration-live",
            artifact_ref=f"cache://releases/{release_id.replace(':', '-')}/packs/production-hydration-live.json",
            sha256=_literal_digest("workload-pack"),
            bytes=512,
            pack_fingerprint=_literal_digest("workload-pack-fingerprint"),
        ),
        execution=RuntimeExecutionSelection(
            kind="runtime",
            selector="production-hydration-live",
        ),
        verify={"checksums": "required", "attestations": "optional"},
    )
    encoded_plan = canonical_runtime_init_fetch_plan_bytes(plan)
    from dpone.runtime.credentials.runtime_context import (
        RUNTIME_CONNECTION_CONTEXT_ENV,
        RUNTIME_INIT_FETCH_PLAN_B64_ENV,
        RUNTIME_INIT_FETCH_PLAN_SHA256_ENV,
    )

    return {
        RUNTIME_CONNECTION_CONTEXT_ENV: str(context_root),
        RUNTIME_INIT_FETCH_PLAN_B64_ENV: base64.b64encode(encoded_plan).decode("ascii"),
        RUNTIME_INIT_FETCH_PLAN_SHA256_ENV: _digest(encoded_plan),
        _PG_USERNAME_ENV: coordinates.postgres_username,
        _PG_PASSWORD_ENV: coordinates.postgres_password,
        _MSSQL_USERNAME_ENV: coordinates.mssql_username,
        _MSSQL_PASSWORD_ENV: coordinates.mssql_password,
    }


def _mssql_connection(
    coordinates: VendorCoordinates,
    *,
    database: str,
    schema: str,
    database_authorities: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "host": coordinates.mssql_host,
        "port": coordinates.mssql_port,
        "database": database,
        "schema": schema,
        "database_authorities": dict(database_authorities),
        "parameters": {
            "driver": coordinates.mssql_driver,
            "encrypt": "yes",
            "trust_server_certificate": "yes",
            "bcp_path": coordinates.mssql_bcp_path,
        },
    }


def _database_authority_payload(identity: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "database_id": int(identity["database_id"]),
        "create_token": str(identity["create_token"]),
        "database_guid": str(identity["database_guid"]).lower(),
    }


def _env_credentials(*, username: str, password: str) -> dict[str, Any]:
    return {
        "resolver": "env_var",
        "support": "development_only",
        "fields": {"username": username, "password": password},
    }


def _artifact(artifact_ref: str, token: str) -> RuntimeArtifactDescriptor:
    return RuntimeArtifactDescriptor(
        artifact_ref=artifact_ref,
        sha256=_literal_digest(token),
        bytes=128,
    )


def _canonical_json_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _digest(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _literal_digest(value: str) -> str:
    return _digest(value.encode("utf-8"))


def _safe(value: str) -> str:
    if _SAFE_IDENTIFIER.fullmatch(value) is None:
        raise ValueError(f"unsafe integration identifier: {value!r}")
    return value


__all__ = [
    "SOURCE_SCHEMA",
    "STAGING_SCHEMA",
    "STATE_SCHEMA",
    "TARGET_SCHEMA",
    "ProductionHydrationLiveFixture",
    "VendorCoordinates",
    "close_runtime_bindings",
    "database_identity",
    "generic_receipt_readback",
    "generic_state_row_counts",
    "production_hydration_live_fixture",
    "state_user_tables",
]
