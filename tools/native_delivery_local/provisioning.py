"""One-time explicit synthetic fixture setup, kept outside measured delivery."""

from __future__ import annotations

import itertools
import re
from dataclasses import asdict, dataclass
from datetime import datetime
from typing import Any

from tools.mssql_stress_governance_mssql import (
    bind_factual_mssql_database_authority,
    bind_target_identity,
    ensure_target_identity_registry,
)
from tools.native_delivery_live_support.profiles import WINDOW_END, WINDOW_START, Dataset

from dpone.runtime.connector_logging import etl_logger
from dpone.runtime.etl.mssql_schema_preplan import MSSQL_SCHEMA_PREPLAN_OPTION
from dpone.runtime.etl.mssql_transaction_admission import ADMISSION_OPTION, MssqlTransactionAdmissionService
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sinks.mssql_native_recovery import mutation_snapshot
from dpone.runtime.sinks.strategies.mssql.mssql_native_lineage import resolve_mssql_native_lineage_columns
from dpone.runtime.sources.clickhouse import ClickHouseSource
from dpone.runtime.state.mssql_generic_transaction_ddl import render_generic_transaction_catalog_ddl
from dpone.runtime.state.mssql_generic_transaction_storage import MssqlGenericTransactionStateStorage

from .configuration import load_configuration
from .layout import business_definitions, observed_layout
from .source_guard import ddl_lock


@dataclass(frozen=True)
class RunContext:
    run_id: str
    config: dict[str, str]


@dataclass(frozen=True)
class LoadRecord:
    load_id: str


class AuthorityCapture:
    """Capture the helper-issued real verifier while binding actual storage."""

    def __init__(self, storage):
        self.storage = storage
        self.verifier = None

    def bind_database_authority(self, verifier):
        self.verifier = verifier
        self.storage.bind_database_authority(verifier)


def json_value(value: Any) -> Any:
    """Detach known non-secret request fields without dynamic type deserialization."""
    from collections.abc import Mapping

    if isinstance(value, Mapping):
        return {k: json_value(v) for k, v in value.items()}
    if isinstance(value, (tuple, list)):
        return [json_value(v) for v in value]
    if isinstance(value, bytes):
        return value.hex()
    if isinstance(value, datetime):
        return value.isoformat()
    return value


def provision(environment, store, inventory):
    """Return frozen requests and planner decisions from real connector catalogs."""
    config = load_configuration(inventory)
    dataset = Dataset(inventory.profile, inventory.rows, inventory.seed)
    ch = environment.clickhouse()
    try:
        with (
            ddl_lock(store.directory(inventory.invocation_id), writer=True),
            environment.sql_scope() as target,
            environment.sql_scope() as state_connector,
            environment.sql_scope("master") as master,
        ):
            schema = inventory.schema
            target.execute_query(f"CREATE SCHEMA [{schema}]")
            for batch in re.split(
                r"(?im)^GO\s*$",
                render_generic_transaction_catalog_ddl(database=inventory.state_database, schema=schema),
            ):
                if batch.strip():
                    state_connector.execute_query(batch)
            columns = dataset.schema()
            definitions = business_definitions(dataset)
            lineage = resolve_mssql_native_lineage_columns(config)
            definitions += [
                f"[{name}] {dtype} {'NULL' if nullable else 'NOT NULL'}" for name, dtype, nullable, _, _ in lineage
            ]
            target.execute_query(f"CREATE TABLE [{schema}].[business] ({', '.join(definitions)})")
            ensure_target_identity_registry(target, database=inventory.target_database)
            bind_target_identity(target, database=inventory.target_database, schema=schema, table="business")
            ch_columns = ", ".join(f"`{c['name']}` {c['source']}" for c in columns)
            ch.execute_query(
                f"CREATE TABLE `{inventory.source_database}`.`{schema}` ({ch_columns}) ENGINE=MergeTree ORDER BY tuple()"
            )
            iterator = dataset.generate()
            while batch := tuple(itertools.islice(iterator, 1000)):
                ch.connection.execute(
                    f"INSERT INTO `{inventory.source_database}`.`{schema}` VALUES",
                    [tuple(row[c["name"]] for c in columns) for row in batch],
                )
            old = next(Dataset(inventory.profile, 1, inventory.seed).generate())
            dates = (WINDOW_START, WINDOW_END) if inventory.strategy == "partition_replace" else (WINDOW_START,)
            names = [c["name"] for c in columns] + [c[0] for c in lineage]
            for ordinal, date in enumerate(dates):
                row = dict(old, id=-1 - ordinal, event_at=date)
                values = [row[c["name"]] for c in columns]
                values += [datetime(2025, 1, 1) if "datetime" in c[1] else "fixture-before" for c in lineage]
                target.execute_query(
                    f"INSERT INTO [{schema}].[business] ({','.join('[' + n + ']' for n in names)}) VALUES ({','.join('?' for _ in values)})",
                    tuple(values),
                )
            storage = MssqlGenericTransactionStateStorage(
                state_connector, database=inventory.state_database, schema=schema
            )
            capture = AuthorityCapture(storage)
            bind_factual_mssql_database_authority(
                capture,
                master,
                target_database=inventory.target_database,
                staging_database=inventory.target_database,
                state_database=inventory.state_database,
            )
            if capture.verifier is None:
                raise ValueError("local_fixture.database_authority_unavailable")
            sink = MSSQLSink(target, state_storage=storage, logger=etl_logger)
            source = ClickHouseSource(ch, etl_logger, sink_connector=target)
            config = MssqlTransactionAdmissionService().prepare(
                config,
                source=source,
                sink=sink,
                run_context=RunContext(inventory.invocation_id, {"pipeline_id": "dda", "task_id": schema}),
                load_record=LoadRecord(inventory.invocation_id[:26]),
                dag_id="dda_local_native",
            )
            operation = config.options[ADMISSION_OPTION].operation
            if operation is None:
                raise ValueError("local_fixture.unexpected_provisioning_replay")
            preplan = config.options[MSSQL_SCHEMA_PREPLAN_OPTION]
            binding = target.get_records(
                "SELECT CONVERT(varchar(36),binding_id) FROM dbo.dpone_target_identity WHERE schema_name=? AND table_name=?",
                (schema, "business"),
            )[0][0]
            results = {
                "target_layout": observed_layout(target, f"[{schema}].[business]"),
                "attempt": json_value(asdict(operation.attempt.request)),
                "operation_request": {
                    "scope_hash": operation.scope_hash.hex(),
                    "owner_digest": operation.owner_digest.hex(),
                    "lease_expires_at_utc": json_value(operation.lease_expires_at_utc),
                },
                "target_properties": json_value(capture.verifier.target_connection.descriptor.properties),
                "state_properties": json_value(capture.verifier.state_connection.descriptor.properties),
                "preplan": {
                    "source_schema_sha256": preplan.source_schema_sha256.hex(),
                    "target_column_types": list(preplan.target_column_types),
                    "mutation": mutation_snapshot(preplan.target_mutation_plan),
                },
                "target_object_id": target.get_records("SELECT OBJECT_ID(?)", (f"[{schema}].[business]",))[0][0],
                "binding_id": str(binding),
                "source_uuid": ch.get_records(
                    "SELECT toString(uuid) FROM system.tables WHERE database=%(database)s AND name=%(table)s",
                    {"database": inventory.source_database, "table": schema},
                )[0][0],
            }
            store.seal(inventory.invocation_id, results)
            return results
    finally:
        ch.close()
