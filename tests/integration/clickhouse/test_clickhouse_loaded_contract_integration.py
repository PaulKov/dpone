"""Live ClickHouse proof for an opted-in native contract.

The fixture is a disposable database on the integration ClickHouse. It checks
three facts the unit doubles cannot: a non-nullable column rejects NULL at
insert, a nullable column that violates the contract fails the staging
observation, and a row-count mismatch fails it too.
"""

from __future__ import annotations

import uuid

import pytest

from dpone.readiness.schema_contracts import SchemaContract
from dpone.runtime.etl.contract_artifacts import ContractValidatedFileArtifact
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.sinks.clickhouse_loaded_contract import (
    ClickHouseLoadedContractError,
    NativeContractObservation,
)
from dpone.runtime.sinks.load_payload import LoadPayload

pytestmark = [pytest.mark.integration, pytest.mark.integration_clickhouse]


def test_loaded_contract_matches_rejects_nulls_and_row_count(clickhouse_connector, tmp_path) -> None:
    database = "loaded_contract_" + uuid.uuid4().hex[:12]
    connector = clickhouse_connector
    try:
        connector.execute_query(f"CREATE DATABASE `{database}`")
        connector.execute_query(f"CREATE TABLE `{database}`.strict (id Int64) ENGINE = MergeTree ORDER BY id")
        with pytest.raises(Exception, match="(?i)null"):
            connector.execute_query(f"INSERT INTO `{database}`.strict (id) VALUES (NULL)")

        connector.execute_query(
            f"CREATE TABLE `{database}`.observed (id Int64, note Nullable(String)) ENGINE = MergeTree ORDER BY id"
        )
        connector.execute_query(f"INSERT INTO `{database}`.observed (id, note) VALUES (1, 'a'), (2, 'b')")
        _observe(connector, tmp_path, database, "observed", rows=2, staged_rows=2, nullable=False)

        connector.execute_query(f"INSERT INTO `{database}`.observed (id, note) VALUES (3, NULL)")
        with pytest.raises(ClickHouseLoadedContractError) as nulls:
            _observe(connector, tmp_path, database, "observed", rows=3, staged_rows=3, nullable=False)
        assert nulls.value.blocker == "not_null_violation:note"

        with pytest.raises(ClickHouseLoadedContractError) as mismatch:
            _observe(connector, tmp_path, database, "observed", rows=2, staged_rows=3, nullable=True)
        assert mismatch.value.blocker == "row_count_mismatch"
    finally:
        connector.execute_query(f"DROP DATABASE IF EXISTS `{database}`")


def _observe(connector, tmp_path, database: str, table: str, *, rows: int, staged_rows: int, nullable: bool) -> None:
    path = tmp_path / f"{table}-{rows}-{staged_rows}.bcp"
    path.write_bytes(b"\x00" * rows)
    inner = FileExportArtifact(str(path), ("id", "note"), format="mssql-bcp-native", rows_exported=rows)
    contract = SchemaContract.from_config(
        {
            "opaque_native_proof": "clickhouse_staging",
            "columns": {
                "id": {"type": "bigint", "nullable": False},
                "note": {"type": "string", "nullable": nullable},
            },
        }
    )
    wrapper = ContractValidatedFileArtifact(
        inner,
        contract=contract,
        schema=(("id", "bigint"), ("note", "string")),
        run_id="run",
        load_id="load",
    )
    observation = NativeContractObservation(
        wrapper=wrapper,
        payload=LoadPayload(artifact=inner, schema=[("id", "bigint"), ("note", "string")]),
        contract=contract,
    )
    observation.require(connector, database=database, table=table, staged_rows=staged_rows)
