"""Real staging/lineage authorities prepare duplicate-preserving native rows."""

import json
import re
from contextlib import nullcontext
from dataclasses import replace
from datetime import datetime
from hashlib import sha256
from types import SimpleNamespace

import pytest

from dpone.config.load_config import LoadConfig
from dpone.config.load_strategy import LoadStrategy
from dpone.contracts.mssql_native_chunks import NativeChunkPlan, NativeChunkReceipt
from dpone.runtime.consumed_payload_evidence import ConsumedPayloadEvidence, canonical_source_provenance_sha256
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.mssql_native_chunks_files import native_multiset_digest
from dpone.runtime.mssql_native_encoder import MssqlNativeEncoder
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer, NativeStageContext
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
from dpone.runtime.sinks.strategies.mssql.mssql_concrete_load_strategies import MSSQLFullRefreshStrategy
from tests.test_mssql_native_staged_recovery import prepared_fixture


class MemoryConnector:
    """Only SQL storage is synthetic; staging and lineage services are real."""

    def __init__(self):
        self.tables = {"[db].[stage].[chunk0]": [{"n": 7}, {"n": 7}], "[db].[stage].[chunk1]": [{"n": 7}]}
        self.properties = {}
        self.statements = []
        self.typed_readbacks = []

    def quote_identifier(self, name):
        return "[" + name.replace("]", "]]") + "]"

    def qualified_name(self, schema, table, *, database=None):
        return ".".join(self.quote_identifier(value) for value in (database, schema, table) if value)

    def begin(self):
        pass

    def commit_transaction(self):
        pass

    def rollback(self):
        pass

    def execute_query(self, sql, params=()):
        self.statements.append(sql)
        if sql.startswith("CREATE TABLE "):
            self.tables[sql.split("CREATE TABLE ", 1)[1].split(" (", 1)[0]] = []
        elif sql.startswith("DROP TABLE "):
            target = sql.removeprefix("DROP TABLE ")
            del self.tables[target]
            self.properties.pop(target, None)
        elif "sp_addextendedproperty" in sql:
            self.properties[self.qualified_name(params[1], params[2], database="db")] = params[0]
        elif sql.startswith("INSERT INTO "):
            target = sql.split("INSERT INTO ", 1)[1].split(" (", 1)[0]
            # A deliberately finite SQL storage double: execute the explicit raw
            # SELECT/UNION ALL source and validate the canonical metadata clauses.
            # Full expression/type parity is covered by the helper and live tests.
            sources = re.findall(r"FROM (\[[^]]+\]\.\[[^]]+\]\.\[[^]]+\])", sql)
            for source in sources:
                self.tables[target].extend(dict(row) for row in self.tables[source])
            if "AS [__dpone__load_id]" in sql:
                assert "N'load' AS [__dpone__load_id]" in sql
                assert "HASHBYTES" in sql
                assert "AS [__dpone__loaded_at]" in sql and "AS [__dpone__extracted_at]" in sql
                for row in self.tables[target]:
                    row.update(
                        __dpone__load_id="load",
                        __dpone__loaded_at=datetime(2026, 1, 1),
                        __dpone__row_id="f" * 64,
                        __dpone__extracted_at=datetime(2026, 1, 1),
                    )
        elif sql.startswith("UPDATE n SET "):
            target = sql.rsplit(" FROM ", 1)[1].removesuffix(" AS n")
            assert "[__dpone__load_id] = N'load'" in sql
            assert "HASHBYTES" in sql
            for row in self.tables[target]:
                row.update(
                    {
                        "__dpone__load_id": "load",
                        "__dpone__loaded_at": datetime(2026, 1, 1),
                        "__dpone__row_id": "f" * 64,
                        "__dpone__extracted_at": datetime(2026, 1, 1),
                    }
                )
        return 0

    def get_records(self, sql, params=(), **kwargs):
        if "extended_properties" in sql:
            return [(self.properties[params[0]],)]
        if "OBJECT_ID" in sql:
            return [(11 if params[0] in self.tables else None,)]
        if "COUNT_BIG" in sql:
            return [(len(self.tables[sql.split(" FROM ", 1)[1]]),)]
        return []

    def get_records_iterator(self, sql):
        self.typed_readbacks.append(sql)
        names = re.findall(r"\[([^\]]+)\]", sql.split(" FROM ", 1)[0])
        return iter({name: row[name] for name in names} for row in self.tables[sql.split(" FROM ", 1)[1]])


@pytest.mark.parametrize("counts", [(2, 1), (0,), (1, 1, 1)])
@pytest.mark.parametrize("default_lineage", [False, True])
def test_real_native_prepare_keeps_duplicates_across_chunks_and_persists_prepared_identity(
    tmp_path, counts, default_lineage
):
    base = prepared_fixture()
    connector = MemoryConnector()
    connector.tables.clear()
    manager = MSSQLStagingManager(connector)
    strategy = MSSQLFullRefreshStrategy(connector, SimpleNamespace(), manager)
    config = LoadConfig(
        "source",
        "target",
        "default",
        "events",
        "dbo",
        "target",
        target_database="db",
        staging_database="db",
        staging_schema="stage",
        options={
            "__dpone_load_identity": {"run_id": "run", "load_id": "load"},
            **({} if default_lineage else {"lineage": False}),
        },
    )
    wire = build_mssql_bcp_native_contract(schema=(("n", "int"),), query="test", target_format="mssql_native")
    encoder = MssqlNativeEncoder(wire, max_row_bytes=1024)
    receipts = []
    for ordinal, count in enumerate(counts):
        connector.tables[f"[db].[stage].[chunk{ordinal}]"] = [{"n": 7}] * count
        path = tmp_path / f"chunk{ordinal}.bin"
        path.write_bytes(encoder.encode_row({"n": 7}) * count)
        file = FileExportArtifact(str(path), ("n",), format="mssql-native", rows_exported=count)
        total = int.from_bytes(sha256(encoder.encode_row({"n": 7})).digest(), "big") * count % (1 << 256)
        part = (
            ConsumedPayloadEvidence.empty()
            .append_verified_file(
                file,
                validated_schema=(("n", "int"),),
                source_provenance_sha256=canonical_source_provenance_sha256(
                    relation_dialect=None, relation_schema=None, relation_metadata=None, fallback_schema=(("n", "int"),)
                ),
                actual_raw_rows=count,
                order_key=f"native:{ordinal:020d}",
            )
            .parts[0]
            .to_payload()
        )
        part["native_typed_sum"] = total
        receipts.append(
            NativeChunkReceipt(
                ordinal,
                f"attempt{ordinal}",
                f"[db].[stage].[chunk{ordinal}]",
                count,
                path.stat().st_size,
                sha256(path.read_bytes()).hexdigest(),
                native_multiset_digest(count, total),
                part,
            )
        )

    class Journal:
        @property
        def publication(self):
            return self

        state = None

        def preparation_started(self, binding):
            self.state = binding

        def prepared(self, binding):
            assert binding["planned_stage"] == self.state["planned_stage"]
            self.state = binding

    journal = Journal()

    class Executor:
        def stage(self, plan, rows, contract, lease, **kwargs):
            assert list(rows) == [(7,)] * sum(counts)
            assert kwargs["completion_metadata"]()["lifecycle"]["extraction_completed_at"]
            return SimpleNamespace(receipts=receipts, rows=sum(counts))

    capacity_observations = []

    def capacity(extra_tables):
        observed = len(connector.tables)
        capacity_observations.append((observed, extra_tables))
        if observed + extra_tables > len(counts) + 1:
            raise ValueError("staging_table_limit_exceeded")

    context = NativeStageContext(
        NativeChunkPlan("run", "target", "query", "window", "schema", "wire"),
        wire,
        Executor(),
        object(),
        lambda: iter([(7,)] * sum(counts)),
        lambda receipts: None,
        lambda receipts: None,
        capacity,
        lambda: journal,
        nullcontext,
        max_row_bytes=4,
    )
    payload = SimpleNamespace(
        schema=(("n", "int"),),
        relation_schema=None,
        relation_metadata=None,
        relation_dialect=None,
        target_projection=None,
        mssql_transaction_admission=base.admission,
        mssql_target_mutation_plan=base.mutation_plan,
        require_completed_extraction=lambda: base.source_lifecycle,
    )
    sink = SimpleNamespace(connector=connector, _strategy_map={LoadStrategy.FULL_REFRESH: strategy})
    preparer = MssqlNativeStagePreparer(sink, lambda *args: context)
    prepared = preparer.stage(config, payload)
    assert prepared.staging.row_count == sum(counts)
    assert [row["n"] for row in connector.tables[strategy._staging_name(prepared.staging)]] == [7] * sum(counts)
    if len(counts) > 1:
        assert any(" UNION ALL " in sql for sql in connector.statements)
    assert journal.state["stage"]["consumed_payload_evidence"]["actual_native_rows"] == sum(counts)
    preparer.reverify(prepared)
    # One initial dual digest plus an independent prepublication full digest.
    # Restoring below deliberately adds another boundary and therefore a scan.
    assert len(connector.typed_readbacks) == 2
    assert not any(sql.startswith("UPDATE n SET ") for sql in connector.statements)
    if default_lineage:
        assert tuple(prepared.staging.columns) == (
            "n",
            "__dpone__load_id",
            "__dpone__loaded_at",
            "__dpone__row_id",
            "__dpone__extracted_at",
        )
    from dpone.runtime.sinks.mssql_native_recovery import restore_prepared

    restored = restore_prepared(
        json.loads(json.dumps(journal.state)),
        admission=base.admission,
        staging_manager=manager,
        interval=None,
        resources=prepared.resources,
    )
    preparer.reverify(restored)
    assert restored.staging.consumed_payload_evidence == prepared.staging.consumed_payload_evidence
    assert restored.source_lifecycle == base.source_lifecycle

    if len(counts) == 3:
        # Three completed chunks plus the owned preparing object fill all four
        # slots. Recovery must settle that object before reserving its replacement.
        assert len(connector.tables) == 4
        journal.state = {"planned_stage": journal.state["planned_stage"]}

        class RecoveryExecutor:
            def recover(self, plan, lease):
                return SimpleNamespace(receipts=receipts, rows=sum(counts))

        def forbidden_source():
            raise AssertionError("completed staging recovery must not read the source")

        recovering = replace(
            context,
            recover=True,
            executor=RecoveryExecutor(),
            completed_lifecycle=base.source_lifecycle,
            row_source=forbidden_source,
        )
        rebuilt = preparer.stage(config, payload, context=recovering)
        preparer.reverify(rebuilt)
        assert rebuilt.staging.row_count == 3
        assert len(connector.tables) == 4
        assert (4, 0) in capacity_observations
        assert capacity_observations.count((3, 1)) == 2
        assert all(extra == 0 for existing, extra in capacity_observations if existing == 4)
        assert any(sql.startswith("DROP TABLE ") for sql in connector.statements)

    connector.tables[strategy._staging_name(prepared.staging)].append(
        {
            "n": 99,
            **(
                {
                    "__dpone__load_id": "load",
                    "__dpone__loaded_at": datetime(2026, 1, 1),
                    "__dpone__row_id": "f" * 64,
                    "__dpone__extracted_at": datetime(2026, 1, 1),
                }
                if default_lineage
                else {}
            ),
        }
    )
    with pytest.raises(ValueError, match="prepared_count_mismatch"):
        preparer.reverify(prepared)
