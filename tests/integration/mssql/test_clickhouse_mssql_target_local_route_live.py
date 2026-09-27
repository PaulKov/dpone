"""Live ClickHouse rows through target-local BCP into atomic MSSQL publication."""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from hashlib import sha256

import pytest
from tests.integration.mssql.mssql_live_support import clickhouse_connector, mssql_connector

from dpone.adapters.bounded_window_sqlite import SQLiteWindowStore
from dpone.adapters.mssql_native_custody import NativeTargetCustody
from dpone.contracts.mssql_native_chunks import NativeChunkLimits, NativeChunkPlan
from dpone.contracts.mssql_native_verification import NativeVerificationBackend, NativeVerificationIdentityV2
from dpone.runtime.connectors.mssql_bulk import BcpOptions
from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
from dpone.runtime.sinks.mssql_native_composition import compose_native_stage_context
from dpone.runtime.sinks.mssql_native_target_digest import build_target_digest_sql, decode_target_digest_row

pytestmark = [pytest.mark.integration_live, pytest.mark.integration_mssql, pytest.mark.integration_clickhouse]


def test_clickhouse_rows_use_aggregate_only_verification_and_atomic_publication(tmp_path) -> None:
    clickhouse = clickhouse_connector()
    target = mssql_connector()
    suffix = uuid.uuid4().hex[:16]
    source_table = f"dpone_p1_source_{suffix}"
    target_table = f"dpone_p1_target_{suffix}"
    limited_user = f"dpone_p1_limited_{suffix}"
    target_id = f"synthetic-target-{suffix}"
    schema = (
        ("row_key", "bigint"),
        ("ratio", "float(53) nullable"),
        ("text_value", "nvarchar(max) nullable"),
        ("happened_at", "datetime2(6) nullable"),
    )
    wire = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic", target_format="mssql_native")
    plan = NativeChunkPlan(
        "synthetic-run", target_id, "synthetic-query", "synthetic-window", "synthetic-schema", wire.type_layout_hash
    )
    identity = NativeVerificationIdentityV2(
        plan,
        "bcp",
        NativeVerificationBackend.TARGET_LOCAL,
        sha256(b"no-companion-protocol").hexdigest(),
        sha256(b"no-companion-package").hexdigest(),
        sha256(wire.type_layout_hash.encode()).hexdigest(),
        "mssql-native-sha256-sum-v1",
        sha256(b"synthetic-timeout-policy").hexdigest(),
    )
    store = SQLiteWindowStore(tmp_path / "route-state.sqlite", clock=lambda: 1.0)
    lease = store.acquire(target_id, "synthetic-owner", 300)
    custody = NativeTargetCustody(store, target_id)
    custody.claim(lease, identity.invocation_key)
    stages: list[str] = []
    rows = []
    context = None
    try:
        clickhouse.execute_query(
            f"CREATE TABLE `{source_table}` (row_key Int64, ratio Nullable(Float64), "
            "text_value Nullable(String), happened_at Nullable(DateTime64(6, 'UTC'))) ENGINE=Memory"
        )
        clickhouse.execute_query(
            f"INSERT INTO `{source_table}` VALUES "
            "(1,-1.5,'alpha','2024-02-29 23:59:59.999999'),"
            "(2,0.0,'',NULL),(3,NULL,NULL,'2030-06-01 12:30:45.123456'),"
            "(3,NULL,NULL,'2030-06-01 12:30:45.123456')"
        )
        fetched = clickhouse.get_records(
            f"SELECT row_key,ratio,text_value,happened_at FROM `{source_table}` ORDER BY row_key,happened_at"
        )
        rows = [
            (row_key, ratio, text_value, happened_at.replace(tzinfo=None) if happened_at is not None else None)
            for row_key, ratio, text_value, happened_at in fetched
        ]
        target.execute_query(
            f"CREATE TABLE [dbo].[{target_table}] ("
            "[row_key] bigint NOT NULL,[ratio] float NULL,[text_value] nvarchar(max) NULL,"
            "[happened_at] datetime2(6) NULL)"
        )
        target.execute_query(f"INSERT INTO [dbo].[{target_table}] VALUES (-1,NULL,N'before-publication',NULL)")

        @contextmanager
        def importer_connection():
            connector = mssql_connector()
            connector.get_records_iterator = lambda *_args, **_kwargs: pytest.fail(
                "target-local verification must not return business rows"
            )
            try:
                yield connector
            finally:
                connector.close()

        context = compose_native_stage_context(
            store=store,
            plan=plan,
            lease=lease,
            wire_contract=wire,
            limits=NativeChunkLimits(
                max_total_encoded_bytes=8 << 20,
                stage_allocated_bytes_stop_threshold=1 << 30,
                max_rows=1024,
                max_bytes=4 << 20,
                max_row_bytes=1 << 20,
                max_pending=1,
                max_staging_tables=32,
                parallelism=1,
            ),
            work_dir=tmp_path / "native-files",
            target_connector=target,
            importer_connection=importer_connection,
            bcp_options_factory=lambda **values: BcpOptions(bcp_path=target.bcp_path, **values),
            database=target.database,
            schema="dbo",
            row_source=lambda: iter(rows),
            journal_factory=lambda: pytest.fail("target-local route must select journal v2"),
            cancelled=__import__("threading").Event(),
            required_target_headroom_bytes=8 << 20,
            verification_identity=identity,
            target_local_timeout_seconds=30,
        )

        complete = context.executor.stage(plan, iter(rows), wire, lease)
        assert complete.rows == len(rows)
        assert len(complete.receipts) == 1
        receipt = complete.receipts[0]
        stages.append(receipt.stage_id)
        context.verify_receipts(complete.receipts)
        events = context.journal_factory().data["events"][receipt.attempt_id]
        assert [event["event"] for event in events] == [
            "INTENT",
            "STAGE_OWNED",
            "GRANTED",
            "WRITING",
            "WRITER_TERMINAL",
            "QUIESCENT",
            "VERIFIED",
        ]

        # Rollback proves the previous target remains authoritative until commit.
        target.begin()
        target.execute_query(f"TRUNCATE TABLE [dbo].[{target_table}]")
        target.execute_query(f"INSERT INTO [dbo].[{target_table}] SELECT * FROM {receipt.stage_id}")
        target.rollback()
        assert target.get_records(f"SELECT [row_key] FROM [dbo].[{target_table}]") == [(-1,)]

        target.begin()
        target.execute_query(f"TRUNCATE TABLE [dbo].[{target_table}]")
        target.execute_query(f"INSERT INTO [dbo].[{target_table}] SELECT * FROM {receipt.stage_id}")
        target.commit_transaction()
        aggregate = decode_target_digest_row(
            target.get_records(build_target_digest_sql(f"[{target.database}].[dbo].[{target_table}]", wire, len(rows)))[
                0
            ],
            expected_rows=len(rows),
        )
        assert aggregate.rows == len(rows)
        assert aggregate.typed_digest == receipt.typed_digest

        target.execute_query(f"UPDATE {receipt.stage_id} SET [text_value]=N'drifted' WHERE [row_key]=1")
        with pytest.raises(ValueError, match="typed_digest_mismatch"):
            context.verify_receipts(complete.receipts)
        target.execute_query(f"UPDATE {receipt.stage_id} SET [text_value]=N'alpha' WHERE [row_key]=1")
        context.verify_receipts(complete.receipts)

        with context.executor.importer_factory() as importer:
            importer.drop_exact_owned(plan, receipt, lease)
            importer.drop_exact_owned(plan, receipt, lease)

        target.execute_query(f"CREATE USER [{limited_user}] WITHOUT LOGIN")
        with context.executor.importer_factory() as importer:
            importer.connector.execute_query(f"EXECUTE AS USER = N'{limited_user}'")
            try:
                with pytest.raises(ValueError, match="stage_absence_visibility_unproved"):
                    importer.drop_exact_owned(plan, receipt, lease)
            finally:
                importer.connector.execute_query("REVERT")
        target.execute_query(f"DROP USER [{limited_user}]")

        target.execute_query(
            f"CREATE TABLE {receipt.stage_id} ("
            "[row_key] bigint NOT NULL,[ratio] real NULL,[text_value] nvarchar(max) NULL,"
            "[happened_at] datetime2(6) NULL)"
        )
        with pytest.raises(ValueError, match="stage_identity_mismatch|prepared_owner"):
            context.verify_receipts(complete.receipts)
        with context.executor.importer_factory() as importer:
            with pytest.raises(ValueError, match="stage_identity_mismatch|prepared_owner"):
                importer.drop_exact_owned(plan, receipt, lease)
        assert target.get_records(f"SELECT OBJECT_ID(N'{receipt.stage_id}')")[0][0] is not None

        target.execute_query(f"DROP TABLE {receipt.stage_id}")
    finally:
        for stage in stages:
            target.execute_query(f"DROP TABLE IF EXISTS {stage}")
        target.execute_query(f"DROP USER IF EXISTS [{limited_user}]")
        target.execute_query(f"DROP TABLE IF EXISTS [dbo].[{target_table}]")
        clickhouse.execute_query(f"DROP TABLE IF EXISTS `{source_table}`")
        target.close()
        clickhouse.close()
