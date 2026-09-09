from __future__ import annotations

from typing import Any

from dpone.strategy_intelligence.live_backends import ClickHouseReplayBackend, MssqlReplayBackend
from dpone.strategy_intelligence.replay import ReplayExecutionRequest
from dpone.strategy_intelligence.replay_adapters import DbReplayAdapter


def test_clickhouse_replace_replay_uses_truncate_insert_and_sync_state_mutation() -> None:
    client = _RecordingSqlClient()
    request = ReplayExecutionRequest(
        action="resync",
        run_id="01JREPLAYCH000000000000001",
        source_type="postgres",
        sink_type="clickhouse",
        strategy_mode="replace",
        yes=True,
    )

    result = DbReplayAdapter(
        backend=ClickHouseReplayBackend(
            client=client,
            target_schema="replay_target",
            target_table="orders",
            staging_schema="replay_stage",
        )
    ).execute(request)

    assert result.status == "executed"
    assert result.state_committed is True
    assert client.statements == [
        "TRUNCATE TABLE `replay_target`.`orders`",
        "INSERT INTO `replay_target`.`orders` SELECT * FROM `replay_stage`.`orders__replay_staging`",
        "SELECT 1 FROM `replay_stage`.`orders__replay_staging` LIMIT 1",
        (
            "ALTER TABLE `etl_state`.`__dpone__loads` "
            "UPDATE status = 'committed' WHERE run_id = '01JREPLAYCH000000000000001' SETTINGS mutations_sync = 2"
        ),
    ]


def test_mssql_replay_remains_transactional_delete_insert() -> None:
    client = _RecordingSqlClient()
    request = ReplayExecutionRequest(
        action="resync",
        run_id="01JREPLAYMS000000000000001",
        source_type="postgres",
        sink_type="mssql",
        strategy_mode="incremental_merge",
        yes=True,
    )

    result = DbReplayAdapter(
        backend=MssqlReplayBackend(
            client=client,
            target_schema="dbo",
            target_table="orders",
            staging_schema="staging",
        )
    ).execute(request)

    assert result.status == "executed"
    assert client.statements[:4] == [
        "BEGIN TRANSACTION",
        "DELETE FROM [dbo].[orders] WHERE EXISTS (SELECT 1 FROM [staging].[orders__replay_staging] s)",
        "INSERT INTO [dbo].[orders] SELECT * FROM [staging].[orders__replay_staging]",
        "COMMIT TRANSACTION",
    ]


class _RecordingSqlClient:
    def __init__(self) -> None:
        self.statements: list[str] = []

    def exists(self, schema: str, table: str) -> bool:
        del schema, table
        return True

    def execute(self, statement: str) -> None:
        self.statements.append(statement)

    def scalar(self, statement: str) -> Any:
        self.statements.append(statement)
        return 1
