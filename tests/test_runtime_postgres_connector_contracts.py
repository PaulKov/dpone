from __future__ import annotations

from typing import Any

from dpone.runtime.connectors.postgres import PostgresConnector


def test_postgres_connector_exposes_row_iterator_contract_for_incremental_strategies() -> None:
    connector = PostgresConnector.__new__(PostgresConnector)

    def fake_streaming(query: Any, params=None, batch_size: int = 10000, as_dict: bool = False):
        assert query == "SELECT 1"
        assert params == ("p",)
        assert batch_size == 10000
        assert as_dict is True
        yield [{"id": 1}, {"id": 2}]
        yield [{"id": 3}]

    connector.get_records_streaming = fake_streaming  # type: ignore[method-assign]

    assert list(PostgresConnector.get_records_iterator(connector, "SELECT 1", params=("p",))) == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]
