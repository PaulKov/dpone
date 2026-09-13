"""A native EXCHANGE must never replay an outcome the client cannot establish."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from dpone.runtime.connectors.clickhouse_query_ops import ClickHouseQueryOps


class _ExchangeClient:
    """Model table identities and inject one uncertain response after dispatch."""

    def __init__(self, error: RuntimeError | None, *, apply_before_error: bool = True) -> None:
        self.error = error
        self.apply_before_error = apply_before_error
        self.identities = ("old-generation", "new-generation")
        self.calls: list[tuple[str, Any, Any]] = []

    def execute(self, query: str, params: Any, *, settings: Any) -> None:
        self.calls.append((query, params, settings))
        if self.apply_before_error or len(self.calls) > 1:
            self.identities = self.identities[::-1]
        if len(self.calls) == 1 and self.error is not None:
            raise self.error


def _query_service(client: _ExchangeClient) -> ClickHouseQueryOps:
    return ClickHouseQueryOps(
        SimpleNamespace(
            connection=client,
            settings={
                "skip_unavailable_shards": 0,
                "distributed_ddl_output_mode": "throw",
                "distributed_ddl_task_timeout": 45,
            },
            logger=SimpleNamespace(),
            print_query=lambda *args: None,
        )
    )


@pytest.mark.parametrize("prefix", ["EXCHANGE TABLES", " \n exchange   tables"])
@pytest.mark.parametrize("message", ["Code: 517. Metadata lag", "Metadata on replica is not up to date"])
def test_exchange_uncertain_response_is_dispatched_once(prefix: str, message: str) -> None:
    """An injected applied-then-error response must not swap the old data back."""
    original = RuntimeError(message)
    client = _ExchangeClient(original)
    query = f"{prefix} `synthetic`.`items` AND `synthetic`.`generation` ON CLUSTER `synthetic_cluster`"

    with pytest.raises(RuntimeError, match=message) as caught:
        _query_service(client).execute_query(query)

    assert caught.value.__cause__ is original
    assert len(client.calls) == 1
    assert client.identities == ("new-generation", "old-generation")


def test_exchange_pre_effect_error_is_not_assumed_safe_to_retry() -> None:
    original = RuntimeError("Code: 517. Metadata on replica is not up to date")
    client = _ExchangeClient(original, apply_before_error=False)

    with pytest.raises(RuntimeError) as caught:
        _query_service(client).execute_query("EXCHANGE TABLES `synthetic`.`items` AND `synthetic`.`generation`")

    assert caught.value.__cause__ is original
    assert len(client.calls) == 1
    assert client.identities == ("old-generation", "new-generation")


def test_successful_exchange_keeps_parameters_and_ddl_settings() -> None:
    client = _ExchangeClient(None)
    query = "EXCHANGE TABLES `synthetic`.`items` AND `synthetic`.`generation` ON CLUSTER `synthetic_cluster`"
    parameters = ("synthetic-value",)

    assert _query_service(client).execute_query(query, parameters) == 0

    assert client.calls == [
        (
            query,
            parameters,
            {"skip_unavailable_shards": 0, "distributed_ddl_output_mode": "throw", "distributed_ddl_task_timeout": 45},
        )
    ]
    assert client.identities == ("new-generation", "old-generation")
