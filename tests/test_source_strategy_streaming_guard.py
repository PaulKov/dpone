"""SourceStrategy fail-closed path for empty streaming dict rows."""

from __future__ import annotations

import pytest

from dpone.runtime.sources.strategies.base import SourceStrategy


class _FakeConnector:
    def __init__(self, batches):
        self._batches = batches

    def get_records(self, query, params=None, as_dict=False):
        del query, params, as_dict
        return [(20_001,)]

    def get_records_streaming(self, query, params=None, batch_size=10000, as_dict=False):
        del query, params, batch_size, as_dict
        yield from self._batches


class _Strategy(SourceStrategy):
    def get_state(self, load_config):
        del load_config
        return None

    def extract(self, load_config, last_state):
        del load_config, last_state
        raise NotImplementedError


def test_source_strategy_streaming_fail_closed_on_empty_dicts() -> None:
    strategy = _Strategy()
    artifact = strategy._build_rows_artifact(
        _FakeConnector([[{}]]),
        "SELECT sessionId FROM t",
        schema=[("sessionId", "String")],
    )

    with pytest.raises(RuntimeError, match="empty row mappings"):
        list(artifact._iterator)
