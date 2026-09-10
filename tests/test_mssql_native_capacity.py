"""Capacity observations fail before extraction when permission or space is missing."""

from types import SimpleNamespace

import pytest

from dpone.adapters.mssql_native_capacity import require_native_target_capacity
from dpone.manifest.mssql_native_policy import native_limits
from tests.test_mssql_native_policy import config


def test_target_capacity_requires_log_headroom_and_complete_observations():
    limits = native_limits(config())
    files = [{"type": kind, "allocated_bytes": 100, "available_bytes": 10000} for kind in (0, 1)]
    results = iter([files, [{"free_bytes": 10}], [{"allocated_bytes": 0}]])
    connector = SimpleNamespace(get_records=lambda *args, **kwargs: next(results))
    with pytest.raises(ValueError, match="log_headroom"):
        require_native_target_capacity(connector, limits, required_headroom_bytes=100)


def test_target_capacity_rejects_unavailable_catalog():
    connector = SimpleNamespace(get_records=lambda *args, **kwargs: [])
    with pytest.raises(ValueError, match="capacity_unavailable"):
        require_native_target_capacity(connector, native_limits(config()), required_headroom_bytes=100)
