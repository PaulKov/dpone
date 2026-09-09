"""Fail-closed guards that prevent empty streaming dicts from reaching sinks."""

from __future__ import annotations

import pytest

from dpone.runtime.sources.streaming_row_guards import assert_dict_rows_preserve_columns


def test_guard_accepts_rows_with_expected_business_columns() -> None:
    assert_dict_rows_preserve_columns(
        [{"sessionId": "s1", "ptnDate": "2026-06-10", "GUID": "g1"}],
        expected_columns=("sessionId", "ptnDate", "GUID"),
    )


def test_guard_fail_closed_on_empty_row_mappings() -> None:
    with pytest.raises(RuntimeError, match="empty row mappings"):
        assert_dict_rows_preserve_columns(
            [{}],
            expected_columns=("sessionId", "ptnDate"),
        )


def test_guard_fail_closed_when_expected_columns_missing() -> None:
    with pytest.raises(RuntimeError, match="missing expected columns"):
        assert_dict_rows_preserve_columns(
            [{"sessionId": "s1"}],
            expected_columns=("sessionId", "ptnDate"),
        )


def test_guard_skips_empty_batches_and_empty_expectations() -> None:
    assert_dict_rows_preserve_columns([], expected_columns=("sessionId",))
    assert_dict_rows_preserve_columns([{}], expected_columns=())
