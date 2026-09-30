"""Read-only window aggregates preserve SQL, value fidelity and cursor cleanup."""

from datetime import UTC, datetime

import pytest


class AggregateReader:
    def __init__(self, aggregate, days=(), *, fail_days=False):
        self.aggregate = aggregate
        self.days = days
        self.fail_days = fail_days
        self.queries = []
        self.closed = False

    def get_records(self, query, *, as_dict=False):
        self.queries.append(query)
        return [self.aggregate]

    def get_records_iterator(self, query):
        self.queries.append(query)

        def rows():
            try:
                yield from self.days
                if self.fail_days:
                    raise ConnectionError("day read failed")
            finally:
                self.closed = True

        return rows()


def read(reader, *, window_only):
    from dpone.runtime.sinks.clickhouse_window_queries import read_window_metrics

    return read_window_metrics(
        reader,
        table_sql="`d`.`t`",
        column_sql="`at`",
        predicate_sql="(`at` >= lower AND `at` < upper)",
        columns=(("at", "`at`"), ("value", "`value`")),
        window_only=window_only,
    )


@pytest.mark.parametrize("window_only", [False, True])
def test_aggregate_sql_order_and_typed_values(window_only):
    reader = AggregateReader(
        (3, datetime(1969, 12, 31), datetime(1970, 1, 1, tzinfo=UTC), 1, 1, 2),
        [("1969-12-31", 1), ("1970-01-01", 1)],
    )
    assert read(reader, window_only=window_only) == {
        "row_count": 3,
        "min_window_utc": "1969-12-31T00:00:00+00:00",
        "max_window_utc": "1970-01-01T00:00:00+00:00",
        "outside_window": 1,
        "null_counts": {"at": 1, "value": 2},
        "utc_day_counts": {"1969-12-31": 1, "1970-01-01": 1},
    }
    where = " WHERE (`at` >= lower AND `at` < upper)" if window_only else ""
    day_filter = where + " AND isNotNull(`at`)" if window_only else " WHERE isNotNull(`at`)"
    assert reader.queries == [
        "SELECT count(), minOrNull(`at`), maxOrNull(`at`), "
        "countIf(isNull(`at`) OR NOT (`at` >= lower AND `at` < upper)), "
        f"countIf(isNull(`at`)), countIf(isNull(`value`)) FROM `d`.`t`{where}",
        f"SELECT formatDateTime(`at`, '%Y-%m-%d', 'UTC'), count() FROM `d`.`t`{day_filter} "
        "GROUP BY formatDateTime(`at`, '%Y-%m-%d', 'UTC') "
        "ORDER BY formatDateTime(`at`, '%Y-%m-%d', 'UTC')",
    ]
    assert reader.closed


def test_empty_aggregate_preserves_null_extrema():
    reader = AggregateReader((0, None, None, 0, 0, 0))
    assert read(reader, window_only=True) == {
        "row_count": 0,
        "min_window_utc": None,
        "max_window_utc": None,
        "outside_window": 0,
        "null_counts": {"at": 0, "value": 0},
        "utc_day_counts": {},
    }
    assert reader.closed


def test_day_stream_failure_closes_iterator_and_propagates():
    reader = AggregateReader((1, None, None, 0, 0, 0), [("2026-01-01", 1)], fail_days=True)
    with pytest.raises(ConnectionError, match="day read failed"):
        read(reader, window_only=True)
    assert reader.closed


def test_day_conversion_failure_closes_iterator():
    reader = AggregateReader((1, None, None, 0, 0, 0), [("2026-01-01", "not-an-integer")])
    with pytest.raises(ValueError):
        read(reader, window_only=True)
    assert reader.closed
