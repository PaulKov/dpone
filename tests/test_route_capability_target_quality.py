from __future__ import annotations

from types import SimpleNamespace

from dpone.ops.route_capability_target_quality import collect_target_quality


def test_collect_target_quality_reads_lineage_columns_from_sink_facade() -> None:
    sink = _Sink(
        [
            ("id", "UInt64"),
            ("__dpone__run_id", "String"),
            ("__dpone__load_id", "String"),
            ("__dpone__loaded_at", "DateTime64(6, 'UTC')"),
            ("__dpone__extracted_at", "DateTime64(6, 'UTC')"),
        ]
    )

    quality = collect_target_quality(sink=sink, load_config=SimpleNamespace(target_table="orders"))

    assert quality == {
        "lineage_columns": [
            "__dpone__run_id",
            "__dpone__load_id",
            "__dpone__loaded_at",
            "__dpone__extracted_at",
        ]
    }
    assert sink.seen_config.target_table == "orders"


def test_collect_target_quality_skips_sink_without_schema_facade() -> None:
    assert collect_target_quality(sink=object(), load_config=object()) == {}


class _Sink:
    def __init__(self, schema: list[tuple[str, str]]) -> None:
        self.schema = schema
        self.seen_config = None

    def get_target_schema(self, load_config):
        self.seen_config = load_config
        return self.schema
