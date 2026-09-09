from __future__ import annotations

import csv
import sys
import types
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from dpone.runtime.artifacts import FileExportArtifact
from dpone.runtime.connectors.bigquery import postgres_headerless_csv_load_config
from dpone.runtime.sinks.bigquery_staging_file_loader import BigQueryStagingFileLoader
from dpone.runtime.sinks.bigquery_staging_gcs_loader import BigQueryStagingGcsLoader
from dpone.runtime.sinks.bigquery_staging_manager import BigQueryStagingManager
from dpone.runtime.sinks.postgres_staging_manager import PostgresStagingManager
from dpone.runtime.sinks.staging import BigQueryStagingManager as FacadeBigQueryStagingManager
from dpone.runtime.sinks.staging import PostgresStagingManager as FacadePostgresStagingManager
from dpone.runtime.sinks.staging_managers.bigquery import BigQueryStagingManager as CanonicalBigQueryStagingManager
from dpone.runtime.sinks.staging_managers.postgres import PostgresStagingManager as CanonicalPostgresStagingManager
from dpone.runtime.support.data_type_mapper import DataTypeMapper


class StubLogger:
    def __init__(self):
        self.progress = []
        self.errors = []
        self.warnings = []

    def log_etl_progress(self, event, payload):
        self.progress.append((event, payload))

    def log_etl_error(self, message, payload):
        self.errors.append((message, payload))

    def warning(self, message):
        self.warnings.append(message)


class StubPgConnector:
    def __init__(self):
        self.copied = []

    def copy_from_iter(self, schema, table, columns, csv_rows):
        rows = list(csv_rows)
        self.copied.append((schema, table, tuple(columns), rows))
        return len(rows)


class StubCopy:
    def __init__(self):
        self.writes = []
        self.rows = []

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def write(self, value):
        self.writes.append(value)

    def write_row(self, row):
        self.rows.append(row)


class StubCursor:
    def __init__(self, copy_obj):
        self.copy_obj = copy_obj
        self.copy_sql = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False

    def copy(self, copy_sql):
        self.copy_sql = copy_sql
        return self.copy_obj


class StubConnection:
    def __init__(self, cursor):
        self._cursor = cursor

    def cursor(self):
        return self._cursor


class StubPgConnectorWithCopy:
    def __init__(self, row_count):
        self.copy = StubCopy()
        self.cursor = StubCursor(self.copy)
        self.connection = StubConnection(self.cursor)
        self.row_count = row_count

    def get_records(self, query, params=None, as_dict=False):
        del query, params, as_dict
        return [(self.row_count,)]


@dataclass
class StubBqConnector:
    project_id: str = "demo-proj"
    loads: list = field(default_factory=list)

    def load_from_gcs(self, **kwargs):
        self.loads.append(kwargs)
        return 7


def test_staging_facade_exports_new_manager_classes() -> None:
    assert FacadePostgresStagingManager is PostgresStagingManager
    assert FacadeBigQueryStagingManager is BigQueryStagingManager
    assert CanonicalPostgresStagingManager is PostgresStagingManager
    assert CanonicalBigQueryStagingManager is BigQueryStagingManager


def test_postgres_staging_manager_converts_lists_and_dicts_for_copy() -> None:
    connector = StubPgConnector()
    logger = StubLogger()
    manager = PostgresStagingManager(connector=connector, logger=logger)
    artifact = SimpleNamespace(
        schema="stg", table="events", columns=["id", "tags", "attrs"], qualified_name=lambda: "stg.events"
    )

    inserted = manager.insert_rows(
        artifact,
        [{"id": 1, "tags": ["a", "b"], "attrs": {"x": 1}}],
    )

    assert inserted == 1
    copied_rows = connector.copied[0][3]
    assert copied_rows[0].startswith('1,{a,b},"{""x"": 1}"') or '"{""x"": 1}"' in copied_rows[0]
    assert logger.progress[-1] == ("STAGING_INSERT_ROWS", {"Table": "stg.events", "Rows": 1})


def test_postgres_staging_manager_loads_csv_via_raw_copy_stream() -> None:
    connector = StubPgConnectorWithCopy(row_count=2)
    logger = StubLogger()
    manager = PostgresStagingManager(connector=connector, logger=logger)
    artifact = SimpleNamespace(schema="stg", table="orders", columns=["id", "city"])

    with TemporaryDirectory() as tmp_dir:
        csv_path = Path(tmp_dir) / "orders.csv"
        with csv_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.writer(handle)
            writer.writerow([1, "Paris"])
            writer.writerow([2, "Berlin"])

        file_artifact = FileExportArtifact(str(csv_path), columns=["id", "city"], compressed=False, format="csv")
        inserted = manager.load_from_file(artifact, file_artifact)

    assert inserted == 2
    assert connector.copy.rows == []
    payload = "".join(connector.copy.writes)
    assert "1,Paris" in payload
    assert "2,Berlin" in payload


def test_bigquery_file_loader_chooses_gcs_in_batch_mode_without_google_imports() -> None:
    connector = StubBqConnector()
    logger = StubLogger()
    loader = BigQueryStagingFileLoader(connector, logger)
    artifact = SimpleNamespace(
        schema="stg",
        table="orders__tmp",
        target_schema="landing",
        staging_manager=SimpleNamespace(_last_schema=None),
        qualified_name=lambda: "stg.orders__tmp",
    )
    file_artifact = SimpleNamespace(file_path="/tmp/missing.csv", format="csv")

    loader._load_csv_via_gcs = lambda *args: 11
    loader._load_csv_file = lambda *args: 3

    inserted = loader.load_from_file_batched(artifact, file_artifact)

    assert inserted == 11
    assert logger.progress[0][0] == "BQ_GCS_ROUTE_DECISION"
    assert logger.progress[0][1]["Decision"] == "Use GCS"


def test_bigquery_gcs_loader_tracks_partition_rows_for_multi_partition_load() -> None:
    connector = StubBqConnector()
    logger = StubLogger()
    loader = BigQueryStagingGcsLoader(connector, logger)
    artifact = SimpleNamespace(
        schema="stg",
        table="orders__tmp",
        staging_manager=SimpleNamespace(_last_schema=[("id", "INT64")]),
    )
    gcs_artifact = SimpleNamespace(
        hive_partitioning=True,
        source_uri_prefix="gs://bucket/prefix/",
        new_partitions=["2026-03-01", "2026-03-02"],
        load_patterns=None,
        format="csv",
        pattern=None,
        gcs_uri="gs://bucket/root",
    )

    inserted = loader.load_from_gcs_artifact(artifact, gcs_artifact)

    assert inserted == 14
    assert artifact._partition_rows == {"2026-03-01": 7, "2026-03-02": 7}
    assert connector.loads[0]["csv_config"].skip_leading_rows == 0
    assert logger.progress[-1][0] == "BQ_LOADED_FROM_GCS"


def test_postgres_headerless_csv_load_config_disables_header_skip() -> None:
    config = postgres_headerless_csv_load_config()
    assert config.skip_leading_rows == 0
    assert config.allow_quoted_newlines is True


def test_bigquery_staging_manager_preserves_lists_for_repeated_fields() -> None:
    manager = object.__new__(BigQueryStagingManager)
    manager._last_schema = [
        SimpleNamespace(name="serp_features", mode="REPEATED"),
        SimpleNamespace(name="tags_json", mode="NULLABLE"),
    ]

    rows = manager._prepare_rows_for_dataframe(
        [
            {
                "id": 1,
                "serp_features": ["images", None, "news"],
                "tags_json": ["a", "b"],
                "attrs": {"flag": True},
            }
        ]
    )

    assert rows == [
        {
            "id": 1,
            "serp_features": ["images", "news"],
            "tags_json": '["a", "b"]',
            "attrs": '{"flag": true}',
        }
    ]


def test_bigquery_staging_manager_normalizes_temporal_columns_for_dataframe_load(monkeypatch) -> None:
    class FakeILoc:
        def __init__(self, values):
            self._values = list(values)

        def __getitem__(self, index):
            return self._values[index]

    class FakeSeries:
        def __init__(self, values):
            self._values = list(values)
            self.iloc = FakeILoc(self._values)

        @property
        def empty(self):
            return len(self._values) == 0

        @property
        def values(self):
            return list(self._values)

    class FakeDateTimeAccessor:
        def __init__(self, values):
            self._values = list(values)

        @property
        def date(self):
            return [value.date() if value is not None else None for value in self._values]

        @property
        def time(self):
            return [value.time() if value is not None else None for value in self._values]

        @property
        def tz(self):
            for value in self._values:
                if isinstance(value, datetime) and value.tzinfo is not None:
                    return value.tzinfo
            return None

        def tz_convert(self, tz):
            assert tz is None
            converted = []
            for value in self._values:
                if isinstance(value, datetime) and value.tzinfo is not None:
                    converted.append(value.replace(tzinfo=None))
                else:
                    converted.append(value)
            return FakeParsedSeries(converted)

    class FakeParsedSeries(FakeSeries):
        @property
        def dt(self):
            return FakeDateTimeAccessor(self._values)

    class FakeDataFrame:
        def __init__(self, rows):
            row = rows[0] if rows else {}
            self._data = {key: FakeSeries([value]) for key, value in row.items()}
            self.columns = list(self._data)

        def __getitem__(self, key):
            return self._data[key]

        def __setitem__(self, key, value):
            if isinstance(value, FakeSeries):
                self._data[key] = value
            else:
                self._data[key] = FakeSeries(value)
            if key not in self.columns:
                self.columns.append(key)

    def _parse_datetime(value, *, fmt=None):
        if value in (None, ""):
            return None
        text = str(value)
        if fmt == "%H:%M:%S":
            return datetime.strptime(text, fmt)
        if text.endswith("Z"):
            return datetime.fromisoformat(text.replace("Z", "+00:00"))
        try:
            return datetime.fromisoformat(text)
        except ValueError:
            return datetime.strptime(text, "%Y-%m-%d %H:%M:%S")

    def fake_to_datetime(series, errors="coerce", format=None):
        del errors
        values = series.values if isinstance(series, FakeSeries) else list(series)
        return FakeParsedSeries([_parse_datetime(value, fmt=format) for value in values])

    fake_pandas = types.SimpleNamespace(
        DataFrame=FakeDataFrame,
        to_datetime=fake_to_datetime,
    )
    monkeypatch.setitem(sys.modules, "pandas", fake_pandas)
    pd = fake_pandas

    manager = object.__new__(BigQueryStagingManager)
    manager._last_schema = [
        SimpleNamespace(name="report_date", field_type="DATE"),
        SimpleNamespace(name="install_time", field_type="TIMESTAMP"),
        SimpleNamespace(name="event_time", field_type="DATETIME"),
        SimpleNamespace(name="clock_time", field_type="TIME"),
    ]
    df = pd.DataFrame(
        [
            {
                "report_date": "2026-03-28",
                "install_time": "2026-03-28T10:11:12Z",
                "event_time": "2026-03-28 13:14:15",
                "clock_time": "16:17:18",
            }
        ]
    )

    manager._apply_temporal_pandas_dtypes(df)

    assert df["report_date"].iloc[0].isoformat() == "2026-03-28"
    assert str(df["install_time"].iloc[0]) == "2026-03-28 10:11:12"
    assert str(df["event_time"].iloc[0]) == "2026-03-28 13:14:15"
    assert df["clock_time"].iloc[0].isoformat() == "16:17:18"


def test_data_type_mapper_builds_repeated_bigquery_schema_field_without_google_dependency(monkeypatch) -> None:
    class FakeSchemaField:
        def __init__(self, name, field_type, mode="NULLABLE", fields=()):
            self.name = name
            self.field_type = field_type
            self.mode = mode
            self.fields = tuple(fields)

    google_module = types.ModuleType("google")
    cloud_module = types.ModuleType("google.cloud")
    bigquery_module = types.ModuleType("google.cloud.bigquery")
    bigquery_module.SchemaField = FakeSchemaField
    cloud_module.bigquery = bigquery_module
    google_module.cloud = cloud_module

    monkeypatch.setitem(sys.modules, "google", google_module)
    monkeypatch.setitem(sys.modules, "google.cloud", cloud_module)
    monkeypatch.setitem(sys.modules, "google.cloud.bigquery", bigquery_module)

    field = DataTypeMapper.to_bigquery_schema_field("serp_features", "REPEATED STRING")

    assert field.name == "serp_features"
    assert field.field_type == "STRING"
    assert field.mode == "REPEATED"
