from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.gitops.airflow_compact_pack import AirflowCompactPackBuilder
from dpone.gitops.workload_catalog_models import GitOpsWorkloadDefinition
from dpone.runtime.sinks.clickhouse_payload_ingestion import ClickHousePayloadIngestionService
from dpone.runtime.sinks.load_payload import LoadPayload
from dpone.runtime.sources.clickhouse import ClickHouseSource
from dpone.runtime.sources.strategies.clickhouse.clickhouse_full_extract import ClickHouseFullExtractStrategy
from dpone.runtime.sql_query_artifact import SqlQueryArtifact
from dpone.runtime.sql_query_resolver import SqlQueryResolver


def test_sql_query_resolver_loads_file_relative_to_manifest_and_hashes_rendered_sql(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "dpone_workloads" / "manifests" / "clickhouse"
    sql_dir = tmp_path / "dpone_workloads" / "sql" / "clickhouse"
    manifest_dir.mkdir(parents=True)
    sql_dir.mkdir(parents=True)
    sql_file = sql_dir / "wide_mart.sql"
    sql_file.write_text("SELECT id FROM {{ sources.orders }} WHERE active = 1\n", encoding="utf-8")

    resolved = SqlQueryResolver(repo_root=tmp_path).resolve(
        {
            "mode": "sql_file",
            "sql_file": "../../sql/clickhouse/wide_mart.sql",
            "render": {"context": {"sources": {"orders": "DWH_Raw.orders"}}},
        },
        manifest_dir=manifest_dir,
        dialect="clickhouse",
    )

    assert resolved.sql == "SELECT id FROM DWH_Raw.orders WHERE active = 1"
    assert resolved.source_path == sql_file
    assert resolved.sql_hash.startswith("sha256:")
    assert resolved.evidence["mode"] == "sql_file"


def test_sql_query_resolver_blocks_path_escape_and_mutating_sql(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "repo" / "manifests"
    manifest_dir.mkdir(parents=True)

    with pytest.raises(ValueError, match="sql_file_outside_repo"):
        SqlQueryResolver(repo_root=tmp_path / "repo").resolve(
            {"mode": "sql_file", "sql_file": "../../outside.sql"},
            manifest_dir=manifest_dir,
            dialect="clickhouse",
        )

    with pytest.raises(ValueError, match="sql_query_must_be_readonly_select"):
        SqlQueryResolver(repo_root=tmp_path).resolve(
            {"mode": "inline", "sql": "INSERT INTO target SELECT 1"},
            manifest_dir=manifest_dir,
            dialect="clickhouse",
        )


def test_sql_query_resolver_skips_leading_comments_without_regex_backtracking(tmp_path: Path) -> None:
    manifest_dir = tmp_path / "repo" / "manifests"
    manifest_dir.mkdir(parents=True)
    sql = "/*" + ("*" * 10_000) + "*/\n-- visible transform\nSELECT 1 AS id"

    resolved = SqlQueryResolver(repo_root=tmp_path).resolve(
        {"mode": "inline", "sql": sql},
        manifest_dir=manifest_dir,
        dialect="clickhouse",
    )

    assert resolved.sql.endswith("SELECT 1 AS id")


def test_clickhouse_full_extract_returns_sql_query_artifact_without_row_streaming(tmp_path: Path) -> None:
    sql_file = tmp_path / "wide_mart.sql"
    sql_file.write_text("SELECT 1 AS id, 'Ada' AS name", encoding="utf-8")
    connector = _QueryConnector()
    strategy = ClickHouseFullExtractStrategy(
        connector=connector, logger=SimpleNamespace(log_etl_progress=lambda *a: None)
    )

    result = strategy.extract(
        _load_config(
            options={
                "manifest_dir": str(tmp_path),
                "query": {"mode": "sql_file", "sql_file": "wide_mart.sql"},
            }
        ),
        last_state=None,
    )

    assert isinstance(result.artifact, SqlQueryArtifact)
    assert result.schema == [("id", "UInt8"), ("name", "String")]
    assert result.artifact.estimated_rows == 2
    assert result.artifact.rows_exported == 2
    assert result.artifact.row_count == 2
    assert any("DESCRIBE SELECT" in query for query in connector.queries)
    assert not any("SELECT id FROM" in query for query in connector.queries)


def test_clickhouse_source_uses_rendered_query_schema_instead_of_placeholder_table(tmp_path: Path) -> None:
    """SQL-file sources have no physical placeholder relation to inspect."""

    sql_file = tmp_path / "wide_mart.sql"
    sql_file.write_text("SELECT 1 AS id, 'Ada' AS name", encoding="utf-8")
    connector = _QueryConnector()
    source = ClickHouseSource(
        connector=connector,
        logger=SimpleNamespace(log_etl_progress=lambda *a: None),
    )

    result = source.extract(
        _load_config(
            options={
                "manifest_dir": str(tmp_path),
                "query": {"mode": "sql_file", "sql_file": "wide_mart.sql"},
            }
        ),
        last_state=None,
    )

    assert result.schema == [("id", "UInt8"), ("name", "String")]
    assert result.relation_schema == (("id", "UInt8"), ("name", "String"))
    assert all(column.name in {"id", "name"} for column in result.relation_metadata)
    assert sum("DESCRIBE SELECT" in query for query in connector.queries) == 2
    assert not any("FROM system.columns" in query for query in connector.queries)


def test_clickhouse_source_rejects_rendered_query_schema_drift(tmp_path: Path) -> None:
    sql_file = tmp_path / "wide_mart.sql"
    sql_file.write_text("SELECT 1 AS id", encoding="utf-8")
    source = ClickHouseSource(
        connector=_ChangingQueryConnector(),
        logger=SimpleNamespace(log_etl_progress=lambda *a: None),
    )

    with pytest.raises(RuntimeError, match="clickhouse_source_schema_projection.changed_during_extract"):
        source.extract(
            _load_config(
                options={
                    "manifest_dir": str(tmp_path),
                    "query": {"mode": "sql_file", "sql_file": "wide_mart.sql"},
                }
            ),
            last_state=None,
        )


def test_clickhouse_sql_query_artifact_stages_with_insert_select() -> None:
    connector = _StagingConnector()
    sink = _StagingSink(connector)
    service = ClickHousePayloadIngestionService(sink, sink_factory=lambda _connector: sink)
    artifact = SqlQueryArtifact(
        sql="SELECT id, name FROM DWH_Raw.orders",
        dialect="clickhouse",
        sql_hash="sha256:abc",
        estimated_rows=2,
    )

    inserted = service.insert_payload(
        _load_config(target_schema="Example_Datamarts", target_table="orders__dpone_staging_1234"),
        LoadPayload(artifact=artifact, schema=[("id", "UInt64"), ("name", "String")]),
    )

    assert inserted == 2
    joined = "\n".join(connector.queries)
    assert "INSERT INTO `Example_Datamarts`.`orders__dpone_staging_1234` (`id`, `name`)" in joined
    assert "SELECT `id`, `name` FROM (SELECT id, name FROM DWH_Raw.orders)" in joined


def test_compact_pack_includes_sql_file_dependency_and_hash(tmp_path: Path) -> None:
    manifest = tmp_path / "dpone_workloads" / "manifests" / "clickhouse" / "wide_mart.yaml"
    sql_file = tmp_path / "dpone_workloads" / "sql" / "clickhouse" / "wide_mart.sql"
    hook_sql_file = tmp_path / "dpone_workloads" / "sql" / "clickhouse" / "refresh.sql"
    manifest.parent.mkdir(parents=True)
    sql_file.parent.mkdir(parents=True)
    sql_file.write_text("SELECT 1 AS id\n", encoding="utf-8")
    hook_sql_file.write_text("CREATE OR REPLACE TABLE DWH_Tech.refresh AS SELECT 1\n", encoding="utf-8")
    manifest.write_text(
        """
name: wide_mart
source:
  type: clickhouse
  options:
    query:
      mode: sql_file
      sql_file: ../../sql/clickhouse/wide_mart.sql
    hooks:
      pre_hook:
        - id: refresh
          kind: source_refresh
          type: sql
          connector: source
          sql_file: ../../sql/clickhouse/refresh.sql
          mutates_source: true
sink:
  type: clickhouse
""",
        encoding="utf-8",
    )
    workload = GitOpsWorkloadDefinition(
        workload_id="wide_mart",
        manifest="dpone_workloads/manifests/clickhouse/wide_mart.yaml",
        domain="wide_mart",
        catalog_path="dpone_workloads/gitops/domains/wide_mart.yaml",
        effective_config={"image": "dpone:dev"},
        provenance={},
    )

    report = AirflowCompactPackBuilder().build(
        workload=workload,
        output_path=".dpone/gitops/airflow/wide_mart/airflow-pack.json",
        repo_root=tmp_path,
    )
    payload = report.to_jsonable()

    assert payload["workload_dependencies"] == [
        {
            "kind": "manifest",
            "path": "dpone_workloads/manifests/clickhouse/wide_mart.yaml",
            "sha256": _sha256(manifest),
        },
        {
            "kind": "sql_file",
            "path": "dpone_workloads/sql/clickhouse/refresh.sql",
            "sha256": _sha256(hook_sql_file),
        },
        {
            "kind": "sql_file",
            "path": "dpone_workloads/sql/clickhouse/wide_mart.sql",
            "sha256": _sha256(sql_file),
        },
    ]
    assert "dpone_workloads/sql/clickhouse/wide_mart.sql" in json.dumps(payload["pod_spec"])
    assert "dpone_workloads/sql/clickhouse/refresh.sql" in json.dumps(payload["pod_spec"])


def test_compact_pack_renders_visible_source_refresh_hook_step(tmp_path: Path) -> None:
    manifest = tmp_path / "dpone_workloads" / "manifests" / "clickhouse" / "wide_mart.yaml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text(
        """
name: wide_mart
source:
  type: clickhouse
  options:
    hooks:
      pre_hook:
        - id: refresh_crm
          kind: source_refresh
          type: sql
          connector: source
          sql: "SELECT 1"
          execution:
            airflow: separate_task
sink:
  type: clickhouse
""",
        encoding="utf-8",
    )
    workload = GitOpsWorkloadDefinition(
        workload_id="wide_mart",
        manifest="dpone_workloads/manifests/clickhouse/wide_mart.yaml",
        domain="wide_mart",
        catalog_path="dpone_workloads/gitops/domains/wide_mart.yaml",
        effective_config={"image": "dpone:dev"},
        provenance={},
    )

    payload = (
        AirflowCompactPackBuilder()
        .build(
            workload=workload,
            output_path=".dpone/gitops/airflow/wide_mart/airflow-pack.json",
            repo_root=tmp_path,
        )
        .to_jsonable()
    )

    assert [step["name"] for step in payload["steps"]] == ["pre_hook_refresh_crm", "dpone_runtime", "outcome_gate"]
    assert payload["steps"][1]["depends_on"] == ["pre_hook_refresh_crm"]
    assert "DPONE_SKIP_AIRFLOW_SEPARATE_HOOKS=1" in payload["runtime_command"]


def test_compact_pack_always_pulls_mutable_runtime_images(tmp_path: Path) -> None:
    manifest = tmp_path / "dpone_workloads" / "manifests" / "clickhouse" / "wide_mart.yaml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("name: wide_mart\nsource:\n  type: clickhouse\nsink:\n  type: clickhouse\n", encoding="utf-8")
    workload = GitOpsWorkloadDefinition(
        workload_id="wide_mart",
        manifest="dpone_workloads/manifests/clickhouse/wide_mart.yaml",
        domain="wide_mart",
        catalog_path="dpone_workloads/gitops/domains/wide_mart.yaml",
        effective_config={"image": "harbor.example/dpone:master"},
        provenance={},
    )

    payload = (
        AirflowCompactPackBuilder()
        .build(
            workload=workload,
            output_path=".dpone/gitops/airflow/wide_mart/airflow-pack.json",
            repo_root=tmp_path,
        )
        .to_jsonable()
    )

    assert payload["pod_spec"]["spec"]["containers"][0]["imagePullPolicy"] == "Always"


def test_compact_pack_uses_cached_pull_policy_for_immutable_runtime_images(tmp_path: Path) -> None:
    manifest = tmp_path / "dpone_workloads" / "manifests" / "clickhouse" / "wide_mart.yaml"
    manifest.parent.mkdir(parents=True)
    manifest.write_text("name: wide_mart\nsource:\n  type: clickhouse\nsink:\n  type: clickhouse\n", encoding="utf-8")
    workload = GitOpsWorkloadDefinition(
        workload_id="wide_mart",
        manifest="dpone_workloads/manifests/clickhouse/wide_mart.yaml",
        domain="wide_mart",
        catalog_path="dpone_workloads/gitops/domains/wide_mart.yaml",
        effective_config={"image": "harbor.example/dpone@sha256:" + "1" * 64},
        provenance={},
    )

    payload = (
        AirflowCompactPackBuilder()
        .build(
            workload=workload,
            output_path=".dpone/gitops/airflow/wide_mart/airflow-pack.json",
            repo_root=tmp_path,
        )
        .to_jsonable()
    )

    assert payload["pod_spec"]["spec"]["containers"][0]["imagePullPolicy"] == "IfNotPresent"


def _load_config(
    *,
    target_schema: str = "analytics",
    target_table: str = "wide_mart",
    options: dict[str, object] | None = None,
) -> LoadConfig:
    return LoadConfig(
        source_conn_id="ClickHouse",
        target_conn_id="ClickHouse",
        source_schema="DWH_Raw",
        source_table="wide_mart_query",
        target_schema=target_schema,
        target_table=target_table,
        load_strategy=LoadStrategy.FULL_REFRESH,
        options=options or {},
    )


def _sha256(path: Path) -> str:
    import hashlib

    return hashlib.sha256(path.read_bytes()).hexdigest()


class _QueryConnector:
    database = "DWH_Raw"

    def __init__(self) -> None:
        self.queries: list[str] = []

    def get_records(self, query: str, _params: object = None, *, as_dict: bool = False) -> list[object]:
        self.queries.append(query)
        if "DESCRIBE SELECT" in query:
            rows = [{"name": "id", "type": "UInt8"}, {"name": "name", "type": "String"}]
            return rows if as_dict else [(row["name"], row["type"]) for row in rows]
        if "count()" in query:
            return [(2,)]
        raise AssertionError(query)


class _ChangingQueryConnector(_QueryConnector):
    def __init__(self) -> None:
        super().__init__()
        self.describe_calls = 0

    def get_records(self, query: str, _params: object = None, *, as_dict: bool = False) -> list[object]:
        if "DESCRIBE SELECT" not in query:
            return super().get_records(query, _params, as_dict=as_dict)
        self.queries.append(query)
        self.describe_calls += 1
        column_type = "UInt8" if self.describe_calls == 1 else "UInt64"
        rows = [{"name": "id", "type": column_type}]
        return rows if as_dict else [("id", column_type)]


class _StagingConnector:
    def __init__(self) -> None:
        self.queries: list[str] = []

    def execute_query(self, query: str) -> int:
        self.queries.append(query)
        return 0

    def get_records(self, query: str) -> list[tuple[int]]:
        self.queries.append(query)
        return [(2,)]


class _StagingSink:
    def __init__(self, connector: _StagingConnector) -> None:
        self.connector = connector

    @staticmethod
    def _table(load_config: LoadConfig) -> str:
        return f"`{load_config.target_schema}`.`{load_config.target_table}`"
