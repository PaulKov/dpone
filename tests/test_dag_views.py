from __future__ import annotations

import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.dag.dag_report import DagReport
from dpone.dag.edge_explain import explain_direct_edge, locate_node
from dpone.services.dag.load_context import load_dag_context
from dpone.services.dag.views import build_dag_report_view, build_explain_edge_view


def _make_ctx(tmp: Path) -> AppContext:
    settings = Settings(
        repo_root=tmp,
        project_dir=tmp,
        manifest_dir=tmp,
        sources_registry_paths=(),
    )
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def test_edge_view_json_uses_common_envelope(tmp_path: Path) -> None:
    root = tmp_path / "root.batch.yaml"
    root.write_text(
        """
kind: dpone.batch.v1

vars:
  layer: landing
  src_system: monolite
  src_database: db1

defaults:
  source:
    type: postgres
    connection_id: pg_conn
    table:
      schema: "{{ src_schema }}"
      name: "{{ src_table }}"
  sink:
    type: bigquery
    connection_id: bq_conn
    table:
      schema: "{{ layer }}__{{ src_system }}__{{ src_database }}"
      name: "{{ src_schema }}__{{ src_table }}"
    mode: append

naming:
  process_name: "{{ src_schema }}__{{ src_table }}"

schemas:
  public:
    tables:
      - table: t1
      - table: t2
        depends_on:
          - "#public.t1"
      - table: t3
        depends_on:
          - "#public.t2"
""".lstrip(),
        encoding="utf-8",
    )

    ctx = _make_ctx(tmp_path)
    dag = load_dag_context(Namespace(root="root.batch", base_path=str(tmp_path), registry=[]), ctx=ctx)

    up = locate_node(dag.nodes, token="public__t1", root_file=dag.root_path, base_path=dag.base_path)
    down = locate_node(dag.nodes, token="public__t3", root_file=dag.root_path, base_path=dag.base_path)
    exp = explain_direct_edge(
        dag.edge_ctx,
        upstream_name=up.name,
        downstream_name=down.name,
        max_triggers=10,
        include_transitive_path=True,
    )
    assert exp.direct_edge is False
    assert exp.path == ["public__t1", "public__t2", "public__t3"]

    path_edges = []
    for i in range(len(exp.path) - 1):
        a = exp.path[i]
        b = exp.path[i + 1]
        path_edges.append(
            (
                a,
                b,
                explain_direct_edge(dag.edge_ctx, upstream_name=a, downstream_name=b, max_triggers=10),
            )
        )

    view = build_explain_edge_view(
        dag=dag,
        upstream_name=up.name,
        downstream_name=down.name,
        result=exp,
        path_edges=path_edges,
        path_view="grouped",
        path_output="compact",
    )

    payload = view.to_jsonable()
    assert payload["kind"] == "dag.explain_edge"
    assert payload["options"]["path_view"] == "grouped"
    assert payload["result"]["direct_edge"] is False
    assert len(payload["path_edges"]) == 2
    assert payload["path_segments"]


def test_report_view_json_wraps_report_with_meta(tmp_path: Path) -> None:
    report = DagReport(
        root="/tmp/root.yaml",
        base_path="/tmp",
        generated_at="2026-01-01T00:00:00Z",
        task_count=3,
        edge_count=2,
        group_count=1,
    )

    class _DagStub:
        root_path = Path("/tmp/root.yaml")
        base_path = Path("/tmp")
        nodes = (1, 2, 3)

    view = build_dag_report_view(
        dag=_DagStub(),
        report=report,
        preset="ci",
        max_edges=500,
        md_max_edges=200,
        max_triggers=10,
        include_evidence=False,
        registry_require_fields=("host", "type"),
        lint_enabled=True,
        profile_name="landing_raw_v1",
        fail_on="both",
        fail_severity="ERROR",
    )

    payload = view.to_jsonable()
    assert payload["kind"] == "dag.report"
    assert payload["options"]["preset"] == "ci"
    assert payload["report"]["summary"]["task_count"] == 3
    assert payload["summary"]["task_count"] == 3
