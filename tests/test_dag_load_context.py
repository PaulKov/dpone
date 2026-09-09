from __future__ import annotations

import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.output import OutputFormat
from dpone.services.dag.load_context import load_dag_context, resolve_entry_yaml_path


def _make_ctx(tmp: Path) -> AppContext:
    settings = Settings(
        repo_root=tmp,
        project_dir=tmp,
        manifest_dir=tmp,
        sources_registry_paths=(),
    )
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def test_resolve_entry_yaml_path_appends_extension_and_joins_base(tmp_path: Path) -> None:
    p = resolve_entry_yaml_path("root.batch", base_path=tmp_path)
    assert p == tmp_path / "root.batch.yaml"


def test_load_dag_context_builds_nodes_and_edges_from_batch_manifest(tmp_path: Path) -> None:
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
""".lstrip(),
        encoding="utf-8",
    )

    ctx = _make_ctx(tmp_path)
    args = Namespace(root="root.batch", base_path=str(tmp_path), registry=[], format=OutputFormat.text)

    dag = load_dag_context(args, ctx=ctx)
    assert len(dag.nodes) == 2

    # names are based on naming.process_name template
    assert {n.name for n in dag.nodes} == {"public__t1", "public__t2"}

    # t2 depends_on t1 => edge t1 -> t2
    assert "public__t1" in dag.edge_ctx.adjacency
    assert "public__t2" in dag.edge_ctx.adjacency["public__t1"]
