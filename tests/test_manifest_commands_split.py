from __future__ import annotations

import logging
from argparse import Namespace
from pathlib import Path

from dpone.adapters.fs_local import LocalFileSystem
from dpone.adapters.yaml_pyyaml import PyYamlCodec
from dpone.app.context import AppContext
from dpone.app.settings import Settings
from dpone.commands.manifest.explain_cmd import cmd_manifest_explain
from dpone.commands.manifest.list_cmd import cmd_manifest_list
from dpone.commands.manifest.render_cmd import cmd_manifest_render


def _make_ctx(tmp: Path) -> AppContext:
    settings = Settings(repo_root=tmp, project_dir=tmp, manifest_dir=tmp, sources_registry_paths=())
    return AppContext(settings=settings, logger=logging.getLogger("test"), fs=LocalFileSystem(), yaml=PyYamlCodec())


def _write_batch_manifest(path: Path) -> None:
    path.write_text(
        """
kind: dpone.batch.v1

vars:
  layer: landing
  src_system: monolite
  src_database: example_travel

naming:
  sink_dataset: "{{ layer }}__{{ src_system }}__{{ src_database }}"
  sink_table: "{{ src_schema }}__{{ src_table }}"
  process_name: "{{ src_schema }}__{{ src_table }}"
  task_group: "{{ src_system }}__{{ src_database }}"

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
    strategy:
      mode: full_refresh

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


def test_manifest_command_sources_do_not_use_legacy_adapter() -> None:
    root = Path("src/dpone/commands/manifest")
    for path in sorted(root.glob("*_cmd.py")):
        text = path.read_text(encoding="utf-8")
        assert "run_legacy" not in text
        assert "dpone.cli import legacy" not in text


def test_manifest_list_render_and_explain_commands_work_without_legacy(tmp_path: Path, capsys) -> None:
    manifest = tmp_path / "root.batch.yaml"
    _write_batch_manifest(manifest)
    ctx = _make_ctx(tmp_path)

    list_args = Namespace(path=str(manifest), recursive=False, registry=[])
    assert cmd_manifest_list(list_args, ctx=ctx, logger=ctx.logger) == 0
    out = capsys.readouterr().out
    assert "public.t1" in out
    assert "landing__monolite__example_travel.public__t1" in out

    render_args = Namespace(path=str(manifest), selector="public.t2", all=False, registry=[])
    assert cmd_manifest_render(render_args, ctx=ctx, logger=ctx.logger) == 0
    rendered = capsys.readouterr().out
    assert "depends_on:" in rendered
    assert "#public.t1" in rendered

    explain_args = Namespace(
        path=str(manifest),
        selector="public.t2",
        why=["sink.table.schema"],
        item_provenance=False,
        patches=False,
        patches_mode="user",
        patch_from=None,
        patch_to=None,
        post_parse=False,
        why_parsed=[],
        format="json",
        only=[],
        max_lines=50,
        max_items=20,
        registry=[],
    )
    assert cmd_manifest_explain(explain_args, ctx=ctx, logger=ctx.logger) == 0
    explain_json = capsys.readouterr().out
    assert '"explain"' in explain_json
    assert '"why"' in explain_json
    assert "landing__monolite__example_travel" in explain_json
