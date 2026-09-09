from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace

from dpone.cli_render.dag import report as dag_report
from dpone.cli_render.dag.report import emit_report, render_report
from dpone.cli_render.manifest.list import render_manifest_list_text
from dpone.cli_render.manifest.migrate import render_manifest_migrate_text
from dpone.cli_render.manifest.registry_lint import render_manifest_registry_lint_text
from dpone.cli_render.manifest.render import render_manifest_render_text
from dpone.cli_render.manifest.stats import render_manifest_stats_text
from dpone.cli_render.manifest.validate import render_manifest_validate_text
from dpone.cli_render.manifest.verify import render_manifest_verify_text


def test_manifest_list_renderer_handles_empty_and_table_rows() -> None:
    assert render_manifest_list_text(SimpleNamespace(rows=())) == "No YAML files found.\n"

    row = SimpleNamespace(
        manifest="demo.yaml",
        kind="dpone.batch.v1",
        selector="public.users",
        name="load_users",
        task_group="landing",
        source="postgres",
        sink="bigquery",
    )

    rendered = render_manifest_list_text(SimpleNamespace(rows=(row,)))

    assert "demo.yaml" in rendered
    assert "public.users" in rendered
    assert rendered.endswith("\n")


def test_manifest_render_renderer_emits_yaml_documents() -> None:
    docs = (
        SimpleNamespace(config={"name": "first", "source": {"type": "postgres"}}),
        SimpleNamespace(config={"name": "second", "sink": {"type": "bigquery"}}),
    )

    rendered = render_manifest_render_text(SimpleNamespace(docs=docs))

    assert "name: first" in rendered
    assert "---" in rendered
    assert "name: second" in rendered
    assert rendered.endswith("\n")


def test_manifest_validate_renderer_handles_ok_and_issues() -> None:
    assert render_manifest_validate_text(SimpleNamespace(issues=())) == "OK: no issues\n"

    issue = SimpleNamespace(
        severity=SimpleNamespace(value="ERROR"),
        code="required_field",
        manifest_path=Path("manifest.yaml"),
        selector="public.users",
        message="missing owner",
    )

    rendered = render_manifest_validate_text(SimpleNamespace(issues=(issue,)))

    assert "ERROR required_field manifest.yaml#public.users: missing owner" in rendered


def test_manifest_registry_lint_renderer_handles_ok_and_source_key() -> None:
    assert render_manifest_registry_lint_text(SimpleNamespace(issues=())) == "OK: registry covers all used sources\n"

    issue = SimpleNamespace(
        severity=SimpleNamespace(value="WARN"),
        code="missing_registry_entry",
        manifest_path=Path("manifest.yaml"),
        src_system="demo",
        src_database="db",
        message="not found",
    )

    rendered = render_manifest_registry_lint_text(SimpleNamespace(issues=(issue,)))

    assert "WARN missing_registry_entry manifest.yaml (demo::db): not found" in rendered


def test_manifest_migrate_stats_and_verify_renderers_summarize_views() -> None:
    batch = SimpleNamespace(out_path=Path("out.batch.yaml"), processes=(object(), object()))
    migrate = render_manifest_migrate_text(
        SimpleNamespace(total_legacy=2, plan=SimpleNamespace(batches=(batch,))),
        dry_run=True,
    )
    assert "Legacy processes: 2" in migrate
    assert "- out.batch.yaml: 2 processes" in migrate
    assert "dry-run" in migrate

    stats = render_manifest_stats_text(
        SimpleNamespace(
            total_manifests=2,
            total_processes=3,
            kinds={"single": 1, "batch": 2},
            by_dataset={"landing": 3},
            by_group={"raw": 2},
        )
    )
    assert "Manifests: 2" in stats
    assert "Top sink datasets" in stats
    assert "landing" in stats

    diff = SimpleNamespace(path="sink.table", legacy="old", batch="new")
    issue = SimpleNamespace(
        code="mismatch",
        legacy_path=Path("legacy.yaml"),
        selector="public.users",
        batch_ref="batch.yaml#public.users",
        message="different sink",
        diffs=(diff,),
    )
    report = SimpleNamespace(total_legacy=1, ok=0, failed=1, issues=(issue,))
    verify = render_manifest_verify_text(SimpleNamespace(report=report))
    assert "FAILED: 1" in verify
    assert "legacy.yaml#public.users -> batch.yaml#public.users" in verify
    assert "sink.table" in verify


def test_dag_report_render_and_emit_paths(monkeypatch, tmp_path: Path) -> None:
    writes: list[tuple[str, str]] = []
    files: list[tuple[str, str]] = []

    view = SimpleNamespace(
        meta=SimpleNamespace(options={"md_max_edges": 3}),
        report=SimpleNamespace(to_markdown=lambda max_edges: f"markdown {max_edges}"),
        to_jsonable=lambda: {"summary": {"task_count": 1}},
    )

    rendered = render_report(view)
    assert rendered.markdown == "markdown 3"
    assert rendered.json_obj == {"summary": {"task_count": 1}}

    monkeypatch.setattr(dag_report, "write_text", lambda text: writes.append(("text", text)))
    monkeypatch.setattr(dag_report, "write_json", lambda obj: writes.append(("json", repr(obj))))
    monkeypatch.setattr(
        dag_report,
        "write_text_file",
        lambda path, text: files.append((str(path), text)),
    )

    emit_report(view, fmt="unknown")
    emit_report(view, fmt="json")
    emit_report(view, fmt="both")
    emit_report(view, fmt="both", out_md=str(tmp_path / "report.md"), out_json=str(tmp_path / "report.json"))

    assert ("text", "markdown 3") in writes
    assert any(kind == "json" and "task_count" in payload for kind, payload in writes)
    assert any("```json" in payload for kind, payload in writes if kind == "text")
    assert (str(tmp_path / "report.md"), "markdown 3") in files
    assert any(path == str(tmp_path / "report.json") and "task_count" in text for path, text in files)
