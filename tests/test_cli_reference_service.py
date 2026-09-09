from __future__ import annotations

from pathlib import Path

from dpone.cli.parser import build_parser
from dpone.services.docs.cli_reference import (
    CLI_REF_END,
    CLI_REF_START,
    is_cli_reference_doc_in_sync,
    render_cli_reference_block,
    sync_cli_reference_doc,
)


def test_render_cli_reference_contains_new_docs_commands() -> None:
    parser = build_parser()
    rendered = render_cli_reference_block(parser)
    assert CLI_REF_START in rendered
    assert CLI_REF_END in rendered
    assert "dpone docs update-cli-reference" in rendered
    assert "dpone docs update-deprecation-roadmap" in rendered
    assert "dpone dag explain-edge" in rendered
    assert "dpone gitops airflow render" in rendered


def test_render_cli_reference_documents_runtime_pod_retention_io_contract() -> None:
    rendered = render_cli_reference_block(build_parser())
    cache_apply_section = rendered.split("### `dpone airflow cache-retention-apply`", 1)[1].split(
        "### `dpone airflow cache-recovery-plan`",
        1,
    )[0]
    apply_section = rendered.split("### `dpone airflow runtime-pod-retention-apply`", 1)[1].split(
        "### `dpone airflow runtime-pod-retention-render`",
        1,
    )[0]
    render_section = rendered.split("### `dpone airflow runtime-pod-retention-render`", 1)[1].split(
        "### `dpone airflow source-readiness`",
        1,
    )[0]

    assert "Output and exit contract" in apply_section
    assert "accepted, not observed absent" in apply_section
    assert "preserve stdout evidence" in apply_section
    assert "pre-execution JSON failures use dpone.error.v1" in apply_section
    assert "Exit 0 is clean or has bounded cleanup candidates" in rendered
    assert "Output and exit contract" in cache_apply_section
    assert "committed destructive receipt or schema-valid no-op" in cache_apply_section
    assert "Destructive v3 success requires the reviewed plan" in cache_apply_section
    assert "Output and exit contract" in render_section
    assert "temporary file" in render_section
    assert "default: 17 * * * *" in render_section
    for default in ("plan", "86400", "500", "100", "off", "yaml"):
        assert f"default: {default}" in render_section


def test_render_cli_reference_keeps_init_subcommand_options_separate() -> None:
    rendered = render_cli_reference_block(build_parser())
    legacy = rendered.split("### `dpone init`", 1)[1].split("### `dpone init project`", 1)[0]
    project = rendered.split("### `dpone init project`", 1)[1].split("### `dpone init domain`", 1)[0]
    domain = rendered.split("### `dpone init domain`", 1)[1].split("### `dpone init pipeline`", 1)[0]
    pipeline = rendered.split("### `dpone init pipeline`", 1)[1].split("## `dpone plan`", 1)[0]

    assert "--source-type SOURCE_TYPE" in legacy
    assert "--sink-type SINK_TYPE" in legacy
    assert "--out OUT" in legacy
    assert "--recipe" not in legacy
    assert "Usage: `dpone init project" in project
    assert "--layout {flat,domain-first}" in project
    assert "--recipe" not in project
    assert "--answers" not in project
    assert "Usage: `dpone init domain" in domain
    assert "--owner-team OWNER_TEAM" in domain
    assert "--recipe" not in domain
    assert "Usage: `dpone init pipeline" in pipeline
    assert "--recipe RECIPE" in pipeline
    assert "--answers ANSWERS" in pipeline
    assert "--source-type" not in pipeline
    assert "--out" not in pipeline
    assert "`0`: bounded result on stdout; stderr is empty." in project
    assert "JSON failure entries conform to `dpone.error.v1`." in domain
    assert "`4`: structured security or safety violation on stdout" in pipeline


def test_render_cli_reference_documents_workload_discovery_channels() -> None:
    rendered = render_cli_reference_block(build_parser())
    index = rendered.split("### `dpone workload index`", 1)[1].split(
        "### `dpone workload impact`",
        1,
    )[0]
    impact = rendered.split("### `dpone workload impact`", 1)[1].split("## `dpone plugins`", 1)[0]

    assert "`dpone.workload-index.v1` JSON on stdout" in index
    assert "`dpone.error.v1` entries on stdout" in index
    assert "`dpone.workload-change-impact.v1` JSON on stdout" in impact
    assert "argparse usage error on stderr" in impact


def test_render_cli_reference_documents_beginner_check_and_preview_channels() -> None:
    rendered = render_cli_reference_block(build_parser())
    check = rendered.split("### `dpone check`", 1)[1].split("## `dpone fix`", 1)[0]
    preview = rendered.split("### `dpone airflow preview`", 1)[1].split(
        "### `dpone airflow explain`",
        1,
    )[0]

    assert "`0`: bounded check result on stdout; stderr is empty." in check
    assert "`5`: redacted internal error with trace id on stdout" in check
    assert "`0`: bounded non-runnable preview result on stdout" in preview
    assert "`4`: structured security or cache-integrity violation on stdout" in preview


def test_cli_reference_sync_roundtrip(tmp_path: Path) -> None:
    parser = build_parser()
    doc = tmp_path / "cli-reference.md"
    doc.write_text(
        f"# CLI reference\n\n{CLI_REF_START}\nold\n{CLI_REF_END}\n",
        encoding="utf-8",
    )
    changed, _ = sync_cli_reference_doc(doc, parser=parser)
    assert changed is True
    assert is_cli_reference_doc_in_sync(doc, parser=parser) is True
