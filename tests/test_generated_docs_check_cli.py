from __future__ import annotations

from pathlib import Path

from dpone.cli.parser import build_parser
from dpone.services.docs.check_generated_references_service import (
    CheckGeneratedReferencesService,
)
from dpone.services.docs.cli_reference import (
    CLI_REF_END,
    CLI_REF_START,
    render_cli_reference_block,
    sync_cli_reference_doc,
)
from dpone.services.docs.context import DocsServiceContext
from dpone.services.docs.gitops_schema_reference import (
    GITOPS_SCHEMA_REF_END,
    GITOPS_SCHEMA_REF_START,
    sync_gitops_schema_reference_doc,
)
from dpone.services.docs.manifest_schema_reference import (
    MANIFEST_SCHEMA_REF_END,
    MANIFEST_SCHEMA_REF_START,
    sync_manifest_schema_reference_doc,
)


class _LoggerStub:
    def error(self, *args: object) -> None:
        del args

    def info(self, *args: object) -> None:
        del args


class _SettingsStub:
    def __init__(self, repo_root: Path):
        self.repo_root = repo_root


class _DocsContextStub(DocsServiceContext):
    def __init__(self, repo_root: Path):
        self._settings = _SettingsStub(repo_root)
        self._logger = _LoggerStub()

    @property
    def settings(self) -> _SettingsStub:
        return self._settings

    @property
    def logger(self) -> _LoggerStub:
        return self._logger


def test_generated_references_check_reports_synced_docs(tmp_path: Path) -> None:
    cli_doc, gitops_doc, manifest_doc = _write_generated_docs(tmp_path)
    service = CheckGeneratedReferencesService(ctx=_DocsContextStub(tmp_path))

    exit_code, payload = service.run(
        {
            "format": "json",
            "cli_doc": cli_doc.relative_to(tmp_path).as_posix(),
            "gitops_schema_doc": gitops_doc.relative_to(tmp_path).as_posix(),
            "manifest_schema_doc": manifest_doc.relative_to(tmp_path).as_posix(),
            "manifest_schema_root": "src/dpone/schema",
        }
    )

    assert exit_code == 0
    assert payload["kind"] == "docs.generated_references"
    assert payload["passed"] is True
    assert [item["name"] for item in payload["checks"]] == [
        "cli-reference",
        "gitops-schema-catalog",
        "manifest-schema-reference",
    ]


def test_generated_references_check_reports_stale_doc(tmp_path: Path) -> None:
    cli_doc, gitops_doc, manifest_doc = _write_generated_docs(tmp_path)
    cli_doc.write_text(f"# CLI reference\n\n{CLI_REF_START}\nstale\n{CLI_REF_END}\n", encoding="utf-8")
    service = CheckGeneratedReferencesService(ctx=_DocsContextStub(tmp_path))

    exit_code, payload = service.run(
        {
            "format": "json",
            "cli_doc": cli_doc.relative_to(tmp_path).as_posix(),
            "gitops_schema_doc": gitops_doc.relative_to(tmp_path).as_posix(),
            "manifest_schema_doc": manifest_doc.relative_to(tmp_path).as_posix(),
            "manifest_schema_root": "src/dpone/schema",
        }
    )

    assert exit_code == 2
    assert payload["passed"] is False
    assert payload["checks"][0] == {
        "name": "cli-reference",
        "path": cli_doc.as_posix(),
        "passed": False,
        "fix_command": "dpone docs update-cli-reference",
    }
    assert payload["checks"][1]["passed"] is True
    assert payload["checks"][2]["passed"] is True


def test_cli_reference_contains_generated_references_check_command() -> None:
    rendered = render_cli_reference_block(build_parser())

    assert "dpone docs check-generated-references" in rendered


def _write_generated_docs(tmp_path: Path) -> tuple[Path, Path, Path]:
    cli_doc = tmp_path / "docs" / "cli-reference.md"
    gitops_doc = tmp_path / "docs" / "reference" / "gitops-schema-catalog.md"
    manifest_doc = tmp_path / "docs" / "reference" / "manifest-schemas.md"
    schema_root = tmp_path / "src" / "dpone" / "schema"
    cli_doc.parent.mkdir(parents=True, exist_ok=True)
    gitops_doc.parent.mkdir(parents=True, exist_ok=True)
    cli_doc.write_text(f"# CLI reference\n\n{CLI_REF_START}\nold\n{CLI_REF_END}\n", encoding="utf-8")
    gitops_doc.write_text(
        f"# GitOps schema catalog\n\n{GITOPS_SCHEMA_REF_START}\nold\n{GITOPS_SCHEMA_REF_END}\n",
        encoding="utf-8",
    )
    manifest_doc.write_text(
        f"# Manifest schemas\n\n{MANIFEST_SCHEMA_REF_START}\nold\n{MANIFEST_SCHEMA_REF_END}\n",
        encoding="utf-8",
    )
    schema_root.mkdir(parents=True, exist_ok=True)
    (schema_root / "example.schema.json").write_text(
        '{"$id":"https://example.test/example.schema.json","title":"Example","description":"Example schema.","type":"object","required":["kind"],"properties":{"kind":{"const":"example.v1"}}}\n',
        encoding="utf-8",
    )
    sync_cli_reference_doc(cli_doc, parser=build_parser())
    sync_gitops_schema_reference_doc(gitops_doc)
    sync_manifest_schema_reference_doc(manifest_doc, schema_root=schema_root)
    return cli_doc, gitops_doc, manifest_doc
