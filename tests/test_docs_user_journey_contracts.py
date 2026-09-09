"""Executable contracts for the primary documentation journeys."""

from __future__ import annotations

import json
import shlex
from collections.abc import Mapping
from pathlib import Path

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.commands import run_cmd
from dpone.contracts.process_types import ProcessResult
from dpone.ports import runtime_hydrator
from dpone.ports.runtime_hydrator import RuntimeBindings
from dpone.services.run_manifest import RunManifestService

ROOT = Path(__file__).resolve().parents[1]
DOCS = ROOT / "docs"


def _fenced_blocks(path: Path, language: str) -> tuple[str, ...]:
    blocks: list[str] = []
    current: list[str] | None = None
    opening = f"```{language}"
    for line in path.read_text(encoding="utf-8").splitlines():
        if current is None and line == opening:
            current = []
        elif current is not None and line == "```":
            blocks.append("\n".join(current))
            current = None
        elif current is not None:
            current.append(line)
    return tuple(blocks)


def _primary_manifest(path: Path) -> str:
    for block in _fenced_blocks(path, "yaml"):
        payload = yaml.safe_load(block)
        if isinstance(payload, Mapping) and {"name", "source", "sink"} <= payload.keys():
            return block
    raise AssertionError(f"primary manifest not found in {path}")


@pytest.mark.parametrize(
    ("relative_path", "expected_name"),
    [
        ("getting-started/quickstart.md", "orders_to_landing"),
        ("getting-started/first-local-pipeline.md", "local_postgres_to_mssql"),
    ],
)
def test_beginner_manifest_plans_offline(
    relative_path: str,
    expected_name: str,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    page = DOCS / relative_path
    manifest = tmp_path / "manifest.yaml"
    manifest.write_text(_primary_manifest(page), encoding="utf-8")

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["plan", str(manifest), "--format", "json"])

    captured = capsys.readouterr()
    assert exc.value.code == 0, captured.out
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["process"] == expected_name
    assert {"type", "table"} <= payload["source"].keys()
    assert {"type", "table"} <= payload["sink"].keys()
    assert {"mode"} <= payload["strategy"].keys()
    assert "staging_first" in payload["staging"]
    assert "enabled" in payload["schema_evolution"]


def test_beginner_run_journey_does_not_fabricate_a_success_report() -> None:
    pages = (
        DOCS / "getting-started" / "quickstart.md",
        DOCS / "getting-started" / "first-local-pipeline.md",
    )
    for page in pages:
        content = page.read_text(encoding="utf-8")
        bash = "\n".join(_fenced_blocks(page, "bash"))
        assert "dpone run-report" not in bash
        assert "run-report --latest" not in content
        assert "manual/synthetic" in content


def test_quickstart_run_command_executes_primary_manifest(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Keep the documented beginner command wired to the real run service."""

    page = DOCS / "getting-started" / "quickstart.md"
    manifest = tmp_path / "manifests" / "orders_to_landing.yaml"
    manifest.parent.mkdir()
    manifest.write_text(_primary_manifest(page), encoding="utf-8")
    documented = next(
        line for block in _fenced_blocks(page, "bash") for line in block.splitlines() if line.startswith("dpone run ")
    )
    argv = shlex.split(documented)
    argv[2] = str(manifest)

    class _DocumentedProcess:
        def __init__(self, **kwargs: object) -> None:
            del kwargs

        def run(self, **kwargs: object) -> ProcessResult:
            del kwargs
            return ProcessResult(
                status="success",
                inserted_rows=3,
                updated_rows=0,
                final_rows=3,
                extracted_rows=3,
                duration_seconds=0.1,
                errors=[],
            )

    class _DocumentedHydrator:
        def build(self, **kwargs: object) -> RuntimeBindings:
            del kwargs
            return RuntimeBindings(
                source_obj=object(),
                sink_obj=object(),
                etl_logger=object(),
            )

    monkeypatch.setattr(runtime_hydrator, "_RUNTIME_HYDRATOR", _DocumentedHydrator())
    monkeypatch.setattr(
        run_cmd,
        "RunManifestService",
        lambda: RunManifestService(process_factory=_DocumentedProcess),
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(argv[1:])

    captured = capsys.readouterr()
    assert exc.value.code == 0, captured.out
    assert captured.err == ""
    payload = json.loads(captured.out)
    assert payload["process"] == "orders_to_landing"
    assert payload["run_id"] == "orders_local"
    assert payload["passed"] is True
    assert payload["result"]["inserted_rows"] == 3


def test_airflow_attestation_docs_keep_live_rollout_unverified() -> None:
    required = {
        DOCS / "airflow-artifact-trust.md": "Fail-closed preview",
        DOCS / "airflow-artifact-attestation-operations.md": "Fail-closed preview",
        DOCS / "adr" / "0035-airflow-production-artifact-attestation.md": "UNVERIFIED",
        DOCS / "feature-design-airflow-production-artifact-attestation-v07327.md": (
            "Implementation maturity: FAIL-CLOSED PREVIEW"
        ),
    }
    for path, marker in required.items():
        assert marker in path.read_text(encoding="utf-8"), path

    changelog = (ROOT / "CHANGELOG.md").read_text(encoding="utf-8")
    assert "Production Airflow releases can now" not in changelog
    assert "signed dev restart" in changelog
    assert "staged production cutover/rollback" in changelog
