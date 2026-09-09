from __future__ import annotations

import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.commands import init_route_selection
from dpone.connector_sdk.certification_artifacts import CertificationArtifactPublisher
from dpone.contracts.capability_discovery import RecipeDiscoveryEntry
from dpone.contracts.connector_declarations import built_in_connector_declarations
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.readiness.airflow_self_service_composition import build_airflow_self_service_service
from dpone.readiness.capability_discovery_service import CapabilityDiscoveryService


class _LoggerStub:
    def error(self, _message: str) -> None:
        return None


class _TtyInput:
    def __init__(self, answer: str) -> None:
        self._answer = answer

    def isatty(self) -> bool:
        return True

    def readline(self) -> str:
        return self._answer

    def info(self, _message: str) -> None:
        return None

    def warning(self, _message: str) -> None:
        return None


def _run_cli(
    args: list[str],
    *,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> tuple[int, str, str]:
    logger = _LoggerStub()
    monkeypatch.setattr(cli_main, "setup_logging", lambda: logger)
    monkeypatch.setattr(
        cli_main.AppContext,
        "from_env",
        staticmethod(lambda *, logger: SimpleNamespace(logger=logger)),
    )
    with pytest.raises(SystemExit) as exc:
        cli_main.main(args)
    captured = capsys.readouterr()
    return int(exc.value.code), captured.out, captured.err


def _write_malformed_recipe_catalog(root: Path) -> None:
    (root / "dpone.yaml").write_text(
        """
schema: dpone.project.v1
authoring:
  primary_source_policy: one_per_pipeline
  recipe_catalog:
    path: recipes/catalog.yaml
    trusted_catalog_ids: [data-platform]
""".lstrip(),
        encoding="utf-8",
    )
    catalog = root / "recipes" / "catalog.yaml"
    catalog.parent.mkdir()
    catalog.write_text("schema: [\n", encoding="utf-8")


def test_init_pipeline_fails_closed_for_malformed_recipe_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_malformed_recipe_catalog(tmp_path)
    monkeypatch.chdir(tmp_path)

    code, stdout, _ = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--recipe",
            "mssql-to-clickhouse-incremental",
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_RECIPE_CATALOG_INVALID"
    assert not (tmp_path / "pipelines").exists()


def test_default_init_pipeline_fails_closed_for_malformed_recipe_authority(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _write_malformed_recipe_catalog(tmp_path)
    monkeypatch.chdir(tmp_path)

    code, stdout, _ = _run_cli(
        ["init", "pipeline", "orders_daily", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_RECIPE_CATALOG_INVALID"
    assert not (tmp_path / "pipelines").exists()


def test_direct_scaffold_fails_closed_for_malformed_recipe_authority(
    tmp_path: Path,
) -> None:
    _write_malformed_recipe_catalog(tmp_path)

    result = build_airflow_self_service_service(root=tmp_path).init_pipeline(
        pipeline_id="orders_daily",
        recipe="mssql-to-clickhouse-incremental",
        airflow=True,
    )

    assert result.passed is False
    assert result.errors[0]["code"] == "DPONE_RECIPE_CATALOG_INVALID"
    assert not (tmp_path / "pipelines").exists()


def test_recipe_list_filters_and_show_share_capability_projection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "recipe",
            "list",
            "--source",
            "mssql",
            "--sink",
            "clickhouse",
            "--strategy",
            "incremental_merge",
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0, stderr
    payload = json.loads(stdout)
    assert [item["ref"] for item in payload["recipes"]] == ["mssql-to-clickhouse-incremental"]
    assert payload["recipes"][0]["route_id"] == "mssql:clickhouse:incremental_merge"
    assert payload["recipes"][0]["evidence_status"] == "UNVERIFIED"

    code, stdout, stderr = _run_cli(
        ["recipe", "show", "mssql-to-clickhouse-incremental", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0, stderr
    shown = json.loads(stdout)
    assert shown["source"] == "mssql"
    assert shown["sink"] == "clickhouse"
    assert shown["strategy"] == "incremental_merge"
    assert shown["scaffold_argv"] == [
        "dpone",
        "init",
        "pipeline",
        "<pipeline_id>",
        "--recipe",
        "mssql-to-clickhouse-incremental",
    ]


def test_recipe_list_rejects_unknown_filter_with_stable_json_contract(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["recipe", "list", "--source", "definitely_unknown", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert stderr == ""
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_ROUTE_FILTER_INVALID"


def test_recipe_show_propagates_malformed_catalog_issue(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "dpone.yaml").write_text(
        """
schema: dpone.project.v1
authoring:
  primary_source_policy: one_per_pipeline
  recipe_catalog:
    path: recipes/catalog.yaml
    trusted_catalog_ids: [data-platform]
""".lstrip(),
        encoding="utf-8",
    )
    catalog = tmp_path / "recipes" / "catalog.yaml"
    catalog.parent.mkdir()
    catalog.write_text("schema: [\n", encoding="utf-8")
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["recipe", "show", "mssql-to-clickhouse-incremental", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 1
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["issues"][0]["code"] == "DPONE_RECIPE_CATALOG_INVALID"


def test_recipe_list_accepts_api_endpoint_family_for_rest_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["recipe", "list", "--source", "api", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["recipes"] == []
    assert any(item["source"] == "rest" for item in payload["routes"])


def test_init_pipeline_route_resolves_one_builtin_recipe_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(
        ["init", "project", "--airflow", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0, stderr

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--route",
            "mssql:clickhouse:incremental_merge",
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["recipe"] == "mssql-to-clickhouse-incremental"
    assert (tmp_path / "pipelines/orders_daily/pipeline.yaml").is_file()


_MSSQL_WIDE_CERT_SINKS = ("bigquery", "postgres", "mssql", "clickhouse", "kafka")


def test_recipe_list_surfaces_supported_mssql_wide_cert_sinks(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    """Certified mssql→sink routes remain discoverable without new CLI surface."""

    monkeypatch.chdir(tmp_path)
    code, stdout, stderr = _run_cli(
        ["recipe", "list", "--source", "mssql", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0, stderr
    payload = json.loads(stdout)
    sinks = {item["sink"] for item in payload["routes"] if item.get("support", {}).get("status") == "supported"}
    assert sinks.issuperset(_MSSQL_WIDE_CERT_SINKS)
    recipes = {item["ref"]: item for item in payload["recipes"]}
    clickhouse = recipes["mssql-to-clickhouse-incremental"]
    assert clickhouse["route_id"] == "mssql:clickhouse:incremental_merge"
    assert clickhouse["scaffoldable"] is True
    full_refresh = recipes["mssql-to-clickhouse-full-refresh"]
    assert full_refresh["route_id"] == "mssql:clickhouse:full_refresh"
    assert full_refresh["scaffoldable"] is True


def test_init_pipeline_scaffolds_supported_mssql_clickhouse_full_refresh_route(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    assert (
        _run_cli(
            ["init", "project", "--airflow", "--layout", "domain-first", "--format", "json"],
            monkeypatch=monkeypatch,
            capsys=capsys,
        )[0]
        == 0
    )
    assert (
        _run_cli(
            [
                "init",
                "domain",
                "sales",
                "--owner-team",
                "data-sales",
                "--owner-contact",
                "sales@example.com",
                "--approver-team",
                "data-platform",
                "--format",
                "json",
            ],
            monkeypatch=monkeypatch,
            capsys=capsys,
        )[0]
        == 0
    )

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_full",
            "--domain",
            "sales",
            "--route",
            "mssql:clickhouse:full_refresh",
            "--from",
            "mssql_dev:dbo.orders",
            "--to",
            "clickhouse_dev:analytics.orders_full",
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 0, stderr
    payload = json.loads(stdout)
    assert payload["passed"] is True
    manifest = yaml.safe_load(
        (tmp_path / "workloads/sales/pipelines/orders_full/pipeline.yaml").read_text(encoding="utf-8")
    )
    strategy = manifest["processes"][0]["sink"]["strategy"]
    assert strategy == {"mode": "full_refresh"}


def test_init_pipeline_route_fail_closed_when_mssql_sink_lacks_beginner_recipe(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(
        ["init", "project", "--airflow", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0, stderr

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_pg",
            "--route",
            "mssql:postgres:incremental_merge",
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code != 0
    payload = json.loads(stdout)
    assert payload.get("passed") is False
    errors = payload.get("errors") or payload.get("issues") or []
    assert errors[0]["code"] == "DPONE_ROUTE_NOT_SCAFFOLDABLE"
    assert not (tmp_path / "pipelines/orders_pg/pipeline.yaml").exists()


def test_init_pipeline_explicit_recipe_rejects_unsupported_route_before_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    service = CapabilityDiscoveryService(
        connectors=built_in_connector_declarations(),
        route_profiles=RouteProfileCatalog.default().profiles(),
        recipes=(
            RecipeDiscoveryEntry(
                ref="unsupported-custom@1",
                origin="platform",
                status="stable",
                route_id="unknown_source:unknown_sink:full_refresh",
                scaffoldable=True,
            ),
        ),
    )
    monkeypatch.setattr(
        init_route_selection,
        "build_capability_discovery_service",
        lambda *, root: service,
    )
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "must_not_exist",
            "--recipe",
            "unsupported-custom@1",
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert stderr == ""
    assert json.loads(stdout)["errors"][0]["code"] == "DPONE_ROUTE_NOT_SUPPORTED"
    assert not (tmp_path / "pipelines").exists()


def test_init_pipeline_tty_picker_lists_only_scaffoldable_routes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    code, _, stderr = _run_cli(
        ["init", "project", "--airflow", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0, stderr
    monkeypatch.setattr(sys, "stdin", _TtyInput("\n"))

    code, stdout, stderr = _run_cli(
        ["init", "pipeline", "orders_daily", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 0
    assert "Choose a scaffoldable route" in stderr
    payload = json.loads(stdout)
    assert payload["recipe"] == "mssql-to-clickhouse-incremental"


def test_init_pipeline_unknown_or_unscaffoldable_route_fails_without_partial_files(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        [
            "init",
            "pipeline",
            "orders_daily",
            "--route",
            "postgres:mssql:incremental_merge",
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["errors"][0]["code"] == "DPONE_ROUTE_NOT_SCAFFOLDABLE"
    assert not (tmp_path / "pipelines").exists()

    code, stdout, stderr = _run_cli(
        [
            "recipe",
            "list",
            "--source",
            "postgres",
            "--sink",
            "mssql",
            "--strategy",
            "incremental_merge",
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0, stderr
    discovery = json.loads(stdout)
    assert discovery["recipes"] == []
    assert discovery["routes"][0]["id"] == "postgres:mssql:incremental_merge"
    assert discovery["routes"][0]["beginner"]["recipe_available"] is False


def test_capability_cli_surfaces_invalid_evidence_configuration(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    (tmp_path / "dpone.yaml").write_text(
        """
schema: dpone.project.v1
capability_discovery:
  certification_evidence:
    matrix_path: ../outside.json
    expected_commit: not-a-commit
    evidence_dirs: []
    max_age_hours: 0
""".lstrip(),
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["recipe", "list", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 1
    assert stderr == ""
    payload = json.loads(stdout)
    assert payload["passed"] is False
    assert payload["issues"][0]["code"] == ("DPONE_CAPABILITY_EVIDENCE_CONFIG_INVALID")

    code, stdout, stderr = _run_cli(
        ["connectors", "list", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 1
    assert stderr == ""
    connectors = json.loads(stdout)
    assert connectors["passed"] is False
    assert connectors["issues"][0]["code"] == ("DPONE_CAPABILITY_EVIDENCE_CONFIG_INVALID")

    code, stdout, stderr = _run_cli(
        ["connectors", "list"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 1
    assert stderr == ""
    assert "Capability discovery blocked:" in stdout
    assert "capability_discovery.certification_evidence" in stdout
    assert "dpone connectors list" in stdout

    code, stdout, stderr = _run_cli(
        ["ops", "marketplace", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 1
    assert stderr == ""
    marketplace = json.loads(stdout)
    assert marketplace["passed"] is False
    assert marketplace["issues"][0]["code"] == ("DPONE_CAPABILITY_EVIDENCE_CONFIG_INVALID")


def test_connectors_list_uses_same_snapshot_and_certification_is_strict_by_default(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["connectors", "list", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0, stderr
    payload = json.loads(stdout)
    assert "mysql" in payload["connectors"]
    assert payload["statuses"]["postgres"] == "experimental"
    assert payload["snapshot_id"].startswith("sha256:")

    code, stdout, _ = _run_cli(
        ["connectors", "certify", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 1
    assert json.loads(stdout)["passed"] is False

    code, stdout, stderr = _run_cli(
        ["connectors", "certify", "--report-only", "--format", "json"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )
    assert code == 0, stderr
    assert json.loads(stdout)["passed"] is False


def test_connectors_certification_publishes_json_and_markdown_together(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)
    artifact_dir = tmp_path / "certification"

    code, stdout, _ = _run_cli(
        [
            "connectors",
            "certify",
            "--artifact-dir",
            str(artifact_dir),
            "--format",
            "json",
        ],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 1
    console = json.loads(stdout)
    artifact = json.loads((artifact_dir / "connector-certification.json").read_text(encoding="utf-8"))
    assert artifact["passed"] is False
    assert (artifact_dir / "connector-certification.md").is_file()
    assert not tuple(artifact_dir.glob("*.tmp"))
    assert console["artifact_paths"]["json"].endswith("connector-certification.json")


def test_certification_publication_restores_previous_pair_when_json_commit_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    publisher = CertificationArtifactPublisher()
    publisher.publish(
        tmp_path,
        base_name="report",
        json_content='{"id":"old"}\n',
        markdown_content="id=old\n",
    )
    real_replace = __import__("os").replace

    def fail_json_commit(source: Path, destination: Path) -> None:
        if Path(destination).name == "report.json":
            raise OSError("injected JSON commit failure")
        real_replace(source, destination)

    monkeypatch.setattr("dpone.connector_sdk.certification_artifacts.os.replace", fail_json_commit)

    with pytest.raises(OSError):
        publisher.publish(
            tmp_path,
            base_name="report",
            json_content='{"id":"new"}\n',
            markdown_content="id=new\n",
        )

    assert json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))["id"] == "old"
    assert (tmp_path / "report.md").read_text(encoding="utf-8") == "id=old\n"


def test_concurrent_certification_publication_never_mixes_report_pair(
    tmp_path: Path,
) -> None:
    publisher = CertificationArtifactPublisher()

    def publish(index: int) -> None:
        publisher.publish(
            tmp_path,
            base_name="report",
            json_content=json.dumps({"id": index}) + "\n",
            markdown_content=f"id={index}\n",
        )

    with ThreadPoolExecutor(max_workers=8) as executor:
        list(executor.map(publish, range(20)))

    final_id = json.loads((tmp_path / "report.json").read_text(encoding="utf-8"))["id"]
    assert (tmp_path / "report.md").read_text(encoding="utf-8") == f"id={final_id}\n"


def test_connectors_certification_rejects_conflicting_strictness_flags(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, _stdout, stderr = _run_cli(
        ["connectors", "certify", "--report-only", "--fail-on-missing"],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert "not allowed with argument" in stderr


@pytest.mark.parametrize(
    "format_args",
    (["--format", "json"], ["--format=json"]),
)
def test_studio_cli_rejects_out_of_range_port_before_startup(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    format_args: list[str],
) -> None:
    monkeypatch.chdir(tmp_path)

    code, stdout, stderr = _run_cli(
        ["studio", "--port", "70000", *format_args],
        monkeypatch=monkeypatch,
        capsys=capsys,
    )

    assert code == 2
    assert stdout == ""
    payload = json.loads(stderr)
    assert payload["errors"][0]["schema"] == "dpone.error.v1"
    assert payload["errors"][0]["code"] == "DPONE_CLI_USAGE_INVALID"
    assert "port must be an integer from 1 to 65535" in payload["errors"][0]["message"]
    assert "Traceback" not in stderr
