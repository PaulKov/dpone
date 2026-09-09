from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest
import yaml

from dpone.cli import main as cli_main
from dpone.manifest.authoring import AuthoringCompiler
from dpone.readiness.airflow_authoring_check_service import AirflowAuthoringCheckService


def _unsafe_flow(relative_path: Path, *, source_type: str = "clickhouse") -> dict[str, object]:
    return {
        "kind": "dpone.flow.v1",
        "authoring": {"mode": "flow", "source": relative_path.as_posix()},
        "metadata": {"id": "clickhouse_events", "domain": "marketing", "airflow": True},
        "processes": [
            {
                "name": "clickhouse_events",
                "source": {
                    "type": source_type,
                    "connection_ref": "source_connection",
                    "table": {"schema": "analytics", "name": "events"},
                    "options": {"incremental_column": "event_at"},
                },
                "sink": {
                    "type": "mssql",
                    "connection_ref": "mssql_target",
                    "table": {"database": "DWH", "schema": "landing", "name": "events"},
                    "strategy": {"mode": "incremental_append"},
                },
                "state": {
                    "type": "mssql",
                    "connection_ref": "mssql_state",
                    "atomicity": "target_atomic",
                    "provisioning": "external",
                },
            }
        ],
    }


def _write_flow(root: Path, *, source_type: str = "clickhouse") -> tuple[Path, dict[str, object]]:
    relative_path = Path("pipelines/clickhouse_events/pipeline.yaml")
    source_path = root / relative_path
    source_path.parent.mkdir(parents=True)
    payload = _unsafe_flow(relative_path, source_type=source_type)
    source_path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")
    return source_path, payload


@pytest.mark.parametrize(
    ("source_type", "expected_code"),
    (
        ("clickhouse", "CLICKHOUSE_MSSQL_TARGET_MAX_CURSOR_UNSAFE"),
        ("sqlserver", "MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE"),
        ("sql_server", "MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE"),
    ),
)
def test_dpone_check_rejects_real_flow_v1_cursor_with_structured_stable_error(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
    source_type: str,
    expected_code: str,
) -> None:
    _write_flow(tmp_path, source_type=source_type)
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as raised:
        cli_main.main(["check", "pipelines/clickhouse_events", "--format", "json"])

    captured = capsys.readouterr()
    payload = json.loads(captured.out)
    assert int(raised.value.code) == 1
    assert captured.err == ""
    assert payload["passed"] is False
    assert [error["code"] for error in payload["errors"]] == [expected_code]
    assert payload["errors"][0]["schema"] == "dpone.error.v1"
    assert payload["errors"][0]["stage"] == "static_check"
    assert payload["errors"][0]["docs_url"] == f"docs/errors/{expected_code}.md"
    assert (Path(__file__).parents[1] / payload["errors"][0]["docs_url"]).is_file()
    assert payload["network"] is False
    assert payload["secrets"] is False
    assert payload["source_queries"] is False


def test_self_service_validates_pinned_compilation_not_mutated_source_file(tmp_path: Path) -> None:
    source_path, unsafe_payload = _write_flow(tmp_path)

    class MutatingCompiler(AuthoringCompiler):
        def compile(self, payload, *, source_path, project_root=None, include_content_dependencies=True):
            compilation = super().compile(
                payload,
                source_path=source_path,
                project_root=project_root,
                include_content_dependencies=include_content_dependencies,
            )
            safe_payload = copy.deepcopy(unsafe_payload)
            safe_process = safe_payload["processes"][0]
            assert isinstance(safe_process, dict)
            safe_sink = safe_process["sink"]
            assert isinstance(safe_sink, dict)
            safe_sink["strategy"] = {"mode": "full_refresh"}
            source_path.write_text(yaml.safe_dump(safe_payload, sort_keys=False), encoding="utf-8")
            return compilation

    checked = AirflowAuthoringCheckService(
        root=tmp_path,
        authoring_compiler=MutatingCompiler(),
    ).inspect(source_path)

    assert checked.result.passed is False
    assert [error["code"] for error in checked.result.errors] == ["CLICKHOUSE_MSSQL_TARGET_MAX_CURSOR_UNSAFE"]


def test_cursor_error_pages_are_published_in_mkdocs_navigation() -> None:
    root = Path(__file__).parents[1]
    navigation = (root / "mkdocs.yml").read_text(encoding="utf-8")

    for code in (
        "CLICKHOUSE_MSSQL_TARGET_MAX_CURSOR_UNSAFE",
        "MYSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE",
        "MSSQL_MSSQL_TARGET_MAX_CURSOR_UNSAFE",
        "POSTGRES_MSSQL_COLUMN_CURSOR_UNSAFE",
    ):
        relative_path = f"errors/{code}.md"
        assert (root / "docs" / relative_path).is_file()
        assert relative_path in navigation
