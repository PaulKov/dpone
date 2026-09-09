from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from dpone.cli import main as cli_main
from dpone.load_profile import LoadProfileAdvisor, ProfileAdviceRequest, SourceShape, WorkerProfile
from dpone.runtime.columnar_snapshot_provider import ColumnarSnapshotRequest
from dpone.runtime.sources.strategies.mssql.mssql_columnar_chunks import initial_chunk_rows, next_chunk_rows


class _LoggerStub:
    def error(self, message: str) -> None:
        del message

    def info(self, message: str) -> None:
        del message


def _patch_context(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(cli_main, "setup_logging", lambda: _LoggerStub())
    monkeypatch.setattr(cli_main.AppContext, "from_env", staticmethod(lambda logger: SimpleNamespace(logger=logger)))


def test_profile_advisor_recommends_larger_row_window_for_narrow_columnar_table() -> None:
    decision = LoadProfileAdvisor().advise(
        ProfileAdviceRequest(
            source_type="mssql",
            sink_type="clickhouse",
            route_id="object_storage_pull_s3cluster",
            execution_mode="chunked",
            target_chunk_bytes=512 * 1024 * 1024,
            current_max_chunk_rows=1_000_000,
            source_shape=SourceShape(row_count=33_477_752, column_count=12, estimated_bytes_per_row=24),
            worker_profile=WorkerProfile(name="weak_worker", memory_bytes=2 * 1024 * 1024 * 1024),
        )
    )

    assert decision.selected_profile == "object_storage_pull.chunked.weak_worker"
    assert "row_cap_limits_target_chunk_bytes" in decision.warning_codes
    assert (
        decision.recommended_patch["source"]["options"]["native_transfer"]["snapshot"]["columnar_fast_path"][
            "execution"
        ]["max_chunk_rows"]
        > 1_000_000
    )
    assert (
        decision.recommended_patch["source"]["options"]["native_transfer"]["snapshot"]["columnar_fast_path"][
            "execution"
        ]["max_inflight_chunks"]
        == 1
    )
    assert decision.details["window_count_estimate"] == {
        "current": 34,
        "recommended": 7,
        "reduction": 27,
    }


def test_profile_advisor_performance_goal_prefers_bigger_windows_and_parallel_pull() -> None:
    decision = LoadProfileAdvisor().advise(
        ProfileAdviceRequest(
            source_type="mssql",
            sink_type="clickhouse",
            route_id="object_storage_pull_s3cluster",
            execution_mode="chunked",
            optimization_goal="performance",
            target_chunk_bytes=512 * 1024 * 1024,
            current_max_chunk_rows=1_000_000,
            source_shape=SourceShape(row_count=34_917_463, column_count=12, estimated_bytes_per_row=24),
            worker_profile=WorkerProfile.from_name("throughput"),
        )
    )

    execution = decision.recommended_patch["source"]["options"]["native_transfer"]["snapshot"]["columnar_fast_path"][
        "execution"
    ]
    assert decision.selected_profile == "object_storage_pull.chunked.throughput.performance"
    assert execution["max_chunk_rows"] == 20_000_000
    assert execution["max_inflight_chunks"] == 2
    assert decision.details["window_count_estimate"] == {
        "current": 35,
        "recommended": 2,
        "reduction": 33,
    }
    assert any(item["code"] == "performance_profile_checks_mssql_impact" for item in decision.recommendations)


def test_profile_advisor_keeps_default_row_cap_when_target_chunk_bytes_is_reachable() -> None:
    decision = LoadProfileAdvisor().advise(
        ProfileAdviceRequest(
            source_type="mssql",
            sink_type="clickhouse",
            route_id="object_storage_pull_s3cluster",
            execution_mode="chunked",
            target_chunk_bytes=512 * 1024 * 1024,
            current_max_chunk_rows=1_000_000,
            source_shape=SourceShape(row_count=10_000_000, column_count=200, estimated_bytes_per_row=768),
            worker_profile=WorkerProfile(name="weak_worker", memory_bytes=2 * 1024 * 1024 * 1024),
        )
    )

    assert "row_cap_limits_target_chunk_bytes" not in decision.warning_codes
    assert (
        decision.recommended_patch["source"]["options"]["native_transfer"]["snapshot"]["columnar_fast_path"][
            "execution"
        ]["max_chunk_rows"]
        == 1_000_000
    )


def test_mssql_columnar_chunk_sizing_reads_canonical_execution_row_limits() -> None:
    request = ColumnarSnapshotRequest(
        query="SELECT * FROM src",
        schema=(("id", "Int64"),),
        uri_prefix="s3://bucket/run/",
        run_id="run-1",
        options={"execution": {"target_chunk_rows": "2000000", "max_chunk_rows": "3000000"}},
    )

    assert initial_chunk_rows(request) == 2_000_000
    assert next_chunk_rows(request, current_target_rows=2_000_000, row_count=1_000_000, size_bytes=1) == 3_000_000


def test_profile_advise_cli_outputs_patch_json(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "load.yaml"
    manifest.write_text(
        """
source:
  type: mssql
  table: {schema: reporting, name: EXAMPLE_DIM_history}
  options:
    native_transfer:
      snapshot:
        columnar_fast_path:
          mode: required
          provider: object_storage_pull
          execution:
            mode: chunked
            target_chunk_bytes: 512MiB
            max_inflight_chunks: 1
sink:
  type: clickhouse
  table: {schema: Example_Datamarts, name: example_reporting__example_dim_history}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "profile",
                "advise",
                str(manifest),
                "--row-count",
                "33477752",
                "--column-count",
                "12",
                "--estimated-bytes-per-row",
                "24",
                "--worker-profile",
                "weak_worker",
                "--goal",
                "performance",
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["schema_version"] == "dpone.profile.advice.v1"
    assert payload["selected_profile"] == "object_storage_pull.chunked.weak_worker.performance"
    assert "row_cap_limits_target_chunk_bytes" in payload["warnings"]
    assert (
        payload["recommended_patch"]["source"]["options"]["native_transfer"]["snapshot"]["columnar_fast_path"][
            "execution"
        ]["max_chunk_rows"]
        > 1_000_000
    )


def test_profile_advise_compiles_flow_authoring_before_reading_route(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "marketing_wau.yaml"
    manifest.write_text(
        f"""
kind: dpone.flow.v1
authoring:
  mode: flow
  source: {manifest.name}
metadata:
  id: marketing_wau
  domain: marketing
processes:
  - name: marketing_wau
    source:
      type: clickhouse
      connection_ref: clickhouse_marketing
      table: {{schema: marketing_datamarts, name: wau_for_da}}
      options:
        native_transfer:
          snapshot:
            columnar_fast_path:
              provider: native_file
              execution: {{mode: chunked}}
    sink:
      type: mssql
      connection_ref: mssql_marketing
      table: {{schema: ch, name: marketing__wau_for_da}}
      strategy: {{mode: full_refresh}}
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(["profile", "advise", str(manifest), "--format", "json"])

    assert exc.value.code == 0
    details = json.loads(capsys.readouterr().out)["details"]
    assert details["source_type"] == "clickhouse"
    assert details["sink_type"] == "mssql"


def test_profile_wizard_writes_manifest_patch(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
) -> None:
    _patch_context(monkeypatch)
    manifest = tmp_path / "load.yaml"
    output = tmp_path / "profile.patch.yaml"
    manifest.write_text(
        """
source:
  type: mssql
sink:
  type: clickhouse
""".strip()
        + "\n",
        encoding="utf-8",
    )

    with pytest.raises(SystemExit) as exc:
        cli_main.main(
            [
                "profile",
                "wizard",
                str(manifest),
                "--estimated-bytes-per-row",
                "24",
                "--worker-profile",
                "weak_worker",
                "--output",
                str(output),
                "--format",
                "json",
            ]
        )

    assert exc.value.code == 0
    payload = json.loads(capsys.readouterr().out)
    assert payload["patch_path"] == str(output)
    patch_text = output.read_text(encoding="utf-8")
    assert "columnar_fast_path:" in patch_text
    assert "max_chunk_rows:" in patch_text
