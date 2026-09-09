from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest


def _load_tool_module():
    path = Path("tools/mssql_clickhouse_bcp_native_type_certification.py")
    spec = importlib.util.spec_from_file_location("dpone_tools_mssql_clickhouse_bcp_native_type_certification", path)
    assert spec is not None
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_bcp_native_fixture_covers_supported_edge_types() -> None:
    module = _load_tool_module()

    columns = module.build_bcp_native_columns(200)
    by_name = {column.name: column for column in columns}

    assert len(columns) == 200
    assert by_name["amount_high"].mssql_type == "numeric(38,9)"
    assert by_name["business_time"].mssql_type == "time(7)"
    assert by_name["offset_at"].mssql_type == "datetimeoffset(7)"
    assert by_name["created_dt"].mssql_type == "datetime"
    assert by_name["ascii_max"].mssql_type == "varchar(max)"
    assert by_name["unicode_max"].mssql_type == "nvarchar(max)"
    assert by_name["payload_bin_max"].mssql_type == "varbinary(max)"
    assert by_name["fixed_ascii"].mssql_type == "char(8)"
    assert by_name["fixed_unicode"].mssql_type == "nchar(8)"


def test_bcp_native_load_config_selects_safe_worker_typed_binary_route() -> None:
    module = _load_tool_module()
    config = module.BcpNativeCertificationConfig(
        rows=10000,
        release_id="0.74.0",
        column_count=200,
        source_schema="src",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch",
        output_dir=Path("out"),
        mssql_params={"host": "127.0.0.1", "password": "secret", "bcp_path": "bcp"},
        clickhouse_params={"host": "127.0.0.1", "port": 9000, "http_port": 8123, "password": "secret"},
        target_rows_per_partition=2500,
        export_workers=1,
        load_workers=1,
        batch_size=5000,
        binary_format="rowbinary",
    )

    load_config = module.build_load_config(config)

    assert load_config.options["bulk"]["bcp"]["file_format"] == "native"
    assert load_config.options["native_transfer"]["wire"] == {
        "mode": "typed_binary",
        "source_native_format": "bcp_native",
        "binary_format": "rowbinary",
        "block_rows": 65536,
        "block_bytes": "64MiB",
        "acceleration": {"mode": "auto"},
    }
    assert load_config.options["native_transfer"]["execution"]["profile"] == "safe_worker"
    assert load_config.options["native_transfer"]["execution"]["resource_policy"]["max_active_files"] == 1
    assert load_config.options["clickhouse_bulk"]["ingest_contract"] == "typed_binary_staging"


def test_bcp_native_load_config_can_select_clickhouse_native_format() -> None:
    module = _load_tool_module()
    config = module.BcpNativeCertificationConfig(
        rows=10000,
        release_id="0.74.0",
        column_count=200,
        source_schema="src",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch_native",
        output_dir=Path("out"),
        mssql_params={"host": "127.0.0.1", "password": "secret", "bcp_path": "bcp"},
        clickhouse_params={"host": "127.0.0.1", "port": 9000, "http_port": 8123, "password": "secret"},
        target_rows_per_partition=2500,
        export_workers=1,
        load_workers=1,
        batch_size=5000,
        binary_format="native",
        acceleration_mode="required",
    )

    load_config = module.build_load_config(config)

    assert load_config.options["native_transfer"]["wire"]["binary_format"] == "native"
    assert load_config.options["native_transfer"]["wire"]["acceleration"] == {"mode": "required"}


def test_bcp_native_cli_can_reuse_a_prepared_dbt_relation() -> None:
    module = _load_tool_module()

    config = module.build_config(
        module.parse_args(
            [
                "--release-id",
                "0.74.0",
                "--skip-source-prepare",
                "--source-schema",
                "dbt_calc",
                "--source-table",
                "wide_dbt_result",
                "--upstream-evidence",
                "dbt_materialization.json",
            ]
        )
    )

    assert config.skip_source_prepare is True
    assert config.source_schema == "dbt_calc"
    assert config.source_table == "wide_dbt_result"
    assert config.upstream_evidence == Path("dbt_materialization.json")


def test_bcp_cli_defaults_match_host_ports_published_by_local_compose(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    module = _load_tool_module()
    for name in (
        "DPONE_IT_MSSQL_PORT",
        "DPONE_IT_MSSQL_PORT_FORWARD",
        "DPONE_IT_MSSQL_DATABASE",
        "DPONE_IT_CH_PORT",
        "DPONE_IT_CH_PORT_FORWARD",
        "DPONE_IT_CH_HTTP_PORT",
        "DPONE_IT_CH_HTTP_PORT_FORWARD",
        "DPONE_IT_CH_DATABASE",
    ):
        monkeypatch.delenv(name, raising=False)

    args = module.parse_args(["--release-id", "0.74.0"])

    assert args.mssql_port == 51_433
    assert args.mssql_database == "dpone_it"
    assert args.clickhouse_port == 59_000
    assert args.clickhouse_http_port == 58_123
    assert args.clickhouse_database == "dpone_it"


def test_bcp_native_certification_requires_observed_runtime_backend() -> None:
    module = _load_tool_module()
    config = module.BcpNativeCertificationConfig(
        rows=10,
        release_id="0.74.0",
        column_count=23,
        source_schema="src",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch_native",
        output_dir=Path("out"),
        mssql_params={"password": "secret"},
        clickhouse_params={"password": "secret"},
        target_rows_per_partition=5,
        export_workers=1,
        load_workers=1,
        batch_size=5,
        binary_format="native",
        acceleration_mode="required",
    )
    observed = module.RuntimeDecision(
        decision_id="native_transfer.acceleration",
        phase="load",
        component="native_wire_transcoder",
        category="backend_selection",
        requested="required",
        selected="native_accelerated",
        fallback_allowed=False,
    )

    module.assert_acceleration_decisions(config, [observed, observed])


def test_bcp_native_certification_rejects_python_fallback_in_required_mode() -> None:
    module = _load_tool_module()
    config = module.BcpNativeCertificationConfig(
        rows=10,
        release_id="0.74.0",
        column_count=23,
        source_schema="src",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch_native",
        output_dir=Path("out"),
        mssql_params={},
        clickhouse_params={},
        target_rows_per_partition=5,
        export_workers=1,
        load_workers=1,
        batch_size=5,
        binary_format="native",
        acceleration_mode="required",
    )
    fallback = module.RuntimeDecision(
        decision_id="native_transfer.acceleration",
        phase="load",
        component="native_wire_transcoder",
        category="backend_selection",
        requested="required",
        selected="python_reference",
        fallback_allowed=False,
    )

    try:
        module.assert_acceleration_decisions(config, [fallback])
    except RuntimeError as exc:
        assert str(exc) == "native_acceleration_runtime_backend_mismatch"
    else:
        raise AssertionError("required native mode accepted a Python fallback")


def test_bcp_native_certification_accepts_explicit_python_reference_warning() -> None:
    module = _load_tool_module()
    config = module.BcpNativeCertificationConfig(
        rows=10,
        release_id="0.74.0",
        column_count=23,
        source_schema="src",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch_python",
        output_dir=Path("out"),
        mssql_params={},
        clickhouse_params={},
        target_rows_per_partition=5,
        export_workers=1,
        load_workers=1,
        batch_size=5,
        binary_format="native",
        acceleration_mode="off",
    )
    reference = module.RuntimeDecision(
        decision_id="native_transfer.acceleration",
        phase="load",
        component="native_wire_transcoder",
        category="backend_selection",
        requested="off",
        selected="python_reference",
        fallback_allowed=False,
        fallback_reason="native_acceleration_disabled",
        release_gate="warning",
        warnings=("native_acceleration_disabled",),
    )

    module.assert_acceleration_decisions(config, [reference])


def test_single_native_artifact_projects_one_exact_loaded_slice() -> None:
    module = _load_tool_module()

    class Artifact:
        rows_exported = 10_000

    assert module.loaded_slice_closure(Artifact(), loaded_rows=10_000) == (
        {
            "partition_index": 0,
            "slice_index": 0,
            "rows_exported": 10_000,
            "rows_loaded": 10_000,
        },
    )
    assert module.loaded_slice_closure(Artifact()) == ()


def test_failed_live_attempt_removes_only_partial_evidence_owned_by_the_attempt(tmp_path: Path) -> None:
    module = _load_tool_module()
    output = tmp_path / "attempt"
    (output / "partition_files").mkdir(parents=True)
    (output / "partition_files" / "part-000.native").write_bytes(b"partial")
    (output / "mssql_clickhouse_wide_type_certification.json").write_text(
        '{"passed":true}\n',
        encoding="utf-8",
    )
    (output / "local_route_certification_receipt.json").write_text(
        '{"passed":true,"evidence_status":"PASS"}\n',
        encoding="utf-8",
    )

    config = module.BcpNativeCertificationConfig(
        rows=10,
        release_id="0.74.0",
        column_count=23,
        source_schema="src",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch_native",
        output_dir=output,
        mssql_params={},
        clickhouse_params={},
        target_rows_per_partition=5,
        export_workers=1,
        load_workers=1,
        batch_size=5,
        binary_format="native",
        acceleration_mode="required",
    )

    module._remove_owned_partial_evidence(config)

    assert output.is_dir()
    assert (output / "partition_files" / "part-000.native").read_bytes() == b"partial"
    assert (output / "mssql_clickhouse_wide_type_certification.json").exists() is False
    assert (output / "local_route_certification_receipt.json").exists() is False


def test_create_once_directory_rejects_a_preexisting_empty_path(tmp_path: Path) -> None:
    module = _load_tool_module()
    output = tmp_path / "attempt"
    output.mkdir()

    with pytest.raises(ValueError, match="output_already_exists"):
        module.prepare_create_once_directory(output)


def test_bcp_source_schema_is_escaped_as_a_sql_literal() -> None:
    module = _load_tool_module()

    class Connector:
        def __init__(self) -> None:
            self.queries: list[str] = []

        def execute_query(self, query: str) -> None:
            self.queries.append(query)

    connector = Connector()
    config = module.BcpNativeCertificationConfig(
        rows=1,
        release_id="0.74.0",
        column_count=1,
        source_schema="safe'; THROW 51000, 'boom', 1;--",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch",
        output_dir=Path("out"),
        mssql_params={},
        clickhouse_params={},
        target_rows_per_partition=1,
        export_workers=1,
        load_workers=1,
        batch_size=1,
        typed_hash_rows=1,
    )

    module._prepare_source(connector, config)

    assert "N'safe''; THROW 51000, ''boom'', 1;--'" in connector.queries[0]
    assert "N'safe'; THROW" not in connector.queries[0]


@pytest.mark.parametrize(
    ("skip_source_prepare", "upstream_evidence", "typed_hash_rows", "message"),
    (
        (True, None, 10_000, "skip_source_prepare_requires_upstream_evidence"),
        (False, None, 0, "wide_release_requires_full_typed_hash"),
    ),
)
def test_bcp_release_preflight_fails_before_connectors(
    monkeypatch: pytest.MonkeyPatch,
    skip_source_prepare: bool,
    upstream_evidence: Path | None,
    typed_hash_rows: int,
    message: str,
) -> None:
    module = _load_tool_module()
    monkeypatch.setattr(module, "capture_local_source_snapshot", lambda: object())
    config = module.BcpNativeCertificationConfig(
        rows=10_000,
        release_id="0.74.0",
        column_count=202,
        source_schema="src",
        source_table="orders",
        target_database="analytics",
        target_table="orders_ch",
        output_dir=Path("out"),
        mssql_params={"password": "secret"},
        clickhouse_params={"password": "secret"},
        target_rows_per_partition=2_500,
        export_workers=1,
        load_workers=1,
        batch_size=5_000,
        typed_hash_rows=typed_hash_rows,
        skip_source_prepare=skip_source_prepare,
        upstream_evidence=upstream_evidence,
    )

    with pytest.raises(ValueError, match=message):
        module.run_live_certification(config)
