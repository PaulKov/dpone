"""Keep the synthetic live corpus capable of detecting fidelity regressions."""

from collections import Counter
from datetime import datetime

import pytest

from tests.integration.mssql.clickhouse_raw_snapshot_live_support import raw_case


@pytest.mark.parametrize("width", [7, 100])
def test_corpus_exercises_duplicates_versions_markers_and_window_boundaries(width):
    case = raw_case(width)
    assert len(case.columns) == width
    assert all(len(row) == width for row in case.rows)
    inside = case.window_rows(datetime(2024, 1, 2), datetime(2024, 1, 3))
    assert len(inside) == 4
    assert sorted(Counter(inside).values()) == [1, 1, 2]
    assert {row[1] for row in inside} == {1, 2, 3}
    assert {row[2] for row in inside} == {0, 1}
    assert {row[4] for row in inside} == {None, "", "λ\tline\n雪"}
    assert len(case.window_rows(datetime(2030, 1, 1), datetime(2030, 1, 2))) == 0
    assert {row[3] for row in case.rows} >= {datetime(2024, 1, 2), datetime(2024, 1, 3)}


def test_invalid_width_cannot_silently_change_certified_profile():
    with pytest.raises(ValueError, match="width"):
        raw_case(99)


def test_fail_closed_oracle_requires_a_direct_domain_rejection():
    from tests.integration.mssql.clickhouse_raw_snapshot_live_support import require_causal_source_rejection

    error = ValueError("mssql_native.source_provenance_incomplete")
    require_causal_source_rejection(error)
    error.add_note("mssql_native.source_disconnect_failed")
    with pytest.raises(AssertionError, match="cleanup"):
        require_causal_source_rejection(error)


def test_fail_closed_oracle_rejects_suppressed_vendor_or_cleanup_failure():
    from tests.integration.mssql.clickhouse_raw_snapshot_live_support import require_causal_source_rejection

    try:
        try:
            raise RuntimeError("synthetic disconnect failure")
        except RuntimeError:
            raise ValueError("mssql_native.source_provenance_incomplete") from None
    except ValueError as error:
        with pytest.raises(AssertionError, match="underlying"):
            require_causal_source_rejection(error)


def test_failed_fixture_preserves_all_authority(tmp_path):
    from tests.integration.mssql.clickhouse_raw_snapshot_live_support import preserve_on_failure

    authority = tmp_path / "authority"
    target = tmp_path / "target"
    authority.write_text("held")
    target.write_text("unsettled")
    with pytest.raises(RuntimeError, match="unsettled") as caught:
        with preserve_on_failure([authority.unlink, target.unlink], "synthetic retained route"):
            raise RuntimeError("unsettled")
    assert authority.read_text() == "held"
    assert target.read_text() == "unsettled"
    assert "synthetic retained route" in caught.value.__notes__[0]


def test_successful_fixture_attempts_every_exact_cleanup_even_if_one_fails(tmp_path):
    from tests.integration.mssql.clickhouse_raw_snapshot_live_support import preserve_on_failure

    remaining = tmp_path / "remaining"
    remaining.write_text("settled")
    with pytest.raises(ExceptionGroup):
        with preserve_on_failure([(tmp_path / "missing").unlink, remaining.unlink], "synthetic"):
            pass
    assert not remaining.exists()


@pytest.mark.parametrize("width", [7, 100])
def test_fixture_target_layout_is_admitted_by_real_target_local_verifier(width):
    from dpone.runtime.native_wire_mssql import build_mssql_bcp_native_contract
    from dpone.runtime.sinks.mssql_native_target_digest import require_target_local_raw_layout

    schema = tuple(
        (name, dtype.removesuffix(" NOT NULL").removesuffix(" NULL")) for name, _, dtype in raw_case(width).columns
    )
    contract = build_mssql_bcp_native_contract(schema=schema, query="SELECT synthetic_fixture")
    require_target_local_raw_layout(contract)


@pytest.mark.parametrize("backend", ["bcp", "mssql_sqlclient"])
@pytest.mark.parametrize("window", ["full", "nonempty", "empty"])
def test_fixture_preplan_widens_uint8_marker_using_public_physical_design(tmp_path, backend, window):
    from types import SimpleNamespace

    from dpone.manifest.mssql_native_policy import validate_native_config
    from dpone.runtime.etl.mssql_fresh_target_preplan import resolved_mssql_target_column_type
    from tests.integration.mssql.clickhouse_raw_snapshot_live_support import route_process

    connector = SimpleNamespace(database="synthetic")
    _, config = route_process(
        tmp_path, connector, connector, SimpleNamespace(logger=None), "source", "target", backend, window
    )
    validate_native_config(config)
    assert resolved_mssql_target_column_type(config, "is_deleted", "UInt8") == "bigint"
