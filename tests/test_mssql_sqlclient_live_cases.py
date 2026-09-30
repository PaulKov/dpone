import subprocess
import sys
from itertools import islice
from pathlib import Path

import pytest

from tests.integration.mssql.mssql_sqlclient_live_cases import sqlclient_live_case, sqlclient_live_cases

ROOT = Path(__file__).parents[1]


def test_profiles_are_closed_deterministic_and_wide_is_exactly_100_columns() -> None:
    first = sqlclient_live_cases()
    second = sqlclient_live_cases()

    assert first == second
    assert [case.fixture_id for case in first] == ["narrow-sqlclient-v1", "wide100-sqlclient-v1"]
    assert len(first[0].columns) == 4
    assert len(first[1].columns) == 100
    assert all(len(case.schema_sha256) == 64 for case in first)
    assert tuple(islice(first[1].rows(20), 20)) == tuple(islice(second[1].rows(20), 20))


def test_profiles_preserve_null_duplicate_and_type_contracts() -> None:
    case = sqlclient_live_case("narrow-sqlclient-v1")
    rows = tuple(case.rows(15))

    assert rows[13] == rows[12]
    assert rows[0][1] is not None
    assert rows[10][1] is None
    assert isinstance(rows[1][0], int)
    assert isinstance(rows[1][1], float)
    assert isinstance(rows[1][2], str)
    assert rows[1][3].microsecond >= 0


@pytest.mark.parametrize("fixture_id", ["narrow-sqlclient-v1", "wide100-sqlclient-v1"])
def test_text_distribution_is_bounded_and_keeps_rare_boundary_rows(fixture_id: str) -> None:
    case = sqlclient_live_case(fixture_id)
    text_indexes = [index for index, (_name, declared) in enumerate(case.columns) if declared.startswith("nvarchar")]

    common = next(islice(case.rows(255), 254, None))
    large = next(islice(case.rows(257), 256, None))
    maximum = next(islice(case.rows(65_536), 65_535, None))

    assert max(len(common[index] or "") for index in text_indexes) <= 255
    assert sum(len(large[index] or "") == 4_095 for index in text_indexes) == 1
    assert sum(len(maximum[index] or "") == 65_535 for index in text_indexes) == 1


@pytest.mark.parametrize(("fixture_id", "row_count"), [("unknown", 1), ("narrow-sqlclient-v1", -1)])
def test_invalid_profile_or_count_fails_closed(fixture_id: str, row_count: int) -> None:
    with pytest.raises(ValueError, match="mssql_sqlclient"):
        tuple(sqlclient_live_case(fixture_id).rows(row_count))


def test_checked_fixture_descriptors_are_current() -> None:
    subprocess.run(
        [sys.executable, "-m", "tools.generate_mssql_sqlclient_fixtures", "--check"],
        cwd=ROOT,
        check=True,
    )
