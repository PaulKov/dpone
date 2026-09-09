import importlib.util


def test_psycopg_test_stub_is_discoverable_by_find_spec() -> None:
    spec = importlib.util.find_spec("psycopg")

    assert spec is not None


def test_psycopg_sql_test_stub_is_discoverable_by_find_spec() -> None:
    spec = importlib.util.find_spec("psycopg.sql")

    assert spec is not None
