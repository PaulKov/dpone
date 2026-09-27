"""Target identity must use the bare MSSQL schema, not the database label."""

from types import SimpleNamespace

from dpone.runtime.etl.mssql_physical_preplan import _table_name
from dpone.runtime.etl.mssql_transaction_request import live_target_coordinates
from dpone.runtime.sinks.mssql_physical_introspection import _qualified_table
from dpone.runtime.sinks.strategies.mssql.mssql_generic_target_contract import MssqlGenericTargetContract


def test_live_target_coordinates_peel_the_database_prefix_from_the_schema_label():
    load_config = SimpleNamespace(
        target_database="DWH_Dev",
        target_schema="DWH_Dev.ch",
        target_table="marketing__wau_for_da",
        options={},
    )

    assert live_target_coordinates(load_config) == ("DWH_Dev", "ch", "marketing__wau_for_da")
    assert _table_name(load_config) == "[DWH_Dev].[ch].[marketing__wau_for_da]"
    assert _table_name(load_config) == _qualified_table(load_config)


def test_generic_target_catalog_reads_the_bare_schema():
    class Connector:
        def __init__(self) -> None:
            self.params = None

        def quote_identifier(self, value: str) -> str:
            return f"[{value}]"

        def get_records(self, query: str, params=None, as_dict: bool = False):
            self.params = params
            return []

    connector = Connector()
    load_config = SimpleNamespace(
        target_database="DWH_Dev",
        target_schema="DWH_Dev.ch",
        target_table="marketing__wau_for_da",
        options={},
    )
    assert MssqlGenericTargetContract(SimpleNamespace(connector=connector))._metadata(load_config) == {}
    assert connector.params == ("ch", "marketing__wau_for_da")
