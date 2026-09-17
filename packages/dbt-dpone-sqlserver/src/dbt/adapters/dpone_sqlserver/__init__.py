"""Normal dbt AdapterPlugin discovery; no registry mutation or import-time I/O."""

from typing import Any, cast

from dbt.adapters.base import AdapterPlugin
from dbt.adapters.dpone_sqlserver.credentials import DpOneSQLServerCredentials
from dbt.adapters.dpone_sqlserver.impl import DpOneSQLServerAdapter
from dbt.adapters.protocol import AdapterProtocol
from dbt.include import dpone_sqlserver

Plugin = AdapterPlugin(
    # dbt-sqlserver's inherited annotations do not satisfy the generic upstream
    # structural protocol; the normal installed loader is exercised by tests.
    adapter=cast(type[AdapterProtocol[Any, Any, Any, Any]], DpOneSQLServerAdapter),
    credentials=DpOneSQLServerCredentials,
    include_path=dpone_sqlserver.PACKAGE_PATH,
    dependencies=["sqlserver"],
)
