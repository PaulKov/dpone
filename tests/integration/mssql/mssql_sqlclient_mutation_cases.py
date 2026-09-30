"""Real SQL mutations at the final verification boundary of synthetic routes.

The injector preserves the real verifier, journal, locks and publication path.
It first proves the prepared route is valid, mutates one owned stage, and lets
the unmodified verifier decide whether publication is still admissible.
"""

from __future__ import annotations

from typing import Any

import pytest

from dpone.runtime.sinks.mssql_native_prepare import MssqlNativeStagePreparer
from dpone.runtime.sinks.mssql_native_prepare_models import require_prepared_resources, strategy_for_prepared


def inject_stage_mutation(
    monkeypatch: pytest.MonkeyPatch, target: Any, stage: str, mutation: str, cleanup_stages: list[str]
) -> list[str]:
    """Inject exactly one mutation and return its observed boundary identity."""
    assert stage in {"raw", "prepared"}
    assert mutation in {"insert", "update", "delete", "truncate", "delete_insert"}
    original = MssqlNativeStagePreparer.reverify
    observed: list[str] = []

    def reverify(preparer: MssqlNativeStagePreparer, prepared: Any) -> None:
        original(preparer, prepared)
        assert not observed, "mutation must happen once, before publication"
        resources = require_prepared_resources(prepared)
        assert resources.mutation_watermark is not None
        prepared_name = strategy_for_prepared(preparer._sink, prepared)._staging_name(prepared.staging)
        cleanup_stages.extend([receipt.stage_id for receipt in resources.receipts] + [prepared_name])
        qualified = resources.receipts[0].stage_id if stage == "raw" else prepared_name
        _mutate(target, qualified, mutation)
        observed.append(f"{stage}:{mutation}")
        original(preparer, prepared)

    monkeypatch.setattr(MssqlNativeStagePreparer, "reverify", reverify)
    return observed


def _mutate(target: Any, qualified: str, mutation: str) -> None:
    if mutation == "update":
        target.execute_query(f"UPDATE {qualified} SET [row_key] = [row_key] + 10000 WHERE [row_key] = 1")
    elif mutation == "delete":
        target.execute_query(f"DELETE FROM {qualified} WHERE [row_key] = 1")
    elif mutation == "truncate":
        target.execute_query(f"TRUNCATE TABLE {qualified}")
    else:
        # Include native hashes and prepared lineage, while allowing SQL Server
        # to generate a fresh rowversion. Copying the sealed hash deliberately
        # proves that the repeat guard detects mutation independently of it.
        columns = target.get_records(
            "SELECT name FROM sys.columns WHERE object_id = OBJECT_ID(?) "
            "AND is_computed = 0 AND is_identity = 0 AND system_type_id <> 189 ORDER BY column_id",
            (qualified,),
        )
        assert columns
        projection = ",".join("[" + name.replace("]", "]]") + "]" for (name,) in columns)
        if mutation == "insert":
            target.execute_query(
                f"INSERT INTO {qualified} ({projection}) SELECT {projection} FROM {qualified} WHERE [row_key] = 1"
            )
        else:
            # Restore identical business values and cardinality; only the
            # database-generated mutation watermark must expose this change.
            target.execute_query(
                f"SELECT {projection} INTO #dpone_mutation_copy FROM {qualified} WHERE [row_key] = 1; "
                f"DELETE FROM {qualified} WHERE [row_key] = 1; "
                f"INSERT INTO {qualified} ({projection}) SELECT {projection} FROM #dpone_mutation_copy; "
                "DROP TABLE #dpone_mutation_copy;"
            )
