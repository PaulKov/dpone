# Postgres -> MSSQL Native Hardening Implementation Plan

> **Historical design record.** Commands below are preserved as implementation evidence and are not current runbooks. Use [Module-size debt ratchet](../../module-size-ratchet.md) for the supported exact-SHA command.

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make `postgres -> mssql` a production-grade native transfer path with clear plan UX, exact type mapping, clean staging/export taxonomy, live certification, resume safety, and industrial documentation.

**Architecture:** Keep the existing source strategy -> artifact -> sink staging -> finalizer -> state/quality flow. Add only reusable, top-level abstractions where they apply to more than one path; otherwise prefer focused services behind existing Postgres/MSSQL adapters. Preserve public CLI, Python API, manifests, extras, and compatibility facades.

**Tech Stack:** Python 3.11+, psycopg PostgreSQL COPY, Microsoft `bcp`, pyodbc, dpone artifacts/staging/finalizer contracts, Docker local Postgres + SQL Server integration stack, MkDocs docs.

---

## File map and responsibilities

- Modify: `src/dpone/strategy_intelligence/native_paths.py`
  - Keep native fast path catalog credential-free and honest.
  - Remove stale unsafe `NULL '__DPONE_NULL__'` command example.
  - Render the documented `BulkTextCodec + NULL '' + bcp` flow.

- Modify: `tests/test_strategy_intelligence.py`
  - Assert Postgres -> MSSQL native path command examples do not advertise deprecated/null-marker behavior.

- Modify: `tests/test_cli_strategy_intelligence_commands.py`
  - Assert `dpone strategy preflight` / plan output exposes the updated native command text and path summary.

- Create: `src/dpone/runtime/sinks/staging_managers/mssql.py`
  - Move `MSSQLStagingManager` out of `src/dpone/runtime/sinks/mssql.py`.
  - Keep the manager focused on create/load/drop/count for SQL Server staging.

- Modify: `src/dpone/runtime/sinks/mssql.py`
  - Import `MSSQLStagingManager` from the new focused module.
  - Keep `MSSQLSink` as the public sink facade/orchestrator.

- Modify: `tests/test_runtime_mssql_contracts.py`
  - Preserve MSSQL staging behavior after extraction.
  - Cover safe bcp metadata propagation and raw unsafe file rejection.

- Create: `src/dpone/runtime/connectors/partition_clone.py`
  - Define a small `PartitionCloneFactory` protocol/service for partition worker connectors.
  - Avoid source strategy checking `connector.__class__.__name__`.

- Modify: `src/dpone/runtime/connectors/postgres.py`
  - Add `clone_for_partition(partition_index: int)` returning an isolated `PostgresConnector`.

- Modify: `src/dpone/runtime/sources/strategies/postgres/postgres_partition_export_mixin.py`
  - Use `PartitionCloneFactory`/`clone_for_partition` instead of concrete class-name checks.

- Modify: `tests/test_runtime_postgres_strategy_split.py`
  - Assert partition export uses clone hook when available and preserves partition metadata.

- Create: `src/dpone/type_system/source_sink/postgres_mssql.py`
  - Define pair-specific type compatibility and rendering metadata for Postgres -> MSSQL.
  - Keep this as a focused mapping profile, not a broad god mapper.

- Modify: `src/dpone/readiness/schema_evolution.py` or the active compatibility service module if already split
  - Route Postgres -> MSSQL comparisons through canonical compatibility instead of raw dtype strings.

- Modify: `tests/test_type_mapping_matrix_contracts.py` or create `tests/test_postgres_mssql_type_mapping.py`
  - Cover `integer`, `bigint`, `numeric(p,s)`, `uuid`, `json/jsonb`, `bytea`, `timestamp`, `timestamptz`, `date`, `time`, `boolean`, arrays/ranges fallback behavior.

- Create: `tests/integration/mssql/test_postgres_to_mssql_native_transfer_integration.py`
  - Local Docker Postgres + SQL Server integration suite for native transfer.
  - Marker: `integration_postgres` and `integration_mssql`.

- Modify: `docs/source-sink/postgres-to-mssql.md`
  - Add exact native algorithm, type matrix link, bcp/staging tuning, fault/resume runbook, and examples for every supported strategy.

- Modify: `docs/type-mapping-matrix.md`
  - Add source-specific `Postgres -> MSSQL` table.

- Modify: `docs/mssql.md`
  - Add operational guidance for bcp load/finalize bottlenecks and target tuning.

- Modify: `docs/performance.md`
  - Add Postgres -> MSSQL tuning interpretation and benchmark SLO guidance.

- Modify: `docs/testing/native-transfer-benchmarks-2026-06-09.md`
  - Cross-link new live integration test and explain what remains benchmark vs integration evidence.

---

## Task 1: Fix stale Postgres -> MSSQL native path catalog UX

**Files:**
- Modify: `src/dpone/strategy_intelligence/native_paths.py`
- Modify: `tests/test_strategy_intelligence.py`
- Modify: `tests/test_cli_strategy_intelligence_commands.py`
- Modify: `docs/strategy-intelligence.md`

- [ ] **Step 1: Add catalog regression test**

Add assertions to `tests/test_strategy_intelligence.py` near the existing Postgres -> MSSQL native path test:

```python
def test_postgres_mssql_native_path_documents_lossless_bulk_text_codec() -> None:
    path = NativeFastPathCatalog().resolve("postgres", "mssql")

    command_text = "\n".join(path.commands)
    assert path.path_id == "postgres_copy_to_mssql_bcp"
    assert "BulkTextCodec" in path.summary
    assert "NULL '__DPONE_NULL__'" not in command_text
    assert "NULL ''" in command_text
    assert "QUOTE E'\\x1f'" in command_text
    assert "bcp target_staging in data.bcp" in command_text
```

- [ ] **Step 2: Update catalog command text**

Change the Postgres -> MSSQL catalog entry in `src/dpone/strategy_intelligence/native_paths.py` to:

```python
("postgres", "mssql"): NativeFastPath(
    path_id="postgres_copy_to_mssql_bcp",
    source_type="postgres",
    sink_type="mssql",
    summary=(
        "Export with PostgreSQL COPY using dpone BulkTextCodec projection, "
        "write mssql-delimited artifacts, import with MSSQL bcp into staging."
    ),
    commands=(
        "COPY (SELECT dpone BulkTextCodec-projected columns FROM source WHERE ...) "
        "TO STDOUT WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE E'\\x1f', ESCAPE E'\\x1f', NULL '')",
        "bcp target_staging in data.bcp -c -C 65001 -t\\t -r\\n -b 100000 -S <server> -d <db> -k",
    ),
    required_tools=("psycopg", "bcp"),
    fallback_path="pyodbc_fast_executemany",
    expected_impact="highest for 1M+ rows; avoids row-by-row inserts",
),
```

- [ ] **Step 3: Update strategy intelligence docs**

In `docs/strategy-intelligence.md`, add a short note under the native path matrix:

```markdown
For `postgres_copy_to_mssql_bcp`, the displayed `COPY` command is a schematic command. Runtime exports first wrap text-like source columns with `BulkTextCodec` projections, then use `NULL ''` and a non-printable quote/escape character so SQL Server `bcp` can load a fast character file without collapsing `NULL` and empty strings.
```

- [ ] **Step 4: Targeted verification**

Run:

```bash
uv run pytest tests/test_strategy_intelligence.py tests/test_cli_strategy_intelligence_commands.py -q
```

Expected: all selected tests pass.

---

## Task 2: Extract MSSQL staging manager into focused module

**Files:**
- Create: `src/dpone/runtime/sinks/staging_managers/mssql.py`
- Modify: `src/dpone/runtime/sinks/mssql.py`
- Modify: `tests/test_runtime_mssql_contracts.py`

- [ ] **Step 1: Add compatibility/import test**

Add to `tests/test_runtime_mssql_contracts.py`:

```python
def test_mssql_staging_manager_imports_from_focused_module() -> None:
    from dpone.runtime.sinks.mssql import MSSQLSink
    from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager

    assert MSSQLSink is not None
    assert MSSQLStagingManager.__name__ == "MSSQLStagingManager"
```

- [ ] **Step 2: Create focused staging manager module**

Move the full `MSSQLStagingManager` class from `src/dpone/runtime/sinks/mssql.py` into `src/dpone/runtime/sinks/staging_managers/mssql.py` with imports local to staging responsibilities only:

```python
from __future__ import annotations

import os
import uuid
from collections.abc import Iterable, Mapping, Sequence
from typing import Any

from dpone.config import LoadConfig
from dpone.runtime.artifacts import FileExportArtifact, PartitionedFileExportArtifact, StagingManager, StagingTableArtifact
from dpone.runtime.bulk_options import BulkOptionsResolver
from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.connectors.mssql import MSSQLConnector
from dpone.runtime.connectors.mssql_bulk import DelimitedBulkFile, is_mssql_character_bulk_unsafe_type
from dpone.runtime.etl_logging import ETLLogger, etl_logger
from dpone.runtime.support.mssql_types import MSSQLTypeMapper


class MSSQLStagingManager(StagingManager):
    """Creates and fills SQL Server staging tables through safe bcp paths."""

    # Move the existing implementation here unchanged first.
```

- [ ] **Step 3: Make `mssql.py` delegate**

In `src/dpone/runtime/sinks/mssql.py`, remove the class body and import:

```python
from dpone.runtime.sinks.staging_managers.mssql import MSSQLStagingManager
```

Keep `MSSQLSink` public behavior unchanged.

- [ ] **Step 4: Targeted verification**

Run:

```bash
uv run pytest tests/test_runtime_mssql_contracts.py -q
```

Expected: tests pass; no behavior change.

---

## Task 3: Replace Postgres partition connector class-name check with DI clone service

**Files:**
- Create: `src/dpone/runtime/connectors/partition_clone.py`
- Modify: `src/dpone/runtime/connectors/postgres.py`
- Modify: `src/dpone/runtime/sources/strategies/postgres/postgres_partition_export_mixin.py`
- Modify: `tests/test_runtime_postgres_strategy_split.py`

- [ ] **Step 1: Add clone behavior test**

Add to `tests/test_runtime_postgres_strategy_split.py`:

```python
def test_postgres_partition_export_uses_connector_clone_hook(tmp_path: Path) -> None:
    class CloneableCopyConnector(CopyConnector):
        def __init__(self):
            super().__init__()
            self.clone_calls: list[int] = []

        def clone_for_partition(self, partition_index: int):
            self.clone_calls.append(partition_index)
            return self

    connector = CloneableCopyConnector()
    strategy = DummyPostgresStrategy(connector=connector, logger=StubLogger())
    load_config = _partitioned_mssql_export_config(tmp_path, load_workers=2)

    strategy._export_to_file(
        "SELECT id, name FROM public.orders",
        [("id", "integer"), ("name", "text")],
        load_config,
        batch_size=1000,
    )

    assert connector.clone_calls == [0, 1, 2]
```

If `_partitioned_mssql_export_config` does not exist, extract it from the existing test setup in the same file.

- [ ] **Step 2: Create clone protocol/service**

Create `src/dpone/runtime/connectors/partition_clone.py`:

```python
from __future__ import annotations

from typing import Protocol, TypeVar

TConnector = TypeVar("TConnector")


class SupportsPartitionClone(Protocol[TConnector]):
    def clone_for_partition(self, partition_index: int) -> TConnector:
        """Return an isolated connector safe for one partition worker."""


class PartitionCloneFactory:
    """Resolve partition-worker connectors without source strategies knowing concrete connector classes."""

    def clone(self, connector: TConnector, partition_index: int) -> TConnector:
        clone_method = getattr(connector, "clone_for_partition", None)
        if callable(clone_method):
            return clone_method(partition_index)
        return connector
```

- [ ] **Step 3: Add Postgres connector clone hook**

Add to `PostgresConnector`:

```python
def clone_for_partition(self, partition_index: int) -> "PostgresConnector":
    return PostgresConnector(
        host=self.host,
        port=self.port,
        database=self.database,
        user=self.user,
        password=self.password,
        application_name=f"{self.application_name}-partition-{partition_index}",
        autocommit=self.autocommit,
    )
```

- [ ] **Step 4: Update partition export mixin**

Replace `_clone_partition_connector` implementation with:

```python
from dpone.runtime.connectors.partition_clone import PartitionCloneFactory

...

connector = PartitionCloneFactory().clone(self.connector, partition.index)
```

Remove direct import of `PostgresConnector` from the mixin.

- [ ] **Step 5: Targeted verification**

Run:

```bash
uv run pytest tests/test_runtime_postgres_strategy_split.py -q
```

Expected: partition export tests pass and no concrete connector class-name check remains.

---

## Task 4: Add Postgres -> MSSQL pair-specific type mapping contract

**Files:**
- Create: `src/dpone/type_system/source_sink/postgres_mssql.py`
- Modify: `docs/type-mapping-matrix.md`
- Create: `tests/test_postgres_mssql_type_mapping.py`

- [ ] **Step 1: Add mapping tests**

Create `tests/test_postgres_mssql_type_mapping.py`:

```python
from dpone.type_system.source_sink.postgres_mssql import PostgresMssqlTypeMapper


def test_postgres_mssql_exact_scalar_mappings() -> None:
    mapper = PostgresMssqlTypeMapper()

    assert mapper.resolve("integer").target_type == "int"
    assert mapper.resolve("bigint").target_type == "bigint"
    assert mapper.resolve("numeric(18,4)").target_type == "decimal(18,4)"
    assert mapper.resolve("uuid").target_type == "uniqueidentifier"
    assert mapper.resolve("bytea").target_type == "varbinary(max)"
    assert mapper.resolve("boolean").target_type == "bit"


def test_postgres_mssql_temporal_mappings() -> None:
    mapper = PostgresMssqlTypeMapper()

    assert mapper.resolve("timestamp without time zone").target_type == "datetime2(6)"
    assert mapper.resolve("timestamp with time zone").target_type == "datetimeoffset(6)"
    assert mapper.resolve("date").target_type == "date"
    assert mapper.resolve("time without time zone").target_type == "time(6)"


def test_postgres_mssql_complex_types_land_as_json_or_string() -> None:
    mapper = PostgresMssqlTypeMapper()

    assert mapper.resolve("jsonb").target_type == "nvarchar(max)"
    assert mapper.resolve("text[]").target_type == "nvarchar(max)"
    assert mapper.resolve("int4range").target_type == "nvarchar(max)"
    assert mapper.resolve("my_enum").requires_explicit_contract is True
```

- [ ] **Step 2: Implement focused mapper**

Create a small dataclass-based mapper:

```python
from __future__ import annotations

import re
from dataclasses import dataclass


@dataclass(frozen=True)
class PostgresMssqlTypeDecision:
    source_type: str
    target_type: str
    transfer_representation: str
    compatible: bool = True
    requires_explicit_contract: bool = False
    warning: str | None = None


class PostgresMssqlTypeMapper:
    """Pair-specific Postgres -> MSSQL type decisions for planning and docs/tests."""

    def resolve(self, source_type: str) -> PostgresMssqlTypeDecision:
        normalized = _normalize(source_type)
        if match := re.fullmatch(r"numeric\((\d+),(\d+)\)", normalized):
            return PostgresMssqlTypeDecision(source_type, f"decimal({match.group(1)},{match.group(2)})", "decimal text")
        if normalized in _EXACT:
            target, representation = _EXACT[normalized]
            return PostgresMssqlTypeDecision(source_type, target, representation)
        if normalized.endswith("[]") or "range" in normalized or normalized in {"json", "jsonb"}:
            return PostgresMssqlTypeDecision(source_type, "nvarchar(max)", "json/text", warning="Complex type lands as JSON/text unless schema_contract overrides it.")
        return PostgresMssqlTypeDecision(
            source_type,
            "nvarchar(max)",
            "text",
            requires_explicit_contract=True,
            warning="Unknown or custom PostgreSQL type requires explicit schema_contract for production loads.",
        )


_EXACT = {
    "smallint": ("smallint", "integer text"),
    "integer": ("int", "integer text"),
    "bigint": ("bigint", "integer text"),
    "real": ("real", "float text"),
    "double precision": ("float", "float text"),
    "boolean": ("bit", "0/1 text"),
    "text": ("nvarchar(max)", "BulkTextCodec text"),
    "character varying": ("nvarchar(max)", "BulkTextCodec text"),
    "uuid": ("uniqueidentifier", "uuid text"),
    "bytea": ("varbinary(max)", "hex/binary-safe text"),
    "date": ("date", "ISO date text"),
    "timestamp without time zone": ("datetime2(6)", "timestamp text"),
    "timestamp with time zone": ("datetimeoffset(6)", "offset timestamp text"),
    "time without time zone": ("time(6)", "time text"),
    "time with time zone": ("nvarchar(64)", "text"),
    "json": ("nvarchar(max)", "json text"),
    "jsonb": ("nvarchar(max)", "json text"),
}


def _normalize(value: str) -> str:
    return " ".join(str(value).strip().lower().split())
```

- [ ] **Step 3: Document exact matrix**

Add to `docs/type-mapping-matrix.md` a `Postgres -> MSSQL` section with columns:

```markdown
| PostgreSQL source type | MSSQL target type | Bulk representation | Notes |
| --- | --- | --- | --- |
| `integer` | `int` | integer text | Compatible with nullable target `int`. |
| `bigint` | `bigint` | integer text | Preserves signed 64-bit range. |
| `numeric(p,s)` | `decimal(p,s)` | decimal text | Fails if precision/scale cannot be represented. |
| `uuid` | `uniqueidentifier` | UUID text | Uses SQL Server native GUID type. |
| `json` / `jsonb` | `nvarchar(max)` | JSON text via `BulkTextCodec` | Use schema contracts for stricter shape. |
| `bytea` | `varbinary(max)` | binary-safe representation | Must be validated by typed reconciliation. |
| `timestamp without time zone` | `datetime2(6)` | timestamp text | Timezone-naive; no implicit conversion. |
| `timestamp with time zone` | `datetimeoffset(6)` | offset timestamp text | Preserves instant and offset semantics. |
| arrays/ranges/custom | `nvarchar(max)` by default | JSON/text | Production loads should declare `schema_contract`. |
```

- [ ] **Step 4: Targeted verification**

Run:

```bash
uv run pytest tests/test_postgres_mssql_type_mapping.py -q
```

Expected: all mapping tests pass.

---

## Task 5: Add local live Postgres -> MSSQL native integration suite

**Files:**
- Create: `tests/integration/mssql/test_postgres_to_mssql_native_transfer_integration.py`
- Modify: `docs/testing/index.md`
- Modify: `docs/testing/manual-integration-matrix.md`

- [ ] **Step 1: Add integration test skeleton with skips**

Create the test file:

```python
from __future__ import annotations

import os
from pathlib import Path

import pytest

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.sinks.base import LoadPayload
from dpone.runtime.sinks.mssql import MSSQLSink
from dpone.runtime.sources.strategies.postgres.postgres_full_extract import PostgresFullExtractStrategy

pytestmark = [pytest.mark.integration_postgres, pytest.mark.integration_mssql, pytest.mark.integration_live]


def _enabled() -> bool:
    return os.environ.get("DPONE_RUN_INTEGRATION") == "1"


@pytest.mark.skipif(not _enabled(), reason="Set DPONE_RUN_INTEGRATION=1 to run local Postgres -> MSSQL integration")
def test_postgres_to_mssql_native_full_refresh_lossless_codec(local_postgres_connector, local_mssql_connector, tmp_path: Path):
    # Arrange: create source rows with NULL, empty string, tabs, newlines, unicode, numeric, timestamp.
    # Act: extract with export_format=mssql-delimited and load with MSSQL bcp.
    # Assert: target rows match source semantics exactly.
```

Use existing integration fixtures if present; otherwise add small local helpers in the same file following existing integration style.

- [ ] **Step 2: Cover full refresh**

Implement full-refresh scenario with a source table containing:

```sql
id integer primary key,
name text,
description text,
amount numeric(18,4),
created_at timestamp,
external_id uuid,
payload jsonb,
empty_value text,
nullable_value integer
```

Rows must include:

```text
NULL integer, empty string, tab, newline, unicode, decimal scale, timestamp, json payload.
```

Expected assertions:

```python
assert count == 5
assert empty string remains empty string
assert NULL remains NULL
assert tab/newline text round-trips
assert decimal value round-trips as decimal text-compatible value
```

- [ ] **Step 3: Cover incremental_merge delete_insert**

Add test:

```python
@pytest.mark.skipif(not _enabled(), reason="Set DPONE_RUN_INTEGRATION=1")
def test_postgres_to_mssql_incremental_merge_delete_insert(local_postgres_connector, local_mssql_connector, tmp_path: Path):
    # Seed target with old rows.
    # Source exports changed rows.
    # Load strategy incremental_merge with unique_key id.
    # Assert old matching target rows were replaced and non-matching rows remain.
```

- [ ] **Step 4: Cover partitioned export**

Add test using `partitioning.column=id`, `bounds`, `num_partitions=2`, `export_workers=2`, `load_workers=2`.

Expected:

```python
assert isinstance(extract.artifact, PartitionedFileExportArtifact)
assert len(extract.artifact.partitions) == 2
assert final target count == source count
```

- [ ] **Step 5: Targeted local command**

Run only when local services are available:

```bash
DPONE_RUN_INTEGRATION=1 uv run pytest tests/integration/mssql/test_postgres_to_mssql_native_transfer_integration.py -q
```

Expected: full refresh, incremental merge, and partitioned export pass.

---

## Task 6: Add fault/resume evidence for Postgres -> MSSQL

**Files:**
- Modify: `tools/mssql_clickhouse_fault_injection.py` or create `tools/postgres_mssql_fault_injection.py`
- Create: `tests/test_postgres_mssql_fault_injection.py`
- Modify: `docs/source-sink/postgres-to-mssql.md`

- [ ] **Step 1: Add unit-level fault model tests**

Create `tests/test_postgres_mssql_fault_injection.py`:

```python
from dpone.runtime.lineage.partition_checkpoint import PartitionCheckpoint, PartitionCheckpointStatus
from dpone.runtime.lineage.partition_resume import PartitionResumePlanner


def test_postgres_mssql_resume_does_not_skip_exported_only_partition() -> None:
    checkpoint = PartitionCheckpoint(
        transfer_partition_id="a" * 64,
        status=PartitionCheckpointStatus.EXPORTED,
        query_hash="sha256:q",
        schema_hash="sha256:s",
        artifact_checksum="sha256:file",
        source_table="public.orders",
        target_table="dbo.orders",
        partition_bounds={"lower": 1, "upper": 10},
    )

    assert checkpoint.can_skip(query_hash="sha256:q", schema_hash="sha256:s") is False
```

- [ ] **Step 2: Add tool scenario names**

Add Postgres -> MSSQL scenarios:

```text
postgres_mssql_after_export
postgres_mssql_during_bcp_load
postgres_mssql_before_finalizer
```

Each scenario writes:

```json
{
  "scenario": "postgres_mssql_during_bcp_load",
  "source": "postgres",
  "sink": "mssql",
  "state_advanced_before_success": false,
  "duplicates_after_retry": false,
  "final_count_matches_source": true
}
```

- [ ] **Step 3: Document runbook**

Add to `docs/source-sink/postgres-to-mssql.md`:

```markdown
## Failure/resume certification

Run `tools/postgres_mssql_fault_injection.py` before minor/major releases when this path changes. The three mandatory scenarios are `after_export`, `during_bcp_load`, and `before_finalizer`. A partition may be skipped on retry only after the checkpoint is committed and query/schema/artifact hashes match.
```

- [ ] **Step 4: Verification**

Run:

```bash
uv run pytest tests/test_postgres_mssql_fault_injection.py -q
```

Expected: resume semantics are enforced.

---

## Task 7: Improve Postgres -> MSSQL performance advisor and docs

**Files:**
- Modify: `src/dpone/readiness/managed_performance.py`
- Modify: `docs/source-sink/postgres-to-mssql.md`
- Modify: `docs/mssql.md`
- Modify: `docs/performance.md`
- Modify: `tests/test_managed_ux_contracts.py`

- [ ] **Step 1: Add advisor test**

Add to `tests/test_managed_ux_contracts.py`:

```python
def test_performance_advisor_recommends_mssql_target_tuning_for_postgres_mssql() -> None:
    advisor = PerformanceAdvisor()
    recommendations = advisor.advise(
        {
            "source": {"type": "postgres", "options": {"export_format": "mssql-delimited"}},
            "sink": {"type": "mssql", "options": {"bulk": {"mode": "bcp"}}},
        }
    )

    codes = {item.code for item in recommendations}
    assert "postgres_to_mssql_bcp" in codes
    assert "mssql_bulk_target_finalize_tuning" in codes
```

- [ ] **Step 2: Implement advisor recommendation**

Add recommendation text:

```python
Recommendation(
    code="mssql_bulk_target_finalize_tuning",
    severity="info",
    message=(
        "For Postgres -> MSSQL, benchmarks usually bottleneck on SQL Server bcp load/finalize, "
        "not PostgreSQL COPY. Review bulk.bcp.batch_size, table_lock, recovery/log throughput, "
        "staging indexes, finalizer policy, and post-load statistics."
    ),
    expected_impact="medium-to-high on 1M+ row loads",
)
```

- [ ] **Step 3: Expand docs**

Add a tuning table with:

```markdown
| Symptom | Likely bottleneck | Action |
| --- | --- | --- |
| `postgres_to_mssql.source_export` slow | Postgres scan/COPY | Increase `export_workers`, add partition column, check source indexes. |
| `postgres_to_mssql.target_load_finalize` slow | SQL Server bcp/finalizer/log | Tune `bulk.bcp.batch_size`, `table_lock`, log throughput, staging indexes, finalizer policy. |
| Lock waits during finalizer | Target mutation | Try `shadow_swap`, smaller partitions, or safe-window execution. |
| bcp rejects rows | File/type mismatch | Inspect bcp error file and type mapping matrix. |
```

- [ ] **Step 4: Targeted verification**

Run:

```bash
uv run pytest tests/test_managed_ux_contracts.py -q
```

Expected: advisor includes the new recommendation.

---

## Task 8: Documentation and release gate update

**Files:**
- Modify: `docs/source-sink/postgres-to-mssql.md`
- Modify: `docs/type-mapping-matrix.md`
- Modify: `docs/testing/index.md`
- Modify: `docs/testing/manual-integration-matrix.md`
- Modify: `docs/cicd/runbooks.md`

- [ ] **Step 1: Add copy/paste examples**

Ensure `docs/source-sink/postgres-to-mssql.md` has examples for:

```yaml
strategy:
  mode: full_refresh
```

```yaml
strategy:
  mode: incremental_merge
  unique_key: [id]
  merge_policy: delete_insert
```

```yaml
strategy:
  mode: partition_replace
  partition:
    column: business_date
    values_from_staging: true
```

```yaml
strategy:
  mode: xmin
```

```yaml
strategy:
  mode: cdc_apply
```

- [ ] **Step 2: Add troubleshooting runbooks**

Add sections:

```markdown
### bcp error file has rejected rows
### NULL and empty string look identical
### Target load/finalize is the bottleneck
### Schema evolution reports false type changes
### Partitioned export skips or overlaps ranges
### SQL Server locks during finalizer
```

- [ ] **Step 3: Run docs checks**

Run when implementation is complete:

```bash
uv run dpone docs check-docs
uv run mkdocs build --strict
```

Expected: docs checks and strict MkDocs pass.

---

## Final full gate for this block

Run after all tasks are complete:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live"
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json
uv run dpone docs check-architecture-fitness --max-avg-clustering 0.18
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-docs
uv run mkdocs build --strict
rm -rf dist && uv build && uv run twine check dist/*
```

Run live gate when Docker services are available:

```bash
docker compose -f docker/docker-compose.integration.yml up -d postgres mssql
DPONE_RUN_INTEGRATION=1 uv run pytest tests/integration/mssql/test_postgres_to_mssql_native_transfer_integration.py -q
```

Run benchmark evidence when this path changes materially:

```bash
PYTHONUNBUFFERED=1 uv run python tools/mssql_benchmark_suite.py \
  --rows 10000,1000000,10000000 \
  --partitions 4 \
  --export-workers 2 \
  --load-workers 2 \
  --batch-size 100000 \
  --bcp-path /opt/homebrew/bin/bcp \
  --optimizer-profile high_throughput_safe \
  --output-dir test_artifacts/live_certification/benchmarks/postgres_mssql_release_suite_latest \
  --markdown-output test_artifacts/live_certification/benchmarks/postgres_mssql_release_suite_latest/postgres_mssql_native_benchmark_summary.md
```

---

## Acceptance criteria

- `dpone plan` / strategy intelligence no longer advertises unsafe or stale `__DPONE_NULL__` Postgres -> MSSQL examples.
- MSSQL staging logic lives in a focused staging manager module; `MSSQLSink` remains the public facade.
- Postgres partition export uses a reusable connector clone boundary, not class-name checks.
- Postgres -> MSSQL type mapping has pair-specific exact docs and unit tests.
- Local live Postgres -> MSSQL integration validates codec, bcp load, partitioning, and incremental merge.
- Fault/resume semantics are documented and tested for export/load/finalizer boundaries.
- Performance advisor explains why SQL Server target load/finalize is often the bottleneck and what to tune.
- Docs contain copy/paste manifests, troubleshooting runbooks, and cross-links.
- No new god modules, no side architecture, no public API removals.
