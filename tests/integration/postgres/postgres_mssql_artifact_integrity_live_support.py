"""Real-vendor support for PostgreSQL→MSSQL artifact integrity faults.

The helpers in this module keep fault injection outside production code.  A
real PostgreSQL COPY creates the source-owned file, a real SQL Server BCP
process consumes it, and the governed generic transaction path owns target
mutation and its durable receipt.  Tests may only alter the physical file at
the reviewed boundary; they do not replace connectors, target DML, catalog
checks, or transaction finalization.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
import shutil
from collections.abc import Iterator
from concurrent.futures import Future
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from threading import Event
from typing import Any

from dpone.config import LoadConfig, LoadStrategy
from dpone.runtime.artifact_integrity import ArtifactIntegrityError
from dpone.runtime.etl.processor import ETLProcessor
from dpone.runtime.file_artifacts import FileExportArtifact
from dpone.runtime.state.mssql_generic_transaction_names import RECEIPT_TABLE
from tests.integration.postgres.postgres_mssql_governance_live_support import (
    STATE_SCHEMA,
    GovernedEtlResult,
    GovernedMssqlRoute,
    GovernedPostgresSnapshotSource,
    GovernedRunContext,
    bind_factual_postgres_source_authority,
)

SOURCE_SCHEMA = "dpone_artifact"
SOURCE_TABLE = "artifact_integrity_source"
TARGET_SCHEMA = "dpone_it"
STAGING_SCHEMA = "staging"

BASE_ROWS = (
    (1, "stable-one"),
    (2, "stable-two"),
)
CHANGED_ROWS = (
    (1, "changed-one"),
    (3, "changed-three"),
)

_SAFE_CASE = re.compile(r"[^a-z0-9]+")
_AFTER_BCP_FAULTS = frozenset({"append_after_verification", "truncate_after_verification"})


class QuietArtifactLogger:
    """Runtime logger that retains the production call surface without noise."""

    def info(self, *_args: object, **_kwargs: object) -> None:
        return None

    def warning(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_etl_start(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_etl_end(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_etl_error(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_etl_progress(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_run_state_info(self, *_args: object, **_kwargs: object) -> None:
        return None

    def log_sql_query(self, *_args: object, **_kwargs: object) -> None:
        return None


@dataclass(slots=True)
class ArtifactFaultProbe:
    """Capture one real source artifact and inject one reviewed file fault."""

    fault: str | None
    owned_root: Path
    pause_after_extract: bool = False
    extraction_count: int = 0
    original_path: Path | None = None
    effective_path: Path | None = None
    _ready: Event = field(default_factory=Event, init=False, repr=False)
    _release: Event = field(default_factory=Event, init=False, repr=False)
    _injected_paths: list[Path] = field(default_factory=list, init=False, repr=False)

    def observe(self, artifact: object) -> None:
        """Capture the production artifact, then alter only its filesystem view."""

        if not isinstance(artifact, FileExportArtifact):
            raise TypeError("artifact_integrity.file_export_artifact_required")
        self.extraction_count += 1
        self.original_path = Path(artifact.file_path)
        self.effective_path = self.original_path
        if not self.original_path.is_relative_to(self.owned_root):
            raise AssertionError("PostgreSQL COPY artifact escaped the configured work directory")
        self._inject_preflight_fault(artifact)
        if self.pause_after_extract:
            self._ready.set()
            if not self._release.wait(timeout=60):
                raise TimeoutError("artifact_integrity.concurrent_extract_release_timeout")

    def wait_until_extracted(self, future: Future[Any], *, timeout: float = 60) -> None:
        """Wait for the concurrent source image or surface its earlier failure."""

        if self._ready.wait(timeout=timeout):
            return
        if future.done():
            future.result()
        raise TimeoutError("artifact_integrity.concurrent_extract_ready_timeout")

    def release(self) -> None:
        """Allow a pre-admitted concurrent extraction to reach SQL Server."""

        self._release.set()

    def mutate_after_bcp(self, file_path: str) -> None:
        """Alter the exact file after the vendor importer has returned."""

        if self.fault not in _AFTER_BCP_FAULTS:
            return
        path = Path(file_path)
        payload = path.read_bytes()
        if not payload:
            raise AssertionError("post-BCP integrity fault requires a non-empty payload")
        if self.fault == "truncate_after_verification":
            path.write_bytes(payload[:-1])
        else:
            path.write_bytes(payload + b"\n")

    def cleanup_injected_paths(self) -> None:
        """Remove test-owned aliases or escaped copies after runtime assertions."""

        self.release()
        for path in reversed(self._injected_paths):
            try:
                path.unlink()
            except FileNotFoundError:
                pass

    def _inject_preflight_fault(self, artifact: FileExportArtifact) -> None:
        path = self.original_path
        if path is None:
            raise AssertionError("artifact path was not captured")
        if self.fault == "missing_payload":
            path.unlink()
        elif self.fault == "digest_mismatch":
            payload = bytearray(path.read_bytes())
            if not payload:
                raise AssertionError("digest fault requires a non-empty payload")
            payload[0] ^= 1
            path.write_bytes(payload)
        elif self.fault == "size_mismatch":
            path.write_bytes(path.read_bytes() + b"\n")
        elif self.fault == "inode_replacement":
            details = path.stat()
            replacement = path.with_name(f"{path.name}.replacement")
            replacement.write_bytes(path.read_bytes())
            os.utime(replacement, ns=(details.st_atime_ns, details.st_mtime_ns))
            os.replace(replacement, path)
        elif self.fault == "scope_escape":
            escaped = self.owned_root.parent / f"escaped-{path.name}"
            shutil.copyfile(path, escaped)
            details = path.stat()
            os.utime(escaped, ns=(details.st_atime_ns, details.st_mtime_ns))
            self._injected_paths.append(escaped)
            artifact.file_path = str(escaped)
            self.effective_path = escaped
        elif self.fault == "symlink_alias":
            alias = path.with_name(f"{path.name}.alias")
            alias.symlink_to(path)
            self._injected_paths.append(alias)
            artifact.file_path = str(alias)
            self.effective_path = alias


class ProbedPostgresSnapshotSource(GovernedPostgresSnapshotSource):
    """Production PostgreSQL snapshot adapter with a post-COPY test observer."""

    def __init__(self, *args: Any, probe: ArtifactFaultProbe, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._artifact_probe = probe

    def extract(self, load_config: LoadConfig, last_state: Any | None) -> Any:
        result = super().extract(load_config, last_state)
        self._artifact_probe.observe(result.artifact)
        return result


class ArtifactGovernedRunner:
    """Run the standard processor with a real source and one boundary probe."""

    def __init__(
        self,
        route: GovernedMssqlRoute,
        postgres: Any,
        probe: ArtifactFaultProbe,
        *,
        logger: Any,
    ) -> None:
        self.route = route
        self.probe = probe
        self.postgres = postgres
        self.source = ProbedPostgresSnapshotSource(
            postgres,
            None,
            logger,
            sink_connector=route.target,
            probe=probe,
        )
        self.sink = route.sink(logger=logger)
        self._processor = ETLProcessor(self.source, self.sink, etl_logger=logger)

    def run(
        self,
        load_config: LoadConfig,
        *,
        label: str,
        run_context: GovernedRunContext | None = None,
    ) -> GovernedEtlResult:
        bind_factual_postgres_source_authority(
            self.source,
            self.postgres,
            load_config=load_config,
        )
        context = run_context or self.route.run_context(label)
        result = self._processor.run(
            load_config,
            run_context=context,
            dag_id=f"DAG__integration__postgres_mssql__artifact_{safe_case(label)}",
        )
        return GovernedEtlResult(result)


def ensure_source(postgres: Any) -> None:
    """Create the disposable PostgreSQL relation used by every case."""

    postgres.execute_query(f'CREATE SCHEMA IF NOT EXISTS "{SOURCE_SCHEMA}"')
    postgres.execute_query(
        f'CREATE TABLE IF NOT EXISTS "{SOURCE_SCHEMA}"."{SOURCE_TABLE}" (id integer NOT NULL, value text NULL)'
    )


def set_source_rows(postgres: Any, rows: tuple[tuple[object, object], ...]) -> None:
    """Replace the real PostgreSQL business image deterministically."""

    postgres.execute_query(f'TRUNCATE TABLE "{SOURCE_SCHEMA}"."{SOURCE_TABLE}"')
    for row in rows:
        postgres.execute_query(
            f'INSERT INTO "{SOURCE_SCHEMA}"."{SOURCE_TABLE}" (id, value) VALUES (%s, %s)',
            row,
        )


def load_config(case_id: str, *, target_database: str, owned_root: Path) -> LoadConfig:
    """Build one full-refresh file route pinned to its owned work directory."""

    owned_root.mkdir(parents=True, exist_ok=True)
    return LoadConfig(
        source_conn_id="artifact_postgres_source",
        target_conn_id="artifact_mssql_sink",
        target_database=target_database,
        staging_database=target_database,
        source_schema=SOURCE_SCHEMA,
        source_table=SOURCE_TABLE,
        target_schema=TARGET_SCHEMA,
        target_table=f"artifact_{safe_case(case_id)}"[:120],
        staging_schema=STAGING_SCHEMA,
        load_strategy=LoadStrategy.FULL_REFRESH,
        export_format="csv",
        compress_export=False,
        options={
            "source_type": "postgres",
            "sink_type": "mssql",
            "batch_commit_mode": "whole",
            "partition_tmp_dir": str(owned_root),
            "runtime_storage": {"work_dir": str(owned_root)},
            "technical_columns": "forbidden",
            "lineage": False,
            "bulk": {"mode": "bcp", "bcp": {"batch_size": 1000}},
        },
    )


def drop_target(mssql: Any, config: LoadConfig) -> None:
    """Remove only this case's disposable target and staging remnants."""

    mssql.execute_query(f"DROP TABLE IF EXISTS [{TARGET_SCHEMA}].[{config.target_table}]")
    rows = mssql.get_records(
        "SELECT QUOTENAME(s.name) + '.' + QUOTENAME(t.name) "
        "FROM sys.tables AS t INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name LIKE ?",
        (STAGING_SCHEMA, f"stg_{config.target_table}_%"),
    )
    for (name,) in rows:
        mssql.execute_query(f"DROP TABLE IF EXISTS {name}")


def target_rows(mssql: Any, config: LoadConfig) -> list[dict[str, Any]]:
    """Read the exact real-vendor business image, excluding no columns."""

    exists = mssql.get_records(
        "SELECT CASE WHEN OBJECT_ID(?, 'U') IS NULL THEN 0 ELSE 1 END",
        (f"{TARGET_SCHEMA}.{config.target_table}",),
    )
    if not exists or not int(exists[0][0]):
        return []
    return mssql.get_records(
        f"SELECT [id], [value] FROM [{TARGET_SCHEMA}].[{config.target_table}] ORDER BY [id]",
        as_dict=True,
    )


def rows_sha256(rows: list[dict[str, Any]]) -> str:
    """Hash a vendor-read target image using the recorder's stable boundary."""

    encoded = json.dumps(rows, default=str, ensure_ascii=True, separators=(",", ":"), sort_keys=True)
    return hashlib.sha256(encoded.encode()).hexdigest()


def staging_count(mssql: Any, config: LoadConfig) -> int:
    """Count all physical staging objects belonging to this target case."""

    rows = mssql.get_records(
        "SELECT COUNT_BIG(*) FROM sys.tables AS t "
        "INNER JOIN sys.schemas AS s ON s.schema_id = t.schema_id "
        "WHERE s.name = ? AND t.name LIKE ?",
        (STAGING_SCHEMA, f"stg_{config.target_table}_%"),
    )
    return int(rows[0][0]) if rows else 0


def receipt_count(route: GovernedMssqlRoute) -> int:
    """Count durable generic receipts in the disposable external catalog."""

    rows = route.state.get_records(f"SELECT COUNT_BIG(*) FROM [{STATE_SCHEMA}].[{RECEIPT_TABLE}]")
    return int(rows[0][0]) if rows else 0


def owned_files(root: Path) -> tuple[str, ...]:
    """Return remaining regular files and aliases relative to the owned root."""

    if not root.exists():
        return ()
    return tuple(str(path.relative_to(root)) for path in sorted(root.rglob("*")) if path.is_file() or path.is_symlink())


@contextmanager
def mutate_after_real_bcp(route: GovernedMssqlRoute, probe: ArtifactFaultProbe) -> Iterator[None]:
    """Inject a fault after the unmodified connector's real BCP call returns."""

    if probe.fault not in _AFTER_BCP_FAULTS:
        yield
        return
    original = route.target.bcp_import

    def wrapped(*args: Any, **kwargs: Any) -> int:
        copied = original(*args, **kwargs)
        file_path = str(args[2] if len(args) >= 3 else kwargs["file_path"])
        probe.mutate_after_bcp(file_path)
        return int(copied)

    route.target.bcp_import = wrapped
    try:
        yield
    finally:
        route.target.bcp_import = original


def safe_case(value: str) -> str:
    """Project a reviewed identifier into a SQL/Airflow-safe token."""

    token = _SAFE_CASE.sub("_", value.strip().lower()).strip("_")
    if not token:
        raise ValueError("artifact_integrity.case_id_required")
    return token


def require_artifact_released(probe: ArtifactFaultProbe) -> None:
    """Assert that runtime released the physical file it originally acquired."""

    if probe.original_path is None:
        raise AssertionError("artifact_integrity.source_artifact_not_observed")
    if probe.original_path.exists():
        raise AssertionError(f"runtime-owned artifact was not released: {probe.original_path}")


def require_integrity_code(error: BaseException, expected: str) -> None:
    """Require the stable typed integrity diagnostic for a reviewed reject."""

    if not isinstance(error, ArtifactIntegrityError):
        raise AssertionError(f"expected ArtifactIntegrityError, got {type(error).__name__}") from error
    if error.code != expected:
        raise AssertionError(f"expected {expected!r}, got {error.code!r}")


__all__ = [
    "ArtifactFaultProbe",
    "ArtifactGovernedRunner",
    "BASE_ROWS",
    "CHANGED_ROWS",
    "QuietArtifactLogger",
    "drop_target",
    "ensure_source",
    "load_config",
    "mutate_after_real_bcp",
    "owned_files",
    "receipt_count",
    "require_artifact_released",
    "require_integrity_code",
    "rows_sha256",
    "set_source_rows",
    "staging_count",
    "target_rows",
]
