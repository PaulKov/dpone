"""File-export enrichment for strategy metadata (CSV / TSV wire formats).

Production SCD2 / snapshot_diff require ``__dpone__row_hash`` (and SCD2 validity
columns) in staging. Row-oriented artifacts are enriched in-memory; file exports
used by MySQL/Postgres/MSSQL/ClickHouse bulk paths must rewrite the wire file
before ``sink.load`` materializes staging — otherwise every source→sink CSV/TSV
route silently drops strategy columns.

Wire fidelity rules:
- ``csv``: rewrite with the same ``csv`` dialect as MySQL/Postgres CSV export.
- ``mssql-delimited`` / ``clickhouse-tsv`` / plain TSV: preserve existing field
  text byte-for-byte and only append newly added strategy columns using the
  matching native scalar formatters (never re-encode business columns through
  ``csv.writer``, which would quote/escape differently from BCP/CH producers).
"""

from __future__ import annotations

import csv
import os
import tempfile
from collections.abc import Iterator, Sequence
from importlib import import_module
from pathlib import Path
from typing import TYPE_CHECKING

from dpone.runtime.connectors.bulk_text_codec import BulkTextCodec
from dpone.runtime.file_artifacts import (
    BatchedFileExportArtifact,
    FileExportArtifact,
    PartitionedFileExportArtifact,
)
from dpone.runtime.lineage.strategy_metadata_rows import StrategyMetadataRowEnricher
from dpone.runtime.lineage.strategy_metadata_wire import (
    assert_columns_append_only,
    decode_tab_cell,
    encode_csv_cell,
    encode_strategy_tab_cell,
    is_header,
    open_text,
    open_text_path,
    sha256_file,
)
from dpone.runtime.physical_chunking import PhysicalChunkedFileExportArtifact, PhysicalTransferChunk

if TYPE_CHECKING:
    from dpone.config.load_strategy import LoadStrategy

_CSV_FORMATS = frozenset({"csv"})
_TAB_FORMATS = frozenset(
    {
        "tsv",
        "tab",
        "tabseparated",
        "tab_separated",
        "mssql-delimited",
        "clickhouse-tsv",
    }
)
_SUPPORTED_FORMATS = _CSV_FORMATS | _TAB_FORMATS


class StrategyMetadataFileEnricher:
    """Rewrite local delimited file artifacts with strategy metadata columns."""

    def __init__(self, row_enricher: StrategyMetadataRowEnricher) -> None:
        self._rows = row_enricher

    def enrich(
        self,
        artifact: object,
        *,
        strategy: LoadStrategy,
        effective_iso: str,
        output_columns: Sequence[str],
        output_schema: tuple[tuple[str, str], ...],
        schema_contract: object | None,
    ) -> object:
        if isinstance(artifact, FileExportArtifact):
            return self._enrich_file(
                artifact,
                strategy=strategy,
                effective_iso=effective_iso,
                output_columns=output_columns,
                output_schema=output_schema,
                schema_contract=schema_contract,
            )
        if isinstance(artifact, PartitionedFileExportArtifact):
            partitions = [
                self._enrich_file(
                    partition,
                    strategy=strategy,
                    effective_iso=effective_iso,
                    output_columns=output_columns,
                    output_schema=output_schema,
                    schema_contract=schema_contract,
                )
                for partition in artifact.partitions
            ]
            return artifact.rebind_partitions(partitions, output_columns)
        if isinstance(artifact, BatchedFileExportArtifact):
            return self._enrich_batched(
                artifact,
                strategy=strategy,
                effective_iso=effective_iso,
                output_columns=output_columns,
                output_schema=output_schema,
                schema_contract=schema_contract,
            )
        if isinstance(artifact, PhysicalChunkedFileExportArtifact):
            return self._enrich_physical_chunked(
                artifact,
                strategy=strategy,
                effective_iso=effective_iso,
                output_columns=output_columns,
                output_schema=output_schema,
                schema_contract=schema_contract,
            )
        raise TypeError(
            "Strategy metadata enrichment for snapshot_diff/scd2 requires a row or "
            f"delimited file artifact; got {type(artifact).__name__}. "
            "Supported: InMemoryRowsArtifact, StreamingRowsArtifact, FileExportArtifact, "
            "PartitionedFileExportArtifact, BatchedFileExportArtifact, "
            "PhysicalChunkedFileExportArtifact (csv / mssql-delimited / clickhouse-tsv)."
        )

    def _enrich_batched(
        self,
        artifact: BatchedFileExportArtifact,
        *,
        strategy: LoadStrategy,
        effective_iso: str,
        output_columns: Sequence[str],
        output_schema: tuple[tuple[str, str], ...],
        schema_contract: object | None,
    ) -> BatchedFileExportArtifact:
        original = artifact.batch_generator

        def wrapped_generator() -> Iterator[FileExportArtifact]:
            for batch in original():
                yield self._enrich_file(
                    batch,
                    strategy=strategy,
                    effective_iso=effective_iso,
                    output_columns=output_columns,
                    output_schema=output_schema,
                    schema_contract=schema_contract,
                )

        return artifact.rebind_generator(wrapped_generator, columns=output_columns)

    def _enrich_physical_chunked(
        self,
        artifact: PhysicalChunkedFileExportArtifact,
        *,
        strategy: LoadStrategy,
        effective_iso: str,
        output_columns: Sequence[str],
        output_schema: tuple[tuple[str, str], ...],
        schema_contract: object | None,
    ) -> PhysicalChunkedFileExportArtifact:
        original = artifact.chunk_generator
        input_columns = list(artifact.columns)

        def wrapped_generator() -> Iterator[PhysicalTransferChunk]:
            for chunk in original():
                file_artifact = FileExportArtifact(
                    file_path=chunk.file_path,
                    columns=input_columns,
                    format=chunk.format or artifact.format,
                    estimated_rows=chunk.row_count,
                    rows_exported=chunk.row_count,
                    bulk_text_codec=artifact.bulk_text_codec,
                )
                enriched = self._enrich_file(
                    file_artifact,
                    strategy=strategy,
                    effective_iso=effective_iso,
                    output_columns=output_columns,
                    output_schema=output_schema,
                    schema_contract=schema_contract,
                    replace_path=chunk.file_path,
                )
                path = Path(enriched.file_path)
                yield PhysicalTransferChunk(
                    file_path=enriched.file_path,
                    chunk_index=chunk.chunk_index,
                    byte_count=path.stat().st_size,
                    row_count=chunk.row_count,
                    checksum=sha256_file(path),
                    format=chunk.format,
                )

        return artifact.rebind_generator(wrapped_generator, columns=output_columns)

    def _enrich_file(
        self,
        artifact: FileExportArtifact,
        *,
        strategy: LoadStrategy,
        effective_iso: str,
        output_columns: Sequence[str],
        output_schema: tuple[tuple[str, str], ...],
        schema_contract: object | None,
        replace_path: str | None = None,
    ) -> FileExportArtifact:
        file_format = str(artifact.format or "csv").strip().lower()
        if file_format not in _SUPPORTED_FORMATS:
            raise ValueError(
                "Strategy metadata enrichment supports delimited file formats "
                f"{sorted(_SUPPORTED_FORMATS)}; got format={artifact.format!r} "
                f"path={artifact.file_path!r}"
            )
        input_columns = [str(column) for column in artifact.columns]
        rows_exported = artifact.rows_exported
        if not input_columns:
            raise ValueError("file export artifact must expose columns for strategy metadata enrichment")
        assert_columns_append_only(input_columns, output_columns)

        source_path = Path(artifact.file_path)
        target_path = Path(replace_path) if replace_path else source_path
        tmp_fd, tmp_name = tempfile.mkstemp(
            prefix="dpone-strategy-meta-",
            suffix=source_path.suffix or ".csv",
            dir=str(source_path.parent),
        )
        os.close(tmp_fd)
        tmp_path = Path(tmp_name)
        try:
            if file_format in _CSV_FORMATS:
                self._rewrite_csv(
                    artifact,
                    tmp_path=tmp_path,
                    strategy=strategy,
                    effective_iso=effective_iso,
                    input_columns=input_columns,
                    output_columns=output_columns,
                )
            else:
                self._rewrite_tab_preserving(
                    artifact,
                    tmp_path=tmp_path,
                    strategy=strategy,
                    effective_iso=effective_iso,
                    input_columns=input_columns,
                    output_columns=output_columns,
                    file_format=file_format,
                )
            os.replace(tmp_path, target_path)
        except Exception:
            tmp_path.unlink(missing_ok=True)
            raise

        had_contract_receipt = getattr(artifact, "contract_validation_receipt", None) is not None
        enriched = artifact.rebind(
            file_path=str(target_path),
            columns=list(output_columns),
            rows_exported=rows_exported,
        )
        if had_contract_receipt:
            if schema_contract is None:
                raise ValueError("strategy metadata rewrite lost its schema contract authority")
            import_module("dpone.runtime.etl.file_contract_validation").reissue_file_contract_validation(
                enriched,
                schema=output_schema,
                contract=schema_contract,
            )
        return enriched

    def _rewrite_csv(
        self,
        artifact: FileExportArtifact,
        *,
        tmp_path: Path,
        strategy: LoadStrategy,
        effective_iso: str,
        input_columns: Sequence[str],
        output_columns: Sequence[str],
    ) -> None:
        with (
            open_text(artifact, mode="rt") as reader_handle,
            open_text_path(tmp_path, compressed=artifact.compressed, mode="wt") as writer_handle,
        ):
            reader = csv.reader(reader_handle, delimiter=",")
            writer = csv.writer(
                writer_handle,
                delimiter=",",
                quotechar='"',
                doublequote=True,
                lineterminator="\n",
                quoting=csv.QUOTE_MINIMAL,
            )
            first = True
            for record in reader:
                if first and artifact.has_header:
                    if not is_header(record, input_columns):
                        raise ValueError(
                            "file artifact declared has_header=true but the first record is not its schema"
                        )
                    first = False
                    writer.writerow(list(output_columns))
                    continue
                first = False
                if len(record) != len(input_columns):
                    raise ValueError(
                        f"file row has {len(record)} fields but artifact exposes "
                        f"{len(input_columns)} columns: {artifact.file_path}"
                    )
                row = dict(zip(input_columns, record, strict=True))
                enriched = next(self._rows.enrich_rows([row], strategy=strategy, effective_iso=effective_iso))
                writer.writerow([encode_csv_cell(enriched.get(column)) for column in output_columns])

    def _rewrite_tab_preserving(
        self,
        artifact: FileExportArtifact,
        *,
        tmp_path: Path,
        strategy: LoadStrategy,
        effective_iso: str,
        input_columns: Sequence[str],
        output_columns: Sequence[str],
        file_format: str,
    ) -> None:
        codec = artifact.bulk_text_codec if isinstance(artifact.bulk_text_codec, BulkTextCodec) else BulkTextCodec()
        added_columns = list(output_columns[len(input_columns) :])
        with (
            open_text(artifact, mode="rt") as reader_handle,
            open_text_path(tmp_path, compressed=artifact.compressed, mode="wt") as writer_handle,
        ):
            first = True
            for raw_line in reader_handle:
                line = raw_line.rstrip("\n").rstrip("\r")
                fields = line.split("\t")
                if first and artifact.has_header:
                    if not is_header(fields, input_columns):
                        raise ValueError(
                            "file artifact declared has_header=true but the first record is not its schema"
                        )
                    first = False
                    writer_handle.write("\t".join(output_columns) + "\n")
                    continue
                first = False
                if len(fields) != len(input_columns):
                    raise ValueError(
                        f"file row has {len(fields)} fields but artifact exposes "
                        f"{len(input_columns)} columns: {artifact.file_path}"
                    )
                row = {
                    column: decode_tab_cell(value, file_format=file_format)
                    for column, value in zip(input_columns, fields, strict=True)
                }
                enriched = next(self._rows.enrich_rows([row], strategy=strategy, effective_iso=effective_iso))
                appended = [
                    encode_strategy_tab_cell(
                        enriched.get(column),
                        column=column,
                        file_format=file_format,
                        text_codec=codec,
                    )
                    for column in added_columns
                ]
                writer_handle.write("\t".join([*fields, *appended]) + "\n")


__all__ = ["StrategyMetadataFileEnricher"]
