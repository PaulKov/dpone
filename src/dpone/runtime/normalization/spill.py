"""Spill nested normalization output to per-table native files."""

from __future__ import annotations

import os
import shutil
import tempfile
from collections import OrderedDict
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import TextIO

from dpone.runtime.lineage.identity import LineageIdentityService
from dpone.runtime.normalization.models import NormalizationResult
from dpone.runtime.normalization.native_spill import NativeSpillWriter
from dpone.runtime.normalization.normalizer import NestedNormalizationService
from dpone.runtime.normalization.options import NestedNormalizationOptions
from dpone.runtime.normalization.semantic_spill import read_semantic_rows, write_semantic_rows
from dpone.runtime.normalization.table_identity import GeneratedTableRegistry


@dataclass(frozen=True, slots=True)
class SpilledNormalizationResult:
    """File-backed normalization output metadata."""

    files: dict[str, Path]
    row_counts: dict[str, int]
    schemas: dict[str, list[tuple[str, str]]]
    formats: dict[str, str]
    output_dir: Path
    table_keys: dict[str, tuple[str, ...]] = field(default_factory=dict)
    table_parents: dict[str, str] = field(default_factory=dict)
    semantic_files: dict[str, Path] = field(default_factory=dict)

    @property
    def total_rows(self) -> int:
        return sum(self.row_counts.values())


class SpillToDiskNormalizationService:
    """Normalize root rows incrementally and spill every table to native files."""

    def __init__(
        self,
        *,
        normalizer: NestedNormalizationService | None = None,
        identity_service: LineageIdentityService | None = None,
    ) -> None:
        self._identity_service = identity_service or LineageIdentityService()
        self._normalizer = normalizer or NestedNormalizationService(identity_service=self._identity_service)

    def spill_rows(
        self,
        rows: Iterable[Mapping[str, object]],
        *,
        root_table: str,
        options: NestedNormalizationOptions,
        output_dir: str | Path,
        unique_key: str | Sequence[str] | None = None,
        load_id: str | None = None,
        source_type: str = "nested",
        source_schema: str = "",
        source_table: str | None = None,
        output_format: str = "jsonl",
        staged_validator: Callable[[SpilledNormalizationResult], None] | None = None,
    ) -> SpilledNormalizationResult:
        output = Path(output_dir)
        output.mkdir(parents=True, exist_ok=True)
        _validate_table_file_component(root_table)
        resolved_load_id = load_id or self._identity_service.new_load_id()
        writer = NativeSpillWriter(output_format)
        table_registry = GeneratedTableRegistry(root_table=root_table)
        files: dict[str, Path] = {}
        staged_files: dict[str, Path] = {}
        semantic_files: dict[str, Path] = {}
        staged_semantic_files: dict[str, Path] = {}
        row_counts: dict[str, int] = {}
        schemas: dict[str, list[tuple[str, str]]] = {}
        formats: dict[str, str] = {}
        handles: dict[str, TextIO] = {}
        staging_dir = Path(tempfile.mkdtemp(prefix=".dpone-spill-stage-", dir=output))
        try:
            generation_dir = output / staging_dir.name.replace(
                ".dpone-spill-stage-",
                ".dpone-spill-generation-",
                1,
            )
            for row_index, row in enumerate(rows):
                normalized = self._normalize_one(
                    row,
                    row_index=row_index,
                    root_table=root_table,
                    options=options,
                    load_id=resolved_load_id,
                    unique_key=unique_key,
                    source_type=source_type,
                    source_schema=source_schema,
                    source_table=source_table or root_table,
                    table_registry=table_registry,
                )
                self._write_result(
                    normalized,
                    generation_dir,
                    staging_dir,
                    files,
                    staged_files,
                    semantic_files,
                    staged_semantic_files,
                    row_counts,
                    schemas,
                    formats,
                    handles,
                    writer,
                )
            _close_handles(handles)
            handles.clear()
            for table_name, semantic_path in staged_semantic_files.items():
                writer.render_file(
                    read_semantic_rows(semantic_path),
                    staged_files[table_name],
                    schemas[table_name],
                )
            if files:
                if staged_validator is not None:
                    staged_validator(
                        SpilledNormalizationResult(
                            files=dict(staged_files),
                            row_counts=dict(row_counts),
                            schemas=dict(schemas),
                            formats=dict(formats),
                            output_dir=output,
                            table_keys=table_registry.table_keys(),
                            table_parents=table_registry.table_parents(),
                            semantic_files=dict(staged_semantic_files),
                        )
                    )
                os.replace(staging_dir, generation_dir)
        except BaseException:
            _close_handles(handles)
            raise
        finally:
            shutil.rmtree(staging_dir, ignore_errors=True)
        return SpilledNormalizationResult(
            files=files,
            row_counts=row_counts,
            schemas=schemas,
            formats=formats,
            output_dir=output,
            table_keys=table_registry.table_keys(),
            table_parents=table_registry.table_parents(),
            semantic_files=semantic_files,
        )

    def _normalize_one(
        self,
        row: Mapping[str, object],
        *,
        row_index: int,
        root_table: str,
        options: NestedNormalizationOptions,
        load_id: str,
        unique_key: str | Sequence[str] | None,
        source_type: str,
        source_schema: str,
        source_table: str,
        table_registry: GeneratedTableRegistry,
    ) -> NormalizationResult:
        return self._normalizer._normalize_rows_with_offset(
            [row],
            root_table=root_table,
            options=options,
            load_id=load_id,
            unique_key=unique_key,
            source_type=source_type,
            source_schema=source_schema,
            source_table=source_table,
            table_registry=table_registry,
            row_index_offset=row_index,
        )

    def _write_result(
        self,
        result: NormalizationResult,
        output_dir: Path,
        staging_dir: Path,
        files: dict[str, Path],
        staged_files: dict[str, Path],
        semantic_files: dict[str, Path],
        staged_semantic_files: dict[str, Path],
        row_counts: dict[str, int],
        schemas: dict[str, list[tuple[str, str]]],
        formats: dict[str, str],
        handles: dict[str, TextIO],
        writer: NativeSpillWriter,
    ) -> None:
        for table in result.tables:
            table_file_component = _validate_table_file_component(table.name)
            files.setdefault(table.name, output_dir / f"{table_file_component}{writer.suffix}")
            semantic_files.setdefault(table.name, output_dir / f".dpone-rows-{table_file_component}.jsonl")
            staged_files.setdefault(
                table.name,
                staging_dir / f"{table_file_component}{writer.suffix}",
            )
            path = staged_semantic_files.setdefault(
                table.name,
                staging_dir / f".dpone-rows-{table_file_component}.jsonl",
            )
            handle = handles.get(table.name)
            if handle is None:
                handle = path.open("w", encoding="utf-8")
                handles[table.name] = handle
            merged_schema = _merge_schema(
                schemas.get(table.name, []),
                table.schema,
                table_name=table.name,
                fail_on_incompatible=writer.output_format == "tsv",
            )
            schemas[table.name] = merged_schema
            formats[table.name] = writer.output_format
            write_semantic_rows(handle, table.rows)
            row_counts[table.name] = row_counts.get(table.name, 0) + len(table.rows)


def _merge_schema(
    existing: list[tuple[str, str]],
    incoming: Sequence[tuple[str, str]],
    *,
    table_name: str,
    fail_on_incompatible: bool,
) -> list[tuple[str, str]]:
    merged: OrderedDict[str, str] = OrderedDict(existing)
    for name, logical_type in incoming:
        if name not in merged:
            merged[name] = logical_type
        elif merged[name] == "null":
            merged[name] = logical_type
        elif logical_type == "null":
            continue
        elif {merged[name], logical_type} <= {"integer", "number"}:
            merged[name] = "number"
        elif merged[name] != logical_type:
            if fail_on_incompatible or "bytes" in {merged[name], logical_type}:
                raise ValueError(
                    "nested spill cannot represent schema types losslessly: "
                    f"table `{table_name}`, column `{name}`, types `{merged[name]}` and `{logical_type}`"
                )
            merged[name] = "string"
    return list(merged.items())


def _validate_table_file_component(table_name: str) -> str:
    if (
        not table_name
        or table_name in {".", ".."}
        or Path(table_name).name != table_name
        or "/" in table_name
        or "\\" in table_name
    ):
        raise ValueError(
            f"nested spill table `{table_name}` is not a safe single filename component; "
            "configure a table name without path separators or dot segments"
        )
    return table_name


def _close_handles(handles: Mapping[str, TextIO]) -> None:
    for handle in handles.values():
        handle.close()
