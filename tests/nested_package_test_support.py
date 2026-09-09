"""Shared fixtures for nested package staged-mutation tests."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.normalization.load_package import NestedPackageStagedMutation
from dpone.runtime.sinks.load_result import LoadResult


def identity_prepare(config: Any, payload: Any, *_args: Any) -> tuple[Any, Any]:
    return config, payload


def identity_evaluate(package_result: LoadResult) -> LoadResult:
    return package_result


def artifact_rows(artifact: object) -> list[object]:
    if hasattr(artifact, "_rows"):
        return list(artifact._rows)
    if hasattr(artifact, "_iterator"):
        return list(artifact._iterator)
    try:
        return list(artifact)  # type: ignore[arg-type]
    except TypeError:
        return []


def artifact_row_count(artifact: object) -> int:
    estimated = getattr(artifact, "estimated_rows", None)
    if isinstance(estimated, int) and not isinstance(estimated, bool) and estimated >= 0:
        return estimated
    rows = artifact_rows(artifact)
    if rows:
        return len(rows)
    return 1


class RecordingStagedSink:
    """Minimal staged sink for NestedLoadService / package-mutation tests."""

    supports_staged_validation_receipts = True

    def __init__(
        self,
        *,
        fail_stage_table: str | None = None,
        fail_stage_error: BaseException | None = None,
        fail_finalize_table: str | None = None,
        mutate_then_fail_finalize_table: str | None = None,
        fail_validate_table: str | None = None,
        fail_abort: bool = False,
        zero_staged_rows: bool = False,
    ) -> None:
        self.staged_tables: list[str] = []
        self.validated_tables: list[str] = []
        self.loaded_tables: list[str] = []
        self.aborted_tables: list[str] = []
        self.artifacts: dict[str, object] = {}
        self._fail_stage_table = fail_stage_table
        self._fail_stage_error = fail_stage_error or RuntimeError("child load failed")
        self._fail_finalize_table = fail_finalize_table
        self._mutate_then_fail_finalize_table = mutate_then_fail_finalize_table
        self._fail_validate_table = fail_validate_table
        self._fail_abort = fail_abort
        self._zero_staged_rows = zero_staged_rows

    def stage_payload(self, load_config: Any, payload: Any) -> StagedLoadHandle:
        if self._fail_stage_table == load_config.target_table:
            raise self._fail_stage_error
        # Do not consume streaming iterators here; tests may read artifacts later.
        staged_rows = 0 if self._zero_staged_rows else artifact_row_count(payload.artifact)
        self.staged_tables.append(load_config.target_table)
        self.artifacts[load_config.target_table] = payload.artifact
        return StagedLoadHandle(
            staging_config=SimpleNamespace(
                target_schema=load_config.target_schema,
                target_table=load_config.target_table,
            ),
            staged_rows=staged_rows,
            payload_schema=tuple(payload.schema or ()),
        )

    def validate_staged_load(self, load_config: Any, handle: Any) -> str:
        del handle
        self.validated_tables.append(load_config.target_table)
        if self._fail_validate_table == load_config.target_table:
            raise RuntimeError("staged validation failed")
        return "validated"

    def finalize_staged_load(self, load_config: Any, receipt: Any) -> LoadResult:
        _token, validated_config, handle = receipt.frozen_inputs(sink=self, load_config=load_config)
        if self._fail_finalize_table == validated_config.target_table:
            raise RuntimeError("finalize failed")
        self.loaded_tables.append(validated_config.target_table)
        if self._mutate_then_fail_finalize_table == validated_config.target_table:
            raise RuntimeError("finalize failed after mutation")
        rows = int(handle.staged_rows)
        return LoadResult(inserted_rows=rows, updated_rows=0, total_rows=rows, staging_rows=rows)

    def abort_staged_load(self, handle: Any) -> None:
        if self._fail_abort:
            raise RuntimeError("abort failed")
        self.aborted_tables.append(str(handle.staging_config.target_table))

    def cleanup_staged_load(self, handle: Any) -> None:
        del handle

    def mutation(self) -> NestedPackageStagedMutation:
        return NestedPackageStagedMutation(self)
