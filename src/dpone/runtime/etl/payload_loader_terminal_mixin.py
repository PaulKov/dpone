"""Load-identity and native-terminal adapters for payload loading."""

from __future__ import annotations

from importlib import import_module
from typing import Any

from dpone.runtime.etl.payload_loader_native_terminal import mark_native_failed
from dpone.runtime.etl.result_metrics import with_runtime_metrics


class PayloadLoadTerminalMixin:
    load_identity_service: Any
    native_transfer_runtime_service: Any

    def empty_load_result(self) -> Any:
        """Build the neutral result used by empty source artifacts."""

        module = import_module("dpone.runtime.sinks.load_result")
        return module.LoadResult(inserted_rows=0, updated_rows=0, total_rows=0)

    def _mark_load_committed(self, load_record: Any, extract_result: Any, load_result: Any) -> None:
        staged_record = self.load_identity_service.mark_staged(
            load_record, extracted_rows=getattr(extract_result.artifact, "estimated_rows", None)
        )
        self.load_identity_service.mark_committed(staged_record or load_record, load_result)

    def _mark_native_failed(self, context: Any, exc: Exception) -> None:
        """Retain the strict private adapter used by compatibility callers."""

        mark_native_failed(self.native_transfer_runtime_service, context, exc)

    def _with_runtime_metrics(
        self,
        load_result: Any,
        runtime_metrics: dict[str, Any],
        evidence_path: str | None,
    ) -> Any:
        return with_runtime_metrics(load_result, runtime_metrics, evidence_path)
