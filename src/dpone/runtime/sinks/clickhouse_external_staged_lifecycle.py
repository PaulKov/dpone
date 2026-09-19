"""External publication integration for the generic ClickHouse staged lifecycle."""

from __future__ import annotations

from dataclasses import replace
from typing import Any

from dpone.runtime.governance.ports import StagedLoadHandle
from dpone.runtime.sinks.clickhouse_external_replication_context import ExternalStagedContext


class ClickHouseExternalStagedLifecycle:
    def __init__(self, sink: Any) -> None:
        self._sink = sink

    def is_enabled(self, load_config: Any) -> bool:
        publication = getattr(self._sink, "_full_refresh_publication", None)
        predicate = getattr(publication, "is_external", None)
        return bool(callable(predicate) and predicate(load_config))

    def stage(self, load_config: Any, payload: Any) -> StagedLoadHandle:
        context = self._sink._full_refresh_publication.stage_external(load_config, payload)
        return StagedLoadHandle(
            staging_config=replace(load_config, target_table=context.candidate_name),
            payload_schema=tuple(getattr(payload, "schema", ())),
            staged_rows=context.request.artifact.row_count,
            metadata={"external_publication": context.staged_receipt.to_dict()},
            sink_state=context,
        )

    def validate(self, handle: StagedLoadHandle) -> object | None:
        context = self.context(handle)
        return None if context is None else self._sink._full_refresh_publication.validate_external(context)

    def publish(self, handle: StagedLoadHandle, validation: object) -> Any | None:
        context = self.context(handle)
        if context is None:
            return None
        receipt = self._sink._full_refresh_publication.publish_external(context, validation)
        return self._sink._full_refresh_publication.external_result(receipt, staged_rows=handle.staged_rows)

    def cleanup(self, handle: StagedLoadHandle, *, abort: bool) -> bool:
        context = self.context(handle)
        if context is None:
            return False
        publication = self._sink._full_refresh_publication
        publication.abort_external(context) if abort else publication.cleanup_external(context)
        return True

    @staticmethod
    def context(handle: Any) -> ExternalStagedContext | None:
        state = getattr(handle, "sink_state", None)
        return state if isinstance(state, ExternalStagedContext) else None


__all__ = ["ClickHouseExternalStagedLifecycle"]
