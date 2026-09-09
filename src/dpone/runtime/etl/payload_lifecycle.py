"""Payload preparation shared by regular and immutable snapshot loads."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import replace
from importlib import import_module
from typing import Any

from dpone.runtime.etl.snapshot_envelope_lifecycle import SnapshotEnvelopeLifecyclePolicy
from dpone.runtime.support.postgres_mssql_projection import ensure_postgres_mssql_payload_projection


class PayloadLifecyclePreparer:
    """Select the correct schema lifecycle before a payload reaches a sink."""

    def __init__(
        self,
        *,
        schema_identity_service: Any,
        schema_evolution_service: Any,
        runtime_lifecycle_service: Any,
        temporal_fidelity_projector_cls: Any | None,
        snapshot_envelope_lifecycle_policy: Any | None = None,
    ) -> None:
        self.schema_identity_service = schema_identity_service
        self.schema_evolution_service = schema_evolution_service
        self.runtime_lifecycle_service = runtime_lifecycle_service
        self.temporal_fidelity_projector_cls = temporal_fidelity_projector_cls
        self.snapshot_policy = snapshot_envelope_lifecycle_policy or SnapshotEnvelopeLifecyclePolicy()

    def prepare(
        self,
        *,
        load_config: Any,
        payload: Any,
        logger: Any,
        sink: Any,
        run_id: str,
        load_id: str,
        assert_current: Callable[[], None] | None = None,
    ) -> Any:
        """Prepare one payload without mutating an immutable envelope."""

        snapshot_context = self.snapshot_policy.prepare(load_config=load_config, payload=payload)
        if snapshot_context is not None:
            return snapshot_context

        payload = self.schema_identity_service.prepare_payload(load_config, payload, logger)
        payload = ensure_postgres_mssql_payload_projection(load_config, payload)
        context = self.runtime_lifecycle_service.prepare_before_schema_evolution(
            load_config=load_config,
            payload=payload,
            run_id=run_id,
            load_id=load_id,
        )
        payload = self.project_temporal_fidelity(load_config, context.payload)
        if assert_current is not None:
            assert_current()
        payload = self.schema_evolution_service.prepare_payload(load_config, sink, payload, logger)
        return self.runtime_lifecycle_service.prepare_after_schema_evolution(
            load_config=load_config,
            sink=sink,
            context=replace(context, payload=payload),
        )

    def project_temporal_fidelity(self, load_config: Any, payload: Any) -> Any:
        """Apply the configured temporal type projection to a regular payload."""

        if payload.target_projection is not None:
            # The PostgreSQL catalog projection was authorized before COPY.
            # Rewriting only the schema of an immutable file here would detach
            # the declaration from its bytes and can reinterpret offsets.
            return payload
        module = import_module("dpone.runtime.support.temporal_fidelity")
        raw_options = getattr(load_config, "options", {}) or {}
        policy = module.TemporalFidelityPolicy.from_config(raw_options.get("type_fidelity"))
        projector_cls = self.temporal_fidelity_projector_cls or module.TemporalFidelityProjector
        return projector_cls(policy).project_payload(payload)

    def prepare_nested_member(
        self,
        *,
        load_config: Any,
        payload: Any,
        logger: Any,
        sink: Any,
        run_id: str,
        load_id: str,
    ) -> Any:
        """Prepare one nested package member without target finalization."""

        payload = self.schema_identity_service.prepare_payload(load_config, payload, logger)
        payload = ensure_postgres_mssql_payload_projection(load_config, payload)
        context = self.runtime_lifecycle_service.prepare_before_schema_evolution(
            load_config=load_config,
            payload=payload,
            run_id=run_id,
            load_id=load_id,
        )
        payload = self.project_temporal_fidelity(load_config, context.payload)
        payload = self.schema_evolution_service.prepare_payload(load_config, sink, payload, logger)
        context = self.runtime_lifecycle_service.prepare_after_schema_evolution(
            load_config=load_config,
            sink=sink,
            context=replace(context, payload=payload),
        )
        return context.payload


__all__ = ["PayloadLifecyclePreparer"]
