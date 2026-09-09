"""Self-service backfill planning, execution and status inspection."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from pathlib import Path
from typing import TYPE_CHECKING, Any

from dpone.backfill import BackfillStateStoreFactory
from dpone.backfill.diagnostic_connector import BackfillDiagnosticConnectorResolver
from dpone.backfill.plan_projection import build_backfill_plan_payload
from dpone.backfill.portable_scope_runtime import is_mssql_backfill_route
from dpone.backfill.service_helpers import (
    backfill_operation_status,
    doctor_next_actions,
    sink_type_from_options,
)
from dpone.config.mssql_strategy_contract import normalize_mssql_backfill_campaign_strategy
from dpone.services.manifest import ManifestCommandContext, build_manifest_context, resolve_single_process
from dpone.services.run_manifest import RunManifestService

# Compared by enum value to keep this service decoupled from the runtime
# strategy contract (see dpone.config.load_strategy.LoadStrategy.BACKFILL).
_BACKFILL_MODE = "backfill"

if TYPE_CHECKING:
    from dpone.config.load_config import LoadConfig


class _RegistryArgs:
    """Minimal args view for building a manifest context from registry paths."""

    def __init__(self, registry: list[str] | tuple[str, ...]) -> None:
        self.registry = list(registry)


class BackfillCommandService:
    """Plan-first orchestration facade over the backfill runtime."""

    def __init__(
        self,
        *,
        run_service: RunManifestService | None = None,
        state_store_factory: BackfillStateStoreFactory | None = None,
        sink_connector: Any | None = None,
        sink_type: str | None = None,
        connector_resolver: BackfillDiagnosticConnectorResolver | None = None,
    ) -> None:
        self._run_service = run_service or RunManifestService()
        self._state_store_factory = state_store_factory or BackfillStateStoreFactory()
        self._sink_connector = sink_connector
        self._sink_type = sink_type
        self._connector_resolver = connector_resolver or BackfillDiagnosticConnectorResolver()

    def plan(
        self,
        *,
        path: str | Path,
        manifest_ctx: ManifestCommandContext | None = None,
        registry: list[str] | tuple[str, ...] = (),
        selector: str | None = None,
        overrides: dict[str, Any] | None = None,
        advisor: bool = False,
        advisor_evidence_paths: tuple[str | Path, ...] = (),
    ) -> dict[str, Any]:
        """Build the chunk plan and merge in any persisted ledger statuses."""

        context = self._context(manifest_ctx, registry)
        load_config = self._load_config(path=path, manifest_ctx=context, selector=selector)
        backfill_options = self._backfill_options(load_config, overrides)
        return build_backfill_plan_payload(
            load_config,
            backfill_options,
            manifest=str(path),
            executed=False,
            advisor=advisor,
            state_store_provider=lambda: self._state_store(load_config, backfill_options),
            advisor_evidence_paths=advisor_evidence_paths,
        )

    def status(
        self,
        *,
        path: str | Path,
        manifest_ctx: ManifestCommandContext | None = None,
        registry: list[str] | tuple[str, ...] = (),
        selector: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Report ledger progress for the campaign derived from the manifest."""

        payload = self.plan(
            path=path,
            manifest_ctx=manifest_ctx,
            registry=registry,
            selector=selector,
            overrides=overrides,
        )
        payload["kind"] = "dpone.backfill_status"
        return payload

    def run(
        self,
        *,
        path: str | Path,
        manifest_ctx: ManifestCommandContext | None = None,
        registry: list[str] | tuple[str, ...] = (),
        selector: str | None = None,
        overrides: dict[str, Any] | None = None,
        execute: bool = False,
        run_id: str | None = None,
        dag_id: str | None = None,
        execution_date: Any | None = None,
    ) -> dict[str, Any]:
        """Dry-run (plan) by default; execute chunk loads with ``execute=True``."""

        context = self._context(manifest_ctx, registry)
        if not execute:
            payload = self.plan(path=path, manifest_ctx=context, selector=selector, overrides=overrides)
            payload["next_actions"] = ["re-run with --execute to load the pending chunks"]
            return payload

        # Validate the effective campaign before RunManifestService performs
        # route admission or constructs any runtime process. The mutator below
        # repeats the same pure check on the execution-owned config instance.
        self._apply_overrides(
            self._load_config(path=path, manifest_ctx=context, selector=selector),
            overrides,
        )

        report = self._run_service.run(
            path=Path(path),
            manifest_ctx=context,
            selector=selector,
            run_id=run_id,
            dag_id=dag_id,
            execution_date=execution_date,
            load_config_mutator=lambda config: self._apply_overrides(config, overrides),
        )
        payload = report.to_dict()
        payload["kind"] = "dpone.backfill_run"
        payload["executed"] = True
        return payload

    def retry_failed(
        self,
        *,
        path: str | Path,
        manifest_ctx: ManifestCommandContext | None = None,
        registry: list[str] | tuple[str, ...] = (),
        selector: str | None = None,
        overrides: dict[str, Any] | None = None,
        run_id: str | None = None,
        dag_id: str | None = None,
        execution_date: Any | None = None,
    ) -> dict[str, Any]:
        """Execute only failed chunks; pending chunks stay untouched."""

        merged = dict(overrides or {})
        merged["retry_policy"] = "failed_only"
        payload = self.run(
            path=path,
            manifest_ctx=manifest_ctx,
            registry=registry,
            selector=selector,
            overrides=merged,
            execute=True,
            run_id=run_id,
            dag_id=dag_id,
            execution_date=execution_date,
        )
        payload["kind"] = "dpone.backfill_retry_failed"
        operation_status = backfill_operation_status(payload)
        if operation_status is not None:
            payload["operation_status"] = operation_status
        payload["operation_passed"] = operation_status == "success" if operation_status else bool(payload.get("passed"))
        return payload

    def cancel(
        self,
        *,
        path: str | Path,
        manifest_ctx: ManifestCommandContext | None = None,
        registry: list[str] | tuple[str, ...] = (),
        selector: str | None = None,
        overrides: dict[str, Any] | None = None,
        reason: str,
        requested_by: str,
    ) -> dict[str, Any]:
        """Mark an existing campaign as cancel-requested without moving data."""

        context = self._context(manifest_ctx, registry)
        load_config = self._load_config(path=path, manifest_ctx=context, selector=selector)
        backfill_options = self._backfill_options(load_config, overrides)
        plan = build_backfill_plan_payload(
            load_config,
            backfill_options,
            manifest=str(path),
            executed=False,
            advisor=False,
            state_store_provider=lambda: self._state_store(load_config, backfill_options),
        )
        store = self._state_store(load_config, backfill_options)
        store.request_cancel(plan["run_key"], reason=reason, requested_by=requested_by)
        ledger = store.load(plan["run_key"])
        return {
            "kind": "dpone.backfill_cancel",
            "manifest": str(path),
            "dataset": plan["dataset"],
            "run_key": plan["run_key"],
            "status": ledger.status if ledger else "unknown",
            "cancel_reason": ledger.cancel_reason if ledger else reason,
            "requested_by": ledger.cancel_requested_by if ledger else requested_by,
        }

    def doctor(
        self,
        *,
        path: str | Path,
        manifest_ctx: ManifestCommandContext | None = None,
        registry: list[str] | tuple[str, ...] = (),
        selector: str | None = None,
        overrides: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """Return a self-service health summary for the derived campaign."""

        payload = self.status(
            path=path,
            manifest_ctx=manifest_ctx,
            registry=registry,
            selector=selector,
            overrides=overrides,
        )
        blockers: list[str] = []
        warnings: list[str] = []
        counts = payload.get("counts", {})
        if counts.get("failed"):
            warnings.append("failed chunks exist; run `dpone backfill retry-failed`")
        if counts.get("running"):
            warnings.append("running chunks exist; check lease TTL before retry")
        if counts.get("pending"):
            warnings.append("pending chunks remain; run `dpone backfill resume`")
        state_capabilities = payload.get("state_capabilities")
        if (
            payload.get("state_backend") == "audit_schema"
            and isinstance(state_capabilities, Mapping)
            and state_capabilities.get("distributed_lock") is False
        ):
            warnings.append(f"state lock is not distributed; lock_scope={state_capabilities.get('lock_scope')}")
        return {
            "kind": "dpone.backfill_doctor",
            "status": "blocked" if blockers else "warning" if warnings else "passed",
            "blockers": blockers,
            "warnings": warnings,
            "next_actions": doctor_next_actions(str(path), counts),
            "campaign": payload,
        }

    # -- internals ----------------------------------------------------------

    def _context(
        self,
        manifest_ctx: ManifestCommandContext | None,
        registry: list[str] | tuple[str, ...],
    ) -> ManifestCommandContext:
        if manifest_ctx is not None:
            return manifest_ctx
        return build_manifest_context(_RegistryArgs(registry), ctx=None)

    def _load_config(
        self,
        *,
        path: str | Path,
        manifest_ctx: ManifestCommandContext,
        selector: str | None,
    ) -> LoadConfig:
        manifest = manifest_ctx.loader.load(Path(path), metadata_only=True)
        spec = resolve_single_process(manifest, selector=selector)
        load_config = spec.config.load_config
        if load_config.load_strategy.value != _BACKFILL_MODE:
            raise ValueError(
                f"Manifest process {spec.name!r} uses mode={load_config.load_strategy.value}; "
                "dpone backfill requires sink.strategy.mode: backfill"
            )
        return load_config

    def _backfill_options(self, load_config: LoadConfig, overrides: dict[str, Any] | None) -> dict[str, Any]:
        mutated = self._apply_overrides(load_config, overrides)
        return dict((mutated.options or {}).get("backfill") or {})

    def _apply_overrides(self, load_config: LoadConfig, overrides: dict[str, Any] | None) -> LoadConfig:
        if overrides:
            options = deepcopy(load_config.options) if load_config.options is not None else {}
            backfill_options = dict(options.get("backfill") or {})
            chunk = dict(backfill_options.get("chunk") or {})
            for key in ("column", "from", "to", "step", "kind"):
                if overrides.get(key) is not None:
                    chunk[key] = overrides[key]
            if chunk:
                backfill_options["chunk"] = chunk
            for key in ("inner_mode", "parallel_workers", "state_dir", "backfill_id", "max_chunks"):
                if overrides.get(key) is not None:
                    backfill_options[key] = overrides[key]
            for key in ("retry_policy", "predicate_dialect"):
                if overrides.get(key) is not None:
                    backfill_options[key] = overrides[key]
            options["backfill"] = backfill_options
            load_config.options = options
        self._validate_campaign_authoring(load_config)
        return load_config

    @staticmethod
    def _validate_campaign_authoring(load_config: LoadConfig) -> None:
        if is_mssql_backfill_route(load_config):
            normalize_mssql_backfill_campaign_strategy(load_config)

    def _state_store(self, load_config: LoadConfig, backfill_options: dict[str, Any]):
        connector = self._sink_connector or self._connector_resolver.resolve(load_config, backfill_options)
        return self._state_store_factory.build(
            backfill_options,
            sink_type=self._sink_type or sink_type_from_options(load_config.options),
            sink_connector=connector,
        )


__all__ = ["BackfillCommandService"]
