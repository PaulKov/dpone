"""Provider-neutral post-apply verification for schema migration packs."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.schema_migration_post_apply_support import (
    POST_APPLY_CERTIFICATE_SCHEMA,
    POST_APPLY_PLAN_SCHEMA,
    PROFILES,
    bundle_blockers,
    bundle_id,
    default_profile,
    ledger_blockers,
    ledger_records,
    ledger_summary,
    normalize_canaries,
    pack_blockers,
    physical_blockers,
    post_apply_options,
    rollback_required_but_closed,
    rollback_window,
    run_canaries,
    run_data_profile,
    run_payload,
    strings,
    target_blockers,
    target_connection_public,
    target_key,
)
from dpone.readiness.schema_migration_post_apply_support import (
    checks as resolve_checks,
)


class PostApplyTargetInspector(Protocol):
    """Read-only target inspection port."""

    def inspect(self, plan: dict[str, Any]) -> PhysicalTableState: ...


class TargetCanaryExecutor(Protocol):
    """Read-only canary query execution port."""

    def execute(self, canary: dict[str, Any]) -> Sequence[Mapping[str, Any]]: ...


class PostApplyDataProfiler(Protocol):
    """Read-only target data profile port."""

    def profile(self, plan: dict[str, Any]) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class PostApplyVerificationPlanner:
    """Builds deterministic post-apply verification plans."""

    def plan(
        self,
        *,
        pack: Mapping[str, Any],
        bundle: Mapping[str, Any] | None,
        ledger: Mapping[str, Any] | None,
        manifest: Mapping[str, Any] | None,
        target_connection: Mapping[str, Any],
        environment: str,
    ) -> dict[str, Any]:
        migration_pack = MigrationPack.from_mapping(dict(pack))
        options = post_apply_options(manifest)
        checks = resolve_checks(options)
        canaries, canary_blockers = normalize_canaries(options.get("canaries", []))
        records = ledger_records(ledger)
        blockers = [
            *pack_blockers(migration_pack),
            *bundle_blockers(migration_pack, bundle),
            *ledger_blockers(migration_pack, records, environment),
            *target_blockers(migration_pack, target_connection),
            *canary_blockers,
        ]
        payload: dict[str, Any] = {
            "schema_version": POST_APPLY_PLAN_SCHEMA,
            "status": "blocked" if blockers else "planned",
            "pack_id": migration_pack.pack_id,
            "bundle_id": bundle_id(bundle),
            "environment": environment,
            "target": migration_pack.target.to_dict(),
            "target_connection": target_connection_public(target_connection),
            "desired": migration_pack.desired,
            "checks": checks,
            "mode": str(options.get("mode") or "gate"),
            "profile": str(options.get("profile") or default_profile(environment)),
            "canaries": canaries,
            "rollback": dict(migration_pack.rollback),
            "ledger_summary": ledger_summary(migration_pack, records, environment),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": [],
        }
        payload["post_apply_plan_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "bundle_id": payload["bundle_id"],
                "environment": environment,
                "target": payload["target"],
                "checks": checks,
                "canaries": canaries,
                "rollback": payload["rollback"],
                "blockers": payload["blockers"],
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class PostApplyVerificationRunner:
    """Runs read-only post-apply checks through injected target ports."""

    def run(
        self,
        *,
        plan: Mapping[str, Any],
        inspector: PostApplyTargetInspector | None,
        canary_executor: TargetCanaryExecutor | None,
        data_profiler: PostApplyDataProfiler | None = None,
        execute: bool = False,
    ) -> dict[str, Any]:
        if plan.get("status") == "blocked":
            return run_payload(plan, status="blocked", execute=execute, blockers=strings(plan.get("blockers", [])))
        if not execute:
            return run_payload(
                plan,
                status="dry_run",
                execute=False,
                warnings=("schema_migration_post_apply.not_executed",),
            )
        if inspector is None:
            return run_payload(
                plan,
                status="blocked",
                execute=True,
                blockers=("schema_migration_post_apply.target_inspector_required",),
            )
        start = time.perf_counter()
        check_results: list[dict[str, Any]] = [{"name": "ledger_state", "status": "passed"}]
        blockers = physical_blockers(plan, inspector.inspect(dict(plan)), check_results)
        warnings: list[str] = []
        profile_result = run_data_profile(plan, data_profiler)
        check_results.extend(profile_result["checks"])
        blockers.extend(profile_result["blockers"])
        warnings.extend(profile_result["warnings"])
        canary_result = run_canaries(plan, canary_executor)
        check_results.extend(canary_result["checks"])
        blockers.extend(canary_result["blockers"])
        warnings.extend(canary_result["warnings"])
        rollback = rollback_window(plan)
        check_results.append({"name": "rollback_window", "status": rollback["status"]})
        payload = run_payload(
            plan,
            status="blocked" if blockers else "passed",
            execute=True,
            blockers=blockers,
            warnings=warnings,
            checks=check_results,
            rollback_window_value=rollback,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )
        payload["metrics"].update(profile_result.get("metrics", {}))
        return payload


@dataclass(frozen=True, slots=True)
class PostApplyCertifier:
    """Turns a post-apply run into a release closeout certificate."""

    def certify(self, *, run: Mapping[str, Any], profile: str = "stage") -> dict[str, Any]:
        profile = profile if profile in PROFILES else "stage"
        blockers = list(strings(run.get("blockers", [])))
        warnings = list(strings(run.get("warnings", [])))
        if run.get("status") == "dry_run":
            if profile == "advisory":
                warnings.append("schema_migration_post_apply.dry_run_certificate")
            else:
                blockers.append("schema_migration_post_apply.execution_required")
        if run.get("status") == "blocked":
            blockers.append("schema_migration_post_apply.run_blocked")
        if profile in {"prod_strict", "regulated"} and rollback_required_but_closed(run):
            blockers.append("schema_migration_post_apply.rollback_window_closed")
        status = "blocked" if blockers else "warning" if warnings else "verified"
        payload: dict[str, Any] = {
            "schema_version": POST_APPLY_CERTIFICATE_SCHEMA,
            "status": status,
            "profile": profile,
            "pack_id": run.get("pack_id"),
            "bundle_id": run.get("bundle_id"),
            "environment": run.get("environment"),
            "target": dict(run.get("target", {})) if isinstance(run.get("target"), Mapping) else {},
            "post_apply_run_id": run.get("post_apply_run_id"),
            "checks": list(run.get("checks", [])) if isinstance(run.get("checks"), list) else [],
            "rollback_window": dict(run.get("rollback_window", {}))
            if isinstance(run.get("rollback_window"), Mapping)
            else {},
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "metrics": dict(run.get("metrics", {})) if isinstance(run.get("metrics"), Mapping) else {},
        }
        payload["certificate_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "run_id": payload["post_apply_run_id"],
                "profile": profile,
                "status": status,
                "checks": payload["checks"],
                "blockers": payload["blockers"],
                "warnings": payload["warnings"],
            }
        )
        return payload


def render_post_apply_markdown(payload: Mapping[str, Any]) -> str:
    lines = [
        "# Schema Migration Post-Apply Certificate",
        "",
        f"- status: {payload.get('status')}",
        f"- pack_id: {payload.get('pack_id')}",
        f"- bundle_id: {payload.get('bundle_id')}",
        f"- environment: {payload.get('environment')}",
        f"- target: {target_key(payload.get('target'))}",
        "",
        "## Checks",
    ]
    check_values = payload.get("checks", [])
    if isinstance(check_values, list) and check_values:
        lines.extend(
            f"- {item.get('name')}: {item.get('status')}" for item in check_values if isinstance(item, Mapping)
        )
    else:
        lines.append("- none")
    for key in ("blockers", "warnings"):
        values = strings(payload.get(key, []))
        if values:
            lines.extend(["", f"## {key.title()}"])
            lines.extend(f"- {item}" for item in values)
    return "\n".join(lines) + "\n"


def render_post_apply_table(payload: Mapping[str, Any]) -> str:
    lines = ["schema migration post-apply", "field | value", "--- | ---"]
    for key in ("status", "pack_id", "bundle_id", "environment"):
        lines.append(f"{key} | {payload.get(key)}")
    lines.append(f"target | {target_key(payload.get('target'))}")
    return "\n".join(lines) + "\n"


__all__ = [
    "POST_APPLY_CERTIFICATE_SCHEMA",
    "POST_APPLY_PLAN_SCHEMA",
    "PostApplyCertifier",
    "PostApplyDataProfiler",
    "PostApplyTargetInspector",
    "PostApplyVerificationPlanner",
    "PostApplyVerificationRunner",
    "TargetCanaryExecutor",
    "render_post_apply_markdown",
    "render_post_apply_table",
]
