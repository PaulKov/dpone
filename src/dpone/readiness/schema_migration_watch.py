"""Provider-neutral production release watch for schema migration packs."""

from __future__ import annotations

import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any, Protocol

from dpone.readiness.migration_control import MigrationPack, stable_fingerprint
from dpone.readiness.physical_state import PhysicalTableState
from dpone.readiness.schema_migration_watch_support import (
    PROFILES,
    WATCH_CERTIFICATE_SCHEMA,
    WATCH_PLAN_SCHEMA,
    WATCH_RUN_SCHEMA,
    checks,
    checks_enabled,
    default_profile,
    disabled_plan,
    mapping,
    matches_rollback_signal,
    normalize_canaries,
    normalize_window,
    physical_blockers,
    planned_samples,
    post_apply_blockers,
    profile_required,
    query_health_options,
    remediation_options,
    rollback_command,
    rollback_on,
    rollback_window,
    run_canaries,
    sample_summary,
    should_stop_early,
    sleep_between_samples,
    strings,
    target_blockers,
    target_connection_public,
    watch_options,
)


class TargetWatchProbe(Protocol):
    """Read-only target probe for watch samples."""

    def inspect(self, plan: dict[str, Any]) -> PhysicalTableState: ...

    def execute(self, canary: dict[str, Any]) -> Sequence[Mapping[str, Any]]: ...

    def profile(self, plan: dict[str, Any]) -> Mapping[str, Any]: ...

    def query_health(self, plan: dict[str, Any]) -> Mapping[str, Any]: ...


@dataclass(frozen=True, slots=True)
class MigrationWatchRemediationDecision:
    """Recommend-only remediation output for a watch run."""

    decision: str
    commands: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        return {"decision": self.decision, "commands": list(self.commands)}


@dataclass(frozen=True, slots=True)
class MigrationWatchPlanner:
    """Build deterministic, read-only release watch plans."""

    def plan(
        self,
        *,
        pack: Mapping[str, Any],
        post_apply_certificate: Mapping[str, Any],
        manifest: Mapping[str, Any] | None,
        target_connection: Mapping[str, Any],
        environment: str,
    ) -> dict[str, Any]:
        migration_pack = MigrationPack.from_mapping(dict(pack))
        options = watch_options(manifest)
        if not bool(options.get("enabled", False)):
            return disabled_plan(migration_pack, post_apply_certificate, target_connection, environment)
        mode = str(options.get("mode") or "gate")
        window, window_blockers = normalize_window(mapping(options.get("window")))
        canaries, canary_blockers = normalize_canaries(options.get("canaries", []))
        resolved_checks = checks(options)
        blockers = [
            *(["schema_migration_watch.pack_blocked"] if migration_pack.blockers else []),
            *post_apply_blockers(
                pack=migration_pack, certificate=post_apply_certificate, mode=mode, environment=environment
            ),
            *target_blockers(migration_pack, target_connection),
            *window_blockers,
            *canary_blockers,
        ]
        payload: dict[str, Any] = {
            "schema_version": WATCH_PLAN_SCHEMA,
            "status": "blocked" if blockers else "planned",
            "pack_id": migration_pack.pack_id,
            "post_apply_certificate_id": post_apply_certificate.get("certificate_id"),
            "environment": environment,
            "target": migration_pack.target.to_dict(),
            "target_connection": target_connection_public(target_connection),
            "desired": migration_pack.desired,
            "mode": mode,
            "profile": str(options.get("profile") or default_profile(environment)),
            "window": {key: window[key] for key in ("duration_seconds", "interval_seconds", "min_successful_samples")},
            "samples": {"planned": window["planned_samples"]},
            "checks": resolved_checks,
            "canaries": canaries,
            "query_health": query_health_options(options),
            "rollback": dict(migration_pack.rollback),
            "post_apply": dict(post_apply_certificate),
            "remediation": remediation_options(options),
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": [],
        }
        payload["watch_plan_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "post_apply_certificate_id": payload["post_apply_certificate_id"],
                "environment": environment,
                "target": payload["target"],
                "window": payload["window"],
                "samples": payload["samples"],
                "checks": resolved_checks,
                "canaries": canaries,
                "query_health": payload["query_health"],
                "blockers": payload["blockers"],
            }
        )
        return payload


@dataclass(frozen=True, slots=True)
class MigrationWatchRunner:
    """Executes bounded watch samples through injected read-only target ports."""

    def run(self, *, plan: Mapping[str, Any], probe: TargetWatchProbe | None, execute: bool = False) -> dict[str, Any]:
        if plan.get("status") == "blocked":
            return _run_payload(
                plan=plan, status="blocked", execute=execute, samples=(), blockers=strings(plan.get("blockers", []))
            )
        if not execute:
            return _run_payload(
                plan=plan,
                status="dry_run",
                execute=False,
                samples=(),
                warnings=("schema_migration_watch.not_executed",),
            )
        if probe is None:
            return _run_payload(
                plan=plan,
                status="blocked",
                execute=True,
                samples=(),
                blockers=("schema_migration_watch.target_probe_required",),
            )
        planned = planned_samples(plan)
        samples: list[dict[str, Any]] = []
        blockers: list[str] = []
        warnings: list[str] = []
        start = time.perf_counter()
        for index in range(planned):
            sample = _run_sample(plan, probe, index=index + 1)
            samples.append(sample)
            blockers.extend(strings(sample.get("blockers", [])))
            warnings.extend(strings(sample.get("warnings", [])))
            if should_stop_early(plan, blockers):
                break
            sleep_between_samples(plan)
        remediation = MigrationWatchRemediationAdvisor().decide(
            blockers=blockers,
            warnings=warnings,
            rollback_on=rollback_on(plan),
            pack_id=str(plan.get("pack_id") or ""),
        )
        status = "blocked" if blockers or remediation.decision == "rollback_required" else "passed"
        return _run_payload(
            plan=plan,
            status=status,
            execute=True,
            samples=tuple(samples),
            blockers=blockers,
            warnings=warnings,
            remediation=remediation,
            duration_ms=int((time.perf_counter() - start) * 1000),
        )


@dataclass(frozen=True, slots=True)
class MigrationWatchRemediationAdvisor:
    """Maps watch signals to recommend-only remediation decisions."""

    def decide(
        self,
        *,
        blockers: Sequence[str],
        warnings: Sequence[str],
        rollback_on: Sequence[str],
        pack_id: str = "",
    ) -> MigrationWatchRemediationDecision:
        blocker_set = {str(item) for item in blockers}
        if any(matches_rollback_signal(item, rollback_on) for item in blocker_set):
            return MigrationWatchRemediationDecision(
                "rollback_required",
                (rollback_command(pack_id),) if pack_id else (),
            )
        if blocker_set:
            return MigrationWatchRemediationDecision(
                "rollback_recommended", (rollback_command(pack_id),) if pack_id else ()
            )
        if warnings:
            return MigrationWatchRemediationDecision("extend_watch")
        return MigrationWatchRemediationDecision("continue")


@dataclass(frozen=True, slots=True)
class MigrationWatchCertifier:
    """Turns watch run evidence into stable, warning, or blocked certificates."""

    def certify(self, *, run: Mapping[str, Any], profile: str = "stage") -> dict[str, Any]:
        profile = profile if profile in PROFILES else "stage"
        blockers = list(strings(run.get("blockers", [])))
        warnings = list(strings(run.get("warnings", [])))
        remediation = mapping(run.get("remediation"))
        samples = mapping(run.get("samples"))
        if run.get("status") == "dry_run":
            if profile == "advisory":
                warnings.append("schema_migration_watch.dry_run_certificate")
            else:
                blockers.append("schema_migration_watch.execution_required")
        if run.get("status") == "blocked":
            blockers.append("schema_migration_watch.run_blocked")
        if profile in {"stage", "prod_strict", "regulated"} and int(samples.get("passed", 0) or 0) < 1:
            blockers.append("schema_migration_watch.successful_sample_required")
        if profile in {"prod_strict", "regulated"} and remediation.get("decision") in {
            "rollback_recommended",
            "rollback_required",
        }:
            blockers.append("schema_migration_watch.remediation_required")
        status = "blocked" if blockers else "warning" if warnings else "stable"
        payload: dict[str, Any] = {
            "schema_version": WATCH_CERTIFICATE_SCHEMA,
            "status": status,
            "profile": profile,
            "pack_id": run.get("pack_id"),
            "post_apply_certificate_id": run.get("post_apply_certificate_id"),
            "environment": run.get("environment"),
            "target": dict(run.get("target", {})) if isinstance(run.get("target"), Mapping) else {},
            "watch_run_id": run.get("watch_run_id"),
            "samples": dict(samples),
            "remediation": dict(remediation),
            "checks": list(run.get("checks", [])) if isinstance(run.get("checks"), list) else [],
            "blockers": list(dict.fromkeys(blockers)),
            "warnings": list(dict.fromkeys(warnings)),
            "metrics": dict(run.get("metrics", {})) if isinstance(run.get("metrics"), Mapping) else {},
        }
        payload["certificate_id"] = stable_fingerprint(
            {
                "pack_id": payload["pack_id"],
                "run_id": payload["watch_run_id"],
                "profile": profile,
                "status": status,
                "samples": payload["samples"],
                "remediation": payload["remediation"],
                "blockers": payload["blockers"],
                "warnings": payload["warnings"],
            }
        )
        return payload


def _run_sample(plan: Mapping[str, Any], probe: TargetWatchProbe, *, index: int) -> dict[str, Any]:
    check_results: list[dict[str, Any]] = []
    blockers: list[str] = []
    warnings: list[str] = []
    if checks_enabled(plan).get("physical_design", True):
        blockers.extend(physical_blockers(plan, probe.inspect(dict(plan)), check_results))
    if profile_required(plan):
        profile = probe.profile(dict(plan))
        check_results.extend(list(profile.get("checks", [])) if isinstance(profile, Mapping) else [])
        blockers.extend(strings(profile.get("blockers", [])) if isinstance(profile, Mapping) else [])
        warnings.extend(strings(profile.get("warnings", [])) if isinstance(profile, Mapping) else [])
    if checks_enabled(plan).get("canary_queries", True):
        result = run_canaries([item for item in plan.get("canaries", []) if isinstance(item, Mapping)], probe)
        check_results.extend(result["checks"])
        blockers.extend(result["blockers"])
        warnings.extend(result["warnings"])
    if checks_enabled(plan).get("query_health", False):
        health = probe.query_health(dict(plan))
        check_results.extend(list(health.get("checks", [])) if isinstance(health, Mapping) else [])
        blockers.extend(strings(health.get("blockers", [])) if isinstance(health, Mapping) else [])
        warnings.extend(strings(health.get("warnings", [])) if isinstance(health, Mapping) else [])
    if checks_enabled(plan).get("rollback_window", True):
        rollback = rollback_window(plan)
        check_results.append({"name": "rollback_window", "status": rollback["status"]})
        if rollback.get("status") == "closed":
            blockers.append("schema_migration_watch.rollback_window_closing")
    return {
        "sample": index,
        "status": "failed" if blockers else "passed",
        "checks": check_results,
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
    }


def _run_payload(
    *,
    plan: Mapping[str, Any],
    status: str,
    execute: bool,
    samples: Sequence[Mapping[str, Any]],
    blockers: Sequence[str] = (),
    warnings: Sequence[str] = (),
    remediation: MigrationWatchRemediationDecision | None = None,
    duration_ms: int = 0,
) -> dict[str, Any]:
    summary = sample_summary(samples, planned_samples(plan))
    remediation = remediation or MigrationWatchRemediationAdvisor().decide(
        blockers=blockers, warnings=warnings, rollback_on=rollback_on(plan), pack_id=str(plan.get("pack_id") or "")
    )
    payload: dict[str, Any] = {
        "schema_version": WATCH_RUN_SCHEMA,
        "status": status,
        "executed": execute,
        "watch_plan_id": plan.get("watch_plan_id"),
        "pack_id": plan.get("pack_id"),
        "post_apply_certificate_id": plan.get("post_apply_certificate_id"),
        "environment": plan.get("environment"),
        "target": dict(plan.get("target", {})) if isinstance(plan.get("target"), Mapping) else {},
        "samples": summary,
        "sample_results": list(samples),
        "checks": [check for sample in samples for check in sample.get("checks", []) if isinstance(check, Mapping)],
        "remediation": remediation.to_dict(),
        "blockers": list(dict.fromkeys(blockers)),
        "warnings": list(dict.fromkeys(warnings)),
        "metrics": {"duration_ms": duration_ms, "samples_executed": summary["executed"]},
    }
    payload["watch_run_id"] = stable_fingerprint(
        {
            "plan_id": payload["watch_plan_id"],
            "status": status,
            "executed": execute,
            "samples": payload["samples"],
            "checks": payload["checks"],
            "remediation": payload["remediation"],
            "blockers": payload["blockers"],
            "warnings": payload["warnings"],
        }
    )
    return payload


__all__ = [
    "WATCH_CERTIFICATE_SCHEMA",
    "WATCH_PLAN_SCHEMA",
    "WATCH_RUN_SCHEMA",
    "MigrationWatchCertifier",
    "MigrationWatchPlanner",
    "MigrationWatchRemediationAdvisor",
    "MigrationWatchRemediationDecision",
    "MigrationWatchRunner",
    "TargetWatchProbe",
]
