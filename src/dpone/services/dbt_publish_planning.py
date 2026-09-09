"""Build-time strategy and ClickHouse physical-design decisions."""

from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from dpone.contracts.dbt_publish_models import (
    DbtModelArtifact,
    DbtPublishIntent,
    DbtPublishIssue,
    DbtPublishProfile,
    DbtPublishStrategyPolicy,
)
from dpone.contracts.dbt_unique_key_policy import (
    DBT_UNIQUE_KEY_INVALID,
    DbtUniqueKeyPolicyReport,
    evaluate_model_dbt_unique_key,
)


class DbtPublishPlanner:
    """Makes deterministic decisions that are frozen into generated artifacts."""

    def strategy(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        policy: DbtPublishStrategyPolicy,
        *,
        supported_strategies: tuple[str, ...] | None = None,
    ) -> tuple[dict[str, Any], tuple[DbtPublishIssue, ...]]:
        requested_mode = intent.strategy_mode
        mode = requested_mode
        reason = "explicit_model_meta"
        unique_key = intent.unique_key or model.unique_key
        key_report = evaluate_model_dbt_unique_key(unique_key, model=model)
        key_mismatch = bool(intent.unique_key and model.unique_key and intent.unique_key != model.unique_key)
        if mode == "auto":
            candidates = self.strategy_candidates(model, intent, policy)
            supported = set(candidates) if supported_strategies is None else set(supported_strategies)
            mode = next(
                (candidate for candidate in candidates if candidate in supported),
                "unresolved",
            )
            reason = _auto_strategy_reason(mode)
        strategy: dict[str, Any] = {"mode": mode, "decision_reason": reason}
        policy_issues = self._policy_issues(model, mode, policy)
        key_issues = self._key_issues(
            model,
            key_report,
            mismatch=key_mismatch,
            required=(
                mode == "incremental_merge"
                or (requested_mode == "auto" and model.materialized == "incremental" and bool(unique_key))
            ),
        )
        if mode == "incremental_merge" and not key_issues:
            strategy["unique_key"] = list(key_report.keys)
        if mode == "full_refresh":
            strategy["max_source_bytes"] = policy.full_refresh_max_source_bytes
        if mode == "partition_replace":
            strategy["partition"] = {
                "column": intent.partition_key,
                "values_from_staging": True,
                "max_partitions_per_run": max(1, intent.window_days or 64),
                "native_mode": "auto",
            }
            strategy["window_days"] = intent.window_days
            strategy["requires_atomic_capability"] = policy.partition_replace_requires_atomic_capability
        return strategy, (
            *policy_issues,
            *key_issues,
            *self._strategy_issues(model, mode, intent),
        )

    @staticmethod
    def strategy_candidates(
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        policy: DbtPublishStrategyPolicy,
    ) -> tuple[str, ...]:
        """Return policy-authorized candidates in deterministic priority."""

        if intent.strategy_mode != "auto":
            return (intent.strategy_mode,) if policy.allows(intent.strategy_mode) else ()
        candidates = []
        if (
            model.materialized == "incremental"
            and _admitted_effective_key(model, intent)
            and policy.allows("incremental_merge")
        ):
            candidates.append("incremental_merge")
        if intent.partition_key and intent.window_days and policy.allows("partition_replace"):
            candidates.append("partition_replace")
        if policy.allows("full_refresh") and policy.full_refresh_max_source_bytes is not None:
            candidates.append("full_refresh")
        return tuple(candidates)

    def physical_design(
        self,
        model: DbtModelArtifact,
        intent: DbtPublishIntent,
        profile: DbtPublishProfile,
    ) -> tuple[dict[str, Any], tuple[DbtPublishIssue, ...]]:
        defaults = dict(profile.physical_design)
        clickhouse = dict(_mapping(defaults.get("clickhouse")) or defaults)
        replica_count = int(clickhouse.pop("replica_count", 1) or 1)
        engine = intent.engine or _text(clickhouse.get("engine"))
        warnings = []
        if not engine:
            engine = (
                "ReplicatedMergeTree('/clickhouse/tables/{uuid}/{shard}', '{replica}')"
                if replica_count > 1
                else "MergeTree"
            )
        if replica_count > 1 and engine.lstrip().startswith("MergeTree"):
            warnings.append(
                DbtPublishIssue(
                    code="DPONE_DBT_NON_REPLICATED_ENGINE",
                    message=f"Profile declares {replica_count} replicas but model requests {engine}",
                    path=f"{model.original_file_path}#physical_design.engine",
                    severity="warning",
                    remediation="Use the replicated_mart profile or an explicit ReplicatedMergeTree engine.",
                )
            )
        order_by = list(intent.order_by or _string_tuple(clickhouse.get("order_by")))
        if not order_by:
            order_by = list(_admitted_effective_key(model, intent) or model.columns[:1])
        design: dict[str, Any] = {**clickhouse, "engine": engine, "order_by": order_by}
        partition_by = intent.partition_by or _optional_text(clickhouse.get("partition_by"))
        if partition_by:
            design["partition_by"] = partition_by
        return {"storage": {"clickhouse": design}}, tuple(warnings)

    @staticmethod
    def _strategy_issues(
        model: DbtModelArtifact,
        mode: str,
        intent: DbtPublishIntent,
    ) -> tuple[DbtPublishIssue, ...]:
        issues = []
        if mode == "unresolved":
            issues.append(
                _blocker(
                    model,
                    "DPONE_DBT_STRATEGY_UNRESOLVED",
                    "auto strategy requires a certified merge/partition route or an explicit policy-authorized mode",
                )
            )
        if mode == "partition_replace":
            if not intent.partition_key:
                issues.append(
                    _blocker(model, "DPONE_DBT_PARTITION_KEY_MISSING", "partition_replace requires partition_key")
                )
            elif model.columns and intent.partition_key not in model.columns:
                issues.append(
                    _blocker(
                        model,
                        "DPONE_DBT_PARTITION_KEY_NOT_IN_CONTRACT",
                        f"partition_key {intent.partition_key!r} is absent from the dbt contract",
                    )
                )
        return tuple(issues)

    @staticmethod
    def _key_issues(
        model: DbtModelArtifact,
        report: DbtUniqueKeyPolicyReport,
        *,
        mismatch: bool,
        required: bool,
    ) -> tuple[DbtPublishIssue, ...]:
        if mismatch:
            return (
                _blocker(
                    model,
                    DBT_UNIQUE_KEY_INVALID,
                    ("publish strategy unique_key must exactly match dbt config.unique_key when both are declared"),
                ),
            )
        if not required:
            return ()
        return tuple(
            _blocker(model, issue.code, f"incremental_merge unique_key must {issue.expectation}")
            for issue in report.issues
        )

    @staticmethod
    def _policy_issues(
        model: DbtModelArtifact,
        mode: str,
        policy: DbtPublishStrategyPolicy,
    ) -> tuple[DbtPublishIssue, ...]:
        if mode == "unresolved":
            return ()
        if not policy.allows(mode):
            return (
                _blocker(
                    model,
                    "DPONE_DBT_STRATEGY_UNRESOLVED",
                    f"strategy {mode!r} is not authorized by the selected publish profile",
                ),
            )
        if mode == "full_refresh" and policy.full_refresh_max_source_bytes is None:
            return (
                _blocker(
                    model,
                    "DPONE_DBT_STRATEGY_UNRESOLVED",
                    "full_refresh requires a positive platform-owned source byte budget",
                ),
            )
        return ()


def _blocker(model: DbtModelArtifact, code: str, message: str) -> DbtPublishIssue:
    return DbtPublishIssue(code=code, message=message, path=model.original_file_path)


def _auto_strategy_reason(mode: str) -> str:
    return {
        "incremental_merge": "dbt_incremental_with_unique_key",
        "partition_replace": "certified_partition_window",
        "full_refresh": "policy_authorized_bounded_full_refresh",
    }.get(mode, "no_policy_and_capability_authorized_safe_strategy")


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _text(value: object) -> str:
    return str(value or "").strip()


def _optional_text(value: object) -> str | None:
    return _text(value) or None


def _admitted_effective_key(
    model: DbtModelArtifact,
    intent: DbtPublishIntent,
) -> tuple[str, ...]:
    if intent.unique_key and model.unique_key and intent.unique_key != model.unique_key:
        return ()
    report = evaluate_model_dbt_unique_key(
        intent.unique_key or model.unique_key,
        model=model,
    )
    return report.keys if report.passed else ()


def _string_tuple(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        return tuple(item.strip() for item in value.split(",") if item.strip())
    if isinstance(value, list | tuple):
        return tuple(str(item).strip() for item in value if str(item).strip())
    return ()


__all__ = ["DbtPublishPlanner"]
