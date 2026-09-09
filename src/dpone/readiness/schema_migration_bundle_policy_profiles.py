"""Built-in schema migration bundle gate profiles."""

from __future__ import annotations

from dpone.readiness.schema_migration_bundle_policy import (
    APPROVAL_IMPACT_REQUIRED,
    APPROVAL_NONE,
    MigrationBundleApprovalPolicy,
    MigrationBundlePolicyOptions,
)


class MigrationBundlePolicyProfileRegistry:
    """Built-in gate profiles for CI/CD release stages."""

    def resolve(self, profile: str | None) -> MigrationBundlePolicyOptions:
        normalized = str(profile or "pr_review").strip().lower()
        try:
            return _PROFILES[normalized]
        except KeyError as exc:
            raise ValueError(f"unknown policy profile: {profile}") from exc

    def names(self) -> tuple[str, ...]:
        return tuple(_PROFILES)


_PROFILES: dict[str, MigrationBundlePolicyOptions] = {
    "advisory": MigrationBundlePolicyOptions(
        profile="advisory",
        require_attestation=False,
        required_artifacts=("migration_pack",),
        require_approved_risks=APPROVAL_NONE,
    ),
    "pr_review": MigrationBundlePolicyOptions(
        profile="pr_review",
        require_attestation=True,
        required_artifacts=("migration_pack", "impact_plan"),
        require_approved_risks=APPROVAL_IMPACT_REQUIRED,
    ),
    "stage_certified": MigrationBundlePolicyOptions(
        profile="stage_certified",
        require_attestation=True,
        required_artifacts=("migration_pack", "impact_plan", "environment_contract", "certification"),
        require_approved_risks=APPROVAL_NONE,
    ),
    "prod_strict": MigrationBundlePolicyOptions(
        profile="prod_strict",
        require_attestation=True,
        required_artifacts=(
            "migration_pack",
            "impact_plan",
            "approval",
            "environment_contract",
            "certification",
            "promotion",
        ),
        require_approved_risks=APPROVAL_IMPACT_REQUIRED,
        fail_on_warnings=True,
    ),
    "regulated": MigrationBundlePolicyOptions(
        profile="regulated",
        require_attestation=True,
        required_artifacts=(
            "migration_pack",
            "impact_plan",
            "approval",
            "environment_contract",
            "certification",
            "promotion",
        ),
        require_approved_risks=APPROVAL_IMPACT_REQUIRED,
        fail_on_warnings=True,
        approval=MigrationBundleApprovalPolicy(require_approved_by=True, require_not_expired=True),
    ),
}


__all__ = ["MigrationBundlePolicyProfileRegistry"]
