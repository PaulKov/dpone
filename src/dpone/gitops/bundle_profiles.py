from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class GitOpsBundlePolicyDefaults:
    profile: str
    verify_lock: bool
    fail_on_empty_impact: bool
    fail_on_warnings: bool
    require_lock: bool


_PROFILES: dict[str, GitOpsBundlePolicyDefaults] = {
    "custom": GitOpsBundlePolicyDefaults(
        profile="custom",
        verify_lock=False,
        fail_on_empty_impact=False,
        fail_on_warnings=False,
        require_lock=False,
    ),
    "advisory": GitOpsBundlePolicyDefaults(
        profile="advisory",
        verify_lock=False,
        fail_on_empty_impact=False,
        fail_on_warnings=False,
        require_lock=False,
    ),
    "pr": GitOpsBundlePolicyDefaults(
        profile="pr",
        verify_lock=True,
        fail_on_empty_impact=True,
        fail_on_warnings=False,
        require_lock=True,
    ),
    "release": GitOpsBundlePolicyDefaults(
        profile="release",
        verify_lock=True,
        fail_on_empty_impact=True,
        fail_on_warnings=True,
        require_lock=True,
    ),
}


def resolve_bundle_policy_defaults(raw_profile: object) -> GitOpsBundlePolicyDefaults | None:
    profile = str(raw_profile or "custom").strip().lower()
    return _PROFILES.get(profile)


def bundle_policy_profile_names() -> tuple[str, ...]:
    return tuple(_PROFILES)


__all__ = ["GitOpsBundlePolicyDefaults", "bundle_policy_profile_names", "resolve_bundle_policy_defaults"]
