"""Bounded snapshots of target-visible CPython user-site state."""

from __future__ import annotations

import site
from dataclasses import dataclass

_MAX_SITE_PATH_CHARACTERS = 64 * 1024


@dataclass(frozen=True, slots=True)
class UserSiteSnapshot:
    """Cached target-visible user-site globals."""

    user_base: str | None
    user_site: str | None
    enabled: bool | None


def capture_user_site_snapshot() -> UserSiteSnapshot | None:
    """Capture bounded CPython site globals without executing site artifacts."""

    user_base = _optional_site_path(site.USER_BASE)
    user_site = _optional_site_path(site.USER_SITE)
    enabled = site.ENABLE_USER_SITE
    if (
        user_base is _INVALID
        or user_site is _INVALID
        or (enabled is not True and enabled is not False and enabled is not None)
    ):
        return None
    assert user_base is None or type(user_base) is str
    assert user_site is None or type(user_site) is str
    return UserSiteSnapshot(
        user_base=user_base,
        user_site=user_site,
        enabled=enabled,
    )


_INVALID = object()


def _optional_site_path(value: object) -> str | None | object:
    if value is None:
        return None
    if type(value) is not str or len(value) > _MAX_SITE_PATH_CHARACTERS or "\x00" in value:
        return _INVALID
    return value


__all__ = ["UserSiteSnapshot", "capture_user_site_snapshot"]
