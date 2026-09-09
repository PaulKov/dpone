"""Confined parse-profile acquisition, separate from publishing-policy lookup."""

from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.manifest.bounded_yaml import load_bounded_yaml
from dpone.manifest.confined_files import read_confined_file


def read_workspace_parse_profile(root: Path, project_path: str, *, profiles_dir: Path | None) -> bytes:
    """Capture one bounded regular file; never return parser contents in errors."""

    try:
        selected_root = root if profiles_dir is None else profiles_dir
        relative = (Path(project_path) / "profiles.yml").as_posix() if profiles_dir is None else "profiles.yml"
        content = read_confined_file(selected_root, relative, max_bytes=1024 * 1024)
        if not isinstance(load_bounded_yaml(content), Mapping):
            raise ValueError("profile must be a mapping")
        return content
    except (OSError, ValueError, TypeError, RecursionError):
        raise _invalid(project_path) from None


def require_parse_profile_targets(content: bytes, names: tuple[tuple[str, str], ...], *, project_path: str) -> None:
    """Check named outputs without rendering Jinja or exposing credential values."""

    try:
        profiles = load_bounded_yaml(content)
        for profile_name, target_name in names:
            profile = profiles.get(profile_name) if isinstance(profiles, Mapping) else None
            outputs = profile.get("outputs") if isinstance(profile, Mapping) else None
            if not isinstance(outputs, Mapping) or not isinstance(outputs.get(target_name), Mapping):
                raise ValueError("named profile or target is missing")
    except (ValueError, TypeError, RecursionError):
        raise _invalid(project_path) from None


def _invalid(project_path: str) -> DbtPublishingError:
    return DbtPublishingError(
        "DPONE_DBT_WORKSPACE_PARSE_PROFILE_INVALID",
        "The project's dbt parse profile is missing, unsafe or invalid",
        path=project_path,
        remediation=(
            "Provide a regular profiles.yml (at most 1 MiB) in this project or --dbt-profiles-dir, "
            "with the policy's runtime.dbt_profile and dbt_target outputs. Use non-secret parse-only "
            "credentials; ambient environment variables are not forwarded."
        ),
    )


__all__ = ["read_workspace_parse_profile", "require_parse_profile_targets"]
