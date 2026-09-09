"""Parse-only profiles are captured, confined, bounded and never diagnostics."""

import pytest

from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.manifest.dbt_workspace_profiles import read_workspace_parse_profile, require_parse_profile_targets

PROFILE = b"parse_only:\n  outputs:\n    check:\n      type: sqlserver\n      password: PRIVATE_MARKER\n"


def test_local_and_explicit_profile_capture(tmp_path):
    project = tmp_path / "alpha"
    project.mkdir()
    (project / "profiles.yml").write_bytes(PROFILE)
    assert read_workspace_parse_profile(tmp_path, "alpha", profiles_dir=None) == PROFILE
    assert read_workspace_parse_profile(tmp_path, "beta", profiles_dir=project) == PROFILE
    require_parse_profile_targets(PROFILE, (("parse_only", "check"),), project_path="alpha")


@pytest.mark.parametrize("invalid", ["missing", "symlink", "directory", "oversized", "malformed", "duplicate", "list"])
def test_invalid_profile_has_safe_project_diagnostic(tmp_path, invalid):
    project = tmp_path / "alpha"
    project.mkdir()
    path = project / "profiles.yml"
    if invalid == "symlink":
        private = tmp_path / "private.yml"
        private.write_bytes(PROFILE)
        path.symlink_to(private)
    elif invalid == "directory":
        path.mkdir()
    elif invalid != "missing":
        path.write_bytes(
            {
                "oversized": b"x" * (1024 * 1024 + 1),
                "malformed": b"PRIVATE_MARKER: [",
                "duplicate": b"PRIVATE_MARKER: 1\nPRIVATE_MARKER: 2",
                "list": b"- PRIVATE_MARKER",
            }[invalid]
        )
    with pytest.raises(DbtPublishingError) as failure:
        read_workspace_parse_profile(tmp_path, "alpha", profiles_dir=None)
    assert failure.value.code == "DPONE_DBT_WORKSPACE_PARSE_PROFILE_INVALID"
    assert failure.value.path == "alpha"
    assert "PRIVATE_MARKER" not in str(failure.value)
    assert "profiles.yml" in failure.value.remediation


@pytest.mark.parametrize("names", [(("missing", "check"),), (("parse_only", "missing"),)])
def test_missing_profile_or_target_does_not_expose_content(names):
    with pytest.raises(DbtPublishingError) as failure:
        require_parse_profile_targets(PROFILE, names, project_path="beta")
    assert failure.value.path == "beta"
    assert "PRIVATE_MARKER" not in str(failure.value)
