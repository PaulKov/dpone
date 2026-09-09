"""Actual isolated dbt profile rendering, no network or SQL execution."""

import json
import subprocess
import sys

import pytest
import yaml

from dpone.adapters.dbt_parse_target import IsolatedDbtParseTargetResolver
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_invocation import DbtInvocationContext
from tests.test_dbt_workspace_real_toolchain import _profile
from tests.test_dbt_workspace_real_toolchain import exact_dbt as exact_dbt


def _context(tmp_path, profiles):
    (tmp_path / "profiles.yml").write_text(yaml.safe_dump(profiles))
    invocation = DbtInvocationContext.canonical()
    return {
        "project_root": tmp_path,
        "profiles_dir": tmp_path,
        "parse_args": (
            "--project-dir",
            str(tmp_path),
            "--profiles-dir",
            str(tmp_path),
            "--profile",
            "alpha",
            "--target",
            "parse_alpha",
            "--target-path",
            str(tmp_path / "target"),
            "--log-path",
            str(tmp_path / "logs"),
            "--vars",
            invocation.selection_vars_json(),
        ),
        "environment": invocation.environment(home=str(tmp_path / "home")),
    }


def test_actual_profile_render_uses_canonical_vars_environment_and_parse_flags(tmp_path, exact_dbt):
    profiles = _profile("alpha")
    selected = profiles["alpha"]["outputs"]["parse_alpha"]
    selected["database"] = "{{ env_var('LANG') | replace('.', '_') | replace('-', '_') }}"
    selected["schema"] = "{{ var('dpone_data_interval_start')[:4] }}_{{ flags.WHICH }}"
    profiles["alpha"]["outputs"]["unused"] = {"type": "sqlserver", "password": "{{ env_var('UNAVAILABLE') }}"}
    result = IsolatedDbtParseTargetResolver().resolve(**_context(tmp_path, profiles))
    assert result.to_dict() == {"database": "C_UTF_8", "schema": "2000_parse"}


@pytest.mark.parametrize("invalid", ["missing_env", "bad_port", "wrong_adapter"])
def test_invalid_profile_returns_sanitized_failure(tmp_path, exact_dbt, invalid):
    profiles = _profile("alpha")
    selected = profiles["alpha"]["outputs"]["parse_alpha"]
    if invalid == "missing_env":
        selected["schema"] = "{{ env_var('DUMMY_SECRET_MUST_NOT_ESCAPE') }}"
    elif invalid == "bad_port":
        selected["port"] = "DUMMY_SECRET_MUST_NOT_ESCAPE"
    else:
        selected["type"] = "DUMMY_SECRET_MUST_NOT_ESCAPE"
    with pytest.raises(DbtPublishingError, match="could not be resolved safely") as caught:
        IsolatedDbtParseTargetResolver().resolve(**_context(tmp_path, profiles))
    assert "DUMMY_SECRET" not in str(caught.value) and caught.value.__suppress_context__


def test_probe_does_not_emit_jinja_log_or_profile_material(tmp_path, exact_dbt):
    profiles = _profile("alpha")
    profiles["alpha"]["outputs"]["parse_alpha"]["schema"] = "{{ log('DUMMY_SECRET_MUST_NOT_ESCAPE', info=True) }}alpha"
    context = _context(tmp_path, profiles)
    result = subprocess.run(
        (sys.executable, "-I", "-m", "dpone.adapters.dbt_parse_target_probe", *context["parse_args"]),
        input=yaml.safe_dump(profiles).encode(),
        capture_output=True,
        env=context["environment"],
        timeout=120,
    )
    assert result.returncode == 0
    assert json.loads(result.stdout) == {"database": "DWH_Stage", "schema": "alpha"}
    assert result.stderr == b"" and b"DUMMY_SECRET" not in result.stdout


def test_profile_symlink_is_rejected_before_sdk_acquisition(tmp_path):
    context = _context(tmp_path, _profile("alpha"))
    profile = tmp_path / "profiles.yml"
    profile.rename(tmp_path / "original.yml")
    profile.symlink_to(tmp_path / "original.yml")
    with pytest.raises(DbtPublishingError, match="could not be resolved safely"):
        IsolatedDbtParseTargetResolver().resolve(**context)


@pytest.mark.parametrize("package", ["dpone", "dbt"])
def test_project_cannot_shadow_probe_or_dbt_sdk(tmp_path, exact_dbt, package):
    shadow = tmp_path / package
    shadow.mkdir()
    (shadow / "__init__.py").write_text('print(\'{"database": "FORGED", "schema": "FORGED"}\')\nraise SystemExit(0)\n')
    result = IsolatedDbtParseTargetResolver().resolve(**_context(tmp_path, _profile("alpha")))
    assert result.to_dict() == {"database": "DWH_Stage", "schema": "alpha"}
