"""Bounded subprocess acquisition of non-secret, dbt-rendered profile identity."""

from __future__ import annotations

import os
import subprocess
import sys
from collections.abc import Mapping
from pathlib import Path

from dpone.adapters.deployment_cache_files import open_regular_file
from dpone.contracts.dbt_contract_validation import DbtPublishingError
from dpone.contracts.dbt_invocation import DbtInvocationTarget
from dpone.contracts.strict_json import strict_json_object


class IsolatedDbtParseTargetResolver:
    """Use the active certified interpreter, without inheriting credential env."""

    def resolve(
        self,
        *,
        project_root: Path,
        profiles_dir: Path,
        parse_args: tuple[str, ...],
        environment: Mapping[str, str],
    ) -> DbtInvocationTarget:
        try:
            fd = open_regular_file(
                profiles_dir / "profiles.yml",
                root=profiles_dir,
                missing_code="dbt_parse_profile_invalid",
                invalid_code="dbt_parse_profile_invalid",
                label="captured dbt parse profile",
            )
            with os.fdopen(fd, "rb") as stream:
                profile = stream.read(1024 * 1024 + 1)
            if not profile or len(profile) > 1024 * 1024:
                raise ValueError("captured profile exceeds its byte bound")
            result = subprocess.run(
                (sys.executable, "-I", "-m", "dpone.adapters.dbt_parse_target_probe", *parse_args),
                input=profile,
                stdout=subprocess.PIPE,
                stderr=subprocess.DEVNULL,
                cwd=project_root,
                env=dict(environment),
                shell=False,
                check=False,
                timeout=120,
            )
            if result.returncode != 0 or len(result.stdout) > 8192:
                raise ValueError("rendered profile is unavailable")
            return DbtInvocationTarget.from_mapping(strict_json_object(result.stdout))
        except Exception:
            # Never expose raw profiles, vendor exceptions or child diagnostics.
            raise DbtPublishingError(
                "DPONE_DBT_PROFILE_INVALID", "Canonical dbt parse profile target could not be resolved safely"
            ) from None
