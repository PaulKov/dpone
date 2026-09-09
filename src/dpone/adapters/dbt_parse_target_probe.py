"""Private subprocess protocol: captured profile in, bounded target identity out.

No tracking, adapter connection registration or SQL execution is initialized.
SDK stdout/stderr (including native writes) is discarded before SDK import.
Only the final non-secret target is emitted through a saved protocol descriptor.
"""

from __future__ import annotations

import importlib.metadata
import json
import os
import sys


def _target(profile_bytes: bytes, parse_args: list[str]) -> dict[str, object]:
    from dbt.cli.flags import Flags
    from dbt.cli.main import cli, parse
    from dbt.clients.yaml_helper import load_yaml_text
    from dbt.config.profile import Profile
    from dbt.config.renderer import ProfileRenderer
    from dbt.flags import set_flags
    from dbt_common.context import set_invocation_context

    from dpone.contracts.dbt_invocation import DbtInvocationTarget
    from dpone.contracts.dbt_toolchain import DBT_SQLSERVER_1_12_CERTIFIED

    certified = DBT_SQLSERVER_1_12_CERTIFIED
    if (
        importlib.metadata.version("dbt-core") != certified.dbt_core_version
        or importlib.metadata.version("dbt-sqlserver") != certified.adapter_version
    ):
        raise ValueError("unsupported profile toolchain")
    set_invocation_context(dict(os.environ))
    with cli.make_context("dbt", ["--quiet", "--no-use-colors", "parse"]) as root:
        with parse.make_context("parse", parse_args, parent=root) as context:
            flags = Flags(context)
            set_flags(flags)
            raw_profiles = load_yaml_text(profile_bytes.decode("utf-8"))
            if not isinstance(raw_profiles, dict):
                raise ValueError("profile must be an object")
            profile = Profile.from_raw_profiles(
                raw_profiles,
                flags.PROFILE,
                ProfileRenderer(flags.VARS),
                target_override=flags.TARGET,
            )
            if profile.credentials.type != certified.adapter_name:
                raise ValueError("unsupported profile adapter")
            return DbtInvocationTarget(profile.credentials.database, profile.credentials.schema).to_dict()


def main() -> int:
    protocol = os.dup(1)
    try:
        with open(os.devnull, "wb") as sink:
            os.dup2(sink.fileno(), 1)
            os.dup2(sink.fileno(), 2)
        profile_bytes = sys.stdin.buffer.read(1024 * 1024 + 1)
        if not profile_bytes or len(profile_bytes) > 1024 * 1024:
            return 2
        payload = json.dumps(_target(profile_bytes, sys.argv[1:]), allow_nan=False, sort_keys=True).encode()
        if len(payload) > 8192:
            return 2
        with os.fdopen(protocol, "wb", closefd=False) as stream:
            stream.write(payload)
        return 0
    except Exception:
        return 2
    finally:
        os.close(protocol)


if __name__ == "__main__":
    raise SystemExit(main())
