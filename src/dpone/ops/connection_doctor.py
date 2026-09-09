"""Credential-free route connection diagnostics."""

from __future__ import annotations

import os
import shutil
import sys
from collections.abc import Callable, Sequence
from pathlib import Path

from dpone.ops.routes.bootstrap_models import ConnectionDoctorReport, OnboardingCheck
from dpone.ops.routes.bootstrap_policy import (
    checks_blockers,
    checks_score,
    checks_warnings,
    next_actions_for_blockers,
    status_from_blockers,
)
from dpone.ops.routes.catalog import RouteProfileCatalog
from dpone.ops.routes.models import RouteKey
from dpone.readiness.python_import_health import PythonImportHealth, probe_python_import
from dpone.readiness.python_import_remediation import python_import_remediation

ToolResolver = Callable[[str], str | None]
ImportProbe = Callable[[str], PythonImportHealth]


class ConnectionDoctorService:
    """Check route prerequisites without opening source or sink connections."""

    def __init__(
        self,
        *,
        catalog: RouteProfileCatalog | None = None,
        tool_resolver: ToolResolver | None = None,
        import_probe: ImportProbe = probe_python_import,
    ) -> None:
        self._catalog = catalog or RouteProfileCatalog.default()
        self._tool_resolver = tool_resolver or _default_tool_resolver
        self._import_probe = import_probe

    def check(
        self,
        *,
        output_dir: str | Path,
        source: str,
        sink: str,
        strategy: str,
        required_tools: Sequence[str] = (),
        optional_tools: Sequence[str] = (),
        required_env: Sequence[str] = (),
        optional_env: Sequence[str] = (),
        python_imports: Sequence[str] = (),
    ) -> ConnectionDoctorReport:
        directory = Path(output_dir)
        route = RouteKey.of(source, sink, strategy)
        profile = self._catalog.get(route)
        checks = [
            *_profile_checks(profile),
            *self._tool_checks(required_tools, required=True),
            *self._tool_checks(optional_tools, required=False),
            *_env_checks(required_env, required=True),
            *_env_checks(optional_env, required=False),
            *self._import_checks(python_imports),
        ]
        blockers = list(checks_blockers(checks))
        if profile is None:
            blockers.insert(0, f"route.unsupported:{route.colon_id}")
        warnings = checks_warnings(checks)
        status = status_from_blockers(blockers, warnings)
        report = ConnectionDoctorReport(
            route=route,
            profile=profile,
            passed=status != "blocked",
            status=status,
            score=checks_score(checks),
            checks=tuple(checks),
            blockers=tuple(dict.fromkeys(blockers)),
            warnings=warnings,
            next_actions=next_actions_for_blockers(
                blockers,
                ready_action="Run source discovery or route-bootstrap for this route.",
            ),
            output_dir=str(directory),
            json_path=str(directory / "connection_doctor.json"),
            markdown_path=str(directory / "connection_doctor.md"),
        )
        report.write()
        return report

    def _tool_checks(self, names: Sequence[str], *, required: bool) -> tuple[OnboardingCheck, ...]:
        checks: list[OnboardingCheck] = []
        for raw_name in names:
            name = str(raw_name).strip()
            if not name:
                continue
            resolved = self._tool_resolver(name)
            checks.append(
                OnboardingCheck(
                    name=f"tool.{name}",
                    kind="tool",
                    required=required,
                    passed=resolved is not None,
                    status="ready" if resolved else ("blocked" if required else "warning"),
                    summary=("Executable is available on PATH." if resolved else "Executable was not found on PATH."),
                    remediation=f"Install `{name}` or make it available on PATH.",
                )
            )
        return tuple(checks)

    def _import_checks(self, names: Sequence[str]) -> tuple[OnboardingCheck, ...]:
        checks: list[OnboardingCheck] = []
        for raw_name in names:
            name = str(raw_name).strip()
            if not name:
                continue
            health = self._import_probe(name)
            checks.append(
                OnboardingCheck(
                    name=f"python_import.{name}",
                    kind="python_import",
                    required=True,
                    passed=health.passed,
                    status="ready" if health.passed else "blocked",
                    summary=health.summary,
                    remediation=(
                        ""
                        if health.passed
                        else python_import_remediation(
                            health.reason_code,
                            not_installed=("Install and verify the requested Python runtime dependency"),
                            load_failed=(
                                "Repair the requested Python module's native runtime dependencies, "
                                "then rerun the doctor"
                            ),
                        )
                    ),
                )
            )
        return tuple(checks)


def _profile_checks(profile: object | None) -> tuple[OnboardingCheck, ...]:
    if profile is None:
        return tuple()
    extras = tuple(getattr(profile, "install_extras", ()))
    return tuple(
        OnboardingCheck(
            name=f"extra.{extra}",
            kind="python_extra",
            required=False,
            passed=True,
            status="ready",
            summary=f"Install with dpone[{extra}] when using this route.",
        )
        for extra in extras
    )


def _env_checks(names: Sequence[str], *, required: bool) -> tuple[OnboardingCheck, ...]:
    checks: list[OnboardingCheck] = []
    for raw_name in names:
        name = str(raw_name).strip()
        if not name:
            continue
        present = bool(os.environ.get(name))
        checks.append(
            OnboardingCheck(
                name=f"env.{name}",
                kind="environment",
                required=required,
                passed=present,
                status="ready" if present else ("blocked" if required else "warning"),
                summary=("Environment variable is set." if present else "Environment variable is not set."),
                remediation=f"Set `{name}` in the execution environment.",
            )
        )
    return tuple(checks)


def _default_tool_resolver(name: str) -> str | None:
    resolved = shutil.which(name)
    if resolved:
        return resolved
    if name == "python" and sys.executable:
        return sys.executable
    return None
