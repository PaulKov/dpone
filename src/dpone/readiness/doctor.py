"""Self-service environment diagnostics for dpone."""

from __future__ import annotations

import os
import platform
import shutil
import sys
from collections.abc import Callable
from dataclasses import dataclass
from importlib import metadata
from urllib.request import urlopen

from dpone.readiness.python_import_health import PythonImportHealth, probe_python_import
from dpone.readiness.python_import_remediation import python_import_remediation


@dataclass(frozen=True)
class DoctorCheck:
    name: str
    status: str
    message: str
    required: bool = True

    def to_dict(self) -> dict[str, object]:
        """Return the stable public doctor-check v1 projection."""

        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "required": self.required,
        }


class DoctorService:
    def __init__(
        self,
        *,
        import_probe: Callable[[str], PythonImportHealth] = probe_python_import,
        binary_resolver: Callable[[str], str | None] = shutil.which,
    ) -> None:
        self._import_probe = import_probe
        self._binary_resolver = binary_resolver

    def run(self, *, profile: str = "local") -> dict[str, object]:
        mssql_required = profile == "mssql"
        pyodbc_health = self._import_probe("pyodbc")
        checks = [
            self._python(),
            self._binary("docker", required=profile == "production"),
            self._module("pyodbc", required=mssql_required, health=pyodbc_health),
            self._module("psycopg", required=False),
            self._module("clickhouse_driver", required=False),
            self._module("confluent_kafka", required=False),
            self._distribution("google-cloud-bigquery", "google.cloud.bigquery", required=False),
            self._binary("bcp", required=mssql_required),
            self._binary("sqlcmd", required=False),
            self._binary("clickhouse-client", required=False),
            self._endpoint("schema-registry", os.getenv("DPONE_SCHEMA_REGISTRY_URL"), required=False),
            self._endpoint("clickhouse-http", os.getenv("DPONE_CLICKHOUSE_HTTP_URL"), required=False),
        ]
        passed = all(item.status == "pass" or not item.required for item in checks)
        return {
            "passed": passed,
            "profile": profile,
            "checks": [item.to_dict() for item in checks],
            "fixes": self._fixes(
                checks,
                profile=profile,
                pyodbc_health=pyodbc_health,
            ),
        }

    def _python(self) -> DoctorCheck:
        version = f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        status = "pass" if sys.version_info >= (3, 10) else "fail"
        return DoctorCheck("python", status, f"Python {version} on {platform.platform()}")

    def _module(
        self,
        module_name: str,
        *,
        required: bool,
        health: PythonImportHealth | None = None,
    ) -> DoctorCheck:
        assessment = health if health is not None else self._import_probe(module_name)
        return DoctorCheck(
            module_name,
            "pass" if assessment.passed else ("fail" if required else "warn"),
            assessment.summary,
            required,
        )

    def _distribution(self, package_name: str, display_name: str, *, required: bool) -> DoctorCheck:
        try:
            version = metadata.version(package_name)
        except metadata.PackageNotFoundError:
            return DoctorCheck(
                display_name,
                "fail" if required else "warn",
                "distribution is not installed",
                required,
            )
        return DoctorCheck(display_name, "pass", f"installed {version}", required)

    def _binary(self, binary: str, *, required: bool) -> DoctorCheck:
        path = self._binary_resolver(binary)
        return DoctorCheck(
            binary,
            "pass" if path else ("fail" if required else "warn"),
            "executable is available on PATH" if path else "executable was not found on PATH",
            required,
        )

    def _endpoint(self, name: str, url: str | None, *, required: bool) -> DoctorCheck:
        if not url:
            return DoctorCheck(
                name,
                "fail" if required else "warn",
                "endpoint is not configured",
                required,
            )
        try:
            with urlopen(url, timeout=2) as response:  # noqa: S310 - diagnostic URL comes from user env.
                status = getattr(response, "status", 200)
            return DoctorCheck(
                name,
                "pass" if status < 500 else ("fail" if required else "warn"),
                f"HTTP {status}",
                required,
            )
        except Exception:  # noqa: BLE001 - diagnostics normalize every transport failure.
            return DoctorCheck(
                name,
                "fail" if required else "warn",
                "endpoint request failed",
                required,
            )

    def _fixes(
        self,
        checks: list[DoctorCheck],
        *,
        profile: str,
        pyodbc_health: PythonImportHealth,
    ) -> list[str]:
        fixes: list[str] = []
        missing = {check.name for check in checks if check.status != "pass"}
        if not pyodbc_health.passed:
            fixes.append(
                python_import_remediation(
                    pyodbc_health.reason_code,
                    not_installed="Install MSSQL extra: pip install 'dpone[mssql]'",
                    load_failed=("Install a loadable unixODBC runtime, then reinstall the MSSQL extra"),
                )
            )
        if profile != "mssql" and "confluent_kafka" in missing:
            fixes.append("Install Kafka extra: pip install 'dpone[kafka]'")
        if "bcp" in missing or "sqlcmd" in missing:
            fixes.append("Install Microsoft mssql-tools18 and ensure bcp/sqlcmd are on PATH")
        if profile != "mssql" and "clickhouse-client" in missing:
            fixes.append("Install clickhouse-client or use ClickHouse HTTP bulk mode")
        if profile != "mssql" and "docker" in missing:
            fixes.append("Install Docker Desktop or run production checks on a host with Docker")
        return fixes
