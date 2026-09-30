"""Read-only readiness for the optional MSSQL SqlClient companion."""

from __future__ import annotations

from importlib import import_module
from typing import Any


class MssqlSqlClientReadinessService:
    """Report stable blocker codes without importing the package on dpone startup."""

    def doctor(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "schema_version": "dpone.mssql-sqlclient.doctor.v1",
            "backend": "mssql_sqlclient",
            "ready": False,
            "blocker_codes": [],
        }
        try:
            provider = import_module("dpone_mssql_sqlclient")
        except ImportError:
            payload["blocker_codes"] = ["mssql_sqlclient.optional_package_required"]
            return payload
        try:
            companion = provider.locate()
        except Exception as error:
            code = str(error)
            payload["blocker_codes"] = [
                code if code.startswith("mssql_sqlclient.") else "mssql_sqlclient.companion_unavailable"
            ]
            return payload
        payload.update(
            ready=True,
            blocker_codes=[],
            package_version=companion.package_version,
            protocol=companion.protocol,
            runtime_major=companion.runtime_major,
            artifact_sha256=companion.artifact_sha256,
            writer_identity_sha256=companion.writer_identity_sha256,
            runtime_identity_sha256=companion.runtime_identity_sha256,
        )
        return payload


__all__ = ["MssqlSqlClientReadinessService"]
