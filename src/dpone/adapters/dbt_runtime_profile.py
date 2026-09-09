"""Render one runtime-only dbt SQL Server profile from a resolved connection."""

from __future__ import annotations

import os
import stat
import tempfile
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING, Any, Protocol

import yaml

from dpone.contracts.dbt_publishing import (
    DbtCredentialVersion,
    DbtProfileSpec,
    DbtPublishingError,
    DbtSqlServerRuntimePolicy,
    dbt_target_identity_sha256,
)
from dpone.ports.dbt_publishing import RenderedDbtProfile

if TYPE_CHECKING:
    from dpone.contracts.runtime_connection import ResolvedBindingConnection


class RuntimeCredentialResolver(Protocol):
    def resolve(self, connection_ref: str) -> ResolvedBindingConnection: ...


class TemporaryDbtProfileStore:
    """Create mode-0600 ``profiles.yml`` and remove its directory on exit."""

    def __init__(self, tmpfs_root: Path) -> None:
        self._root = Path(tmpfs_root).absolute()

    @contextmanager
    def materialize(self, content: bytes) -> Iterator[Path]:
        raw = bytes(content)
        if not raw:
            raise _error("Rendered dbt profile is empty")
        temporary: tempfile.TemporaryDirectory[str] | None = None
        try:
            self._root.mkdir(parents=True, exist_ok=True, mode=0o700)
            mode = self._root.lstat().st_mode
            if not stat.S_ISDIR(mode) or stat.S_ISLNK(mode):
                raise _error("dbt profile tmpfs root is unsafe")
            temporary = tempfile.TemporaryDirectory(prefix="dpone-dbt-profile-", dir=self._root)
            profile_path = Path(temporary.name) / "profiles.yml"
            descriptor = os.open(
                profile_path,
                os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0),
                0o600,
            )
            try:
                pending = memoryview(raw)
                while pending:
                    written = os.write(descriptor, pending)
                    if written <= 0:
                        raise OSError
                    pending = pending[written:]
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
        except DbtPublishingError:
            if temporary is not None:
                temporary.cleanup()
            raise
        except OSError as exc:
            if temporary is not None:
                temporary.cleanup()
            raise _error("Temporary dbt profile could not be materialized safely") from exc
        assert temporary is not None
        try:
            yield profile_path
        finally:
            temporary.cleanup()


class RuntimeDbtProfileRenderer:
    """Convert one workload-scoped credential resolution into private YAML bytes."""

    def __init__(self, resolver: RuntimeCredentialResolver) -> None:
        self._resolver = resolver

    def render(
        self,
        profile: DbtProfileSpec,
        adapter_runtime: DbtSqlServerRuntimePolicy,
    ) -> RenderedDbtProfile:
        if profile.adapter_type != "sqlserver":
            raise _error("Only the certified sqlserver dbt adapter is supported")
        resolved = self._resolver.resolve(profile.connection_ref)
        descriptor = resolved.descriptor
        if descriptor is None or descriptor.connection_type not in {"mssql", "sqlserver"}:
            raise _error("Resolved connection type does not match the sqlserver dbt adapter")
        credentials = resolved.credentials
        values = {
            "server": credentials.host,
            "port": credentials.port or 1433,
            "database": credentials.database,
            "schema": credentials.schema,
            "user": credentials.username,
            "password": credentials.password,
        }
        missing = sorted(name for name, value in values.items() if value in {None, ""})
        if missing:
            raise _error("Resolved SQL Server connection is incomplete")
        if credentials.database != profile.database or credentials.schema != profile.schema:
            raise DbtPublishingError(
                "DPONE_DBT_TARGET_IDENTITY_MISMATCH",
                "Resolved dbt database/schema differs from the release logical target",
            )
        properties = descriptor.properties
        output: dict[str, Any] = {
            "type": "sqlserver",
            "backend": adapter_runtime.backend,
            "driver": str(credentials.driver or properties.get("driver") or "ODBC Driver 18 for SQL Server"),
            **values,
            "authentication": "sql",
            "encrypt": _boolean(
                credentials.encrypt if credentials.encrypt is not None else properties.get("encrypt", True),
                field="encrypt",
            ),
            "trust_cert": _boolean(
                credentials.trust_server_certificate
                if credentials.trust_server_certificate is not None
                else properties.get("trust_cert", False),
                field="trust_cert",
            ),
            "retries": adapter_runtime.retries,
            "login_timeout": adapter_runtime.login_timeout_seconds,
            "query_timeout": adapter_runtime.query_timeout_seconds,
            "threads": profile.threads,
        }
        content = yaml.safe_dump(
            {
                profile.profile_name: {
                    "target": profile.target_name,
                    "outputs": {profile.target_name: output},
                }
            },
            allow_unicode=False,
            sort_keys=True,
        ).encode("utf-8")
        metadata = resolved.safe_metadata
        resolved_version = metadata.get("resolved_version")
        return RenderedDbtProfile(
            content=content,
            credential_versions=(
                DbtCredentialVersion(
                    connection_ref=profile.connection_ref,
                    resolver=str(metadata.get("resolver") or "unknown"),
                    resolved_version=(str(resolved_version) if resolved_version is not None else None),
                ),
            ),
            logical_target_sha256=dbt_target_identity_sha256(profile),
            redaction_values=(str(credentials.password),),
        )


def _boolean(value: object, *, field: str) -> bool:
    if isinstance(value, bool):
        return value
    raise _error(f"Resolved SQL Server {field} must be a boolean")


def _error(message: str) -> DbtPublishingError:
    return DbtPublishingError("DPONE_DBT_PROFILE_INVALID", message)


__all__ = [
    "RuntimeCredentialResolver",
    "RuntimeDbtProfileRenderer",
    "TemporaryDbtProfileStore",
]
