"""Credential-free host interface to explicitly selected disposable Docker services."""

from __future__ import annotations

import os
import subprocess
from dataclasses import dataclass
from pathlib import Path

import pytest


@dataclass(frozen=True)
class DockerNativeStand:
    docker: str
    sql_container: str
    ch_container: str

    def command(self, arguments: list[str], data: bytes | None = None) -> bytes:
        result = subprocess.run([self.docker, *arguments], input=data, capture_output=True, timeout=120)
        if result.returncode:
            raise AssertionError(result.stdout.decode(errors="replace") + result.stderr.decode(errors="replace"))
        return result.stdout

    def sql(self, query: str) -> bytes:
        # Credentials remain inside the owned SQL container and never enter argv/logs.
        return self.command(
            [
                "exec",
                "-i",
                self.sql_container,
                "bash",
                "-c",
                '/opt/mssql-tools18/bin/sqlcmd -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -C -b -h -1 -W',
            ],
            ("SET NOCOUNT ON;\n" + query).encode(),
        )

    def export(self, table: str, path: Path, *, empty: bool = False) -> bytes:
        query = f"SELECT * FROM dbo.[{table}]" + (" WHERE 1=0" if empty else " ORDER BY [before]")
        remote = f"/tmp/{table}.bcp"
        self.command(
            [
                "exec",
                self.sql_container,
                "bash",
                "-c",
                '/opt/mssql-tools18/bin/bcp "$1" queryout "$2" -S localhost -U sa -P "$MSSQL_SA_PASSWORD" -n -u',
                "bcp-export",
                query,
                remote,
            ]
        )
        payload = self.command(["exec", self.sql_container, "cat", remote])
        self.command(["exec", self.sql_container, "rm", remote])
        path.write_bytes(payload)
        return payload

    def ch(self, query: str, data: bytes | None = None) -> bytes:
        return self.command(
            [
                "exec",
                "-i",
                self.ch_container,
                "clickhouse-client",
                "--query",
                query,
            ],
            data,
        )


def configured_stand() -> DockerNativeStand:
    names = [os.getenv(key) for key in ("DPONE_IT_NATIVE_DOCKER_SQL", "DPONE_IT_NATIVE_DOCKER_CH")]
    if not all(names):
        pytest.skip("Explicit disposable Docker container names are required")
    return DockerNativeStand(os.getenv("DPONE_IT_DOCKER", "docker"), str(names[0]), str(names[1]))
