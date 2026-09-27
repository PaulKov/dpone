"""Privacy-safe host harness for an explicitly enabled disposable SQL Server."""

from __future__ import annotations

import os
import shutil
import subprocess
import time
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from decimal import Decimal, InvalidOperation
from pathlib import Path

import pytest
from tests.integration.mssql.mssql_target_local_p1_cases import TargetLocalDigestCase

from dpone.contracts.mssql_native_writer import NativeStageWriteGrant
from dpone.runtime.connectors.mssql_bcp_process import start_bcp_process
from dpone.runtime.connectors.mssql_bcp_supervised import BcpSupervisedResult, observe_bcp_process

_DEFAULT_DOCKER_APP = Path("/Applications/Docker.app/Contents/Resources/bin/docker")


class TargetLocalDockerStand:
    """Execute generic SQL and BCP inside one disposable local container."""

    def __init__(self, docker: str, sql_container: str, database: str) -> None:
        self.docker = docker
        self.sql_container = sql_container
        self.database = database

    def command(
        self,
        arguments: list[str],
        *,
        data: bytes | None = None,
        timeout: int = 120,
        check: bool = True,
    ) -> subprocess.CompletedProcess[bytes]:
        result = subprocess.run(
            [self.docker, *arguments],
            input=data,
            capture_output=True,
            timeout=timeout,
            check=False,
        )
        if check and result.returncode:
            raise AssertionError((result.stdout + result.stderr).decode(errors="replace"))
        return result

    def sql(self, query: str) -> bytes:
        return self.command(
            [
                "exec",
                "-i",
                self.sql_container,
                "bash",
                "-c",
                '/opt/mssql-tools18/bin/sqlcmd -S localhost -d "$1" -U sa -P "$MSSQL_SA_PASSWORD" -C -b -h -1 -W',
                "sql",
                self.database,
            ],
            data=("SET NOCOUNT ON;\n" + query).encode(),
        ).stdout

    def single_record(self, query: str) -> tuple[int | Decimal, ...]:
        output = self.command(
            [
                "exec",
                "-i",
                self.sql_container,
                "bash",
                "-c",
                '/opt/mssql-tools18/bin/sqlcmd -S localhost -d "$1" -U sa '
                '-P "$MSSQL_SA_PASSWORD" -C -b -h -1 -W -s "|"',
                "sql-record",
                self.database,
            ],
            data=("SET NOCOUNT ON;\n" + query).encode(),
        ).stdout.decode()
        lines = [line.strip() for line in output.splitlines() if line.strip()]
        if len(lines) != 1:
            raise AssertionError(f"expected one aggregate record, observed {len(lines)}")
        return tuple(_number(value) for value in lines[0].split("|"))

    def unique_table(self, purpose: str) -> str:
        safe = "".join(character for character in purpose.lower() if character.isalnum())[:16]
        return f"dpone_p1_{safe}_{uuid.uuid4().hex[:16]}"

    def qualified(self, table: str) -> str:
        return f"[{self.database}].[dbo].[{table}]"

    def create_empty_table(self, table: str, case: TargetLocalDigestCase) -> None:
        definitions = []
        for name, declared in case.columns:
            nullable = declared.endswith(" nullable")
            dtype = declared.removesuffix(" nullable")
            definitions.append(f"[{name}] {dtype} {'NULL' if nullable else 'NOT NULL'}")
        self.sql(f"CREATE TABLE {self.qualified(table)} ({', '.join(definitions)});")

    def create_case_table(self, table: str, case: TargetLocalDigestCase) -> None:
        self.create_empty_table(table, case)
        columns = ", ".join(f"[{name}]" for name, _ in case.columns)
        values = ",\n".join("(" + ", ".join(row) + ")" for row in case.sql_rows)
        self.sql(f"INSERT INTO {self.qualified(table)} ({columns}) VALUES {values};")

    def drop_table(self, table: str) -> None:
        self.sql(f"DROP TABLE IF EXISTS {self.qualified(table)};")

    def table_rows(self, table: str) -> int:
        return int(self.single_record(f"SELECT COUNT_BIG(*) FROM {self.qualified(table)};")[0])

    @contextmanager
    def held_table_lock(self, table: str, *, seconds: int = 4) -> Iterator[None]:
        """Hold an exact-stage transaction lock in a separate real SQL session."""
        query = (
            "SET NOCOUNT ON; BEGIN TRAN; "
            f"SELECT COUNT_BIG(*) FROM {self.qualified(table)} WITH (TABLOCKX,HOLDLOCK); "
            f"WAITFOR DELAY '00:00:{seconds:02d}'; ROLLBACK;"
        )
        process = subprocess.Popen(
            [
                self.docker,
                "exec",
                "-i",
                self.sql_container,
                "bash",
                "-c",
                '/opt/mssql-tools18/bin/sqlcmd -S localhost -d "$1" -U sa -P "$MSSQL_SA_PASSWORD" -C -b -Q "$2"',
                "lock-holder",
                self.database,
                query,
            ],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        time.sleep(0.75)
        if process.poll() is not None:
            stdout, stderr = process.communicate()
            raise AssertionError((stdout + stderr).decode(errors="replace"))
        try:
            yield
        finally:
            stdout, stderr = process.communicate(timeout=seconds + 5)
            if process.returncode:
                raise AssertionError((stdout + stderr).decode(errors="replace"))

    def launch_bcp(
        self,
        grant: NativeStageWriteGrant,
        rejects_path: Path,
        *,
        discard_bcp_output: bool = False,
        timeout_seconds: int = 30,
    ) -> BcpSupervisedResult:
        remote_file = f"/tmp/dpone-p1-{uuid.uuid4().hex}.native"
        remote_rejects = remote_file + ".rejects"
        self.command(["cp", str(grant.file_path), f"{self.sql_container}:{remote_file}"])
        self.command(["exec", "-u", "0", self.sql_container, "chmod", "0444", remote_file])
        redirection = " >/dev/null 2>&1" if discard_bcp_output else ""
        script = (
            '/opt/mssql-tools18/bin/bcp "$1" in "$2" -S localhost '
            '-U sa -P "$MSSQL_SA_PASSWORD" -n -u -b 100000 -a 16384 -l 20 -e "$3"' + redirection
        )
        command = [
            self.docker,
            "exec",
            "-i",
            self.sql_container,
            "bash",
            "-c",
            script,
            "bcp-import",
            grant.qualified_stage,
            remote_file,
            remote_rejects,
        ]

        def cleanup() -> None:
            reject = self.command(["exec", self.sql_container, "cat", remote_rejects], check=False)
            if reject.returncode == 0 and reject.stdout:
                rejects_path.write_bytes(reject.stdout)
            self.command(["exec", "-u", "0", self.sql_container, "rm", "-f", remote_file, remote_rejects])

        handle = start_bcp_process(
            command,
            redacted_command=(self.docker, "exec", self.sql_container, "bcp", "<sealed-input>"),
            stdin_text=None,
            timeout_seconds=timeout_seconds,
            environment=None,
            cleanup_callback=cleanup,
            progress_callback=None,
            drain_output=False,
        )
        return observe_bcp_process(handle)


def configured_target_local_stand() -> TargetLocalDockerStand:
    """Select only an explicitly approved disposable local Docker environment."""
    if os.getenv("DPONE_RUN_INTEGRATION") != "1":
        pytest.skip("set DPONE_RUN_INTEGRATION=1 for disposable local Docker certification")
    docker = os.getenv("DPONE_IT_DOCKER") or shutil.which("docker")
    if docker is None and _DEFAULT_DOCKER_APP.is_file():
        docker = str(_DEFAULT_DOCKER_APP)
    if docker is None:
        pytest.fail("Docker CLI is unavailable for required live certification")
    stand = TargetLocalDockerStand(
        docker,
        os.getenv("DPONE_IT_NATIVE_DOCKER_SQL", "dpone-it-mssql"),
        os.getenv("DPONE_IT_MSSQL_DATABASE", "dpone_it"),
    )
    stand.command(["inspect", "-f", "{{.State.Health.Status}}", stand.sql_container])
    return stand


def _number(value: str) -> int | Decimal:
    try:
        parsed = Decimal(value.strip())
    except InvalidOperation as error:
        raise AssertionError("aggregate output contained a nonnumeric value") from error
    return int(parsed) if parsed == parsed.to_integral_value() else parsed


__all__ = ["TargetLocalDockerStand", "configured_target_local_stand"]
