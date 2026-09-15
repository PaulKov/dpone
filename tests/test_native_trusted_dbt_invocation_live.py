"""Real synchronous dbt/SQL observation with synthetic admitted original ports.

This proves recorder integration with the actual process runner only. It does
not qualify production runtime inventory, managed macros, source custody or an
installed route. The enclosing runner owns an isolated disposable SQL container.
"""

from __future__ import annotations

import os
import shutil
import time
from datetime import UTC, datetime
from uuid import uuid4

import pytest
import yaml

from dpone.adapters.dbt_subprocess import SubprocessDbtCommandRunner
from dpone.contracts.native_source_custody import NativeSourceCustodyError
from dpone.contracts.native_source_custody_codec import decode_trusted_dbt_invocation_completion
from tests.native_trusted_dbt_fixtures import InvocationFixture

pytestmark = [
    pytest.mark.integration_live,
    pytest.mark.skipif(
        os.environ.get("DPONE_RUN_TRUSTED_DBT_LIVE") != "1", reason="isolated trusted dbt acceptance disabled"
    ),
]


@pytest.mark.parametrize("successful", [True, False])
def test_real_dbt_sequence_requires_complete_positive_normal_return(tmp_path, successful):
    import pyodbc

    password = os.environ["DPONE_NATIVE_SQL_TEST_PASSWORD"]
    host = os.environ["DPONE_NATIVE_SQL_TEST_HOST"]
    database = "dpone_invocation_" + uuid4().hex
    connection = pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};SERVER="
        + host
        + ";UID=sa;PWD="
        + password
        + ";Encrypt=yes;TrustServerCertificate=yes",
        timeout=5,
        autocommit=True,
    )
    connection.timeout = 10
    connection.execute(f"CREATE DATABASE [{database}]")
    runner = SubprocessDbtCommandRunner(dbt_executable=shutil.which("dbt"), max_output_bytes=65536)
    fixture = InvocationFixture(
        tmp_path,
        delegate=runner,
        timeout=120,
        termination=30,
        clock=lambda: datetime.now(UTC),
        monotonic_clock=time.monotonic,
    )
    fixture.recorder.validate_before_credentials()
    profile_path = fixture.profile / "profiles.yml"
    try:
        profile_path.write_text(
            yaml.safe_dump(
                {
                    "trusted_invocation": {
                        "target": "local",
                        "outputs": {
                            "local": {
                                "type": "sqlserver",
                                "driver": "ODBC Driver 18 for SQL Server",
                                "server": host,
                                "port": 1433,
                                "database": database,
                                "schema": "dbo",
                                "authentication": "sql",
                                "user": "sa",
                                "password": password,
                                "encrypt": True,
                                "trust_cert": True,
                                "threads": 1,
                            }
                        },
                    }
                }
            )
        )
        profile_path.chmod(0o600)
        (fixture.project / "dbt_project.yml").write_text(
            yaml.safe_dump(
                {
                    "name": "trusted_invocation",
                    "version": "1.0",
                    "config-version": 2,
                    "profile": "trusted_invocation",
                    "model-paths": ["models"],
                    "models": {"trusted_invocation": {"+materialized": "table"}},
                }
            )
        )
        models = fixture.project / "models"
        models.mkdir()
        (models / "synthetic.sql").write_text(
            "select cast(1 as int) as id" if successful else "select * from absent_synthetic_relation"
        )
        for position in range(3):
            if position == 2 and not successful:
                with pytest.raises(NativeSourceCustodyError):
                    fixture.recorder.run(
                        fixture.args(position), cwd=fixture.project, timeout_seconds=120, redactions=(password,)
                    )
            else:
                fixture.recorder.run(
                    fixture.args(position), cwd=fixture.project, timeout_seconds=120, redactions=(password,)
                )
        if successful:
            reference = fixture.recorder.require_completion()
            payload = fixture.store.documents[reference.locator]
            completion = decode_trusted_dbt_invocation_completion(payload)
            assert completion.command_count == 3
            assert 0 < completion.elapsed_microseconds <= fixture.plan.total_termination_budget_seconds * 1000000
            observed_finish = datetime.now(UTC)
            recorded_finish = datetime.fromisoformat(completion.finished_at.replace("Z", "+00:00"))
            assert abs((observed_finish - recorded_finish).total_seconds()) < 60
            assert password.encode() not in payload
            assert [
                tuple(row) for row in connection.execute(f"SELECT id FROM [{database}].[dbo].[synthetic]").fetchall()
            ] == [(1,)]
        else:
            with pytest.raises(NativeSourceCustodyError):
                fixture.recorder.require_completion()
            assert fixture.store.publications == 0
    finally:
        profile_path.unlink(missing_ok=True)
        connection.execute(f"ALTER DATABASE [{database}] SET SINGLE_USER WITH ROLLBACK IMMEDIATE")
        connection.execute(f"DROP DATABASE [{database}]")
        connection.close()
