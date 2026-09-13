"""One-time observed environment descriptor for explicitly prepared benchmarks."""

from __future__ import annotations

import os
import platform
import re
import subprocess
import tomllib
from importlib.metadata import distributions
from pathlib import Path

from .layout import layout_digest, template_layouts


def runner_resources(cgroup=Path("/sys/fs/cgroup")):
    """Read actual Linux cgroup v2 limits; omit unavailable memory observations."""
    resources = {"cpu_count": float(os.cpu_count() or 1)}
    cpu = cgroup / "cpu.max"
    if cpu.is_file():
        quota, period = cpu.read_text().split()
        if quota != "max":
            resources["cpu_count"] = min(resources["cpu_count"], int(quota) / int(period))
    memory = cgroup / "memory.max"
    if memory.is_file() and (value := memory.read_text().strip()) != "max":
        resources["memory_bytes"] = int(value)
    return resources


def observe(environment, subject_checkout):
    """Explicit connection boundary, invoked once before any benchmark trial."""
    ch = environment.clickhouse()
    try:
        ch_version = str(ch.get_records("SELECT version()")[0][0])
    finally:
        ch.close()
    resources = runner_resources()
    with environment.sql_scope() as sql:
        sql_version = str(sql.get_records("SELECT CONVERT(varchar(128),SERVERPROPERTY('ProductVersion'))")[0][0])
        layouts = template_layouts(sql)
        resources["sql_memory_bytes"] = int(
            sql.get_records(
                "SELECT CONVERT(bigint,value_in_use)*1048576 FROM sys.configurations WHERE name='max server memory (MB)'"
            )[0][0]
        )
        resources["sql_log_bytes"] = int(
            sql.get_records("SELECT SUM(CONVERT(bigint,size))*8192 FROM sys.database_files WHERE type=1")[0][0]
        )
        bcp = subprocess.run([sql.bcp_path, "-v"], capture_output=True, text=True, check=True)
    match = re.search(r"Version[: ]+([\d.]+)", bcp.stdout + bcp.stderr, re.I)
    if not match:
        raise ValueError("local_fixture.bcp_version_unavailable")
    descriptor = {
        "versions": {
            "python": platform.python_version(),
            "dpone": tomllib.loads((subject_checkout / "pyproject.toml").read_text())["project"]["version"],
            **{"distribution." + d.metadata["Name"].lower(): d.version for d in distributions()},
            "clickhouse": ch_version,
            "mssql": sql_version,
            "bcp": match[1],
        },
        "target_layout_sha256": layout_digest(layouts),
        "resource_profile": resources,
    }
    return descriptor, layouts
