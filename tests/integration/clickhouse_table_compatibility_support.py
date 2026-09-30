"""Controlled Docker transport for the historical-default certification probe.

No network is published or attached. Only this test controller uses Docker exec;
the declared trust boundary excludes privileged host administrators. The server
has a read-only root/configuration, non-root UID and bounded RAM. Data uses
private tmpfs by default; the restart witness uses a private persistent directory
without claiming a disk-quota or power-loss guarantee.
This is not a production deployment provider or a claim of ingress enforcement.
"""

from __future__ import annotations

import hashlib
import json
import subprocess
import time
import uuid
from pathlib import Path

DOCKER = "/Applications/Docker.app/Contents/Resources/bin/docker"
IMAGE = "clickhouse/clickhouse-server@sha256:1ffa82edee000a42c09313bd9f1293d94c570aee74babc1b3ca9983a35fa597b"
CONFIG = """<clickhouse>
<logger><level>warning</level><console>1</console></logger>
<listen_host>127.0.0.1</listen_host><tcp_port>9000</tcp_port>
<path>/var/lib/clickhouse/</path><tmp_path>/var/lib/clickhouse/tmp/</tmp_path>
<user_files_path>/var/lib/clickhouse/user_files/</user_files_path>
<format_schema_path>/var/lib/clickhouse/format_schemas/</format_schema_path>
<max_server_memory_usage>1073741824</max_server_memory_usage>
<max_thread_pool_size>128</max_thread_pool_size>
<background_pool_size>16</background_pool_size>
<background_schedule_pool_size>16</background_schedule_pool_size>
<background_message_broker_schedule_pool_size>4</background_message_broker_schedule_pool_size>
<background_distributed_schedule_pool_size>4</background_distributed_schedule_pool_size>
<profiles><default><max_threads>2</max_threads></default></profiles>
<users><default><password></password><networks><ip>127.0.0.1</ip></networks>
<profile>default</profile><quota>default</quota></default></users>
<quotas><default/></quotas>
</clickhouse>"""


def docker(*arguments: str) -> str:
    result = subprocess.run(
        [DOCKER, "--context", "desktop-linux", *arguments],
        capture_output=True,
        text=True,
        timeout=45,
        check=False,
    )
    if result.returncode:
        # Do not leak command arguments, SQL, or arbitrary server output.
        raise RuntimeError(f"Owned Docker probe command failed ({result.returncode})")
    return result.stdout.strip()


class OwnedClickHouse:
    """One isolated fixture; objects are stopped and retained, never adopted."""

    def __init__(self, name: str, config: Path, data: Path | None = None) -> None:
        self.name, self.config = name, config
        self.data = data

    @classmethod
    def prepare(cls, root: Path, *, persistent: bool = False) -> OwnedClickHouse:
        config = root / "server.xml"
        config.write_text(CONFIG, encoding="utf-8")
        config.chmod(0o444)
        data = root.resolve() / "server-data" if persistent else None
        if data is not None:
            data.mkdir()
            # The containing evidence directory is private (0700). The mount
            # itself permits the isolated container's UID101 to write across
            # Docker Desktop's host/VM UID mapping; no network is attached.
            data.chmod(0o777)
        return cls("dpone-epoch-" + uuid.uuid4().hex[:16], config.resolve(), data)

    def start(self) -> None:
        data_mount = (
            ("--mount", f"type=bind,source={self.data},target=/var/lib/clickhouse")
            if self.data is not None
            else ("--tmpfs", "/var/lib/clickhouse:rw,size=268435456,uid=101,gid=101")
        )
        docker(
            "create",
            "--pull=never",
            "--name",
            self.name,
            "--label",
            "dpone.fixture=table-compatibility-epoch",
            "--network=none",
            "--read-only",
            "--user=101:101",
            "--cap-drop=ALL",
            "--security-opt=no-new-privileges",
            "--cpus=2",
            "--memory=2g",
            "--pids-limit=256",
            *data_mount,
            "--tmpfs",
            "/tmp:rw,size=67108864,uid=101,gid=101",
            "--mount",
            f"type=bind,source={self.config},target=/etc/dpone-server.xml,readonly",
            "--entrypoint=/usr/bin/clickhouse-server",
            IMAGE,
            "--config-file=/etc/dpone-server.xml",
        )
        docker("start", self.name)
        self.ready()

    def ready(self) -> None:
        deadline = time.monotonic() + 30
        while time.monotonic() < deadline:
            try:
                if self.query("SELECT version() AS version")[0]["version"] == "24.8.14.39":
                    return
            except RuntimeError:
                pass
            time.sleep(0.2)
        raise RuntimeError("Owned ClickHouse did not become ready at pinned version")

    def query(self, statement: str) -> list[dict]:
        payload = docker("exec", self.name, "clickhouse-client", "--format=JSONEachRow", "--query", statement)
        return [json.loads(line) for line in payload.splitlines() if line]

    def globals(self) -> dict[str, str]:
        return {row["name"]: row["value"] for row in self.query("SELECT name, value FROM system.merge_tree_settings")}

    def identity(self) -> dict:
        facts = json.loads(docker("inspect", self.name))[0]
        if (
            not facts["State"]["Running"]
            or facts["HostConfig"]["NetworkMode"] != "none"
            or not facts["HostConfig"]["ReadonlyRootfs"]
            or facts["Config"]["User"] != "101:101"
        ):
            raise ValueError("settings_provenance_unverified: deployment controls")
        return {
            "container": facts["Id"],
            "image": facts["Image"],
            "started_at": facts["State"]["StartedAt"],
            "restart_count": facts["RestartCount"],
            "configuration_sha256": hashlib.sha256(self.config.read_bytes()).hexdigest(),
        }

    def restart(self) -> None:
        docker("restart", self.name)
        self.ready()

    def stop(self) -> None:
        docker("stop", "--time=5", self.name)
