"""Compose the root host observer from one administrator-protected configuration.

The dispatcher supplies only its enrolled digest and nonce. No host callback
opens SQL. The dispatcher independently compares returned facts to protected
SQL enrollment originals before granting any capability.
"""

from __future__ import annotations

from collections.abc import Callable
from hashlib import sha256
from pathlib import Path
from typing import Any

from dpone.adapters.composition_clickhouse_supervisor import capture_supervisor_facts
from dpone.adapters.composition_clickhouse_supervisor_docker import LocalDockerSupervisorClient
from dpone.adapters.composition_clickhouse_supervisor_enrollment import ClickHouseSupervisorEnrollment
from dpone.adapters.composition_clickhouse_supervisor_linux import LinuxSupervisorProbe
from dpone.adapters.composition_supervisor_filesystem import read_protected_original
from dpone.adapters.composition_supervisor_probe_rpc import SupervisorFactsServer, SupervisorProbeError
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

CONFIG_SCHEMA = "dpone.composition-host-probe-config.v1"
MAX_CONFIG_BYTES = 131072


def load_host_probe_config(path: Path, *, expected_configuration_sha256: str) -> dict[str, Any]:
    """Reopen root-owned canonical bytes and verify the externally pinned digest.

    Fields: schema, socket_path, dispatcher_uid, dispatcher_gid, timeout_seconds,
    enrollment_sha256 and enrollment_document (the complete enrollment object).
    Policy exists only inside the authenticated enrollment original.
    """
    try:
        original = read_protected_original(Path(path).parent, Path(path).name, max_bytes=MAX_CONFIG_BYTES)
        if "sha256:" + sha256(original).hexdigest() != expected_configuration_sha256:
            raise ValueError
        body = strict_json_object(original)
        if (
            canonical_json_bytes(body) != original
            or set(body)
            != {
                "schema",
                "socket_path",
                "dispatcher_uid",
                "dispatcher_gid",
                "timeout_seconds",
                "enrollment_sha256",
                "enrollment_document",
            }
            or body["schema"] != CONFIG_SCHEMA
            or type(body["socket_path"]) is not str
        ):
            raise ValueError
        ClickHouseSupervisorEnrollment(body["enrollment_sha256"], canonical_json_bytes(body["enrollment_document"]))
        return body
    except Exception:
        raise SupervisorProbeError("supervisor_probe_configuration") from None


def build_supervisor_facts_server(
    config_path: Path,
    *,
    expected_configuration_sha256: str,
    load_config: Callable[..., dict[str, Any]] = load_host_probe_config,
) -> SupervisorFactsServer:
    """Load fixed protected policy and construct the concrete Docker/Linux capture.

    Socket creation occurs on entering the returned server. The entry point must
    retain root host authority and own its bounded serve loop and shutdown.
    """
    config = load_config(config_path, expected_configuration_sha256=expected_configuration_sha256)
    enrollment = ClickHouseSupervisorEnrollment(
        config["enrollment_sha256"],
        canonical_json_bytes(config["enrollment_document"]),
    )
    docker, linux = LocalDockerSupervisorClient(), LinuxSupervisorProbe()

    def capture(deadline: float) -> dict[str, Any]:
        # A fresh decoded policy prevents an observer from mutating future calls.
        return capture_supervisor_facts(docker, linux, enrollment.policy, deadline)

    return SupervisorFactsServer(
        Path(config["socket_path"]),
        enrollment_sha256=enrollment.enrollment_sha256,
        dispatcher_uid=config["dispatcher_uid"],
        dispatcher_gid=config["dispatcher_gid"],
        capture=capture,
        timeout_seconds=config["timeout_seconds"],
    )
