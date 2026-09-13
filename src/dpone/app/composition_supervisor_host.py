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
        enrollment = ClickHouseSupervisorEnrollment(
            body["enrollment_sha256"], canonical_json_bytes(body["enrollment_document"])
        )
        _require_custody_identity(body, enrollment)
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
    _require_custody_identity(config, enrollment)
    docker, linux = LocalDockerSupervisorClient(), LinuxSupervisorProbe()

    def capture(deadline: float) -> dict[str, Any]:
        # A fresh decoded policy prevents an observer from mutating future calls.
        return capture_supervisor_facts(docker, linux, enrollment.policy, deadline)

    def capture_mounts(deadline: float) -> dict[str, Any]:
        # Fixed roles come only from the protected enrollment, never the request.
        original = enrollment.body["facts"]
        before = capture(deadline)
        if canonical_json_bytes(before) != canonical_json_bytes(original):
            raise SupervisorProbeError("supervisor_probe_mount_observation")
        tables = {}
        for identifier in sorted(enrollment.role_id(role) for role in ("dispatcher", "clickhouse")):
            rows = linux.mounts(before["docker"]["containers"][identifier]["pid"], deadline)
            expected = original["linux"]["containers"][identifier]["mounts"]["mountinfo_sha256"]
            if "sha256:" + sha256(canonical_json_bytes(rows)).hexdigest() != expected:
                raise SupervisorProbeError("supervisor_probe_mount_pin")
            tables[identifier] = rows
        if canonical_json_bytes(capture(deadline)) != canonical_json_bytes(original):
            raise SupervisorProbeError("supervisor_probe_mount_observation")
        return tables

    return SupervisorFactsServer(
        Path(config["socket_path"]),
        enrollment_sha256=enrollment.enrollment_sha256,
        dispatcher_uid=config["dispatcher_uid"],
        dispatcher_gid=config["dispatcher_gid"],
        capture=capture,
        capture_mounts=capture_mounts,
        timeout_seconds=config["timeout_seconds"],
    )


def _require_custody_identity(config: dict[str, Any], enrollment: ClickHouseSupervisorEnrollment) -> None:
    """Bind the socket caller to the same explicitly enrolled volume owner."""
    custody = enrollment.policy.get("capture_custody")
    if custody is not None and any(
        type(config.get("dispatcher_" + name)) is not int or config["dispatcher_" + name] != custody[name]
        for name in ("uid", "gid")
    ):
        raise SupervisorProbeError("supervisor_probe_configuration")
