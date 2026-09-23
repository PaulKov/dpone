"""Fixed isolated bootstrap: original guard, bounded pidfd-owned gate, direct exec.

This process never reads credentials and never creates a second managed child.
Source, interpreter and immutable deployment admission belong to its parent.
"""

from __future__ import annotations

SOURCE_SHIM = r"""
import os, runpy, sys
root = sys.argv[sys.argv.index('--package-root') + 1]
parent = int(sys.argv[sys.argv.index('--parent') + 1])
limit = int(sys.argv[sys.argv.index('--address-space') + 1])
if not os.path.isabs(root):
    os._exit(70)
guard = runpy.run_path(os.path.join(root, 'dpone/adapters/mssql_tds_worker_guard.py'), run_name='__dpone_guard__')
guard['install_worker_guard'](expected_parent_pid=parent, max_address_space_bytes=limit)
import resource
resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
sys.path.insert(0, root)
module = runpy.run_path(os.path.join(root, 'dpone/adapters/mssql_sqlclient_launch_shim.py'), run_name='__dpone_sqlclient_shim__')
try:
    module['_main'](sys.argv[1:])
except BaseException:
    os._exit(70)
"""


def _main(argv: list[str]) -> None:
    import argparse
    import os
    import resource
    import time
    from pathlib import Path

    from dpone.adapters.mssql_sqlclient_installation import _read, validate_manifest
    from dpone.adapters.mssql_tds_channels import read_worker_message
    from dpone.adapters.mssql_tds_installation import worker_installation_digest
    from dpone.adapters.mssql_tds_process import LinuxTdsProcess
    from dpone.contracts.mssql_tds_api import SqlClientDescriptors, decode_launch, encode_launch

    parser = argparse.ArgumentParser(add_help=False)
    for name in (
        "package-root",
        "source-sha256",
        "dotnet-host",
        "runtime-root",
        "companion-root",
        "worker-assembly",
        "build-manifest",
        "build-sha256",
    ):
        parser.add_argument("--" + name, required=True)
    for name in (
        "parent",
        "address-space",
        "startup-deadline-ns",
        "operation-deadline-ns",
        "gate-fd",
        "startup-fd",
        "credentials-fd",
        "session-fd",
        "grant-fd",
        "result-fd",
        "input-fd",
    ):
        parser.add_argument("--" + name, required=True, type=int)
    args = parser.parse_args(argv)
    deadline_ns = min(args.startup_deadline_ns, args.operation_deadline_ns)
    roles = SqlClientDescriptors(
        args.startup_fd, args.credentials_fd, args.session_fd, args.grant_fd, args.result_fd, args.input_fd
    )
    surviving = (roles.startup, roles.credentials, roles.session, roles.grant, roles.result, roles.input)
    if args.gate_fd in surviving or args.gate_fd < 3 or os.getppid() != args.parent:
        raise ValueError("mssql_native.sqlclient_gate_invalid")
    for raw in os.listdir("/proc/self/fd"):
        fd = int(raw)
        if fd > 2 and fd not in (*surviving, args.gate_fd):
            try:
                os.close(fd)
            except OSError:
                pass  # The procfs directory iterator has already closed its own fd.
    if worker_installation_digest(Path(args.package_root)) != args.source_sha256:
        raise ValueError("mssql_native.sqlclient_source_changed")
    try:
        launch = decode_launch(read_worker_message(args.gate_fd, deadline=deadline_ns / 10**9, max_payload=16384))
    finally:
        os.close(args.gate_fd)
    if (
        launch.process != LinuxTdsProcess.identify(os.getpid())
        or launch.parent_pid != args.parent
        or launch.descriptors != roles
        or launch.address_space_bytes != args.address_space
        or launch.startup_deadline_ns != args.startup_deadline_ns
        or launch.operation_deadline_ns != args.operation_deadline_ns
        or launch.build_sha256 != args.build_sha256
        or os.getppid() != args.parent
        or resource.getrlimit(resource.RLIMIT_AS) != (args.address_space, args.address_space)
        or resource.getrlimit(resource.RLIMIT_CORE) != (0, 0)
    ):
        raise ValueError("mssql_native.sqlclient_gate_invalid")
    validate_manifest(_read(Path(args.build_manifest), deadline_ns, 2 * 1024**2), launch.build_sha256)
    if time.monotonic_ns() >= deadline_ns:
        raise ValueError("mssql_native.sqlclient_deadline")
    for fd in surviving:
        os.set_inheritable(fd, True)
    command = [
        args.dotnet_host,
        args.worker_assembly,
        "--launch",
        encode_launch(launch).decode("ascii"),
        "--companion-root",
        args.companion_root,
        "--runtime-root",
        args.runtime_root,
        "--deployment-manifest",
        args.build_manifest,
    ]
    environment = {
        "PATH": "/usr/bin:/bin",
        "LANG": "C.UTF-8",
        "DOTNET_ROOT": args.runtime_root,
        "DOTNET_ROLL_FORWARD": "Disable",
        "DOTNET_EnableDiagnostics": "0",
    }
    os.execve(args.dotnet_host, command, environment)
