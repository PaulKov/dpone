"""Real descriptor lifecycle tests; no subprocess or SQL qualification claim."""

import fcntl
import os
import stat
import subprocess
import sys
from hashlib import sha256
from pathlib import Path
from threading import Event
from time import monotonic_ns

import pytest

from dpone.adapters.dbt_physical_transport_subprocess import PhysicalTransportLaunchLease
from dpone.contracts.dbt_physical_transport_delivery import PhysicalTransportDelivery, physical_transport_argv_digest
from dpone.contracts.native_delivery_json import encode_native_delivery_json
from tests.test_dbt_physical_transport_delivery import packet_payload


def make_lease(tmp_path, **kwargs):
    return PhysicalTransportLaunchLease(
        delivery=PhysicalTransportDelivery(encode_native_delivery_json(packet_payload())),
        profile_directory=tmp_path,
        cancellation=kwargs.get("cancellation", Event()),
        clock=kwargs.get("clock", lambda: 2),
    )


def test_descriptor_is_anonymous_readonly_exact_and_closed_once(tmp_path):
    lease = make_lease(tmp_path)
    with lease as context:
        (fd,) = context.pass_fds
        assert dict(context.environment) == {"DPONE_PHYSICAL_TRANSPORT_FD": str(fd)}
        status = os.fstat(fd)
        assert stat.S_ISREG(status.st_mode) and status.st_nlink == 0
        assert status.st_mode & 0o777 == 0o400
        assert fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE == os.O_RDONLY
        assert os.read(fd, status.st_size + 1) == lease.delivery.payload
        assert not list(tmp_path.iterdir())
        with pytest.raises(TypeError):
            context.environment["other"] = "value"
    with pytest.raises(OSError):
        os.fstat(fd)
    lease.close()
    with pytest.raises(ValueError):
        lease.__enter__()


def test_failure_or_cancellation_never_replays(tmp_path):
    event = Event()
    event.set()
    lease = make_lease(tmp_path, cancellation=event)
    with pytest.raises(ValueError):
        lease.__enter__()
    event.clear()
    with pytest.raises(ValueError):
        lease.__enter__()
    assert not list(tmp_path.iterdir())


def test_symlink_directory_rejected(tmp_path):
    target = tmp_path / "target"
    target.mkdir()
    link = tmp_path / "link"
    link.symlink_to(target)
    with pytest.raises((OSError, ValueError)):
        with make_lease(link):
            pytest.fail("symlink accepted")


def test_inherited_descriptor_actual_child_consumer_closes_and_does_not_reexport(tmp_path):
    profile = tmp_path / "profiles.yml"
    profile.write_bytes(b"synthetic-profile-for-descriptor-test")
    profile.chmod(0o600)
    status = profile.stat()
    raw = packet_payload()
    args = (str(profile), raw["model_database"]["database_name"], raw["model_schema"])
    raw.update(
        parent_pid=os.getpid(),
        admitted_monotonic_ns=monotonic_ns(),
        deadline_monotonic_ns=monotonic_ns() + 30_000_000_000,
        argv_sha256=physical_transport_argv_digest(("dbt", *args)),
        profile_file_device=status.st_dev,
        profile_file_inode=status.st_ino,
        profile_file_sha256="sha256:" + sha256(profile.read_bytes()).hexdigest(),
    )
    lease = PhysicalTransportLaunchLease(
        delivery=PhysicalTransportDelivery(encode_native_delivery_json(raw)),
        profile_directory=tmp_path,
        cancellation=Event(),
    )
    root = Path(__file__).parents[1]
    script = """
import os, sys
from pathlib import Path
from types import SimpleNamespace
from dbt.adapters.dpone_sqlserver.delivery import PhysicalTransportDeliveryOwner
fd = int(os.environ['DPONE_PHYSICAL_TRANSPORT_FD'])
owner = PhysicalTransportDeliveryOwner()
owner.consume(profile_name='profile', target_name='target',
    credentials=SimpleNamespace(type='dpone_sqlserver', database=sys.argv[2], schema=sys.argv[3]),
    profile_file=Path(sys.argv[1]))
assert 'DPONE_PHYSICAL_TRANSPORT_FD' not in os.environ
try:
    os.fstat(fd)
except OSError:
    print('closed')
else:
    raise AssertionError('descriptor leaked')
"""
    with lease as launch:
        process = subprocess.Popen(
            [sys.executable, "-c", script, *args],
            close_fds=True,
            pass_fds=launch.pass_fds,
            env={"PYTHONPATH": f"{root / 'src'}:{root / 'packages/dbt-dpone-sqlserver/src'}", **launch.environment},
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
    stdout, stderr = process.communicate(timeout=30)
    assert process.returncode == 0, stderr
    assert stdout.strip() == b"closed"


def test_group_accessible_directory_is_not_private_custody(tmp_path):
    tmp_path.chmod(0o750)
    with pytest.raises(ValueError):
        with make_lease(tmp_path):
            pytest.fail("nonprivate directory accepted")
