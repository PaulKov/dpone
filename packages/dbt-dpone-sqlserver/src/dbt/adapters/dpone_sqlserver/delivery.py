"""Inert child descriptor consumer; consumption alone never qualifies a route."""

import os
import stat
import sys
from hashlib import sha256
from pathlib import Path
from threading import Lock
from time import monotonic_ns
from typing import Any

from dpone.adapters.dbt_physical_transport_subprocess import open_physical_transport_directory
from dpone.contracts.dbt_physical_transport_delivery import (
    DELIVERY_ENVIRONMENT_KEY,
    PhysicalTransportDelivery,
    physical_transport_argv_digest,
)
from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_BYTES


class PhysicalTransportDeliveryOwner:
    """Consume an inherited capability once on executed use, never import/parse.

    The qualified launcher/image under the same host principal is the trust root;
    this is not cryptographic authentication of a hostile process with the same
    UID. Managed execution remains disabled in the preparatory adapter.
    """

    def __init__(self) -> None:
        self._lock, self._used = Lock(), False

    def consume(
        self,
        *,
        profile_name: str,
        target_name: str,
        credentials: Any,
        profile_file: Path,
    ) -> PhysicalTransportDelivery:
        """Validate anonymous FD, actual process/argv, loaded target and profile.

        Failure consumes the attempt. The descriptor is not re-exported and is
        closed exactly once, including malformed packet and profile failures.
        """
        with self._lock:
            if self._used:
                raise ValueError("physical delivery already consumed")
            self._used = True
            if os.name != "posix":
                raise ValueError("physical delivery requires the qualified POSIX launcher")
            token = os.environ.pop(DELIVERY_ENVIRONMENT_KEY, None)
            if token is None or not token.isascii() or not token.isdecimal() or len(token) > 10:
                raise ValueError("physical delivery descriptor is missing or invalid")
            fd = int(token)
            if fd < 3 or str(fd) != token:
                raise ValueError("physical delivery descriptor cannot be a standard stream")
            try:
                os.set_inheritable(fd, False)
                packet = self._read(fd)
                self._require_process(packet, profile_name, target_name, credentials, profile_file)
                return packet
            finally:
                os.close(fd)

    @staticmethod
    def _read(fd: int) -> PhysicalTransportDelivery:
        import fcntl

        status = os.fstat(fd)
        if (
            fcntl.fcntl(fd, fcntl.F_GETFL) & os.O_ACCMODE != os.O_RDONLY
            or not stat.S_ISREG(status.st_mode)
            or status.st_nlink != 0
            or status.st_uid != os.getuid()
            or status.st_mode & 0o222
            or not 0 < status.st_size <= MAX_NATIVE_JSON_BYTES
            or os.lseek(fd, 0, os.SEEK_CUR) != 0
        ):
            raise ValueError("physical delivery descriptor custody is invalid")
        content = bytearray()
        while len(content) <= status.st_size:
            chunk = os.read(fd, status.st_size + 1 - len(content))
            if not chunk:
                break
            content.extend(chunk)
        if len(content) != status.st_size or os.fstat(fd) != status:
            # Access time may change after read on some filesystems; only
            # security/content identity fields are compared below instead.
            after = os.fstat(fd)
            if len(content) != status.st_size or (
                after.st_dev,
                after.st_ino,
                after.st_size,
                after.st_mode,
                after.st_nlink,
                after.st_mtime_ns,
            ) != (status.st_dev, status.st_ino, status.st_size, status.st_mode, status.st_nlink, status.st_mtime_ns):
                raise ValueError("physical delivery descriptor changed or is incomplete")
        return PhysicalTransportDelivery(bytes(content))

    @staticmethod
    def _require_process(
        packet: PhysicalTransportDelivery,
        profile_name: str,
        target_name: str,
        credentials: Any,
        profile_file: Path,
    ) -> None:
        raw = packet.to_dict()
        if (
            raw["parent_pid"] != os.getppid()
            or physical_transport_argv_digest(("dbt", *sys.argv[1:])) != raw["argv_sha256"]
            or (profile_name, target_name) != (raw["profile_name"], raw["target_name"])
            or (credentials.type, credentials.database, credentials.schema)
            != ("dpone_sqlserver", raw["model_database"]["database_name"], raw["model_schema"])
            or monotonic_ns() >= packet.deadline_monotonic_ns
        ):
            raise ValueError("physical delivery differs from the actual child command/target")
        directory = open_physical_transport_directory(profile_file.parent)
        try:
            fd = os.open(profile_file.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=directory)
            try:
                status = os.fstat(fd)
                if (
                    not stat.S_ISREG(status.st_mode)
                    or status.st_uid != os.getuid()
                    or status.st_mode & 0o077
                    or status.st_nlink != 1
                    or (status.st_dev, status.st_ino) != (raw["profile_file_device"], raw["profile_file_inode"])
                    or not 0 < status.st_size <= MAX_NATIVE_JSON_BYTES
                ):
                    raise ValueError("physical profile custody differs")
                content = os.read(fd, status.st_size + 1)
                if (
                    len(content) != status.st_size
                    or "sha256:" + sha256(content).hexdigest() != raw["profile_file_sha256"]
                ):
                    raise ValueError("physical profile bytes differ")
            finally:
                os.close(fd)
        finally:
            os.close(directory)
