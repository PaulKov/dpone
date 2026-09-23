"""Observe borrowed input descriptors without consuming, reopening or owning them.

Exclusive caller custody remains required: identical same-inode reopen ABA and
external writable aliases cannot be ruled out by fstat/access/offset observations.
"""

import os
import stat
from dataclasses import replace

from dpone.contracts.mssql_tds_api import SqlClientFileIdentity, SqlClientInputDescriptor


def require_input_descriptor(
    fd: int, original: SqlClientInputDescriptor, *, parent: bool = False
) -> SqlClientFileIdentity:
    """Deeply validate declarations before OS effects; never close the borrowed FD."""
    if type(fd) is not int or fd < 3 or type(parent) is not bool or type(original) is not SqlClientInputDescriptor:
        raise ValueError("mssql_native.sqlclient_input_descriptor")
    original.__post_init__()
    replace(original.expected)
    replace(original.file_identity)
    if parent and fd != original.fd:
        raise ValueError("mssql_native.sqlclient_input_descriptor")
    import fcntl

    info = os.fstat(fd)
    flags = fcntl.fcntl(fd, fcntl.F_GETFL)
    if (
        not stat.S_ISREG(info.st_mode)
        or flags & os.O_ACCMODE != os.O_RDONLY
        or flags & getattr(os, "O_PATH", 0x200000)
        or os.lseek(fd, 0, os.SEEK_CUR) != 0
    ):
        raise ValueError("mssql_native.sqlclient_input_descriptor")
    observed = SqlClientFileIdentity(info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_ctime_ns)
    if observed != original.file_identity:
        raise ValueError("mssql_native.sqlclient_input_descriptor")
    return observed
