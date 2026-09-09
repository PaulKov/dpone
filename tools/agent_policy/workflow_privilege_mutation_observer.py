"""Descriptor-bound, sticky mutation evidence for one snapshot acquisition lease.

Kernel notifications complement final byte/identity checks: a restored value is
still a mutation. Unsupported platforms and notification loss fail closed.
"""

from __future__ import annotations

import ctypes
import errno
import os
import select
import struct
import sys


class MutationObserver:
    """Watch pinned descriptors until the caller's final validation boundary."""

    def __init__(self) -> None:
        self._scopes: dict[int, set[str]] = {}
        self._changed: set[str] = set()
        self._failed = False
        self._closed = False
        self._queue = None
        self._fd = -1
        if sys.platform == "linux":
            self._libc = ctypes.CDLL(None, use_errno=True)
            self._libc.inotify_init1.argtypes = [ctypes.c_int]
            self._libc.inotify_init1.restype = ctypes.c_int
            self._libc.inotify_add_watch.argtypes = [ctypes.c_int, ctypes.c_char_p, ctypes.c_uint32]
            self._libc.inotify_add_watch.restype = ctypes.c_int
            self._fd = self._libc.inotify_init1(os.O_NONBLOCK | os.O_CLOEXEC)
            if self._fd < 0:
                raise OSError(ctypes.get_errno(), "mutation observer initialization failed")
        elif sys.platform == "darwin":
            self._queue = select.kqueue()
        else:
            raise OSError("mutation observation is unsupported on this platform")

    def watch(self, descriptor: int, *, scope: str, contents: bool) -> None:
        """Install observation before the descriptor's first metadata/read use."""
        try:
            if self._queue is not None:
                flags = select.KQ_NOTE_DELETE | select.KQ_NOTE_RENAME | select.KQ_NOTE_REVOKE
                if contents:
                    flags |= select.KQ_NOTE_WRITE | select.KQ_NOTE_EXTEND | select.KQ_NOTE_ATTRIB | select.KQ_NOTE_LINK
                event = select.kevent(
                    descriptor,
                    filter=select.KQ_FILTER_VNODE,
                    flags=select.KQ_EV_ADD | select.KQ_EV_CLEAR,
                    fflags=flags,
                )
                self._queue.control([event], 0, 0)
                token = descriptor
            else:
                # The proc descriptor link pins the already confined inode; no
                # untrusted pathname is traversed by the observer.
                mask = 0x400 | 0x800  # IN_DELETE_SELF | IN_MOVE_SELF
                if contents:
                    mask |= 0x2 | 0x4 | 0x40 | 0x80 | 0x100 | 0x200
                token = self._libc.inotify_add_watch(
                    self._fd, os.fsencode(f"/proc/self/fd/{descriptor}"), mask | 0x20000000
                )  # IN_MASK_ADD preserves a prior watch on the same inode.
                if token < 0:
                    raise OSError(ctypes.get_errno(), "mutation observer watch failed")
            self._scopes.setdefault(token, set()).add(scope)
        except BaseException:
            self._failed = True
            raise

    def unchanged(self, *, policy_only: bool = False) -> bool:
        """Drain pending events; once observed, a mutation can never clear."""
        if self._closed:
            return False
        try:
            for _ in range(256):
                if self._failed or (self._changed & {"policy"} if policy_only else self._changed):
                    return False
                if self._queue is not None:
                    events = self._queue.control(None, 256, 0)
                    if not events:
                        break
                    for event in events:
                        if event.flags & 0x4000:  # EV_ERROR; decoding also tested on Linux.
                            self._failed = True
                        self._record(event.ident)
                else:
                    try:
                        payload = os.read(self._fd, 65536)
                    except BlockingIOError:
                        break
                    if not payload:
                        raise OSError(errno.EIO, "mutation observer stream closed")
                    self._consume_inotify(payload)
            else:
                self._failed = True  # An unbounded event feed cannot certify a quiet boundary.
        except (OSError, ValueError, struct.error):
            self._failed = True
        return not self._failed and not (self._changed & {"policy"} if policy_only else self._changed)

    def _record(self, token: int) -> None:
        scopes = self._scopes.get(token)
        if scopes is None:
            self._failed = True
        else:
            self._changed.update(scopes)

    def _consume_inotify(self, payload: bytes) -> None:
        offset = 0
        while offset < len(payload):
            token, mask, _, length = struct.unpack_from("iIII", payload, offset)
            offset += 16 + length
            if offset > len(payload):
                raise ValueError("truncated mutation event")
            if mask & 0x4000:  # Queue overflow loses evidence across every scope.
                self._failed = True
            self._record(token)

    def close(self) -> None:
        """Release the notification handle exactly once."""
        if self._closed:
            return
        self._closed = True
        if self._queue is not None:
            self._queue.close()
        elif self._fd >= 0:
            os.close(self._fd)
