"""Root-owned supervisor filesystem simulation for unprivileged developer runs.

Protected composition allocation only accepts root-owned, non-writable paths and
changes ownership with ``fchown``. Developer machines and CI runners are not
root, so these helpers record the ownership the production code requests and
replay it through ``os.fstat``. Only ownership and world traversal are
simulated: exclusive creation, no-follow opens, modes, advisory locks and
``fsync`` remain real.
"""

import os

_ACTUAL_FSTAT = os.fstat
_OWNERS: dict[tuple[int, int], tuple[int, int]] = {}


def simulated_fchown(descriptor, uid, gid):
    """Record the requested ownership instead of requiring real privilege."""

    value = _ACTUAL_FSTAT(descriptor)
    _OWNERS[(value.st_dev, value.st_ino)] = (uid, gid)


def simulated_fstat(descriptor):
    """Report recorded ownership, defaulting to the provisioned root owner."""

    value = _ACTUAL_FSTAT(descriptor)
    uid, gid = _OWNERS.get((value.st_dev, value.st_ino), (0, 0))
    # Developer temporary roots are private; simulate the world-traversable
    # ancestry a provisioned supervisor mount has, without relaxing the exact
    # 0o710 one-shot profile directory contract.
    mode = value.st_mode if value.st_mode & 0o777 == 0o710 else value.st_mode | 0o001
    return os.stat_result((mode, value.st_ino, value.st_dev, value.st_nlink, uid, gid, value.st_size, 0, 0, 0))


def install_supervisor_simulation():
    """Install the simulation process-wide, for spawned concurrency workers."""

    from dpone.adapters import composition_child_identity_allocator

    composition_child_identity_allocator.require_supervisor = lambda: None
    os.fchown = simulated_fchown
    os.fstat = simulated_fstat


def supervisor_simulation(monkeypatch):
    """Install the simulation and the allocator root check for one test."""

    from dpone.adapters import composition_child_identity_allocator

    _OWNERS.clear()
    monkeypatch.setattr(composition_child_identity_allocator, "require_supervisor", lambda: None)
    monkeypatch.setattr(os, "fchown", simulated_fchown)
    monkeypatch.setattr(os, "fstat", simulated_fstat)
