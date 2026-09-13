"""Read-only native runner facts, never an execution or mount-provenance permit.

The enclosing readiness producer binds these bytes to authenticated deployment,
runner and mount originals. No child identity is allocated and no future dbt
execution, durable write or named PVC identity is certified by this observation.
"""

from __future__ import annotations

import math
import os
import stat
import time
from collections.abc import Callable
from contextlib import ExitStack
from hashlib import sha256
from pathlib import Path

from dpone.adapters.composition_supervisor_filesystem import (
    absolute_supervisor_path,
    filesystem_type,
    open_protected,
    require_supervisor,
)
from dpone.contracts.composition_dbt_outcome import DbtCaptureError
from dpone.contracts.composition_supervisor import CompositionSupervisorProjection
from dpone.contracts.strict_json import canonical_json_bytes


def observe_native_execution_profile(
    *,
    supervisor: CompositionSupervisorProjection,
    supervisor_root: Path,
    profiles_root: Path,
    io_deadline: Callable[[], float],
) -> bytes:
    """Observe held protected roots within one fixed, non-renewable deadline.

    OS calls cannot be forcibly cancelled; a late result is rejected. Descriptor
    cleanup is always attempted even after expiry. Call only on the actual Linux
    root execution runner, passing a projection from authenticated originals.
    """
    try:
        deadline = io_deadline()
        if type(deadline) not in (float, int) or not math.isfinite(deadline):
            raise DbtCaptureError("capture_profile_deadline")

        def check() -> None:
            if time.monotonic() >= deadline:
                raise DbtCaptureError("capture_profile_deadline")

        check()
        if type(supervisor) is not CompositionSupervisorProjection:
            raise DbtCaptureError("capture_profile_supervisor")
        projection = CompositionSupervisorProjection.from_mapping(supervisor.to_dict()).to_dict()
        root = absolute_supervisor_path(supervisor_root)
        profiles = absolute_supervisor_path(profiles_root)
        if any(str(path).startswith("//") for path in (root, profiles)):
            raise DbtCaptureError("capture_allocation_path")
        paths = {"supervisor": root, "run": root / "run", "profiles": profiles}
        check()
        require_supervisor()
        check()
        with ExitStack() as stack:
            descriptors: dict[str, int] = {}
            originals: dict[str, dict[str, object]] = {}
            for name, path in paths.items():
                check()
                descriptor = open_protected(path, require_current=check)
                stack.callback(os.close, descriptor)
                check()
                descriptors[name] = descriptor
                originals[name] = _facts(path, descriptor, check)
            if originals["profiles"]["filesystem_type"] != 0x01021994:
                raise DbtCaptureError("capture_profile_not_tmpfs")
            for name, path in paths.items():
                check()
                current = open_protected(path, require_current=check)
                stack.callback(os.close, current)
                check()
                if (
                    _facts(path, descriptors[name], check) != originals[name]
                    or _facts(path, current, check) != originals[name]
                ):
                    raise DbtCaptureError("capture_profile_changed")
            check()
            require_supervisor()
            check()
            result = canonical_json_bytes(
                {
                    "schema": "dpone.composition-native-profile-observation.v1",
                    "supervisor": projection,
                    "supervisor_sha256": "sha256:" + sha256(canonical_json_bytes(projection)).hexdigest(),
                    "roots": originals,
                }
            )
            check()
        check()
        return result
    except DbtCaptureError:
        raise
    except (OSError, ValueError, TypeError, AttributeError, OverflowError):
        raise DbtCaptureError("capture_profile_unavailable") from None


def _facts(path: Path, descriptor: int, check: Callable[[], None]) -> dict[str, object]:
    check()
    info = os.fstat(descriptor)
    check()
    kind = filesystem_type(descriptor)
    check()
    return {
        "path": str(path),
        "device": info.st_dev,
        "inode": info.st_ino,
        "uid": info.st_uid,
        "gid": info.st_gid,
        "mode": stat.S_IMODE(info.st_mode),
        "filesystem_type": kind,
    }
