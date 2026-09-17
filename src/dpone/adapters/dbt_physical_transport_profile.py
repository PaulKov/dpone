"""Whole-rendered-byte profile custody for concrete native delivery composition."""

import os
import stat
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from hashlib import sha256
from pathlib import Path
from threading import RLock

import yaml

from dpone.adapters.dbt_physical_transport_subprocess import open_physical_transport_directory
from dpone.adapters.dbt_runtime_profile import RuntimeDbtProfileRenderer
from dpone.adapters.native_dbt_profile_lease import NativeDbtProfileLease
from dpone.contracts.dbt_publishing import DbtProfileSpec, DbtSqlServerRuntimePolicy
from dpone.contracts.native_delivery_json import MAX_NATIVE_JSON_BYTES
from dpone.ports.dbt_publishing import RenderedDbtProfile


@dataclass(frozen=True, slots=True)
class PhysicalTransportProfileSnapshot:
    """Private ephemeral profile identity; never serialize into public evidence."""

    profile_name: str
    target_name: str
    database: str
    schema: str
    device: int
    inode: int
    sha256: str = field(repr=False)


class PhysicalTransportProfile:
    """Compose the real renderer and lease; caller-provided expected bytes reject.

    Pass this same instance as renderer and profile store. The lease must already
    be entered by the native application and outlive the build. The ordinary
    renderer's existing identity checks are not bypassed; before its distinct
    adapter dispatch is integrated, dpone_sqlserver rendering remains unavailable.
    """

    def __init__(self, *, renderer: RuntimeDbtProfileRenderer, lease: NativeDbtProfileLease) -> None:
        if type(renderer) is not RuntimeDbtProfileRenderer or type(lease) is not NativeDbtProfileLease:
            raise ValueError("physical profile requires the concrete renderer and native lease")
        self._renderer, self._lease, self._lock = renderer, lease, RLock()
        self._content: bytes | None = None
        self._spec: DbtProfileSpec | None = None
        self._directory = self._file = -1
        self._rendered = self._materialized = False

    def render(self, profile: DbtProfileSpec, adapter_runtime: DbtSqlServerRuntimePolicy) -> RenderedDbtProfile:
        """Capture only actual renderer output, once, without changing its bytes."""
        with self._lock:
            if self._rendered:
                raise ValueError("physical profile rendering is one-use")
            self._rendered = True
            if profile.adapter_type != "dpone_sqlserver":
                raise ValueError("physical profile requires the distinct qualified adapter")
            rendered = self._renderer.render(profile, adapter_runtime)
            if type(rendered.content) is not bytes or not 0 < len(rendered.content) <= MAX_NATIVE_JSON_BYTES:
                raise ValueError("physical rendered profile exceeds native custody bounds")
            try:
                document = yaml.safe_load(rendered.content)
                if type(document) is not dict or set(document) != {profile.profile_name}:
                    raise ValueError("physical rendered profile has unexpected profile membership")
                selected = document[profile.profile_name]
                if selected["target"] != profile.target_name or set(selected["outputs"]) != {profile.target_name}:
                    raise ValueError("physical rendered profile has unexpected target membership")
                output = selected["outputs"][profile.target_name]
                if (output["type"], output["database"], output["schema"]) != (
                    "dpone_sqlserver",
                    profile.database,
                    profile.schema,
                ):
                    raise ValueError("physical renderer differs from selected target identity")
            except (yaml.YAMLError, TypeError, KeyError, AttributeError):
                raise ValueError("physical rendered profile is malformed") from None
            self._content, self._spec = rendered.content, profile
            return rendered

    @contextmanager
    def materialize(self, content: bytes) -> Iterator[Path]:
        """Retain actual directory/file handles through the entire command scope."""
        with self._lock:
            if self._materialized or self._content is None or content != self._content or type(content) is not bytes:
                raise ValueError("physical materialization requires the actual one-use renderer bytes")
            self._materialized = True
            try:
                with self._lease.materialize(self._content) as path:
                    try:
                        self._directory = open_physical_transport_directory(path.parent)
                        self._file = os.open(path.name, os.O_RDONLY | os.O_NOFOLLOW, dir_fd=self._directory)
                        self.snapshot(path)
                        yield path
                    finally:
                        for name in ("_file", "_directory"):
                            descriptor = getattr(self, name)
                            setattr(self, name, -1)
                            if descriptor >= 0:
                                os.close(descriptor)
            finally:
                self._content = self._spec = None

    def snapshot(self, profile_file: Path) -> PhysicalTransportProfileSnapshot:
        """Recheck paths, held identities and whole bytes at command admission."""
        with self._lock:
            if self._file < 0 or self._directory < 0 or self._content is None or self._spec is None:
                raise ValueError("physical profile is not held and materialized")
            if profile_file != self._lease.profile_path:
                raise ValueError("physical profile path differs from its concrete lease")
            observed = os.stat(profile_file.name, dir_fd=self._directory, follow_symlinks=False)
            held = os.fstat(self._file)
            directory = os.fstat(self._directory)
            if (
                not stat.S_ISREG(held.st_mode)
                or held.st_uid != os.getuid()
                or held.st_mode & 0o077
                or held.st_nlink != 1
                or directory.st_uid != os.getuid()
                or directory.st_mode & 0o077
                or (observed.st_dev, observed.st_ino, observed.st_size) != (held.st_dev, held.st_ino, held.st_size)
                or held.st_size != len(self._content)
            ):
                raise ValueError("physical profile file custody changed")
            os.lseek(self._file, 0, os.SEEK_SET)
            if os.read(self._file, len(self._content) + 1) != self._content:
                raise ValueError("physical profile differs from whole actual renderer bytes")
            return PhysicalTransportProfileSnapshot(
                self._spec.profile_name,
                self._spec.target_name,
                self._spec.database,
                self._spec.schema,
                held.st_dev,
                held.st_ino,
                "sha256:" + sha256(self._content).hexdigest(),
            )
