"""Immutable invocation inventory outside either benchmark subject checkout.

The digest detects accidental changes; it is not a signature or remote trust
root. Local ownership and filesystem permissions are the authority boundary.
No runtime attach may re-provision objects or discover replacement database pins.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class Inventory:
    """Explicit synthetic coordinates, generator and frozen provisioning results."""

    invocation_id: str
    profile: str
    rows: int
    seed: int
    strategy: str
    target_database: str
    state_database: str
    source_database: str
    schema: str
    table: str
    configuration: dict[str, Any]
    authority: dict[str, Any]
    objects: dict[str, Any]

    def __post_init__(self) -> None:
        require_invocation(self.invocation_id)
        for name in (self.target_database, self.state_database, self.source_database, self.schema, self.table):
            if not re.fullmatch(r"[a-zA-Z_][a-zA-Z0-9_]{0,119}", name):
                raise ValueError("local_fixture.invalid_coordinate")
        if self.schema != "dda_" + self.invocation_id or self.table != "business":
            raise ValueError("local_fixture.inventory_ownership")
        if self.strategy not in {"full_refresh", "partition_replace"}:
            raise ValueError("local_fixture.strategy")
        from tools.native_delivery_live_support.profiles import Dataset

        Dataset(self.profile, self.rows, self.seed)
        _safe(asdict(self))


def require_invocation(value: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"[a-f0-9]{32}", value):
        raise ValueError("local_fixture.invalid_invocation")


def canonical(value: Any) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False).encode()


def _safe(value: Any) -> None:
    if isinstance(value, dict):
        for key, item in value.items():
            if not isinstance(key, str) or any(token in key.lower() for token in ("password", "secret", "credential")):
                raise ValueError("local_fixture.secret_in_inventory")
            _safe(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _safe(item)
    elif value is not None and type(value) not in (str, bool, int, float):
        raise ValueError("local_fixture.non_json_inventory")


class InventoryStore:
    """Create once before provisioning; attach is strictly read-only.

    Provisioning results are separately sealed via ``seal``. A crash before the
    seal leaves an inspectable intent, which is never accepted as runnable.
    """

    def __init__(self, root: Path) -> None:
        self.root = Path(root).absolute()
        if self.root.is_symlink():
            raise ValueError("local_fixture.symlink")
        self.root.mkdir(parents=True, exist_ok=True, mode=0o700)
        if self.root.stat().st_uid != os.getuid():
            raise ValueError("local_fixture.inventory_owner")

    def directory(self, invocation_id: str) -> Path:
        require_invocation(invocation_id)
        path = self.root / invocation_id
        if path.is_symlink():
            raise ValueError("local_fixture.symlink")
        return path

    def create(self, value: Inventory) -> None:
        payload = asdict(value)
        _safe(payload)
        directory = self.directory(value.invocation_id)
        directory.mkdir(mode=0o700)
        self._write(directory / "inventory.json", payload)

    def seal(self, invocation_id: str, results: dict[str, Any]) -> None:
        """Publish immutable actual catalog pins/requests after provisioning."""
        self.load(invocation_id)
        _safe(results)
        self._write(self.directory(invocation_id) / "provisioned.json", results)

    def provisioned(self, invocation_id: str) -> dict[str, Any]:
        self.load(invocation_id)
        return self._read(self.directory(invocation_id) / "provisioned.json")

    def load(self, invocation_id: str) -> Inventory:
        value = self._read(self.directory(invocation_id) / "inventory.json")
        result = Inventory(**value)
        if result.invocation_id != invocation_id:
            raise ValueError("local_fixture.inventory_identity")
        return result

    @staticmethod
    def _write(path: Path, payload: dict[str, Any]) -> None:
        body = canonical({"version": 1, "payload": payload, "sha256": hashlib.sha256(canonical(payload)).hexdigest()})
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(descriptor, "wb") as stream:
            stream.write(body)
            stream.flush()
            os.fsync(stream.fileno())
        descriptor = os.open(path.parent, os.O_RDONLY)
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)

    @staticmethod
    def _read(path: Path) -> dict[str, Any]:
        descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(descriptor, "rb") as stream:
            stat = os.fstat(stream.fileno())
            if stat.st_uid != os.getuid() or stat.st_mode & 0o077:
                raise ValueError("local_fixture.inventory_permissions")
            raw = stream.read(8 * 1024 * 1024 + 1)
        if len(raw) > 8 * 1024 * 1024:
            raise ValueError("local_fixture.inventory_size")
        value = json.loads(raw)
        if set(value) != {"version", "payload", "sha256"} or value["version"] != 1:
            raise ValueError("local_fixture.inventory_version")
        if hashlib.sha256(canonical(value["payload"])).hexdigest() != value["sha256"]:
            raise ValueError("local_fixture.inventory_digest")
        _safe(value["payload"])
        return value["payload"]
