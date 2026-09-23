"""Create the existing deployment manifest from independently approved inventories.

This offline build tool confers no trust: its caller must retain approval of all
input inventories and the resulting manifest digest outside the candidate roots.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import time
from pathlib import Path
from typing import Any

from dpone.adapters.mssql_sqlclient_installation import (
    _deps,
    _file_hash,
    _inventory,
    _profile,
    _read,
    _relative,
    _root,
    _runtime_inventory,
    validate_manifest,
)
from dpone.contracts.mssql_tds_validation import _hash
from dpone.contracts.strict_json import canonical_json_bytes, strict_json_object

STEM = "Dpone.Mssql.SqlClient.Worker"
PROFILE = {
    "schema_version": 1,
    "backend": "mssql_sqlclient",
    "platform": "linux_arm64",
    "runtime_version": "8.0.31",
    "sqlclient_version": "7.0.2",
    "arrow_version": "23.0.0",
}
MANAGED = frozenset(
    """Apache.Arrow.Scalars.dll Apache.Arrow.dll
Microsoft.Bcl.Cryptography.dll Microsoft.Data.SqlClient.Extensions.Abstractions.dll
Microsoft.Data.SqlClient.Internal.Logging.dll Microsoft.Data.SqlClient.dll
Microsoft.Extensions.Caching.Abstractions.dll Microsoft.Extensions.Caching.Memory.dll
Microsoft.Extensions.DependencyInjection.Abstractions.dll Microsoft.Extensions.Logging.Abstractions.dll
Microsoft.Extensions.Options.dll Microsoft.Extensions.Primitives.dll Microsoft.IdentityModel.Abstractions.dll
Microsoft.IdentityModel.JsonWebTokens.dll Microsoft.IdentityModel.Logging.dll
Microsoft.IdentityModel.Protocols.OpenIdConnect.dll Microsoft.IdentityModel.Protocols.dll
Microsoft.IdentityModel.Tokens.dll Microsoft.SqlServer.Server.dll System.Configuration.ConfigurationManager.dll
System.Diagnostics.EventLog.dll System.IdentityModel.Tokens.Jwt.dll System.Security.Cryptography.Pkcs.dll
System.Security.Cryptography.ProtectedData.dll""".split()
) | frozenset(
    locale + "/Microsoft.Data.SqlClient.resources.dll"
    for locale in "cs de es fr it ja ko pl pt-BR ru tr zh-Hans zh-Hant".split()
)
COMPANION_ROLES = {name: "managed_dependency" for name in MANAGED} | {
    STEM + ".dll": "worker_assembly",
    STEM + ".runtimeconfig.json": "worker_runtimeconfig",
    STEM + ".deps.json": "worker_deps",
}


def read_object(path: Path) -> dict[str, Any]:
    """Read a bounded strict JSON object, rejecting aliases and duplicate keys."""
    if not path.is_absolute() or path.resolve(strict=True) != path:
        raise ValueError("producer.input_invalid")
    return strict_json_object(_read(path, time.monotonic_ns() + 30_000_000_000, 2 * 1024**2))


def inventory(root: Path, *, deadline_ns: int) -> dict[str, str]:
    """Measure actual files; measurements are never implicit expected pins."""
    _root(root)
    return {name: _file_hash(root / name, deadline_ns) for name in sorted(_inventory(root, deadline_ns))}


def validate_pins(expected: dict[str, str]) -> None:
    """Validate externally approved, exact, nonempty bounded file inventory."""
    if type(expected) is not dict or not 1 <= len(expected) <= 4096:
        raise ValueError("producer.inventory_invalid")
    for name, digest in expected.items():
        _relative(name)
        if str(Path(name)) != name:
            raise ValueError("producer.inventory_invalid")
        _hash(digest)


def verify_tree(root: Path, expected: dict[str, str], *, deadline_ns: int | None = None) -> dict[str, str]:
    """Require exact names and bytes, including rejection of extra or linked files."""
    validate_pins(expected)
    actual = inventory(root, deadline_ns=deadline_ns or time.monotonic_ns() + 60_000_000_000)
    if actual != expected:
        raise ValueError("producer.inventory_mismatch")
    return actual


def validate_companion(root: Path, expected: dict[str, str], *, deadline_ns: int) -> None:
    """Use the unchanged runtime/dependency validators on the fixed forty files."""
    if set(expected) != set(COMPANION_ROLES):
        raise ValueError("producer.companion_profile_invalid")
    verify_tree(root, expected, deadline_ns=deadline_ns)
    _profile(read_object(root / (STEM + ".runtimeconfig.json")))
    _deps(read_object(root / (STEM + ".deps.json")), expected)


def outside_roots(path: Path, roots: tuple[Path, ...]) -> None:
    """Reject self-inclusion and existing output before any output side effect."""
    if not path.is_absolute() or path.exists() or path.is_symlink():
        raise ValueError("producer.output_invalid")
    resolved = path.resolve()
    if path != resolved or any(path == root or root in path.parents for root in roots):
        raise ValueError("producer.output_invalid")


def produce(
    *,
    companion: Path,
    runtime: Path,
    expected_companion: dict[str, str],
    expected_runtime: dict[str, str],
    build_receipt: Path,
    expected_receipt_sha256: str,
    output: Path,
) -> dict[str, Any]:
    """Emit a create-only manifest/receipt directory after all input checks pass."""
    deadline = time.monotonic_ns() + 120_000_000_000
    _root(companion)
    _root(runtime)
    outside_roots(output, (companion, runtime))
    _hash(expected_receipt_sha256)
    if _file_hash(build_receipt, deadline) != expected_receipt_sha256:
        raise ValueError("producer.receipt_mismatch")
    receipt = read_object(build_receipt)
    if receipt.get("status") != "PASS" or receipt.get("companion") != expected_companion:
        raise ValueError("producer.receipt_invalid")
    validate_companion(companion, expected_companion, deadline_ns=deadline)
    validate_pins(expected_runtime)
    if set(expected_runtime) != _runtime_inventory(runtime, deadline):
        raise ValueError("producer.runtime_inventory_mismatch")
    for name, digest in expected_runtime.items():
        if _file_hash(runtime / name, deadline) != digest:
            raise ValueError("producer.runtime_hash_mismatch")
    files = [
        {"origin": "companion", "path": name, "role": COMPANION_ROLES[name], "sha256": digest}
        for name, digest in sorted(expected_companion.items())
    ] + [
        {
            "origin": "dotnet",
            "path": name,
            "role": "dotnet_host" if name == "dotnet" else "runtime_library",
            "sha256": digest,
        }
        for name, digest in sorted(expected_runtime.items())
    ]
    body = canonical_json_bytes(PROFILE | {"files": files})
    digest = hashlib.sha256(b"dpone.sqlclient.deployment.v1\0" + body).hexdigest()
    validate_manifest(body, digest)
    result = {
        "status": "PASS",
        "build_sha256": digest,
        "companion_inventory": expected_companion,
        "build_receipt_sha256": expected_receipt_sha256,
        "runtime_inventory": expected_runtime,
        "scope": "deployment_manifest_only; no trust, startup, SQL or route certification",
    }
    output.mkdir(mode=0o700)
    (output / "deployment.json").write_bytes(body)
    (output / "admission-receipt.pending").write_bytes(canonical_json_bytes(result))
    (output / "admission-receipt.pending").rename(output / "admission-receipt.json")
    return result


def main() -> int:
    """Consume operator-approved pins separately from candidate roots."""
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("companion", "runtime", "pins", "build-receipt", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    arguments = parser.parse_args()
    try:
        pins = read_object(arguments.pins)
        if set(pins) != {"companion", "runtime", "build_receipt_sha256"}:
            raise ValueError("producer.pins_invalid")
        produce(
            companion=arguments.companion,
            runtime=arguments.runtime,
            expected_companion=pins["companion"],
            expected_runtime=pins["runtime"],
            build_receipt=arguments.build_receipt,
            expected_receipt_sha256=pins["build_receipt_sha256"],
            output=arguments.output,
        )
    except (ValueError, OSError, TypeError, KeyError):
        print(json.dumps({"status": "FAIL", "reason": "producer.inputs_or_output_invalid"}))
        return 1
    print(json.dumps({"status": "PASS", "scope": "deployment_manifest_only"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
