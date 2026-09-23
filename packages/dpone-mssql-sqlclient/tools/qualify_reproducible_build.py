"""Run two isolated locked builds and prove byte-for-byte companion equality."""

from __future__ import annotations

import argparse
import hashlib
import json
from collections.abc import Callable
from pathlib import Path
from typing import Any

from build_companion import build
from produce_admission import outside_roots, read_object

from dpone.contracts.strict_json import canonical_json_bytes

Builder = Callable[..., dict[str, Any]]


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def qualify(
    *,
    source: Path,
    sdk: Path,
    feed: Path,
    pins: dict[str, Any],
    output: Path,
    builder: Builder = build,
) -> dict[str, Any]:
    """Create two independent builds and retain a closed equivalence receipt."""
    outside_roots(output, (source, sdk, feed))
    output.mkdir(mode=0o700)
    first, second = output / "build-one", output / "build-two"
    try:
        one = builder(source=source, sdk=sdk, feed=feed, pins=pins, output=first)
        two = builder(source=source, sdk=sdk, feed=feed, pins=pins, output=second)
        if one.get("status") != "PASS" or two.get("status") != "PASS" or one.get("companion") != two.get("companion"):
            raise ValueError("producer.reproducibility_mismatch")
        receipt = {
            "schema_version": "dpone.mssql-sqlclient.reproducible-build.v1",
            "status": "PASS",
            "platform": "linux_arm64",
            "sdk_image": one["sdk_image_external_controller_pin"],
            "sdk_version": one["sdk_version"],
            "input_pins_sha256": hashlib.sha256(canonical_json_bytes(pins)).hexdigest(),
            "companion_inventory": one["companion"],
            "build_receipt_sha256": [_sha256(first / "build-receipt.json"), _sha256(second / "build-receipt.json")],
        }
        pending = output / "reproducibility-receipt.pending"
        pending.write_bytes(canonical_json_bytes(receipt))
        pending.rename(output / "reproducibility-receipt.json")
        return receipt
    except BaseException:
        (output / "reproducibility-failure.json").write_bytes(
            canonical_json_bytes({"status": "FAIL", "success_receipt_emitted": False})
        )
        raise


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name in ("source", "sdk", "feed", "pins", "output"):
        parser.add_argument("--" + name, type=Path, required=True)
    args = parser.parse_args()
    try:
        qualify(
            source=args.source,
            sdk=args.sdk,
            feed=args.feed,
            pins=read_object(args.pins),
            output=args.output,
        )
    except (ValueError, OSError, TypeError, KeyError):
        print(json.dumps({"status": "FAIL", "reason": "producer.build_or_reproducibility_invalid"}))
        return 1
    print(json.dumps({"status": "PASS", "scope": "reproducible_build_only"}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
