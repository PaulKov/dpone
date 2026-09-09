"""Generate the exact reviewed route-live inventory from observed vendor IDs."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from .contract import CertificationValidationError
from .io_authority import json_object, write_create_only_exact
from .registry import build_inventory


def write_inventory(*, vendor_metadata_path: Path, output: Path) -> dict[str, object]:
    """Validate vendor metadata and create an idempotent reviewed inventory."""

    vendors = json_object(
        vendor_metadata_path.read_bytes(),
        code="inventory_writer.vendor_metadata_invalid",
    )
    inventory = build_inventory(vendors)
    payload = (json.dumps(inventory, ensure_ascii=False, indent=2, allow_nan=False, sort_keys=True) + "\n").encode(
        "utf-8"
    )
    write_create_only_exact(output, payload, code="inventory_writer.output")
    return inventory


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--vendor-metadata", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        inventory = write_inventory(
            vendor_metadata_path=args.vendor_metadata,
            output=args.output,
        )
    except (CertificationValidationError, OSError, ValueError) as exc:
        print(str(exc), file=sys.stderr)
        return 1
    print(
        json.dumps(
            {
                "suite_count": len(inventory["suites"]),
                "case_count": sum(int(suite["case_count"]) for suite in inventory["suites"]),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
