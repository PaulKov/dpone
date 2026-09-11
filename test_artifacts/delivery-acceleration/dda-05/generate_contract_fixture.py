"""Reproduce the retained v1 absence fixture; no service access or live authority."""

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT))


def main() -> None:
    from tools.native_delivery_live_support.artifacts import ArtifactStore
    from tools.native_delivery_live_support.execution import git_identity
    from tools.native_delivery_live_support.profiles import Dataset
    from tools.native_delivery_live_support.runner import absent_run, configuration, route_record

    limits = {
        "max_total_encoded_bytes": 104857600,
        "stage_allocated_bytes_stop_threshold": 104857600,
        "max_rows": 65536,
        "max_bytes": 16777216,
        "max_row_bytes": 1048576,
        "max_pending": 2,
        "max_staging_tables": 1024,
        "parallelism": 1,
    }
    output = Path(__file__).with_name("contract-fixture.json")
    absent_run(
        ArtifactStore(output, overwrite=True),
        Dataset("unicode", 32, 7),
        configuration(limits),
        route_record("full_refresh", "bounded_native"),
        git_identity(ROOT),
        "hermetic_contract_fixture_no_live_authority",
    )
    print(f"{output}: SKIP (fixture only)")


if __name__ == "__main__":
    main()
