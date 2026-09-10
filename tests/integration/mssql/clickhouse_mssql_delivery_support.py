"""DDA-owned opt-in fixture helpers; no shared service setup or credential defaults."""

from __future__ import annotations

import json
import os

import pytest
from tools.native_delivery_live_benchmark import approved
from tools.native_delivery_live_support.execution import load_factory
from tools.native_delivery_live_support.runner import configuration, route_record


def live_factory(strategy: str, mode: str):
    """Gate before factory import, credentials, SQL or subprocess discovery."""
    if not approved():
        pytest.skip("SKIP: DDA disposable environment is not explicitly approved")
    reference = os.environ.get("DPONE_DDA_ROUTE_FACTORY")
    limits_path = os.environ.get("DPONE_DDA_LIMITS_FILE")
    if not reference or not limits_path:
        pytest.skip("UNVERIFIED: real route factory or explicit limits file is unavailable")
    from pathlib import Path

    try:
        config = configuration(json.loads(Path(limits_path).read_text(encoding="utf-8")))
        route = route_record(strategy, mode)
        factory = load_factory(reference, configuration=config, route=route)
    except Exception:
        pytest.fail(
            "Live factory preparation failed; inspect approved configuration (diagnostics redacted)", pytrace=False
        )
    if factory.execution != "live":
        pytest.fail("A hermetic factory cannot execute a live certification test", pytrace=False)
    return factory, config, route
