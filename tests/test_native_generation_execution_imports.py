"""Native application owners import in either order without a module cycle."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "first",
    ["native_generation_build_bridge", "native_generation_build_evidence", "native_generation_invocation_recorder"],
)
def test_application_owners_import_in_fresh_process(first):
    script = f"""
from importlib import import_module
import_module("dpone.services.{first}")
recorder = import_module("dpone.services.native_generation_invocation_recorder")
bridge = import_module("dpone.services.native_generation_build_bridge")
assert bridge.TrustedDbtInvocationRecorder is recorder.TrustedDbtInvocationRecorder
from typing import get_type_hints
get_type_hints(bridge.ReservedDbtBuildBridge.__init__)
get_type_hints(recorder.TrustedDbtInvocationRecorder.__init__)
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
