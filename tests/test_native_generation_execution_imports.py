"""Public native execution imports must retain identity without a module cycle."""

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "first", ["native_generation_execution", "native_generation_build_bridge", "native_generation_invocation_recorder"]
)
def test_execution_public_imports_in_fresh_process(first):
    script = f"""
from importlib import import_module
import_module("dpone.runtime.{first}")
public = import_module("dpone.runtime.native_generation_execution")
recorder = import_module("dpone.runtime.native_generation_invocation_recorder")
bridge = import_module("dpone.runtime.native_generation_build_bridge")
assert public.TrustedDbtInvocationRecorder is recorder.TrustedDbtInvocationRecorder
assert public.ReservedDbtBuildBridge is bridge.ReservedDbtBuildBridge
assert public.NativeGenerationBuildRejected is bridge.NativeGenerationBuildRejected
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=20)
    assert result.returncode == 0, result.stderr
