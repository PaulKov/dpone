"""Fresh-interpreter coverage for the legacy composition import contract."""

import subprocess
import sys


def test_legacy_bootstrap_import_registers_both_runtime_ports_without_optional_sdks():
    script = """
import sys
import dpone.runtime.bootstrap as legacy
from dpone.ports.runtime_hydrator import get_runtime_hydrator
from dpone.ports.process_runner import get_process_runner
assert isinstance(get_runtime_hydrator(), legacy.DefaultRuntimeHydrator)
assert isinstance(get_process_runner(), legacy.DefaultProcessRunner)
assert 'google.cloud' not in sys.modules
"""
    result = subprocess.run([sys.executable, "-c", script], capture_output=True, text=True, timeout=60)
    assert result.returncode == 0, result.stderr
