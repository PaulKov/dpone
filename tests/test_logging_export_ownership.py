"""Fresh-process public logging identities and module ownership contracts."""

from __future__ import annotations

import subprocess
import sys

import pytest


@pytest.mark.parametrize(
    "first",
    [
        "dpone.runtime.etl_logging",
        "dpone.runtime.etl_logging.etl_logger",
        "dpone.runtime.logging_core",
    ],
)
def test_logging_import_order_preserves_public_identity(first: str) -> None:
    script = f"""
from importlib import import_module
import_module({first!r})
package = import_module('dpone.runtime.etl_logging')
implementation = import_module('dpone.runtime.etl_logging.etl_logger')
core = import_module('dpone.runtime.logging_core')
assert package.ETLLogger is implementation.ETLLogger
assert package.etl_logger is implementation.etl_logger is core.etl_logger
"""
    subprocess.run([sys.executable, "-c", script], check=True, capture_output=True, timeout=30)


def test_runtime_port_import_does_not_rewrite_public_package_exports() -> None:
    script = """
from importlib import import_module
from types import ModuleType
package = import_module('dpone.runtime.etl_logging')
writes = []
class ObservedModule(ModuleType):
    def __setattr__(self, name, value):
        if name in {'ETLLogger', 'etl_logger'}:
            writes.append(name)
        super().__setattr__(name, value)
package.__class__ = ObservedModule
import_module('dpone.runtime.logging_core')
assert not writes, writes
"""
    subprocess.run([sys.executable, "-c", script], check=True, capture_output=True, timeout=30)


def test_top_level_import_does_not_initialize_logging() -> None:
    script = """
import sys
import dpone
assert 'dpone.runtime.etl_logging' not in sys.modules
assert 'dpone.runtime.etl_logging.etl_logger' not in sys.modules
"""
    subprocess.run([sys.executable, "-c", script], check=True, capture_output=True, timeout=30)
