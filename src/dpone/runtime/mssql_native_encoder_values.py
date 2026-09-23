"""Compatibility alias for scalar native encoding implementation."""

import sys

from dpone.runtime import mssql_native_encoder as _canonical

sys.modules[__name__] = _canonical
