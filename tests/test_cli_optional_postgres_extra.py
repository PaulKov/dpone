"""CLI must start when only connector extras are installed (no postgres extra)."""

from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_cli_registry_imports_without_psycopg() -> None:
    code = """
import builtins
import sys
from pathlib import Path

sys.path.insert(0, str(Path.cwd() / "src"))
real_import = builtins.__import__

def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
    if name == "psycopg" or name.startswith("psycopg."):
        raise ImportError("psycopg blocked for optional-extra contract test")
    return real_import(name, globals, locals, fromlist, level)

builtins.__import__ = guarded_import
for module_name in list(sys.modules):
    if module_name.startswith("dpone."):
        sys.modules.pop(module_name, None)

from dpone.commands.registry import get_commands

assert get_commands()
"""
    env = dict(os.environ)
    env["PYTHONPATH"] = f"{ROOT / 'src'}{os.pathsep}{env.get('PYTHONPATH', '')}"
    result = subprocess.run(
        [sys.executable, "-c", code],
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr or result.stdout
