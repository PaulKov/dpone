"""Exercise actual built artifacts in a detached target, not a source import."""

import os
import subprocess
import sys
from email.parser import BytesParser
from pathlib import Path
from zipfile import ZipFile

import pytest


def wheels():
    plugin = os.environ.get("DPONE_TEST_DBT_STRICT_WHEEL")
    base = os.environ.get("DPONE_TEST_BASE_WHEEL")
    if not plugin or not base:
        pytest.skip("build exact base/plugin wheels and supply their artifact paths")
    return Path(plugin), Path(base)


def test_wheel_metadata_and_packaged_project():
    plugin, _ = wheels()
    with ZipFile(plugin) as archive:
        metadata = BytesParser().parsebytes(
            archive.read(next(n for n in archive.namelist() if n.endswith("/METADATA")))
        )
        assert metadata["Name"] == "dbt-dpone-sqlserver"
        assert metadata["Metadata-Version"] == "2.4"
        assert set(metadata["Requires-Python"].split(",")) == {">=3.11", "<3.13"}
        assert any("dbt-sqlserver==1.11.1" == dep for dep in metadata.get_all("Requires-Dist"))
        assert "dbt/include/dpone_sqlserver/dbt_project.yml" in archive.namelist()
        assert "dbt/adapters/dpone_sqlserver/delivery.py" in archive.namelist()


def test_detached_built_wheels_normal_loader_and_base_optional_boundary(tmp_path):
    plugin, base = wheels()
    target = tmp_path / "installed"
    subprocess.run(
        ["uv", "pip", "install", "--no-deps", "--target", str(target), str(base), str(plugin)],
        check=True,
        capture_output=True,
    )
    script = """
import sys
sys.path.insert(0, sys.argv[1])
import dpone
assert not any(name == 'dbt' or name.startswith('dbt.') for name in sys.modules)
from dbt.adapters.factory import FACTORY
cls = FACTORY.load_plugin('dpone_sqlserver')
assert cls.type.fget(None) == 'dpone_sqlserver'
import dbt.adapters.dpone_sqlserver as plugin
assert plugin.__file__.startswith(sys.argv[1])
assert dpone.__file__.startswith(sys.argv[1])
from dbt.adapters.sqlserver import SQLServerAdapter
assert 'dpone_physical_protocol_v1' not in SQLServerAdapter._available_
"""
    subprocess.run([sys.executable, "-I", "-c", script, str(target)], cwd=tmp_path, check=True, capture_output=True)
    script = """
import sys
sys.path.insert(0, sys.argv[1])
class NoOptional:
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {'dbt', 'pyodbc', 'mssql_python'}:
            raise ImportError('optional dbt/driver import is prohibited')
sys.meta_path.insert(0, NoOptional())
from dpone.cli.main import main
main(['--help'])
"""
    result = subprocess.run(
        [sys.executable, "-I", "-c", script, str(target)], cwd=tmp_path, check=True, capture_output=True, text=True
    )
    assert "usage:" in result.stdout.lower()
