"""Synthetic policy contracts: inspected source is data, never executable input."""

from __future__ import annotations

import json
import subprocess
import sys
from pathlib import Path

import pytest
from tools.agent_policy.import_mutation_gate import scan


@pytest.mark.parametrize(
    "source",
    [
        "import vendor\nvendor.Client.run = replacement",
        "from vendor import Client as C\nAlias = C\nAlias.run = replacement",
        "class C: pass\nC.run = replacement",
        "def f(): pass\nf.marker = True",
        "import vendor\nif enabled:\n vendor.run = replacement",
        "import vendor\nclass C:\n vendor.run = replacement",
        "import vendor\nsetattr(vendor, 'run', replacement)",
        "import vendor\ns = setattr\ns(vendor, 'run', replacement)",
        "import vendor\ndef install(target):\n target.run = replacement\ninstall(vendor)",
        "import vendor\ndef install(target):\n setattr(target, 'run', replacement)\nf = install\nf(target=vendor)",
        "import vendor\ndef inner(x):\n x.run = replacement\ndef outer(x):\n inner(x)\nouter(vendor)",
        "import vendor\ndef install(x=vendor):\n x.run = replacement\ninstall()",
        "import sys\nsys.modules['vendor'] = replacement",
        "from sys import modules as registry\nregistry.update({'vendor': replacement})",
        "import sys\ntarget = sys.modules.get('vendor')\ntarget.run = replacement",
        "from importlib import import_module\ntarget = import_module('vendor')\ntarget.run = replacement",
        "import gevent.monkey as m\nm.patch_all()",
        "exec('anything')",
        "e = eval\ne('anything')",
        "import builtins\nbuiltins.exec('anything')",
        "import vendor\ntry:\n alias = vendor\nexcept Exception:\n alias = None\nalias.run = replacement",
    ],
)
def test_rejects_import_executed_mutation(tmp_path: Path, source: str) -> None:
    root = tmp_path / "src"
    root.mkdir()
    (root / "sample.py").write_text(source)
    report = scan(tmp_path)
    assert report["status"] == "FAIL"
    assert report["findings"]


@pytest.mark.parametrize(
    "source",
    [
        "from vendor import Client\nExport = Client",
        "class Client:\n def run(self):\n  self.value = 1\nclient = Client()\nclient.value = 2",
        "import vendor\ndef later():\n vendor.run = replacement",
        "text = 'setattr(vendor, run, replacement)'\n# vendor.run = x",
        "from vendor import Client\nclient = Client()\nsetattr(client, 'value', 2)",
        "class Child(Base):\n def run(self):\n  return super().run()",
    ],
)
def test_allows_explicit_definitions_data_and_deferred_code(tmp_path: Path, source: str) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text(source)
    assert scan(tmp_path)["status"] == "PASS"


def test_inventory_errors_and_deterministic_cli(tmp_path: Path) -> None:
    root = tmp_path / "src"
    root.mkdir()
    (root / "bad.py").write_text("def broken(")
    (root / "tests").mkdir()
    (root / "tests" / "fixture.py").write_text("exec('fixture')")
    package = tmp_path / "packages" / "synthetic" / "src"
    package.mkdir(parents=True)
    (package / "bootstrap.py").write_text("raise RuntimeError('must not execute')\nexec('x')")
    command = [sys.executable, "tools/agent_policy/import_mutation_gate.py", "--repo-root", str(tmp_path)]
    first = subprocess.run(command, capture_output=True, text=True, check=False)
    second = subprocess.run(command, capture_output=True, text=True, check=False)
    assert first.returncode == second.returncode == 1
    assert first.stdout == second.stdout
    assert first.stderr == ""
    payload = json.loads(first.stdout)
    assert payload["status"] == "FAIL"
    assert payload["files_scanned"] == 2
    assert {finding["code"] for finding in payload["findings"]} == {"invalid-source", "dynamic-execution"}


def test_rejects_missing_and_symlink_roots(tmp_path: Path) -> None:
    assert scan(tmp_path)["status"] == "FAIL"
    external = tmp_path / "external"
    external.mkdir()
    (tmp_path / "src").symlink_to(external, target_is_directory=True)
    assert scan(tmp_path)["status"] == "FAIL"


def test_rejects_symlink_file(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    target = tmp_path / "external.py"
    target.write_text("pass")
    (tmp_path / "src" / "escape.py").symlink_to(target)
    assert scan(tmp_path)["status"] == "FAIL"


@pytest.mark.parametrize(
    "source",
    [
        "target = __import__('vendor')\ntarget.run = replacement",
        "import vendor\ndef decorate(cls):\n cls.run = replacement\n return cls\n@decorate\nclass C: pass",
        "import vendor\nclass Bootstrap:\n @staticmethod\n def install():\n  vendor.run = replacement\nBootstrap.install()",
        "import vendor\ndef install():\n vendor.run = replacement\ndef f(value=install()): pass",
        "import vendor\nwith context():\n vendor.run = replacement",
        "import vendor\nfor _ in [1]:\n vendor.run = replacement",
        "import vendor\ndel vendor.run",
    ],
)
def test_additional_import_execution_forms(tmp_path: Path, source: str) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text(source)
    assert scan(tmp_path)["status"] == "FAIL"


def test_bounded_report_and_fail_closed_limits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from tools.agent_policy import import_mutation_gate as gate

    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text("exec('x')\n" * 5)
    monkeypatch.setattr(gate, "MAX_FINDINGS", 2)
    report = scan(tmp_path)
    assert report["status"] == "FAIL"
    assert len(report["findings"]) == 2
    assert report["omitted_findings"] == 3
    monkeypatch.setattr(gate, "MAX_BYTES", 1)
    assert scan(tmp_path)["findings"][0]["code"] == "source-size-limit"


def test_rejects_parent_symlink_and_parent_traversal(tmp_path: Path) -> None:
    real = tmp_path / "real"
    (real / "project" / "src").mkdir(parents=True)
    (tmp_path / "alias").symlink_to(real, target_is_directory=True)
    assert scan(tmp_path / "alias" / "project")["status"] == "FAIL"
    assert scan(real / ".." / "real" / "project")["status"] == "FAIL"


def test_cli_pass_and_invalid_argument(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "safe.py").write_text("class C: pass")
    command = [sys.executable, "tools/agent_policy/import_mutation_gate.py"]
    result = subprocess.run(command + ["--repo-root", str(tmp_path)], capture_output=True, text=True, check=False)
    assert result.returncode == 0
    assert json.loads(result.stdout)["status"] == "PASS"
    assert result.stderr == ""
    invalid = subprocess.run(command + ["--unknown"], capture_output=True, text=True, check=False)
    assert invalid.returncode == 2
    assert invalid.stdout == ""
    assert "unrecognized arguments" in invalid.stderr


def test_analysis_and_inventory_limits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from tools.agent_policy import import_mutation_gate as gate

    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text("def recurse():\n recurse()\nrecurse()")
    assert scan(tmp_path)["findings"][0]["code"] == "analysis-limit"
    monkeypatch.setattr(gate, "MAX_FILES", 0)
    assert scan(tmp_path)["findings"][0]["code"] == "file-count-limit"


def test_unreadable_source_is_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text("pass")

    def unreadable(_path: Path) -> bytes:
        raise PermissionError("sensitive detail")

    monkeypatch.setattr(Path, "read_bytes", unreadable)
    report = scan(tmp_path)
    assert report["status"] == "FAIL"
    assert "sensitive detail" not in json.dumps(report)


@pytest.mark.parametrize(
    "source",
    [
        "import vendor\nclass Bootstrap:\n def __init__(self):\n  vendor.run = replacement\nBootstrap()",
        "import vendor\nclass Bootstrap:\n @classmethod\n def install(cls, target):\n  target.run = replacement\nBootstrap.install(vendor)",
        "class Bootstrap:\n @classmethod\n def install(cls):\n  cls.run = replacement\nBootstrap.install()",
        "import vendor\n(a,b)=(vendor,vendor)\na.run = replacement",
        "import vendor\ndef install():\n vendor.run = replacement\ndef f(x: install()): pass",
        "import vendor\ndef install():\n vendor.run = replacement\nvalue: install()",
        "import vendor\ndef resolve():\n return vendor\ntarget = resolve()\ntarget.run = replacement",
        "import vendor\ndef resolve():\n global target\n target = vendor\nresolve()\ntarget.run = replacement",
    ],
)
def test_review_provenance_regressions(tmp_path: Path, source: str) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text(source)
    assert scan(tmp_path)["status"] == "FAIL"


def test_rejects_fifo_without_opening_it(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    import os

    (tmp_path / "src").mkdir()
    os.mkfifo(tmp_path / "src" / "blocked.py")

    def must_not_read(_path: Path) -> bytes:
        pytest.fail("A nonregular source must never be read")

    monkeypatch.setattr(Path, "read_bytes", must_not_read)
    assert scan(tmp_path)["findings"][0]["code"] == "nonregular-source"


@pytest.mark.parametrize(
    "source",
    [
        "from __future__ import annotations\nimport vendor\ndef install():\n vendor.run = replacement\ndef f(x: install()): pass\nvalue: install()",
        "class C:\n def __init__(self, value):\n  self.value = value\nC(1)",
        "import vendor\nclass C: pass\n(instance, module) = (C(), vendor)\ninstance.value = 1",
        "import vendor\ndef later():\n value: vendor.install_patch()\nlater()",
    ],
)
def test_review_safe_forms(tmp_path: Path, source: str) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text(source)
    assert scan(tmp_path)["status"] == "PASS"


def test_helper_return_argument_provenance(tmp_path: Path) -> None:
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text(
        "import vendor\ndef resolve():\n return vendor\nsetattr(resolve(), 'run', replacement)"
    )
    assert scan(tmp_path)["status"] == "FAIL"


def test_callable_alias_returns_are_conservative_across_hash_seeds(tmp_path: Path) -> None:
    import os

    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "sample.py").write_text(
        "import vendor\ndef identity(x): return x\ndef noop(x): return None\n"
        "if flag:\n pick=identity\nelse:\n pick=noop\nalias=pick(vendor)\nalias.run=replacement"
    )
    command = [sys.executable, "tools/agent_policy/import_mutation_gate.py", "--repo-root", str(tmp_path)]
    outputs = []
    for seed in range(8):
        result = subprocess.run(
            command, capture_output=True, text=True, check=False, env={**os.environ, "PYTHONHASHSEED": str(seed)}
        )
        assert result.returncode == 1, f"false PASS for hash seed {seed}"
        outputs.append(result.stdout)
    assert len(set(outputs)) == 1
