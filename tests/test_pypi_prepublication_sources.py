from __future__ import annotations

import http.client
import json
import os
import subprocess
import sys
import urllib.error
from pathlib import Path

import pytest

from tests.pypi_prepublication_test_support import (
    NAMES,
    ROOT,
    VERSION,
    load_gate,
    publication_context,
    write_inventory,
)


def test_candidate_json_rejects_duplicate_unknown_and_nonfinite_fields(tmp_path: Path) -> None:
    module = load_gate()
    inventory, _, _ = write_inventory(tmp_path)
    original = inventory.read_text(encoding="utf-8")
    inventory.write_text(original.replace("{", '{"schema_version":1,', 1), encoding="utf-8")

    with pytest.raises(module.PrepublicationGateError, match="JSON_DUPLICATE_KEY"):
        module.load_candidate_inventory(inventory, expected_version=VERSION)

    payload = json.loads(original)
    payload["unexpected"] = True
    inventory.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(module.PrepublicationGateError, match="INVENTORY_SHAPE_INVALID"):
        module.load_candidate_inventory(inventory, expected_version=VERSION)

    inventory.write_text(original.replace('"schema_version": 1', '"schema_version": NaN'), encoding="utf-8")
    with pytest.raises(module.PrepublicationGateError, match="JSON_NONFINITE_NUMBER"):
        module.load_candidate_inventory(inventory, expected_version=VERSION)

    inventory.write_bytes(b'{"schema_version":' + b"9" * 5000 + b"}")
    with pytest.raises(module.PrepublicationGateError, match="CANDIDATE_JSON_INVALID"):
        module.load_candidate_inventory(inventory, expected_version=VERSION)


def test_candidate_inventory_rejects_nonregular_and_unbounded_sources(tmp_path: Path) -> None:
    module = load_gate()
    from tools import pypi_prepublication_source as source

    inventory, _, _ = write_inventory(tmp_path / "symlink")
    real_inventory = inventory.with_name("real-candidate-inventory.json")
    inventory.rename(real_inventory)
    inventory.symlink_to(real_inventory)
    with pytest.raises(module.PrepublicationGateError, match="INVENTORY_UNAVAILABLE"):
        module.load_candidate_inventory(inventory, expected_version=VERSION)

    oversized = tmp_path / "oversized-candidate-inventory.json"
    oversized.write_bytes(b" " * (source.MAX_CANDIDATE_INVENTORY_BYTES + 1))
    with pytest.raises(module.PrepublicationGateError, match="INVENTORY_OVERSIZED"):
        module.load_candidate_inventory(oversized, expected_version=VERSION)

    inventory, _, _ = write_inventory(tmp_path / "large-candidate")
    payload = json.loads(inventory.read_text(encoding="utf-8"))
    payload["artifacts"][0]["size_bytes"] = source.MAX_CANDIDATE_FILE_BYTES + 1
    inventory.write_text(json.dumps(payload), encoding="utf-8")
    with pytest.raises(module.PrepublicationGateError, match="CANDIDATE_DIGEST_INVALID"):
        module.load_candidate_inventory(inventory, expected_version=VERSION)


def test_local_candidate_tamper_or_stray_file_fails_before_network(tmp_path: Path) -> None:
    module = load_gate()
    inventory, dist, _ = write_inventory(tmp_path)
    (dist / NAMES[0]).write_bytes(b"tampered")
    calls = 0

    def forbidden_fetch(*_args):
        nonlocal calls
        calls += 1
        raise AssertionError("network must remain after local closure")

    with pytest.raises(module.PrepublicationGateError, match="DIST_SIZE_MISMATCH"):
        module.evaluate_prepublication(
            inventory,
            dist,
            expected_version=VERSION,
            context=publication_context(module),
            fetcher=forbidden_fetch,
        )
    assert calls == 0

    inventory, dist, _ = write_inventory(tmp_path / "second")
    (dist / "stray.txt").write_text("stray", encoding="utf-8")
    with pytest.raises(module.PrepublicationGateError, match="DIST_SET_MISMATCH"):
        module.evaluate_prepublication(
            inventory,
            dist,
            expected_version=VERSION,
            context=publication_context(module),
            fetcher=forbidden_fetch,
        )
    assert calls == 0


@pytest.mark.skipif(not hasattr(os, "mkfifo"), reason="FIFO fixture requires POSIX")
def test_local_fifo_and_symlink_candidates_fail_without_network(tmp_path: Path) -> None:
    module = load_gate()
    for index, replacement in enumerate(("fifo", "symlink")):
        inventory, dist, _ = write_inventory(tmp_path / replacement)
        target = dist / NAMES[0]
        target.unlink()
        if replacement == "fifo":
            os.mkfifo(target)
        else:
            target.symlink_to(dist / NAMES[1])

        with pytest.raises(module.PrepublicationGateError, match="DIST_(ENTRY_CHANGED|UNAVAILABLE)"):
            module.evaluate_prepublication(
                inventory,
                dist,
                expected_version=VERSION,
                context=publication_context(module),
                fetcher=lambda *_: (_ for _ in ()).throw(AssertionError(f"network call {index}")),
            )


def test_fetch_rejects_duplicate_json_and_treats_only_404_as_absent(monkeypatch) -> None:
    module = load_gate()
    from tools import pypi_prepublication_pypi as adapter

    class Response:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, _maximum: int) -> bytes:
            return b'{"info":{},"info":{},"urls":[]}'

    monkeypatch.setattr(adapter.urllib.request, "urlopen", lambda *_args, **_kwargs: Response())
    with pytest.raises(module.PrepublicationGateError, match="JSON_DUPLICATE_KEY"):
        module.fetch_pypi_version("dpone", VERSION)

    def missing(*_args, **_kwargs):
        raise urllib.error.HTTPError("https://pypi.org", 404, "missing", {}, None)

    monkeypatch.setattr(adapter.urllib.request, "urlopen", missing)
    assert module.fetch_pypi_version("dpone", VERSION) is None

    for transport_error in (urllib.error.URLError("offline"), http.client.IncompleteRead(b"partial")):

        def unavailable(*_args, error=transport_error, **_kwargs):
            raise error

        monkeypatch.setattr(adapter.urllib.request, "urlopen", unavailable)
        with pytest.raises(module.PrepublicationGateError, match="PUBLIC_NETWORK_ERROR"):
            module.fetch_pypi_version("dpone", VERSION)


def test_tool_is_dependency_neutral() -> None:
    result = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import sys\n"
                "before = set(sys.modules)\n"
                "from tools import pypi_prepublication_gate as gate\n"
                "added = set(sys.modules) - before\n"
                "assert not any(name == 'packaging' or name.startswith('packaging.') for name in added)\n"
                "assert gate.version_url('dpone', '0.74.0').startswith('https://pypi.org/')\n"
            ),
        ],
        cwd=ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
