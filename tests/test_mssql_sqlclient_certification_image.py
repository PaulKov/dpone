from __future__ import annotations

import importlib.util
import json
import sys
from pathlib import Path

import pytest


def _module():
    path = Path("tools/mssql_sqlclient_certification_image.py")
    spec = importlib.util.spec_from_file_location("mssql_sqlclient_certification_image", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_builder_requires_clean_head_and_records_image_identity(tmp_path: Path, monkeypatch) -> None:
    module = _module()
    root = tmp_path / "repository"
    (root / "docker/mssql-sqlclient-certification").mkdir(parents=True)
    (root / "docker/mssql-sqlclient-certification/Dockerfile").write_text("FROM scratch\n")
    commit, tree, image = "a" * 40, "b" * 40, "c" * 64

    def git(_root, *args):
        if args[0] == "status":
            return ""
        return tree if args[-1].endswith("^{tree}") else commit

    monkeypatch.setattr(module, "_git", git)
    monkeypatch.setattr(module, "_run", lambda _command: None)
    monkeypatch.setattr(
        module,
        "_capture",
        lambda _command: json.dumps(
            [
                {
                    "Id": f"sha256:{image}",
                    "Os": "linux",
                    "Architecture": "amd64",
                    "Config": {
                        "Labels": {
                            "org.opencontainers.image.revision": commit,
                            "dev.dpone.certification.source-tree": tree,
                            "dev.dpone.certification.route": "clickhouse-mssql-sqlclient-v1",
                        }
                    },
                }
            ]
        ),
    )
    # The mocked archive command does not create a tar, so provide a deterministic empty archive.
    original_tarfile_open = module.tarfile.open

    class EmptyArchive:
        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def extractall(self, context, *, filter):
            del filter
            dockerfile = Path(context) / "docker/mssql-sqlclient-certification/Dockerfile"
            dockerfile.parent.mkdir(parents=True, exist_ok=True)
            dockerfile.write_text("FROM scratch\n")

    monkeypatch.setattr(module.tarfile, "open", lambda *_args, **_kwargs: EmptyArchive())
    output = tmp_path / "receipt.json"
    receipt = module.build_image(root, docker="docker", tag="candidate", output=output)
    monkeypatch.setattr(module.tarfile, "open", original_tarfile_open)

    assert receipt["schema_version"] == "dpone.mssql-sqlclient.certification-image.v2"
    assert receipt["source_commit_sha"] == commit
    assert receipt["source_tree_oid"] == tree
    assert receipt["runner_image_sha256"] == image
    assert receipt["runner_platform"] == "linux/amd64"
    assert json.loads(output.read_text()) == receipt


def test_builder_rejects_dirty_or_non_head_source(tmp_path: Path, monkeypatch) -> None:
    module = _module()
    root = tmp_path / "repository"
    root.mkdir()
    commit = "a" * 40
    calls = iter((commit, "b" * 40, commit, "dirty"))
    monkeypatch.setattr(module, "_git", lambda *_args: next(calls))
    with pytest.raises(ValueError, match="worktree_dirty"):
        module.build_image(root, docker="docker", tag="candidate", output=tmp_path / "receipt.json")
