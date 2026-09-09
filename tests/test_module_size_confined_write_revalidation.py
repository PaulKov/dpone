"""Final module-size baseline verification must not mutate parent namespaces."""

from __future__ import annotations

from pathlib import Path

import pytest

import dpone.metrics.module_size_confined_write as confined_write


def test_final_revalidation_does_not_recreate_deleted_parent(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    repo = tmp_path / "repo"
    docs = repo / "docs"
    docs.mkdir(parents=True)
    baseline = docs / "module_size_baseline.json"
    before = b'{"schema_version":1}\n'
    candidate = b'{"schema_version":2}\n'
    baseline.write_bytes(before)
    detached_docs = repo / "docs.detached"
    real_require = confined_write._require_current_parent
    checks = 0

    def remove_after_final_old_parent_check(
        anchor: Path,
        parent_parts: tuple[str, ...],
        parent_fd: int,
        *,
        root_identity: confined_write.ModuleSizeRootIdentity | None,
    ) -> None:
        nonlocal checks
        real_require(anchor, parent_parts, parent_fd, root_identity=root_identity)
        checks += 1
        if checks == 2:
            docs.rename(detached_docs)

    monkeypatch.setattr(confined_write, "_require_current_parent", remove_after_final_old_parent_check)

    with pytest.raises(confined_write.ModuleSizeConfinedWriteError, match="canonical baseline is unavailable"):
        confined_write.write_confined_baseline_bytes(
            anchor=repo,
            parts=("docs", baseline.name),
            content=candidate,
            expected_bytes=before,
        )

    assert not docs.exists()
    assert (detached_docs / baseline.name).read_bytes() == before
