"""A restart/concurrent invocation cannot reclaim an attempted plan."""

from concurrent.futures import ThreadPoolExecutor
from importlib import import_module

import pytest


def adapter(directory):
    return import_module("dpone.adapters.publication_retirement_attempts").PrivateRetirementAttempts(directory)


def test_atomic_claim_has_one_winner_across_adapter_instances(tmp_path):
    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(lambda _: adapter(tmp_path).claim("a" * 64), range(8)))
    assert sum(results) == 1
    assert adapter(tmp_path).claim("a" * 64) is False
    assert adapter(tmp_path).claim("b" * 64) is True
    files = tuple(tmp_path.iterdir())
    assert len(files) == 2
    assert all(path.stat().st_mode & 0o077 == 0 for path in files)


@pytest.mark.parametrize("digest", ["", "../outside", "G" * 64, "a" * 65, None])
def test_claim_rejects_non_digest_without_creating_files(tmp_path, digest):
    with pytest.raises(ValueError):
        adapter(tmp_path).claim(digest)
    assert tuple(tmp_path.iterdir()) == ()
