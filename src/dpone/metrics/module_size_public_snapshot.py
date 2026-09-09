"""One audited transition from imported debt to observable public-root provenance."""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass, replace
from hashlib import sha256

from .loc import count_lines, count_sloc
from .module_size_baseline import (
    ModuleSizeBaseline,
    ModuleSizeBaselineError,
    ModuleSizeDebtEntry,
    decode_module_size_baseline,
)

PUBLIC_ROOT = "f8c6a4a5e75d167829c05f65d5d3033acb193878"
PUBLIC_LEDGER = "docs/module_size_baseline.json"
PUBLIC_LEDGER_SHA256 = "0858ac904defebd34c5b8fc94d886da802c78d81565aa286747b660dfd7b821f"


@dataclass(frozen=True)
class PublicSnapshotDebt:
    """Verified immutable inputs, never evidence for unavailable prior ancestry."""

    baseline: ModuleSizeBaseline
    source_digests: tuple[tuple[str, str], ...]

    @classmethod
    def load(cls, source_at_public_root: Callable[[str], bytes]) -> PublicSnapshotDebt:
        raw = source_at_public_root(PUBLIC_LEDGER)
        if sha256(raw).hexdigest() != PUBLIC_LEDGER_SHA256:
            raise ModuleSizeBaselineError("Public snapshot ledger differs from audited immutable bytes")
        baseline = decode_module_size_baseline(raw, source="audited public root")
        digests = []
        for entry in baseline.entries:
            source = source_at_public_root(entry.path)
            try:
                text = source.decode("utf-8")
            except UnicodeDecodeError as exc:
                raise ModuleSizeBaselineError("Public snapshot source is not UTF-8") from exc
            size = count_lines(text), count_sloc(text)
            if size != (entry.max_lines, entry.max_sloc):
                raise ModuleSizeBaselineError("Public snapshot debt caps differ from observed source sizes")
            digests.append((entry.path, sha256(source).hexdigest()))
        return cls(baseline, tuple(digests))

    def permits(self, entry: ModuleSizeDebtEntry, prior: ModuleSizeDebtEntry | None) -> bool:
        """Accept only a provenance-only transition of continuously retained debt."""
        original = next((item for item in self.baseline.entries if item.path == entry.path), None)
        return (
            prior is not None
            and original is not None
            and prior.max_lines <= original.max_lines
            and prior.max_sloc <= original.max_sloc
            and replace(prior, max_lines=original.max_lines, max_sloc=original.max_sloc) == original
            and entry == replace(prior, baseline_commit=PUBLIC_ROOT)
        )
