# Independent metrics and transfer follow-up

Verdict: **APPROVE the bounded follow-up. No actionable findings.** The correction review in `report.md` applies to final successor `c1a56b055f079bcc6dd9957300c5494f00b6080c` through the independently verified identity chain below. Final-head provider CI and broader merge readiness remain the parent's separate audit.

## Reviewed identity chain

- Initial independently reviewed implementation: `81a7f4813dc9a4a4d41f81401cf4490d284b51d2`.
- Frozen source H: `971e9a4a827604292af612052c4b311fd8b11ba1`; sole parent is the reviewed implementation.
- Final successor: `c1a56b055f079bcc6dd9957300c5494f00b6080c`; sole parent is pinned upstream `a1aadcaf6beaa2e82abcac27f2aed0b5113986f7`.
- H and the final successor have identical whole-tree identity `987c7c61fc9ecc4032fab9a31cdacb3d0ad505e0`. Independent recursive mode/blob listings and an empty exact two-commit diff confirm that equality. The final commit contains `Reviewed-source: 971e9a4a827604292af612052c4b311fd8b11ba1`.

The H delta changes only six generated metric values in `docs/quality-metrics.md`. Both surrounding prose sections and every other tree entry are unchanged from the reviewed implementation. No production, test, policy, dependency, threshold, state, evidence format or public-contract change is introduced by this follow-up.

## Metrics provenance and preservation

I inspected the unchanged tracked `test_artifacts/delivery-acceleration/dda-06/refresh_metrics.py` recorder. Its source hash matches `metrics/refresh.json`, and its blob is identical at original PR34, reviewed implementation and H. Its checks require committed tracked inputs, no untracked Python inputs, sole generated-document change, unchanged surrounding prose, successful freshness check, byte-identical repeat, unchanged other tracked bytes and fixed source identity.

The retained logs preserve the actual initial stale check (exit 2), successful generation (exit 0), subsequent freshness check (exit 0), and repeated generation (exit 0). I independently verified all 5,896 inventory entries against H's immutable Git blobs using `git cat-file --batch`. The recorded inventory hash matches the actual inventory file; H's committed document hash is exactly `8131f246535ebdc0a5b053b42624c5f055d73fd033523d7a13143cf029040cd8`, matching the refresh receipt. The producer was not rerun.

Independent Git checks also establish that:

- All 16 newly imported upstream-only paths and all 138 earlier upstream-only paths preserve the modes and blobs from pinned `a1aadcaf`.
- All 786 original PR34 files beneath `test_artifacts/delivery-acceleration/` retain their original modes and blobs.
- The complete upstream changelog from the unique 0.77.1 release header onward remains unchanged.

The retained staged/committed transfer records match the actual source, upstream and final Git objects. Both recorder and wrapper hashes match their retained bytes; the recorder is unchanged from original PR34. The wrapper visibly overrides only the pinned base and approved successor branch. Historical staging was assessed through its retained producer record, not recreated. Committed whole-tree equality and sole-parent/source-trailer claims were independently checked directly.

A fresh read-only `git ls-remote --heads origin` confirmed successor `codex/dda-06-benchmark-integrity` at `c1a56b0`, retained source `codex/dda-06-benchmark-source` at H, and old `codex/dda-06-reviewed-delivery` still at original `fa16c23`. The local reviewed-delivery branch retains H. No branch or remote state was modified by this reviewer.

## Checks and limits

| Scope / risk | Result | Evidence |
| --- | --- | --- |
| Generated-only delta, changed-field boundary, exact source inputs | PASS: independent Git and SHA-256 checks, 1.016s | `verify_source_h.py`, `source-h-followup.json` |
| Metrics freshness and repeat/idempotency | PASS: retained canonical execution inspected, all input/output/recorder hashes independently bound to H | `../metrics/refresh.json`, `../metrics/{before,generate,after,repeat}.log`, `source-h-followup.json` |
| Upstream and historical evidence preservation | PASS: 16 + 138 upstream mode/blob matches, 786 unchanged original artifacts, complete release section retained | `source-h-followup.json` |
| Final transfer identity, sole parent, provenance, remote preservation | PASS: direct Git verification and retained-proof inspection, 1.173s | `verify_transfer.py`, `transfer-followup.json`, `../transfer-staged.json`, `../transfer-committed.json` |
| Unit/contract/mocked integration rerun | N/A: prohibited by this bounded follow-up; unchanged implementation retains the original scoped review evidence | `report.md`; no new exact-head test claim |
| Live integration/certification | SKIP execution; UNVERIFIED certification | No approved live environment or live work |
| Broad final CI / docs rebuild | N/A for this reviewer; parent owns final exact-head provider accounting | Not independently asserted here |

Commands were `.venv/bin/python -B fresh-review/verify_source_h.py` and `.venv/bin/python -B fresh-review/verify_transfer.py`, with `PYTHONDONTWRITEBYTECODE=1`, using the existing Python 3.12.11 environment on macOS 26.3.2 arm64. JSON receipts retain the exact read-only Git argv and audit-script hashes. No source, test, generator, dependency installation or branch mutation was executed; only assigned external review artifacts were written.

The documentation change refreshes generated metrics only and does not alter the user journey. Compatibility, retry/failure semantics and benchmark correction conclusions remain those of the original independent review. This is ready for the parent's final integration/CI assessment; it is not release or merge authorization.
