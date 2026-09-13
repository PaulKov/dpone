# Stage 05: bounded dbt redaction correction

The implemented slice repairs the existing captured-output secrecy guarantee.
Broader diagnostics and interval proposals remain [DRAFT](design-draft.md).
[Ownership](ownership.yml) permits only the runner, a cohesive internal output
helper, the dedicated regression file, narrow threat-model documentation and
this artifact directory.

## Impact and algorithm

On baseline `46830976b214262c7772800523e832a5a6f6d78f`, a repeated synthetic secret
cut at the retained-buffer boundary exposes a 38-byte prefix when an earlier
replacement shortens the text. Overlapping original matches can also be missed
after sequential replacement obscures a later occurrence. The baseline
[observations](synthetic-observations.json) intentionally retain these failures.
Their [producer](synthetic_probe.py) refuses a different production source tree.

The correction preserves process construction, concurrent draining, retained
byte limits and the public result type. After draining, each stream is decoded
with UTF-8 replacement handling; a codepoint made incomplete by retention is
withheld. A bounded coverage mask identifies all original secret occurrences,
including overlaps. If retention truncated the stream, the longest suffix that
could be a prefix of each known secret is masked conservatively too. Replacing
covered runs then cannot expose an unfinished secret after earlier shrinkage.
The internal helper uses KMP prefix lengths: matching and suffix recognition
take linear work per secret instead of rescanning a long secret for every
overlap. Prefix-table storage is proportional to the bounded secret length.
For retained length B, admitted secret count K and total secret length S,
matching costs O(B × K + S); this is not linear independently of secret count.
Auxiliary capture/masking storage is O(B + longest secret + output limit), in
addition to the caller's supplied secret collection.

Output accumulation keeps at most the configured limit plus one byte, allowing
truncation to remain explicit even when replacement expands text. Final clipping
preserves valid UTF-8. Mask storage grows only with already bounded retained
input; no per-match list or unbounded pipe output is accumulated. Overlapping or
adjacent secret runs may share one redaction marker, and an ambiguous trailing
prefix can be conservatively hidden. This is captured-output protection, not
safe streaming, automatic secret discovery or sanitation of dbt-owned files.

There are no new public signatures, flags, schemas, invocation inputs or
migration requirements. Exit codes, stdout protocol, stderr policy and
timeout/cleanup behavior are unchanged. The threat-model addition explains
retention, UTF-8 handling, truncation and the limits of safe sharing; the wider
operator/debug/manual-run journey is not implemented by this slice.

## Validation record

Initial baseline/focused probes used locked base and development dependencies.
Final checks use the isolated all-extras environment documented in [environment.json](environment.json).
Commands below use `uv run --locked --no-sync` after `uv sync --locked --all-extras`.
Transient local logs are excluded from the public artifact set.
This is the candidate-preparation snapshot. Post-commit module validation and
the exact-commit review disposition are recorded in
[PR #52](https://github.com/PaulKov/dpone/pull/52) without changing the frozen tree.

| Check | Status | Observation |
| --- | --- | --- |
| Ownership validator | PASS | 0 errors, 0 warnings |
| Baseline synthetic producer | PASS | Baseline reproduced; its three FAIL properties are unresolved baseline findings, not a passing product claim |
| Baseline existing focused suite | PASS | 159 tests; real Airflow rendering module SKIP because Airflow was unavailable |
| Initial boundary regression | FAIL | 12 failures / 31 passes, including two fixture byte-count errors |
| Corrected red regression before production edits | FAIL | 10 genuine failures / 33 passes; 15-byte synthetic token fixture corrected before implementation |
| Intermediate implementation | FAIL | 1 existing output-fill assertion failed / 98 passed; no test weakened |
| Refined focused implementation | PASS | 107 tests before six additional UTF-8/retention controls |
| First expanded final test scope | FAIL | 2 failures / 111 passes: the test helper admitted malformed 0xff but incorrectly excluded incomplete UTF-8 at true EOF; corrected the helper without changing the explicit expected output |
| Pre-review focused runner/supervision/runtime suite | PASS | 113 tests with all UTF-8/retention controls |
| Initial independent review | FAIL | Candidate dbd26a02 required a cohesive size extraction and linear overlap matching; no secrecy failure found |
| Initial committed module-size gate | FAIL | Candidate dbd26a02 added unbaselined warning debt: 359 SLOC versus warning threshold 350 |
| Revised focused runner/supervision/runtime suite | PASS | 119 tests, including maximum-size repeated secrets in both streams |
| Ruff / formatting | PASS | Repository check and format check |
| Mypy | PASS | 1210 source files after helper extraction |
| Import rules / layer metrics | PASS | Existing production boundaries and budgets preserved |
| Documentation check | PASS | Existing docs contracts accepted |
| Documentation language tests | PASS | 32 passed |
| Strict MkDocs | PASS | Build completed |
| Generated references | PASS | 3/3 in sync |
| Change-aware selector | PASS | Python, dbt and docs categories; complete suite remains required |
| Module-size gate on committed candidate | UNVERIFIED | Pending candidate commit |
| Complete non-live pytest gate | UNVERIFIED | Coordinator owns one frozen integrated-candidate run; this stage's gate is deferred to it, not waived |
| Independent final-commit review | UNVERIFIED | Pending frozen candidate |
| Live services and release publication | SKIP | Outside authorization |

Focused command:

```bash
uv run --locked --no-sync pytest \
  tests/test_dbt_subprocess_redaction_boundaries.py \
  tests/test_dbt_subprocess_supervision.py \
  tests/test_dbt_runtime_execution.py -o addopts= -q
```

The new file uses the public runner and process factory, plus a finite local
Python subprocess emitting 4 MiB to each pipe. It tests both streams, repeated
and overlapping secrets, read/retention/output boundaries, Unicode and malformed
UTF-8, replacement expansion, no-secret controls and real exit preservation.
These checks establish local capture behavior, not live dbt/Airflow certification
or a measured peak-memory claim.

An additional public-runner resource observation used 1,052,673 bytes of repeated
synthetic output and the default 1 MiB limit. With 1-, 64- and 4,096-character
secrets, the revised implementation took approximately 0.141, 0.147 and 0.175
seconds respectively in separate untraced local samples. A separate traced
4,096-character sample peaked at 4,443,484 Python-allocated bytes after the caller
payload had already been allocated. This excludes native allocations and is not
process RSS or live latency certification. Source hashes, precise measurements
and review results remain in local/PR records so operational reporting does not
change the frozen candidate.

## Remaining work and boundaries

Committed module-size validation and independent review are required for the
stage handoff. The coordinator's complete integrated non-live gate remains
required before merge readiness. The coordinator also owns CHANGELOG
integration. Suggested entry: fix bounded dbt output redaction for overlapping
secrets and secret fragments at retained-output boundaries while preserving
UTF-8 limits and subprocess exit semantics.

Timeout/cleanup precedence, authored start-date precision, manual intervals,
live logging, debug configuration, artifact excerpts and new metrics/public
composition inputs remain outside this implementation. The DRAFT handoff
records their dependency, identity, authority and documentation requirements.
No release readiness or publication authority is established.
