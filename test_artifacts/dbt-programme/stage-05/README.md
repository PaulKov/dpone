# Stage 05: bounded dbt redaction correction

The implemented slice repairs the existing captured-output secrecy guarantee.
Broader diagnostics and interval proposals remain [DRAFT](design-draft.md).
[Ownership](ownership.yml) permits only the runner, its dedicated regression
file, narrow threat-model documentation and this artifact directory.

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
| Final focused runner/supervision/runtime suite | PASS | 113 tests with all UTF-8/retention controls |
| Ruff / formatting | PASS | Repository check and format check |
| Mypy | PASS | 1209 source files |
| Import rules / layer metrics | PASS | Existing production boundaries and budgets preserved |
| Documentation check | PASS | Existing docs contracts accepted |
| Documentation language tests | PASS | 32 passed |
| Strict MkDocs | PASS | Build completed |
| Generated references | PASS | 3/3 in sync |
| Change-aware selector | PASS | Python, dbt and docs categories; complete suite remains required |
| Module-size gate on committed candidate | UNVERIFIED | Pending candidate commit |
| Complete non-live pytest gate | UNVERIFIED | Queued under coordinator shared-host scheduling; not waived |
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

## Remaining work and boundaries

The complete non-live gate, committed module-size validation and independent
review remain required before merge readiness. The coordinator owns CHANGELOG
integration. Suggested entry: fix bounded dbt output redaction for overlapping
secrets and secret fragments at retained-output boundaries while preserving
UTF-8 limits and subprocess exit semantics.

Timeout/cleanup precedence, authored start-date precision, manual intervals,
live logging, debug configuration, artifact excerpts and new metrics/public
composition inputs remain outside this implementation. The DRAFT handoff
records their dependency, identity, authority and documentation requirements.
No release readiness or publication authority is established.
