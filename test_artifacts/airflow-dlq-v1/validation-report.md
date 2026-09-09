# Airflow DLQ v1 validation report

- Date: 2026-07-17
- Branch: `codex/airflow-self-service-roadmap`
- Base commit: `803e9552`
- Specification: `docs/feature-design-airflow-dlq-v1.md`
- Live connector environment: not provided

## Results

| Check | Status | Observed result |
| --- | --- | --- |
| Focused DLQ/runtime/ops tests | PASS | All selected tests passed, including replay identity, PII, integrity, retention, streaming, and lifecycle gates |
| Full non-live test suite | PASS | `4890 passed, 473 skipped` in 279.95s |
| Ruff lint | PASS | All checks passed |
| Ruff format | PASS | 3176 files formatted |
| mypy | PASS | No issues in 534 source files |
| Import rules | PASS | No architectural import violations |
| Layer metrics | PASS | 4824 edges; cross-layer ratio `0.300` within gate |
| Architecture fitness | PASS | Average clustering `0.179`; no class-responsibility findings |
| Module-size budget | PASS | No new DLQ module warnings or hard-limit violations |
| Compatibility policy | PASS | 19 registry entries and documentation are synchronized |
| Documentation links | PASS | 499 Markdown files and 1815 local links checked |
| Documentation language contracts | PASS | 4 tests passed |
| Generated references | PASS | 2/2 references synchronized |
| Strict MkDocs build | PASS | Documentation built successfully |
| Main package build | PASS | `dpone-0.72.3` wheel and sdist built |
| Native acceleration package build | PASS | `dpone_native_accel-0.72.3` wheel and sdist built |
| Airflow pack package build | PASS | `dpone_airflow_pack-0.72.3` wheel and sdist built |
| Twine metadata validation | PASS | Every artifact currently in `dist/` passed |
| Wheel content smoke | PASS | DLQ contracts, ports, services, stores, and manifest schemas are present in the main wheel |
| Generic replay CLI safety | PASS | `--yes` returned exit 4 and `dpone.error.v1`; `applied=false`, `replayed_rows=0` |
| 10,000-record benchmark | PASS | 0 leaked values, 0 false applied claims, stable IDs over 20 plans, peak 40,372,499 bytes |
| Route-specific live replay | UNVERIFIED | No approved source/sink environment or credentials were supplied |
| Fresh-context reviewer | UNVERIFIED | Built-in reviewer failed with stale agent-limit state; three isolated `codex exec` model attempts were blocked by account quota |

## Benchmark

Canonical evidence: `test_artifacts/airflow-dlq-v1/benchmark.json`.

- write and finalized index: 30.869281s;
- 20 replay plans: 15.738274s;
- peak tracked memory: 40,372,499 bytes;
- unmasked artifact values: 0;
- false applied claims: 0;
- stable replay plan IDs: true.

The benchmark is credential-free and does not certify a connector-specific
resolver or sink. A missing live environment is `UNVERIFIED`, not `PASS`.

## Remaining risk

- A fresh independent reviewer is still required before merge because reviewer
  infrastructure and model quota were unavailable during this run.
- The local file adapter rejects traversal and symlink escape and verifies
  checksums, but a hostile same-host actor racing filesystem components remains
  outside this local adapter's tested threat model.
- Route-specific replay correctness depends on the injected sink enforcing the
  immutable idempotency key; live certification remains pending.

## Readiness

The implementation is ready for draft PR review. It is not yet marked
`IMPLEMENTED` or ready to merge until the fresh-context review gate is complete.
