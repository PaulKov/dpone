# Airflow cache promotion integrity validation report

- Verified at: `2026-07-14T10:27:13+03:00`
- Base commit: `d9ace7bd47fce423495b79ae3a739b07198821d9`
- Assessed implementation commit: `e1224e3da2333b473821cdccdacc5df481f7e832`
- Certification envelope commit: `2c81ef07a7aa76643bbc3f0a63ad3a9bbf75cfb9`
- Branch: `codex/airflow-self-service-next`
- Pull request: `#315`
- Approved specification:
  `docs/feature-design-airflow-cache-promotion-integrity.md`
- Task contract:
  `test_artifacts/agent-policy/2026-07-13-airflow-cache-promotion-integrity.yml`
- Toolchain: `uv 0.9.18`, Python `3.11.14`, Darwin `24.3.0 arm64`
- Package version built: `0.72.1`
- Target release: `0.72.2`; version bump, tag, and publication remain a
  separate post-merge release-protocol step

## Result

**IMPLEMENTATION GO.**

The local POSIX Airflow cache promotion slice satisfies the approved
fail-closed contract. It validates content-addressed release and deployment
projections, activates a sealed snapshot, commits authorized pointer/audit
state before the atomic `current` switch, and protects recovery candidates when
active state is damaged. The lightweight provider enforces the same canonical
identity grammar as the public schema without importing full dpone runtime
dependencies during Airflow parsing.

The certification envelope changes only task-contract ownership and
producer-generated quality metrics above the assessed implementation commit.
An independent fresh-context architect confirmed that all 77 base-to-head
changed paths are owned, generated metrics are current, and the runtime code is
unchanged between the assessed implementation and certification envelope.

Remote production publishing/materialization and physical power-loss behavior
remain `UNVERIFIED`. The 471 skipped tests and unavailable live environment are
not reported as passes.

## Acceptance evidence

| Contract | Status | Evidence |
|---|---|---|
| Valid projection promotes through current-pointer contract | PASS | `tests/test_airflow_cache_materializer.py` |
| Release/deployment fingerprints and indexed artifacts are verified | PASS | `tests/test_airflow_cache_promotion_integrity.py` |
| Traversal, symlink escape, FIFO, size, checksum, and malformed controls fail closed | PASS | adversarial cache integrity tests |
| Candidate mutation cannot change active bytes after validation | PASS | sealed-snapshot and copy-window mutation tests |
| Pointer and audit are durable before atomic current activation | PASS | commit ordering and fsync failure tests |
| Promotion, recovery, and retention share lock/CAS semantics | PASS | thread/process race tests |
| Retention fully validates active current before classifying another deployment | PASS | corrupt fingerprint/index/release/artifact matrix; zero deletions |
| Retention validates the complete delete set before mutation | PASS | later-candidate failure leaves all candidates present |
| Partial deletion reports completed ID, failed ID, step, and mutation state | PASS | filesystem failure injection |
| Unidentified quarantine is visible without topology leakage | PASS | `NEEDS_ATTENTION`, stable error code, JSON-only path |
| Null destructive retention identities are schema-invalid | PASS | plan/apply schema negative tests |
| Domain prevalidation retains no-mutation details and cause code | PASS | corrupt second-candidate regression |
| Canonical release/deployment identities are lowercase end to end | PASS | materializer, provider, optional-ref, and pinned-URI tests |
| Provider executable validation matches public schema | PASS | `tests/test_airflow_deployment_index_provider.py` and schema matrix |
| Provider parsing has no writes, network, DB, secret, or subprocess side effects | PASS | fresh reviewer parser audit |
| Local safe sample reuses only fully verified current | PASS | safe-sample execution-plan tests |
| Immutable local release and activation are create-or-compare | PASS | corrupt/missing/reuse conflict tests |
| Diagnostic recovery/retention payloads remain schema-valid | PASS | malformed pointer/directory regressions |
| Documentation, ADR, schemas, generated metrics, and task contract are synchronized | PASS | docs and governance gates |

## Executed checks

| Status | Check | Result |
|---|---|---|
| PASS | Focused Airflow cache/provider/schema suites | 325 tests in fresh certification; all passed |
| PASS | `uv run pytest -m "not integration_live" -n auto --dist loadfile` | 4204 passed, 471 skipped, 0 failed in 252.27 seconds |
| PASS | `uv run ruff check .` | No findings |
| PASS | `uv run ruff format --check .` | 2996 files formatted |
| PASS | `uv run mypy --config-file mypy.ini` | 479 source files, no issues |
| PASS | Import rules | No violations |
| PASS | Layer metrics | 4465 edges; cross ratio 0.297; max cross flow 90 |
| PASS | Architecture fitness | Average clustering 0.177; no findings |
| PASS | Module-size gate | No failures and no new warning debt |
| PASS | Documentation links | 407 Markdown files, 1723 links |
| PASS | Documentation language contracts | 4 passed |
| PASS | Generated CLI/schema references | 2 of 2 synchronized |
| PASS | Generated quality metrics | Producer check reports current |
| PASS | Compatibility registry | 19 entries synchronized |
| PASS | `uv run mkdocs build --strict` | Strict build completed |
| PASS | Root package build | `dpone` 0.72.1 wheel and sdist built |
| PASS | Native acceleration package build | `dpone-native-accel` 0.72.1 wheel and sdist built |
| PASS | Airflow provider package build | `dpone-airflow-pack` 0.72.1 wheel and sdist built |
| PASS | Twine metadata check | All six distributions passed |
| PASS | Fresh wheel installation/import | Three wheels imported in clean Python 3.11 venv |
| PASS | Agent task contract validation | 0 errors, 0 warnings; 77 changed paths, 0 unowned |
| PASS | Fresh-context implementation review | `IMPLEMENTATION GO` for `2c81ef07` |
| UNVERIFIED | Remote publisher/materializer integration | No approved production environment or credentials |
| UNVERIFIED | Power-loss behavior on production storage | Requires target filesystem fault injection |

Reviewed distributions are retained transiently at
`/tmp/dpone-cache-integrity-e1224e3d-dist.5BeJ8j/`. The durable evidence is this
report and the executable repository tests.

## Public contract and compatibility

Cache mutation commands require an explicit allowed platform identity, an
exact or expected-absent CAS guard, and plan/apply recovery. Release and
deployment projections remain immutable. Existing release artifact `path` and
legacy `artifact_ref` declarations are compatible only when exactly one safe
relative locator is present.

This is a migration-requiring fail-closed security correction. Uppercase cache
identities, non-content-addressed deployments, incomplete releases, and copied
`current` directories must be rematerialized. Text output remains
topology-free; JSON preserves stable `dpone.error.v1` codes, paths, and truthful
partial-mutation details. Versioning and publication follow the separate release
protocol after merge.

## Documentation and CJM impact

The operator journey is documented from trusted materialization through
promotion, recovery, retention, and error-family remediation. Architecture and
ordering are recorded in ADR 0009; canonical identity rules remain in ADR
0011. Provider docs explain parse-time purity and schema parity. The beginner
First DAG and safe-sample journey are unchanged; unsafe cache state now fails
before runtime handoff or destructive retention.

## Fresh-context review

The final fresh-context architect reported no blocking findings and issued
`IMPLEMENTATION GO` for exact certification commit
`2c81ef07a7aa76643bbc3f0a63ad3a9bbf75cfb9`.

Earlier reviews found and drove regression coverage for uppercase release-set
identity, hidden quarantine state, missing prevalidation evidence, nullable
destructive actions, corrupt-active retention deletion, provider/schema
identity divergence, task ownership, and generated-metrics drift. Each blocker
was closed and independently rechecked before this report was finalized.

## Remaining risk and follow-up

- Correctness is certified for one shared local POSIX filesystem.
- Distributed materialization and multi-host locking remain out of scope.
- Production artifact-store identity, attestation, cache materialization, and
  power-loss behavior require environment-specific live certification.
- **Release `v0.72.2` published** on 2026-07-15 (`597cb410` on `master`). PyPI
  resolver smoke, GitHub Release assets, and runtime image
  `ghcr.io/paulkov/dpone-runtime:0.72.2` verified — see
  `test_artifacts/release/v0.72.2/release_verify.json`.
- Existing unrelated module-size warnings remain visible; this change adds no
  warning or hard-limit regression.
