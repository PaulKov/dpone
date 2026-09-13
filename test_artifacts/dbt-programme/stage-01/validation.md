# Stage 01 validation and correction report

Baseline source: `46830976b214262c7772800523e832a5a6f6d78f`.
Documentation correction: `ba2860b`.
Tested code candidate: `228697c084523218c4ff6a889a73277709aee739`.
PR: [50](https://github.com/PaulKov/dpone/pull/50), draft pending final gates/review.

## Changes and compatibility

Five active dbt pages now use the already implemented Core 1.12.3 / SQL Server
adapter 1.11.1 toolchain and expose the existing policy-v3 schema link. Runtime
pins, historical schemas and certification claims are unchanged. The complementary
two-row compatibility-table correction belongs to [PR49](https://github.com/PaulKov/dpone/pull/49).
Active macro-count text also matches the current generated authority: 153
framework records plus seven invocation records, digest
`sha256:660dec027fad7e4b9351563146bed01c79ae1bf5a83ba828d145d4e878c2b95f`.
The historical candidate's 142-record evidence remains unchanged. Counts and
digest are imported by `probe_profile.py`, not inferred from historical prose.

The model compiler now emits exactly two braces for the lower interval token,
matching the upper token. A synthetic model is compiled, parsed through
`ETLProcessConfig.from_dict(metadata_only=True)` and bound by the real
`IntervalContextService` for one- and two-day lookbacks. Exact SQL bounds are
asserted. Source relation and existing normalized MSSQL schema label are retained.
No extraction width, partition expression, finalizer, empty-input, state, replay,
native transfer selection or composition activation contract changes.

New generated artifacts naturally have corrected predicate bytes and identities.
Regenerate immutable branch-local artifacts through their producer into a new
root; do not hand-edit previously generated manifests or execution packs.

## Focused evidence

| Check | Status | Observation |
|---|---|---|
| Existing baseline selection | PASS with environment correction | 230 cases passed in cached Python3.12.11/pytest9.1.1; three `python -I` imports initially failed because dpone was not installed; only those three reran and passed in the frozen editable environment |
| Profile/identity/renderer probe | PASS for observations | `profile-baseline.json` records closed admission and the interval defect; no live execution |
| TLS projection probe | SKIP / rejected evidence | Earlier monkey-patched probe and its PASS JSON removed; source inspection retained, dynamic projection UNVERIFIED |
| New interval regressions before source fix | FAIL, expected RED | Both exact-bound assertions observed `{{2026-09-01T00:00:00Z}}` in generated runtime SQL |
| New interval + service + schema tests after fix | PASS | 54 passed in 1.52s |
| Docs contracts + language | PASS | 39 passed in 5.71s; repeated after the final active macro-count correction |
| Candidate profile probe | PASS | `profile-candidate.json` records both resolved bounds and preserved admitted options |
| Rendered documentation | PASS | `rendered-docs.json`: current versions and navigation in five HTML pages, policy-v3 link present |

RED was acquired before the production fix. During test preparation, a missing
checkpoint-table fixture was completed; after the interval fix, an auxiliary
schema-label expectation was corrected to the existing `warehouse.mart`
normalization. Neither adjustment changed the failing exact interval assertion.
The retained baseline JSON independently reproduces the defect on the base SHA.

Focused candidate commands:

```sh
uv run --frozen --offline pytest -o addopts= -q \
  tests/test_dbt_publish_interval_projection.py \
  tests/test_run_interval_context.py tests/test_dbt_publish_schema_contracts.py
uv run --frozen --offline pytest -o addopts= -q \
  tests/test_dbt_self_service_docs_contracts.py tests/test_docs_language_contracts.py
uv run --frozen --offline python -B test_artifacts/dbt-programme/stage-01/probe_profile.py
```

The initial baseline used an offline cached dependency environment without an
editable installation. It selected seven existing files:
`test_dbt_inline_publishing.py`, `test_dbt_execution_pack_versions.py`,
`test_dbt_invocation_target.py`, `test_dbt_schema_readiness.py`,
`test_runtime_mssql_clickhouse_native_transfer.py`,
`test_runtime_credentials_contracts.py`, `test_dbt_runtime_execution.py`.
The real demo parse was deselected. Collection confirmed 233/234 cases; progress
and failure listing give 230 passes and three import-environment failures. This
is the independent test agent's recorded result, not a retained raw suite log.

The correction command was:

```sh
uv run --frozen --offline --no-python-downloads python -B -m pytest \
  -p no:cacheprovider -o addopts= -q tests/test_dbt_invocation_target.py \
  -k test_target_import_does_not_acquire_execution_or_sdk_contracts
```

It reported `3 passed, 27 deselected in 3.54s`. The frozen environment is Darwin
arm64, Python3.12.11, pytest9.0.3, editable dpone0.79.2; no lockfile changed.

## Broad gates

Every command below used `uv run --frozen --offline` in the same isolated
worktree. Raw logs remain local execution artifacts; this public summary and the
reproducible synthetic observations contain no local machine paths or secrets.

| Command suffix | Status | Result |
|---|---|---|
| `ruff check .` | PASS | All checks passed |
| `ruff format --check .` | PASS | 6081 files already formatted |
| `mypy --config-file mypy.ini` | PASS | No issues in 1209 source files |
| `dpone docs check-import-rules` | PASS | No violations |
| `dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json` | PASS | Issues: none |
| `dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref 46830976b214262c7772800523e832a5a6f6d78f --head-ref 228697c084523218c4ff6a889a73277709aee739` | PASS | No module-size issues, ratchet-v2 |
| `dpone docs check-docs` | PASS | 844 Markdown files / 3414 local links |
| `dpone docs check-generated-references` | PASS | 3/3 in sync |
| `mkdocs build --strict` | PASS | Built in 94.46s; rendered text/link inspection passed |
| `python tools/agent_policy/select_checks.py --base-ref origin/master` | PASS | Change-aware plan generated; focused tests above and broad non-live suite selected |
| `pytest -m "not integration_live" -n auto --dist loadfile` | UNVERIFIED | Running at report creation; final outcome pending |
| Live route / production / release / benchmark | SKIP | Outside authorized environment and scope |

## Documentation and remaining work

The first-success and recovery instructions now reference the existing runtime
toolchain. Broader CJM gaps remain in the baseline report: complete runtime
registry/schema example, strict TLS field placement, exact option admission,
partition completeness and separate execution-family semantics. The profile
extension remains a DRAFT and requires a fully enumerated approved contract.

The custom-CA and TLS omission questions are source-inspection findings, with
dynamic behavior UNVERIFIED. The coordinator rejected the earlier SDK/module
replacement probe under the no-monkey-patching task boundary; it is not valid
acceptance evidence and has been removed. This PR does not claim these findings
are regressions or alter them. No current live adapter or
end-to-end certification is implied by unit/contract passes.

Independent fresh-context review: UNVERIFIED pending source/evidence freeze.
Ready for review as a draft; merge/release readiness is not established until
the remaining suite and independent review are reconciled.
