# Stage 02 validation ledger

This authored ledger separates the final B01 scope from the rejected B01/B02
attempt and baseline analysis, starting at `46830976b214262c7772800523e832a5a6f6d78f`.
It is not a route certification receipt. Only the assigned repair paths changed.
Operational paths, host identities and transient logs are intentionally omitted.

## Final B01 candidate

B02 production behavior, acceptance tests and fast-path documentation changes
were withdrawn after the P1 review. Only B01 changes runtime behavior.

| Check | Status | Observed result / scope |
| --- | --- | --- |
| B01 before the correction | FAIL (expected RED) | The retained initial run contains six exact-byte failures for the implicit nullable spellings on Python RowBinary/Native; the other six explicit-scale/accelerated controls passed. |
| Reduced-scope native/type/receipt/quality selection | PASS | 727 tests, no skips: all 715 original cases plus the 12 B01 cases. Direct pytest entrypoint with the unchanged locked environment. |
| Ruff and formatting | PASS | Global checks; 6,082 files already formatted. |
| Mypy | PASS | No issues in 1,209 configured source files. |
| Import rules and layer metrics | PASS | No violations or layer-metric issues. |
| Architecture fitness | PASS with existing warning | Status OK; average clustering 0.1818679053176943 remains above the 0.180 target. The final runtime change adds no import or graph edge; no budget is changed. |
| Documentation check and language contracts | PASS | 844 Markdown files, 3,413 links; 32 language tests. |
| Generated references and strict MkDocs | PASS | 3/3 references in sync; strict build completed. |
| Exact-commit module-size check and independent follow-up review | Pending frozen commit | Recorded separately in the final coordinator handoff; the rejected candidate verdict below is not carried forward. |
| Mandatory combined full non-live and clean installed-runtime gates | UNVERIFIED | Coordinator-owned on the frozen combined candidate. |

Historical counts below do not substitute for final B01 validation. Raw logs and
operational contracts remain outside the public commit.

## Rejected B01/B02 attempt

Commit: `69013d51dbdaff578767cee54ae49a2119cebc40`. Overall **FAIL / CHANGES REQUESTED**.
The numeric acceptance tests were insufficient to prove logical codec fidelity
and have been removed from the final scope. Retained PASS entries describe
individual executed checks, not B02 capability acceptance.

| Check | Status | Observed result / scope |
| --- | --- | --- |
| Initial new public-boundary regressions before implementation | FAIL (expected RED) | 18 failed, 12 passed; source baseline unchanged. |
| First implementation probe | FAIL (test harness) | 35 passed, two failed because the new test used nonexistent `RETAIN` instead of existing `RETAIN_COMMIT_UNKNOWN`. Corrected the test enum; did not change product ownership behavior. |
| Corrected original focused selection | PASS | 37 tests. |
| Previous-success summary regression without the reset | FAIL (expected RED) | One test independently reproduced stale accepted-row evidence on a failed later consumption. |
| Expanded native/type/receipt/quality regression selection | PASS | 746 tests, no skips; includes all 715 initial cases plus 31 new cases. |
| Direct native/wrapper/streaming selection, including failed-attempt readmission assertions | PASS (insufficient coverage) | 38 tests; the independent reviewer repeated these successfully before reproducing the codec blocker. |
| Ruff check and formatting | PASS | Global checks and subsequent changed-test/artifact checks passed. |
| Mypy | PASS | No issues in 1,209 configured source files. |
| Import rules and layer metrics | PASS | No violations. |
| Architecture fitness | PASS | Repository fitness command exited zero with the repaired source. |
| Documentation validation | PASS | 844 Markdown files, 3,413 local links. |
| Documentation language contracts | PASS | 32 tests. |
| Generated references | PASS | 3/3 references in sync. |
| Strict MkDocs build | PASS | Completed successfully. |
| Exact-commit module-size check | FAIL | Full baseline/head SHAs: `contract_artifacts.py` introduced unbaselined warning debt at 367 SLOC, above warn_sloc=350. No baseline change is authorized. Earlier invocation with symbolic HEAD was a configuration failure, not this result. |
| Independent fresh-context review | FAIL / CHANGES REQUESTED | One P1: genuine source-receipted text markers are silently accepted without decoding. Source receipt, bytes and counts agree while logical values differ. [Review and disposition](rejected-b02-review.md). |
| Extended public-DI codec probe | FAIL (logical fidelity) | Eight accepted rows; empty/TAB/LF/CR/marker text changes and literal quotes disappear. NULL/Unicode controls survive. [Generated observation](evidence/rejected-b02-codec-observation.json). |
| Mandatory combined full non-live gate and clean installed-runtime acceptance | UNVERIFIED | Coordinator owns one frozen combined candidate and exact included-commit map. No standalone full rerun is required before this stage's integration handoff. |

## Historical analysis checks

| Check | Status | Observed result / scope |
| --- | --- | --- |
| `uv sync --locked --offline --all-extras` | PASS | Prepared the locked local environment, including the optional accelerator; no lockfile mutation. Python 3.12.11. |
| Coordinator's exact artifact-only task contract validation, including the retained operational copy | PASS | Zero errors and warnings; only stage-02 artifact writes granted. |
| `reproduce_binary.py --output-dir <synthetic-output>` | PASS (observation producer) | All six baseline groups reproduced. JSON distinguishes unequal bytes, rejected values, mapping divergence and preserved legacy behavior. This does not make the defective cases correctness PASS. |
| `reproduce_wrappers.py --output-dir <synthetic-output>` | PASS (observation producer) | Public real-sink stage/abort plus constructor DI; exact two-row raw control, wrapper failure, safe rename failure and boolean-authority observations reproduced. No method replacement. |
| Initial focused native/type/receipt regression suite | PASS | 715 tests, no failures/errors/skips. Existing prefix fixes preserved. This baseline observation preceded the artifact-only patch. |
| `uv run --frozen --no-sync ruff check .` | PASS | Global check passed; changed artifact scripts were checked again after their final annotations/docstrings. |
| `uv run --frozen --no-sync ruff format --check .` | PASS | 6,080 files already formatted; final changed-script check also passed. |
| `uv run --frozen --no-sync mypy --config-file mypy.ini` | PASS | No issues in 1,209 configured source files. |
| `uv run --frozen --no-sync dpone docs check-import-rules` | PASS | No architectural import violations. |
| `uv run --frozen --no-sync dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json` | PASS | No layer-metric issues. |
| `dpone docs check-module-size` with repository baseline and required baseline-parent comparison | PASS | No module-size issues; 51 tracked debt entries, ratchet-v2. Baseline `2639abc0bbe5531a27b8478cfe579e6915f717d6`, head `46830976b214262c7772800523e832a5a6f6d78f`. |
| `uv run --frozen --no-sync python tools/agent_policy/select_checks.py --base-ref origin/master` | PASS (selection) | Selected dbt/Python categories from artifact paths, including broader tests. Selection is not execution success. |
| Full non-live suite, `pytest -o addopts='' -q -m 'not integration_live' -n auto --dist loadfile` | UNVERIFIED (interrupted) | Started before coordinator queue steering, then only this worktree's run was interrupted with exit 130. Actual partial output ended in `KeyboardInterrupt`; no completed test total or full-suite PASS is claimed. |
| Focused dbt subset selected by router | UNVERIFIED (combined gate) | No dbt implementation changed; coordinator's combined validation owns the cross-stage selection. |
| Live/exporter/target readback/retry certification | SKIP / UNVERIFIED | No authorized environment; task explicitly prohibits live and production execution. |
| Packaging / publication during analysis | N/A | No release operation. |

The initial B02 patched-loader probe is INADMISSIBLE for acceptance and is
superseded by the retained public-DI producer. Raw operational probe logs and the
copied task contracts remain untracked. No analysis-only PR was created; the
coordinator requested useful artifacts accompany the scoped repair PR.

The exact-commit handoff must include module-size results and independent final
review. Mandatory combined validation remains open until the coordinator runs
it. None of these local results authorize changes to shared files, live execution,
merge or publication.
