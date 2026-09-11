# DDA-05 completion and evidence

> Historical completion at `38f6603`. A subsequent user-requested independent
> review withdrew this approval and identified recovery/counter assertion gaps.
> See [the corrective review record](review-followup/report.md) for the successor
> implementation, validation and review status. Logs below retain their original
> identities and results; they do not certify the corrective commit.

## Result and scope

DDA-05 adds an opt-in real-row delivery certification harness: deterministic
typed profiles, baseline/candidate adapters, retained v1 reports and correctness
artifacts, process-set RSS sampling, source-free recovery checks, and guarded
recovery/cleanup commands. The CLI and 19 live fixtures collect and report absence
without importing a factory or discovering services. No live certification or
performance improvement is claimed.

The application/environment must supply a reviewed real route factory and an
authoritative target-visibility probe. DDA-06 owns shared runtime integration and
navigation; neither contribution bundles a default live factory. A missing
approval or factory produces SKIP/UNVERIFIED, and native SWITCH remains publicly
rejected. Its live fixtures target an isolated component only.

Branch: `codex/dda-05-real-row-harness`.
PR: [DDA-05 / #30](https://github.com/PaulKov/dpone/pull/30).

Production base: `d5ad9aaecc900c24df421b160ed36b4cfc726e45`.
Approved planning dependency: `f3682940f8864563cde0e6b6ecee60f746b49020`,
imported by an ordinary fast-forward merge after fetching `origin/master`.
The planning dependency's shared-file changes are separate from the DDA-05
implementation audit. No history rewrite, force push, merge to master, tag,
release, service/container startup, or credential use was performed.

Implementation commits, in order:

1. `d406dbf5644b02cd8483f3990249e55c075b0e54`: harness, fixtures and guide.
2. `5cc2296b2f01cc0f6181880a69ee411dae1f2ef0`: independent recovery/identity fixes.
3. `518cc16bd5971bebd784497d4196c69759ca2561`: reusable hermetic fixtures.
4. `f09af019e291113cf88c5f0525e9e49670087769`: frozen typed before-images and
   full-refresh recovery regression coverage; final Python code checkpoint.

Guide clarification commit `5761738a123f349507e3a13f075699181f450123` documents
the exact baseline command and application-supplied live authority. This
completion report and retained validation are artifact-only changes after it.

## Owned changes and compatibility

All implementation changes after the planning dependency are within the task
contract. The machine-generated [ownership audit](ownership-audit.json) lists
every delivery path: PASS, 50 paths and zero ownership violations.

| Owned path | Responsibility |
| --- | --- |
| `tools/native_delivery_live_benchmark.py` | Run, inspect, recover and cleanup CLI; early opt-in gating and sanitized failure output. |
| `tools/native_delivery_live_support/` | Profiles, adapters/protocols, immutable artifacts, exact typed correctness, resources, orchestration, validation, maintenance and explicit hermetic fixtures. |
| `tests/test_native_delivery_live_benchmark.py` | 70 hermetic contract/regression cases. |
| `tests/integration/mssql/clickhouse_mssql_delivery_support.py` | Live-only factory gate and redacted exception boundary. |
| `tests/integration/mssql/test_clickhouse_mssql_bounded_native_delivery_integration.py` | Seven profiles across full-refresh and partition replacement. |
| `tests/integration/mssql/test_clickhouse_mssql_partition_switch_integration.py` | Five isolated SWITCH cases with invocation ownership and final cleanup status. |
| `docs/delivery-acceleration/certification.md` | Self-service preparation, baseline/candidate run, observation, recovery, cleanup and factory protocol. |
| `test_artifacts/delivery-acceleration/dda-05/` | Retained producer fixture, review record, validation logs and this report. |

Public-contract impact is compatible and additive. Existing production CLI,
runtime behavior, imports, manifests/schemas, state/checkpoints and native wire,
journal and receipt formats are unchanged. There is no migration. Shared
registries, factories, fixtures, navigation, changelog, dependency metadata and
release workflows were not edited by DDA-05.

## Validation

Environment: macOS arm64, Python 3.12.11. Dependencies were synchronized with
`uv sync --locked --all-extras`; [environment-sync.log](environment-sync.log)
records the operation. No dependency/lockfile changes were made. The final runs
use `uv run --locked --no-sync` so optional dependencies remain installed.

| Check | Status | Evidence |
| --- | --- | --- |
| Focused producer tests on final Python code | PASS: 70 cases | [focused.log](focused.log), [collection](focused-collection.log) |
| Ruff and Ruff format | PASS | [ruff.log](ruff.log), [format.log](format.log) |
| Mypy | PASS: 1,163 source files | [mypy.log](mypy.log) |
| Import and layer constraints | PASS | [import-rules.log](import-rules.log), [layer-metrics.log](layer-metrics.log) |
| Exact base/head source module-size gate | PASS at `f09af01` | [module-size.log](module-size.log) |
| Harness support package module-size gate | PASS; no issues | [tool-module-size.log](tool-module-size.log) |
| Documentation links and generated references | PASS | [docs-check.log](docs-check.log), [generated-references.log](generated-references.log) |
| Documentation language and strict MkDocs | PASS: 32 language cases | [docs-language.log](docs-language.log), [mkdocs.log](mkdocs.log) |
| Task-contract validation | PASS: zero errors/warnings | [task-contract.log](task-contract.log) |
| Change-aware plan | PASS: produced by selector | [validation-plan.json](validation-plan.json) |
| Help/import and absence behavior | PASS; 19 live cases SKIP | [cli-help.log](cli-help.log), [live-absence.log](live-absence.log), focused tests |
| Complete non-live pytest | FAIL: 2 failures, 20,863 passes, 570 skips; exit 1; 1,439.34 seconds | [pytest-broad.log](pytest-broad.log) |
| Isolated Airflow permission diagnosis | FAIL reproduced in default temporary group; PASS: all 10 cases with effective-group parent | [recheck](airflow-permissions-recheck.log), [controlled recheck](airflow-permissions-controlled.log), [group probe and base hashes](airflow-permissions-diagnosis.json) |
| Fresh independent architecture and certification reviews | PASS after fixes | [review.md](review.md) |
| DDA-01/DDA-06 actual producer/consumer interoperability | PASS for hermetic shape; result UNVERIFIED/null ratio | Owner-reported evidence under their respective artifact directories; see review record |
| SQL/BCP, live recovery, live SWITCH and performance comparison | SKIP/UNVERIFIED: no approved disposable environment or supplied live factory | No live performance evidence |
| Packaging, publication and release readiness | N/A for this contribution | No packaging or release change |

The complete non-live run started at `518cc16`. The final before-image correction
was made while that run was active and is covered separately by all 70 focused
tests on `f09af01` plus the independent reviewer reproductions. The broad run is
not represented as a frozen-final-commit execution; the integrator must assess
its own combined source identity.

The two broad failures are
`tests/test_airflow_cache_shared_directory_mode.py::test_ensure_shared_directory_accepts_foreign_owned_superset`
and `::test_ensure_cache_layout_succeeds_on_foreign_owned_shared_superset`.
The default `/tmp` parent inherited GID 0; `chmod(02777)` retained only 0777.
After assigning a newly created diagnostic parent the process effective GID 20,
the same chmod retained 02777 and all 10 unchanged tests passed. The test and
`cache_permissions.py`/`cache_layout.py` match the production base byte-for-byte;
their hashes are retained in the diagnosis. No permission checks or shared tests
were modified. The original full run remains FAIL; the controlled targeted
recheck establishes the temporary-directory cause, not a new full-suite pass.

An earlier run was interrupted after 16 failures, 10,129 passes, 737 skips and
two collection errors. It began without optional dependencies and also overlapped
review edits. Its original [interim log](pytest-broad-interim.log) is retained as
FAIL/interrupted, not a passing gate. The locked all-extras environment and final
focused run address its dependency and stale-process issues.

Commands (repository root):

```bash
uv sync --locked --all-extras
uv run --locked --no-sync python tools/agent_policy/select_checks.py \
  --base-ref origin/master --format json
uv run --locked --no-sync pytest tests/test_native_delivery_live_benchmark.py -q
uv run --locked --no-sync ruff check .
uv run --locked --no-sync ruff format --check .
uv run --locked --no-sync mypy --config-file mypy.ini
uv run --locked --no-sync dpone docs check-import-rules
uv run --locked --no-sync dpone docs check-layer-metrics \
  --baseline docs/layer_metrics_baseline.json
HEAD_SHA="$(git rev-parse HEAD)"
BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"
if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run --locked --no-sync dpone docs check-module-size \
  --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"
uv run --locked --no-sync dpone docs check-module-size \
  --package tools/native_delivery_live_support --no-baseline
DPONE_RUN_INTEGRATION=0 DPONE_RUN_INTEGRATION_LIVE=0 \
DPONE_DDA_DISPOSABLE_APPROVED=0 PYTEST_XDIST_AUTO_NUM_WORKERS=2 \
  uv run --locked --no-sync pytest -m 'not integration_live' -n auto --dist loadfile \
  --basetemp /tmp/dpone-dda05-broad-518cc16
uv run --locked --no-sync dpone docs check-docs
uv run --locked --no-sync dpone docs check-generated-references
uv run --locked --no-sync pytest tests/test_docs_language_contracts.py -q
uv run --locked --no-sync mkdocs build --strict
uv run --locked --no-sync python tools/agent_policy/task_contract.py \
  test_artifacts/delivery-acceleration/agent-task-contracts/dda-05-certification.yml
uv run --locked --no-sync python tools/native_delivery_live_benchmark.py --help
DPONE_RUN_INTEGRATION=0 DPONE_RUN_INTEGRATION_LIVE=0 DPONE_DDA_DISPOSABLE_APPROVED=0 \
  uv run --locked --no-sync pytest -q \
  tests/integration/mssql/test_clickhouse_mssql_bounded_native_delivery_integration.py \
  tests/integration/mssql/test_clickhouse_mssql_partition_switch_integration.py
```

## Evidence and user journey

The [certification guide](../../../docs/delivery-acceleration/certification.md)
covers safe absence, explicit preparation, canonical limits, exact baseline
checkout execution, typed profiles, inspect, invocation-owned recovery and
cleanup, status/exit meanings and the application-supplied factory protocol.
DDA-06 owns the overview/navigation links. Developers receive typed seams and
method-level invariants for commit visibility, receipt authority, fault events,
source poisoning, publication counts and durable resource ownership.

The [v1 contract fixture](contract-fixture.json) and its six retained raw/proof
artifacts were generated by [generate_contract_fixture.py](generate_contract_fixture.py).
They deliberately contain SKIP/hermetic status and a dirty producer identity.
They prove input compatibility, not a live run. Generated artifacts were not
hand-edited to create passing evidence. Trial artifacts bind exact raw bytes,
scope/fixture and source/environment/configuration identities.

## Remaining work and readiness

Component implementation, independent review and broad-test diagnosis are
complete. The PR is ready for review. The two environment-dependent broad
failures are disclosed above; merge/release readiness and performance
certification are not asserted. Hosted CI completion must be assessed on the
actual PR head; local checks are not a claim that hosted checks passed.

DDA-06 already imported all four implementation commits with `cherry-pick -x`.
The guide clarification was sent separately so its source/docs freeze need not
wait for artifact collection. The final artifact-only commit is the remaining
handoff. Combined integration checks belong to that owner. A future approved live exercise requires
an application-supplied factory and authority probe, exact clean baseline and
candidate identities, comparable environment/layout/limits, successful fidelity
and recovery proofs, one warmup and at least three successful timed trials.
Unknown outcomes retain resources until authoritative recovery resolves them.
