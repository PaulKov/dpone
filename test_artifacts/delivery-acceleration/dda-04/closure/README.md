# DDA-04 controlled validation closure

Date: 2026-09-10. PR: [29](https://github.com/PaulKov/dpone/pull/29), draft.
Exact validation subject: `e0fbad8b62f9b432fbba42c14af646ca3960919b`.
This follow-up contains validation evidence only. Production code, tests,
dependency declarations and lock file were not changed.

## Result

The isolated component has no unresolved confirmed defect within DDA-04's owned
scope. In the documented locked environment, all **107 focused/producer cases
PASS**. Replaying all 13 files associated with the historical failures and
collection errors produced **684 PASS, 2 FAIL, zero collection errors**. Both
remaining failures are unchanged architecture-clustering assertions. The earlier
26 non-architecture failures no longer reproduce after installing the declared
optional dependencies. No assertion, fixture, skip condition or budget was changed.

The component is complete for scoped integration. **Standalone merge remains
HOLD**: layer and architecture gates still fail, and integrated full validation
remains outstanding. Live behavior and performance remain **UNVERIFIED**.

## Environment and check identity

Preparation followed the existing CI/testing contract:
`uv sync --locked --all-extras`. It installed the workspace native accelerator,
Psycopg, PyArrow, GCP SDKs and pinned dbt/SQL Server adapter, among other declared
extras, in this worktree's virtual environment. No package version or lock entry
was edited. Subsequent commands used `uv run --locked --no-sync`.

The [environment record](environment.log) retains Python/platform, lock-file
SHA-256 and relevant installed versions. Checks used the existing
`../run_checks.py` producer with OUTPUT redirected here. Each JSON includes the
actual command, exit status, duration and unchanged source/producer identities
before and after execution:

- HEAD: `e0fbad8b62f9b432fbba42c14af646ca3960919b`.
- Tree: `92126b2bd4deaf3eb1c60e792051264e0a9ec032`.
- Source SHA-256: `9aa7ab446560064dd950e812d6117af5a98ae42aaab5f3c9a308c1fe63085997`.
- Producer SHA-256: `f18ac4a1b4eef923a81f38700f28fa0c598a34adcc23232dacdb044201d58707`.

`dirty: true` records these new evidence files. They do not change the checked
source. A later evidence commit does not imply checks executed on that commit.

| Check | Status and result | Seconds | Record |
| --- | --- | ---: | --- |
| Environment inventory | PASS | 0.998 | [environment.json](environment.json) |
| Component, public rejection and producer cases | PASS, 107 cases | 21.409 | [focused-current.json](focused-current.json) |
| Historical failed-file replay | FAIL, 684 passed and 2 architecture failures | 238.265 | [failed-replay.json](failed-replay.json) |
| Layer metrics | FAIL, runtime-to-contracts 217 > 214 | 10.169 | [layers.json](layers.json) |
| Architecture fitness | FAIL, clustering 0.1823846551601551 > 0.182 | 45.400 | [architecture.json](architecture.json) |

The replay selected whole files, including adjacent positive, negative, boundary,
retry and recovery cases, using `-m "not integration_live" -n 2 --dist loadfile`.
The live PostgreSQL module now collects successfully; live cases remain excluded.
Pytest reports 229.25 seconds; the producer's 238.265 seconds include command
startup and evidence bookkeeping. The remaining failed tests are:

- `test_architecture_fitness_current_repo_stays_inside_green_clustering_target`;
- `test_architecture_fitness_current_repo_stays_inside_pre_release_cross_layer_budget`.

Both stop on the clustering assertion. The latter does not establish a separate
cross-layer-ratio failure. The layer-command failure is independently retained.

## Independent analysis and review

Read-only reviewer `triage_remaining_tests` (`dpone_test_certifier`) reconciled
the original 28 failures and two collection errors before accepting any replay
result. The failure groups were: five native-provider/backend cases, four
PyArrow semantic cases, two Parquet-sentinel preflight cases, three route-factory
fallback cases, ten GCP state/configuration cases, one dbt parse case, one
Psycopg-dependent CLI case plus two Psycopg collection errors, and two architecture
assertions. The completed replay now confirms the non-architecture cases pass.
This is not a claim that every historical failure was pre-existing.

Fresh-context reviewer `review_closure` (`dpone_release_auditor`, used for evidence
identity review only) independently checks this final candidate before commit.
Its final verdict is retained in the task transcript. Preliminary checks confirmed
unchanged source/producer fingerprints, owned paths, the original full FAIL and
the current graph FAIL. Earlier component and prerequisite corrections were
independently reviewed as recorded in [the prior follow-up](../review-followup/README.md).

## Scope, documentation and handoff

Public native SWITCH rejection before I/O remains unchanged. There is no runtime
wiring, transaction-ownership, manifest, API or compatibility change; no migration
is required. The guide already documents the dependency-view SELECT prerequisite.
Its five documentation checks remain recorded against the same source fingerprint
in the prior follow-up. This closure adds maintainer validation guidance only;
the end-user journey is unchanged.

The original [full-suite FAIL](../pytest.json) and
[completion report](../completion.md) remain unmodified historical evidence:
20,583 passed, 815 skipped, 28 failed and two collection errors at `d8b09de`.
The bounded replay does not replace that full run with a PASS. No new full suite
was launched; the planning owner explicitly assigned final combined validation
to DDA-06 and coordinated shared-host test capacity.

The planning owner and DDA-06 confirmed that graph remediation belongs exclusively
on the integrated DDA-06 branch. There is no fully passing reviewed shared fix to
import into this component branch. DDA-04 must not duplicate shared changes or
weaken budgets to manufacture a standalone PASS. DDA-06 already imported the
reviewed `e0fbad8` prerequisite correction as `1243bf3` with provenance.

Next integration actions: optionally import this evidence-only commit with
`cherry-pick -x`; preserve the current FAIL/HOLD; complete authorized shared graph
work and run required checks on the exact integrated commit; independently review
every subsequent revision. Public activation remains outside this task.

Live SQL/services/credentials: **SKIP**, no approved environment used.
Real SQL syntax, locking, rollback, transaction bridge and performance:
**UNVERIFIED**. Version changes, publication and release certification: **N/A**.
