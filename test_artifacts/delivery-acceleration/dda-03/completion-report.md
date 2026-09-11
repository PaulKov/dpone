# DDA-03 completion and integration handoff

The owned frame component, compatibility adapter, tests and maintainer guide are
ready for review and integration into DDA-06. **Standalone merge readiness is
HOLD:** the complete non-live run remains FAIL, with a separately passing
filesystem-environment retest, and generated development metrics require the
shared-file owner's update. This report does not grant release or live readiness.

## Source and ownership

- Baseline: `d5ad9aaecc900c24df421b160ed36b4cfc726e45` (0.76.0).
- Immutable planning dependency: `f3682940f8864563cde0e6b6ecee60f746b49020`.
- Component implementation: `a029878ac638c8ad7a2a59c28ac503fa860e7e9d`.
- Final Python source and tests: `ad5c117f89c7df33f2bdc2e1aa42d671ca8e0f1b`.
- Reviewed integration-guide follow-up: `4851bd3f8c701c161732c8416a891c02dee7e1b3`.
- Review PR: [dpone #27](https://github.com/PaulKov/dpone/pull/27), branch
  `codex/dda-03-sized-native-frames`.

`ownership.json` compares the implementation against the imported planning
commit. Only the two runtime modules, two focused test files, framing guide and
this artifact directory are owned. Shared scheduler, navigation, changelog,
metrics, manifests, schemas, dependencies, providers and workflows are unchanged.
The planning dependency's inherited paths are not presented as DDA-03 edits.

## Behavior and compatibility

`SizedNativeFrame(rows, encoded_bytes)` carries the native byte sum already used
for splitting. `sized_native_frames` owns the single framing algorithm; existing
`native_frames` still returns row tuples through a closing adapter. Mapping and
sequence containers are detached, and mutable binary fields are frozen before
sizing or pulling the next source row. Dictionaries retain their original pickle
representation; internal consumers must not mutate these snapshots.

Row/native/pickle ceilings, exact boundaries, empty authority, one-row lookahead,
cancellation cadence and caller-owned source closure remain equivalent. The file
encoder and its independent validation, byte/digest, exclusive-write and cleanup
behavior are unchanged. No CLI, manifest, public transformation, retained state,
receipt, recovery or dependency change is introduced; no migration is required.
The public native partition SWITCH prohibition remains in force.

The local handoff fixture demonstrates two sizing calls per row in the former
scheduler pattern and one with cached sizes, while producing identical files.
This is deterministic structural evidence, not a throughput benchmark. DDA-06
owns real scheduler wiring, exact frame/task/cumulative guards, worker-size
comparison before journal/import acceptance, and spawned-worker regression.

## Validation

`run_checks.py` records command, duration, exit status, HEAD and implementation
hashes before/after each check. The complete run used clean `ad5c117`; subsequent
changes affect only the guide and evidence. Final guide gates recorded stable
bytes that were committed unchanged as `4851bd3`. Earlier failing/interrupted
outputs remain under `initial/`. Environment versions are in `environment.json`.

| Command or check | Status | Observed result / duration | Evidence |
| --- | --- | --- | --- |
| Task contract and change-aware check selection | PASS | Valid contract; 0.43 / 0.38 s | `task-contract.*`, `selected-checks.*` |
| Four contract-specified focused test files | PASS | 121 passed; 5.21 s | `focused.*` |
| `uv run ruff check .` / `ruff format --check .` | PASS | 0.34 / 0.40 s | `ruff-check.*`, `ruff-format.*` |
| Required mypy / explicit runtime-module mypy | PASS | 24.68 / 0.96 s | `mypy.*`, `runtime-mypy.*` |
| `dpone docs check-import-rules` | PASS | 37.15 s | `import-rules.*` |
| Layer metrics with required baseline | PASS | runtime-to-contract 214 <= 214; 47.36 s | `layer-metrics.*` |
| Module size, exact merge-base/head recipe | PASS | Base `d5ad9aa`, head `ad5c117`; 10.94 s | `module-size.*` |
| Complete non-live pytest, two workers | **FAIL** | 2 failed, 20,845 passed, 570 skipped; 1659.04 s | `full-pytest.*` |
| Airflow directory-permission module retest | PASS | 10 passed with corrected temporary root; 0.71 s | `airflow-permissions-retest.*`, `temporary-directory-probe.json` |
| Optional-environment regression selection | PASS | Four previously affected test files; 183.02 s | `environment-regressions.*` |
| Docs / generated references / language contracts | PASS | 1.40 / 0.86 / 1.77 s | `docs.*`, `generated-references.*`, `docs-language.*` |
| `uv run mkdocs build --strict` | PASS | Final guide; 10.06 s | `mkdocs.*` |
| Guide example and rendered-page inspection | PASS | Expected frames, anchors and links | `docs-example.log`, `rendered-docs.log` |
| Fresh source review and post-fix review | PASS | No actionable findings; independent parity checks | `review.md` |
| Fresh guide review and corrected-source re-review | PASS | Correct immutable source/receipt references | `review.md` |
| Final fresh evidence review and identity audit | PASS | Receipt hashes, counts and ownership independently verified | `review.md`, `final-identity.json`, `ownership.json` |
| Hosted quality preflight on `ad5c117` | **FAIL** | Generated `docs/quality-metrics.md` is outdated | `hosted-preflight.json` |
| Local `dpone docs update-dev-metrics --check` | **FAIL** | Same generated-doc drift; shared owner follow-up | `dev-metrics.*` |
| Packaging | N/A | No packaging/dependency edits in DDA-03 | Task ownership contract |
| Live SQL, containers and route certification | SKIP | No approved disposable environment | `environment.json` |
| Live throughput/RSS improvement | UNVERIFIED | No live measurements | Component counter is structural only |
| Complete rerun with corrected temporary root / final hosted head | UNVERIFIED | Not completed for this component handoff | DDA-06 final integration gate remains required |

The selected `-k` CLI/runtime/state subsets are included in the complete non-live
run. Their inclusion does not promote that failing full run to PASS. The 570
skips remain skips; live checks were not run. Raw pytest tracebacks retain their
original trailing spaces: unrestricted `git diff --check` reports those log lines.
The scoped whitespace check excluding captured `.log` output passes; logs were
not rewritten to conceal this distinction.

## Full-suite failure diagnosis

Both failures are in `tests/test_airflow_cache_shared_directory_mode.py`:
`test_ensure_shared_directory_accepts_foreign_owned_superset` and
`test_ensure_cache_layout_succeeds_on_foreign_owned_shared_superset`.
The isolated `/tmp/dpone-dda03-full-pytest` root inherits directory GID 0 while
the test process has effective GID 20. A requested mode `02777` becomes `0777`,
losing SGID, so the simulated foreign-owned directory fails the expected `02775`
minimum. A probe under the platform user temporary directory retains GID 20 and
mode `02777`. All ten tests pass there without changing provider code, tests or
fixtures. `airflow-permissions-retest.json` also records byte identity to the
baseline for the two provider modules, test module and shared fixture.

The runner now derives its isolated root from `tempfile.gettempdir()`. This is an
evidence-environment correction. The original complete-run FAIL is retained;
there is no clean complete-run PASS for the corrected root. DDA-06 received this
finding and confirmed its temporary directory preserves SGID before its final
integration run.

The earlier interrupted full run also lacked optional development packages.
`uv sync --locked --all-extras` repaired the private environment without changing
`uv.lock` or `pyproject.toml`; the final complete run used those installed extras.
The initial graph FAIL was fixed by moving an annotation-only limits import under
`TYPE_CHECKING`, explicitly reviewed and approved by the integrator. Baselines
and budgets were not changed. See `review.md` for the architecture rationale.

## Documentation and user journey

[The framing guide](../../../docs/delivery-acceleration/frames.md) gives a verified
credential-free example, explains native/IPC/RSS limits and snapshot ownership,
and maps diagnostics to recovery actions. It records already implemented DDA-06
wiring against immutable `fefeab9` source, including the unchanged positional
IPC task guard. The pinned 417-case focused receipt binds that integration source;
it is distinct from this component's 121 tests and a complete final gate.

The guide was corrected after independent review and a concurrent DDA-06 IPC
fix; its final review found no issues. Shared navigation, changelog and generated
metrics remain DDA-06-owned. First-time operators gain an explanation of limits
and failure recovery without new configuration or migration steps.

## Remaining work and readiness

DDA-06 already integrated the two Python commits and received the separately
reviewed guide commit for `cherry-pick -x`. It owns final shared-document generation,
combined graph checks, the complete frozen integration run, spawned-worker
coverage and any approved live evidence. Its focused 417-case PASS is linked in
the guide; it does not replace the complete-run requirement.

This component is ready for review and integration with the disclosed failures.
Standalone merge and release readiness remain HOLD. No live route, performance,
publication, production readiness or native SWITCH authorization is claimed.
