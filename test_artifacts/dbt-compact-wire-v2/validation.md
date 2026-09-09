# Native compact dbt wire-v2 implementation validation

Date: 2026-09-09. Code/test commit: `00607ad1a363717ed083ba10c46dd65129513397`. [Implementation PR #2](https://github.com/PaulKov/dpone/pull/2).
Baseline: `f8c6a4a5e75d167829c05f65d5d3033acb193878`.
Specification: [native compact delivery](../../docs/feature-design-dbt-compact-wire-v2.md).
This is an implementation observation, not a route certificate or release receipt.

## Result

The native path is implemented through workspace compile, compact materialization,
deployment/index, provider/init-fetch and verified launcher preflight. It preserves
project-owned source closure, producer/selection/provenance metadata and ordered
runtime trios. Transport changes regenerate descriptors, release identity and
integrity through their producers. Legacy wire v1 and physical-admission gates
remain. Unsafe report destinations and aliases fail before publication.

Pure metadata and rewrite invariants live in `CompactWorkspaceReleasePlan`;
acquisition, mandatory schema/source verification and private staging live in
`CompactWorkspaceReleaseBuilder`. Shared descriptor checks and transfer ownership
were moved into their existing contract modules. This closes the introduced
architecture regression without weakening thresholds or manufacturing imports.
Eight pre-existing test-format defects were corrected without behavior changes.

## Checks

| Check | Status | Observed result |
| --- | --- | --- |
| Architecture fitness regression test | PASS | Cross-layer ratio 2685/8952 = 0.299932976, below 0.300; original implementation was 0.30110528 |
| Fresh correctness/architecture review | PASS | Independent read-only review approves the final refactor and parent-alias compatibility fix; source closure, mutability, failure ordering and compatibility reviewed |
| Fresh documentation/CLI UX review | PASS | Output safety, projection instructions, recovery and output semantics reviewed; seven-case CLI coverage includes aliases |
| Refactor focused suite | PASS | 149 cases in 123.92 s, including real offline toolchain and shared source-policy consumers |
| Complete host offline suite after refactor | FAIL | 19186 passed, 571 skipped, seven historical-evidence failures in 949.48 s; before final durability/cache fixes |
| Native I/O fault and path-boundary suite | PASS | 33 boundary/fault cases in 8.14 s at the code/test commit above |
| Local Docker focused matrix | PASS | 278 passed in 91.24 s; exact code/test commit above, network disabled, uid 10001 |
| Local Docker complete offline suite | FAIL | 19135 passed, 567 skipped, 89 failed in 1050.60 s; initial image lacked jq and init, corrected rerun follows |
| Corrected Docker regression rerun | FAIL | 595 passed, 3 skipped, 10 baseline failures in 208.80 s; all 17 initially failing files plus all 13 native-matrix files |
| Docker public-baseline ABA reproduction | FAIL | Initial 2 failed/1 passed; three repetitions 1/2, 1/2, 2/1; rename, hardlink and inventory failures all observed at f8c6a4 |
| Ruff lint and format | PASS | Entire repository passes both |
| Mypy | PASS | 1142 source files |
| Import rules | PASS | No violations |
| Layer metrics baseline gate | PASS | 8952 edges; maximum cross flow 199, allowed baseline delta 5 |
| Module-size historical baseline | FAIL | Exact code commit `5ae7d4d30c6c803e81203404381da82b88955af4`: 51 unavailable/non-ancestor baseline commits in unchanged debt entries |
| Documentation links/generated references/language/build | PASS | 793 Markdown files, 3125 links, 3 references, 32 language tests and strict MkDocs |
| Airflow public contracts and compatibility | PASS | CLI 25/25, Python 16/16, modules 9/9, schemas 75/75, package 1/1; 19 compatibility entries |
| Four package builds and Twine | PASS | Four wheels and four source distributions rebuilt successfully |
| Changed-content privacy check | PASS | No new host paths or credential patterns; reformatted tests independently verified AST-identical |
| Task contract | PASS | 0 errors, 0 warnings |
| Live SQL / hosted Airflow / physical target admission | SKIP | No authorized live environment; synthetic route inputs are not certification |
| Production activation / package publication | N/A | Outside task; no authority requested or exercised |

## Evidence map and limits

- `tests/test_dbt_compact_wire_v2.py`: two projects, mixed transfer packs,
  four workflows sharing only project-owned source objects, ordered trios,
  metadata retention, corruption and symlink rejection, immutable retries,
  concurrent publication, retained content conflict and mutation after READY.
- Its exact-toolchain case uses real dbt-core 1.12.3 / dbt-sqlserver 1.11.1 parse,
  compile and final runtime preflight parse/ls against synthetic projects.
  Structural cases inject deterministic subprocess-port inputs and typed
  synthetic route observations into real validators. The new two-project full-path
  cases do not override validators or modify compile results. Singleton
  compatibility cases reuse the existing synthetic singleton fixture. Neither
  route executes warehouse SQL.
- `tests/test_dbt_compact_wire_v2_cli.py`: JSON/Markdown/API parity and prevention
  of input/cache/escaped-path/symlink/hardlink report overwrites.
- `tests/test_dbt_compact_wire_v2_failures.py`: private-stage write/integrity/cleanup faults,
  rename and parent-fsync failures, persistent-fault and restored retries, fresh
  cache ancestor durability, source mutation during/after capture, malformed and
  dangling native metadata without legacy fallback, fully resealed hazardous
  transfer archives, source/cache containment, cyclic cache aliases, compatible
  parent aliases and rejected root aliases. Valid singleton wire-v1 roots with
  release-set v1/v2 metadata are rejected without native publication. Fault tests
  retain real validators.
- `tests/test_dbt_compact_wire_v2_boundaries.py`: exact 256 MiB object, 512 MiB
  aggregate, 64-object, manifest 16 MiB and selection 1 MiB metadata limits and one-over failures. Metadata checks
  do not claim delivery of 512 MiB of actual bytes.
- Existing payload-limit tests cover confined-read/aggregate overflow using
  reduced limits as fault injection. Existing artifact-index tests recheck every
  byte observation, detach mutable metadata and reject identical duplicates.
- Existing workspace-cache tests cover frozen capture despite later source
  mutation, cleanup failure preventing publication and retained activation gates.
  These component cases do not claim every possible end-to-end interruption timing.
- `tests/test_dbt_source_inventory_binding.py` and workspace/cache tests exercise
  the moved source and descriptor policies through their actual consumers.

## Local Docker environment and reproduction

Runtime source is `00607ad1a363717ed083ba10c46dd65129513397`; Linux ARM64,
Python 3.12.14, uv 0.12.10, dbt-core 1.12.3, dbt-sqlserver 1.11.1,
pytest 9.0.3. The initial container ran as uid 10001 with `--network none --cpus 2`.
The corrected container adds `--init` and jq 1.6, with source commit
`99166c85a23c151c73c33cd40a05101570add66b`; code, tests and dependency manifests
are unchanged from `00607ad` (verified by Git diff).
No host worktree or credentials are mounted and no database is contacted.

The pinned Python base digest is
`sha256:782412e85d0f0984994c290652577d4018aff08145c85b262bb63dc0c7522254`.
The initial locally built image digest is
`sha256:27a93b351e6879866d0d459459214302cf42b7e13bfd38720e6de892f5fbd5d8`.
The corrected image adds only Debian jq and its dependencies to that image; its
digest is `sha256:1ab663f7a0c1f6b786cf241965ebf98b2e48a2d00c0bcde59839c152c5e1dc7f`.
The dependency layer was built from public commit `c89ad54`; the public Git bundle
then supplied the exact runtime source above. A Git diff confirmed no changes to
`pyproject.toml`, `uv.lock` or package dependency manifests between those commits.
The initial superseded image build lacked a compiler and was cancelled; the final
image includes `build-essential` for the locked ARM64 source distributions.

The [Dockerfile](../../docker/dbt-compact-tests/Dockerfile) defines the 13-file
focused command. The [delivery guide](../../docs/dbt-compact-delivery.md) describes
building directly from a committed public bundle and running focused/full suites.
The focused matrix retains actual schema, source and integrity validators and
includes the real offline dbt toolchain preflight. See the evidence map above for
metadata-only boundary cases and synthetic certification limits.

## Historical evidence debt, separate from feature implementation

The seven non-architecture failures in the previous full run are in
`test_ci_shadow_pr3b_scope_contracts.py` (two),
`test_ci_shadow_pr3b_spec_contracts.py` (one),
`agent_policy/test_ci_shadow_pr3b_implementation_contract.py` (two),
`agent_policy/test_ci_shadow_pr3a_implementation_contract.py` (one), and
`test_ci_shadow_pr3b_output_amendment_contracts.py` (one). Their frozen historical
Git objects are unavailable in this public snapshot. Present-file hashes cannot
prove historical ancestry, so assertions and receipts remain unchanged.

All 51 module-size debt modules and their ledger are unchanged by this feature.
Independent read-only measurement of their bytes at immutable public root
`f8c6a4a5e75d167829c05f65d5d3033acb193878` matches every recorded LOC/SLOC cap.
That observation does not restore missing ancestry or make the ratchet PASS.
A public-snapshot ledger migration needs its own governed producer/ADR work.
No private history, other repository or alternate evidence source was accessed.

The full Linux run additionally exposed unchanged governance-reader ABA tests
(`test_workflow_privilege_snapshot.py`): rename/inventory in the initial run and
rename/hardlink/inventory in the corrected rerun. Baseline repetitions reproduce
the timestamp-sensitive failures; passing individual attempts do not erase them. A bounded stat observation confirmed unchanged directory and
file inode/size/link-count/mtime_ns/ctime_ns across an add/remove operation while
the old reader reported complete. This is a baseline timestamp-observation risk;
it is not an altered-byte acceptance in native compact capture. No sleeps or
weakened assertions were added to hide it. A separate governance-reader fix remains.

The corrected rerun clears all jq and process-reaping failures. It includes the
complete native matrix with no native failures; its ten remaining failures are
seven historical-object checks and three baseline governance ABA cases. This is
a targeted rerun after environment correction, not a claim of a second full-suite
PASS. All test assertions remain unchanged.

The feature can be reviewed independently of these baseline evidence issues;
no all-green repository gate, merge GO, SQL certification or release readiness
is claimed. Initial PR Agent receipt CI also could not obtain prerequisite GitHub
evidence; CI results must be assessed for their exact head.

## Documentation and customer journey

The [delivery guide](../../docs/dbt-compact-delivery.md), compatibility notes,
ADR 0052 amendment, CLI reference and changelog explain the complete delivery
journey, identity preservation, migration, failure recovery and production limits.
Raw host logs, temporary source trees and local user paths are not published.

Focused command for the final refactor:

```bash
uv run pytest tests/test_dbt_compact_wire_v2.py tests/test_dbt_compact_wire_v2_cli.py tests/test_dbt_compact_wire_v2_boundaries.py tests/test_dbt_release_wire_dispatch.py tests/test_dbt_workspace_cache_installation.py tests/test_dbt_source_inventory_binding.py tests/test_dbt_release_artifact_index.py -q -o addopts=''
```

Host full-suite command: `uv run pytest -m "not integration_live" -n auto --dist loadfile`.
Docker full suite uses `-n 2 --dist loadfile`; the corrected regression rerun
uses the 13 Dockerfile test paths plus the 17 files named by the initial full-run
failure summary, with `-n 2 --dist loadfile -q -o addopts=`.
