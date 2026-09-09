# Native compact dbt wire-v2 implementation validation

Date: 2026-09-09. [Implementation PR #2](https://github.com/PaulKov/dpone/pull/2).
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
| Fresh correctness/architecture review | PASS | Independent read-only review approves the final refactor; source closure, mutability, failure ordering and compatibility reviewed |
| Fresh documentation/CLI UX review | PASS | Output safety, projection instructions, recovery and output semantics reviewed; seven-case CLI coverage includes aliases |
| Refactor focused suite | PASS | 149 cases in 123.92 s, including real offline toolchain and shared source-policy consumers |
| Complete offline suite after refactor | UNVERIFIED | Running; preceding dependency-complete run had 19183 passed, 571 skipped and eight failures |
| Ruff lint and format | PASS | Entire repository passes both |
| Mypy | PASS | 1142 source files |
| Import rules | PASS | No violations |
| Layer metrics baseline gate | PASS | 8952 edges; maximum cross flow 199, allowed baseline delta 5 |
| Module-size historical baseline | FAIL | Exact code commit `9556727ea2282c83367a1511735924e3db98649c`: 51 unavailable/non-ancestor baseline commits in unchanged debt entries |
| Documentation links/generated references/language/build | PASS | 793 Markdown files, 3124 links, 3 references, 32 language tests and strict MkDocs |
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
  synthetic route observations into real validators. No validator monkeypatch or
  modified compile result is used. Neither route executes warehouse SQL.
- `tests/test_dbt_compact_wire_v2_cli.py`: JSON/Markdown/API parity and prevention
  of input/cache/escaped-path/symlink/hardlink report overwrites.
- `tests/test_dbt_compact_wire_v2_boundaries.py`: exact 256 MiB object, 512 MiB
  aggregate and 64-object metadata limits and one-over failures. Metadata checks
  do not claim delivery of 512 MiB of actual bytes.
- Existing payload-limit tests cover confined-read/aggregate overflow using
  reduced limits as fault injection. Existing artifact-index tests recheck every
  byte observation, detach mutable metadata and reject identical duplicates.
- Existing workspace-cache tests cover frozen capture despite later source
  mutation, cleanup failure preventing publication and retained activation gates.
  These component cases do not claim every possible end-to-end interruption timing.
- `tests/test_dbt_source_inventory_binding.py` and workspace/cache tests exercise
  the moved source and descriptor policies through their actual consumers.

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

The feature can be reviewed independently of these historical evidence issues;
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

Full-suite command: `uv run pytest -m "not integration_live" -n auto --dist loadfile`.
