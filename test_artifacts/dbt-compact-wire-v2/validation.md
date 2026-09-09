# Native compact dbt wire-v2 implementation validation

Date: 2026-09-09. Approved specification:
[`docs/feature-design-dbt-compact-wire-v2.md`](../../docs/feature-design-dbt-compact-wire-v2.md).
Baseline: `f8c6a4a5e75d167829c05f65d5d3033acb193878`.
Implementation commit: `fc176c5281c37277fd9f095d3d0ca351456f6b15`.
[Draft PR #2](https://github.com/PaulKov/dpone/pull/2).
This is an implementation observation, not a route certificate or release receipt.

## Result and public contracts

Native workspace compilation now feeds compact materialization without flattening
project-owned payload identity. Both index producers retain ordered runtime refs;
wire v2 requires an explicit v2 release schema. Derived releases preserve source
closure, producer, selection authority/fingerprint and provenance, add the closed
compact promotion marker, and recompute only transport descriptors and release
integrity through their producers. Invalid inputs fail before publication.
Legacy compact/wire-v1 paths and physical-admission restrictions remain.
CLI report destinations inside input/cache trees, escaped paths and report aliases
that could overwrite captured files are rejected before publication.

## Observed checks

| Check | Status | Result |
| --- | --- | --- |
| New full-path and boundary tests plus existing payload/index/cache contracts | PASS | 183 tests in 36.62 s at implementation commit; command below |
| Exact real offline dbt path | PASS | dbt-core 1.12.3 / dbt-sqlserver 1.11.1: real parse, workspace compile, compact, projection, provider/init-fetch, launcher and actual preflight parse/ls; included above |
| Final CLI output safety and parity | PASS | 7 tests, including symlink/hardlink aliases, in 9.11 s |
| Task contract | PASS | 0 errors and 0 warnings |
| Changed-content privacy check | PASS | Added content and new files contain no host paths or credential patterns; raw logs excluded |
| Ruff lint | PASS | Repository check exits 0 |
| Ruff format | FAIL | Eight unchanged baseline test files need formatting; changed files pass |
| Mypy | PASS | 1142 source files |
| Import rules | PASS | No violations |
| Layer metrics baseline gate | PASS | 8957 edges; cross ratio 0.301; maximum flow 199, allowed baseline delta 5 |
| Architecture fitness cross-layer test | FAIL | Change raises ratio from baseline 0.29973148 to 0.30110528, above test limit 0.300; unresolved merge blocker |
| Architecture average clustering observation | PASS | Decreases from baseline 0.18152796 to 0.18140714; existing baseline debt is not a new compliance claim |
| Exact committed module-size gate | FAIL | 51 unchanged debt entries reference unavailable/non-ancestor baseline commits; baseline-to-baseline rerun is not supported while this head is checked out |
| Documentation links | PASS | 793 Markdown files / 3124 local links |
| Generated references | PASS | 3/3 synchronized through generators |
| Documentation language tests | PASS | 32 tests |
| Strict MkDocs | PASS | Documentation builds successfully |
| Airflow public contracts | PASS | CLI 25/25, Python 16/16, modules 9/9, schemas 75/75, package 1/1 |
| Compatibility registry | PASS | 19 entries |
| Four package builds and Twine | PASS | Four wheels and four source distributions; no publication |
| First complete offline suite | FAIL | 19091 passed, 580 skipped, 30 failed, 2 collection errors, 510.19 s; included one now-corrected schema fixture, missing optional dependencies and historical Git objects |
| Complete offline rerun with optional dependencies | FAIL | 19183 passed, 571 skipped, 8 failed in 291.87 s: architecture ratio plus seven historical Git-identity tests |
| Fresh architecture/correctness review | PASS | Sidecar finding fixed; no further production blocker identified; evidence and broad gates remain binding |
| Fresh docs/UX review | PASS | Output overwrite finding and projection documentation gaps fixed; reviewer reran five CLI tests |
| Live SQL / hosted Airflow / physical target admission | SKIP | No authorized live environment; synthetic route inputs are not certification |
| Initial PR CI observation | FAIL | Agent receipt cannot obtain prerequisite GitHub evidence; Quality preflight failed; other jobs still running when observed, not claimed green |
| Production activation / package publication | N/A | Outside task; no authority requested or exercised |

Focused command:

```bash
uv run pytest tests/test_dbt_compact_wire_v2.py tests/test_dbt_compact_wire_v2_cli.py tests/test_dbt_compact_wire_v2_boundaries.py tests/test_dbt_release_wire_dispatch.py tests/test_airflow_compact_pack_runtime_payload_limits.py tests/test_dbt_runtime_payload_contract.py tests/test_dbt_workspace_cache_installation.py tests/test_dbt_release_artifact_index.py -q -o addopts=''
```

The two report-alias cases were added after broad-suite collection. The complete
focused set was then rerun at the implementation commit and all 183 cases pass.
No broad green result is claimed for the committed head. The broad suite command
is the repository-required
`uv run pytest -m "not integration_live" -n auto --dist loadfile`.

## Acceptance evidence map

- `tests/test_dbt_compact_wire_v2.py`: two projects, mixed transfer packs,
  four workflows sharing only project-owned source objects, actual delivery path,
  ordered trios, metadata retention, cross-project/role/order/hash/path/schema
  corruption, symlinks, immutable retries, concurrent publication and retained
  content conflict; downloaded bytes changed after READY fail launcher admission.
- `tests/test_dbt_compact_wire_v2_boundaries.py`: exact 256 MiB object, 512 MiB
  aggregate and 64-object metadata boundaries plus one-over failures. These
  metadata-only checks do not claim delivery of 512 MiB of actual bytes.
- `tests/test_airflow_compact_pack_runtime_payload_limits.py`: existing confined
  read/aggregate overflow and CLI no-publication tests use reduced limits as fault
  injection. They supplement the unchanged canonical limits, not live evidence.
- `tests/test_dbt_release_artifact_index.py`: detached inventory and every byte
  observation rechecked, identical duplicates rejected.
- `tests/test_dbt_workspace_cache_installation.py`: shared verified capture uses
  frozen bytes despite later source mutation; stage cleanup failure prevents cache
  publication; wire-v1 activation and wire-v2 physical-admission gates retained.
  These are existing component fault-injection tests, not a new end-to-end claim
  for every interruption timing.
- New end-to-end composition does not monkeypatch validators or alter compile
  results. Structural cases inject deterministic subprocess-port inputs and a
  typed synthetic route snapshot into real validation. The real-toolchain case
  uses actual parse/ls. Neither executes warehouse SQL.

## Review and remaining work

User and developer documentation covers compile-to-provider delivery, source versus
transport identity, migration, error recovery, output safety and production limits.
See the [delivery guide](../../docs/dbt-compact-delivery.md), compatibility notes,
ADR 0052 amendment, CLI reference and changelog. The task contract is adjacent.
Raw host logs and temporary source trees are not published.

The implementation is suitable for draft review, **not merge or release**. Resolve
the new architecture cross-layer budget failure without weakening the gate or
adding artificial imports. The baseline formatter debt and final broad-suite
failures must remain visible; unrelated historical CI evidence is not repaired in
this task. No deployment, certification or release readiness is claimed.

## Failure attribution and follow-up

The seven non-architecture failures in the dependency-complete run are in
`test_ci_shadow_pr3b_scope_contracts.py` (two),
`test_ci_shadow_pr3b_spec_contracts.py` (one),
`agent_policy/test_ci_shadow_pr3b_implementation_contract.py` (two),
`agent_policy/test_ci_shadow_pr3a_implementation_contract.py` (one), and
`test_ci_shadow_pr3b_output_amendment_contracts.py` (one). Their frozen historical
Git objects cannot be resolved from this public snapshot. No alternate repository
or private history was accessed to satisfy them.

The eight format failures are unchanged baseline files:
`test_airflow_asset_uri_mssql_aip60.py`, `test_airflow_desired_state_authority.py`,
`test_airflow_pack_runtime_v3.py`, `test_cli_gitops_workload_catalog.py`,
`test_gitops_remote_artifacts.py`, `test_object_storage_retention_cli.py`,
`test_oss_code_quality_benchmark.py`, and `test_source_materialization.py` under
`tests/`. No unrelated formatter sweep is included.

The architecture ratio failure is introduced by this change, not attributed to
baseline debt. Independent review recommends separating pure compact release
policy from orchestration for cohesion, but calculates that this alone would not
close the ratio gap. Further boundary work or an explicitly governed standards
decision remains necessary; no exception or artificial dependency is applied.
