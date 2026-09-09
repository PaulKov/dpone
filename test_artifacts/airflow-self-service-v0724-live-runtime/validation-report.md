# v0.72.4 safe-sample live runtime integrity validation

- Date: 2026-07-15
- Branch: `codex/v0.72.4-safe-sample-live-runtime`
- Base commit: `4880c52d30b10222520e8181ab2deb61b69f329d`
- Specification: `docs/feature-design-safe-sample-live-runtime-integrity-v0724.md`
- Task contract: `test_artifacts/agent-policy/2026-07-15-v0724-safe-sample-live-runtime-integrity.yml`
- Review status: ready for code review; not live-certified or release-certified

## Implemented contract

- Explicit live safe-sample execution re-authorizes policy from verified
  pipeline facts and trusted injected route identities.
- Binding-set, connection-registry, credential-runtime, workload pack, and the
  exact primary authoring bytes are verified before credential or database I/O.
- The project source root is explicit. Runtime never infers authoring identity
  from `.dpone-cache`.
- One workload-scoped credential resolver is shared by ClickHouse target
  create/TTL, bounded copy, and cleanup. A new assembly creates a new scope.
- ClickHouse native inserts use quoted columns plus `VALUES`; the native driver
  must acknowledge the exact integer inserted-row count.
- Legacy and non-live paths stay network-free or fail closed. Production stays
  blocked without an external trusted route-certification verifier.

The ClickHouse correction follows the official `clickhouse-driver` insertion
contract: Python data inserts must end with `VALUES`; the driver returns the
inserted row count for this path. See the
[clickhouse-driver quickstart](https://clickhouse-driver.readthedocs.io/en/0.2.9/quickstart.html#inserting-data).

## Validation

| Check | Result | Evidence |
|---|---|---|
| Required focused safe-sample contract suite | PASS | `pytest` completed with no failures |
| Full offline regression suite | PASS | `4298 passed, 472 skipped in 289.71s` |
| Ruff lint | PASS | `All checks passed` |
| Ruff formatting | PASS | `3043 files already formatted` |
| Mypy | PASS | `485 source files`, no issues |
| Import rules | PASS | no architectural import violations |
| Architecture fitness | PASS | clustering `0.179`, cross-layer ratio `0.300`, no findings |
| Hard cross-layer budget test | PASS | focused architecture-fitness gate |
| Layer metrics | PASS | cross-layer delta `+0.002`, within budget |
| Module-size budget | PASS | no new hard-limit violations |
| Documentation links/contracts | PASS | `416` Markdown files and `1732` local links checked |
| Generated references | PASS | `2/2` synchronized |
| Documentation language tests | PASS | `4 passed` |
| MkDocs strict build | PASS | site built successfully |
| Compatibility registry | PASS | `19` entries synchronized |
| Workflow security | PASS | `0` errors, `0` warnings |
| Main package build and isolated import | PASS | `dpone-0.72.2` wheel/sdist |
| Airflow-pack build and isolated import | PASS | `dpone_airflow_pack-0.72.2` wheel/sdist |
| Native-accel build and isolated import | PASS | `dpone_native_accel-0.72.2` wheel/sdist |
| Twine metadata check | PASS | all six wheel/sdist artifacts |
| Live MSSQL to ClickHouse route | UNVERIFIED | no approved live environment or credentials used |
| Vault Kubernetes Auth | UNVERIFIED | no approved Vault/Kubernetes environment used |
| Production KPO/init-fetch execution | UNVERIFIED | no approved Airflow/Kubernetes environment used |
| New-user usability experiment | UNVERIFIED | must be run after merge with at least five new users |

Skipped tests are not counted as passed. The offline suite deliberately excludes
`integration_live` tests.

## Independent review

The architecture reviewer initially reported three issues:

1. Cache-root fallback weakened source identity. Fixed by requiring an explicit
   source root and adding a fail-before-cache-read contract test.
2. Ambiguous ClickHouse driver results could produce false success. Fixed by
   requiring an exact non-boolean integer acknowledgement and adding negative
   tests for `None`, list, boolean, and row-count mismatch results.
3. Credential caching could retain a rotated value. Rejected as a defect after
   lifecycle review: the resolver is instantiated inside one execution
   assembly, is not global, and intentionally implements the approved
   `resolution_scope: workload_start` contract. Re-resolving inside a workload
   would allow target DDL and DML to observe different credential versions.

After the fixes and lifecycle clarification, the architecture reviewer returned
`GO` with no remaining P0-P2 findings. The independent test certifier also
returned code-integrity `GO` with no P0-P2 findings. Its only P3 was missing
direct regression coverage for project-root traversal and symlink escape; both
negative tests were added and pass.

## Compatibility and release notes

- The CLI adds no new beginner command or required live flag. The real CLI
  composition root already supplies `Settings.project_dir`.
- Old compact packs remain parse-compatible but cannot enter explicit live
  execution until rebuilt with the primary manifest dependency.
- The legacy live-copy helper can no longer bypass the stronger assembly; it
  fails with `DPONE_SAFE_SAMPLE_LIVE_ASSEMBLY_REQUIRED`.
- Built artifacts still report repository version `0.72.2`. A version bump is a
  separate release-integration step and is not claimed by this feature branch.

## Remaining risk and next slice

This slice is ready for review and merge. It is not production-certified. The
next Phase 1B vertical slice must implement and verify the external
content-addressed route
certification/attestation adapter, then execute approved MSSQL to ClickHouse,
Vault Kubernetes Auth, and KPO/init-fetch live certification. Until then,
production authorization remains intentionally fail closed.
