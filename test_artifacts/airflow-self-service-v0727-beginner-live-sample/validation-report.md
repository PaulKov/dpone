# v0.72.7 beginner auto-live safe sample validation

- Task: `DPONE-V0727-BEGINNER-AUTO-LIVE-SAMPLE`
- Branch: `codex/v0.72.7-beginner-live-sample`
- Base commit: `2d7bc6888a7a6d10fa9db31d51d2f5e88690d061`
- Validated at: `2026-07-15T21:14:45Z`
- Implementation status: `PASS`
- Review status: `PASS`
- Live certification status: `UNVERIFIED`

## Scope proved

The beginner command automatically selects the existing signed live assembly
only when the deployment-scoped authorization overlay is complete and trusted.
An absent overlay preserves the network-free local handoff. Partial, unsafe,
untrusted, expired, or mismatched inputs fail closed before credential or
database I/O. No beginner command or flag was added, and runtime never resolves
the mutable `current` pointer.

## Automated checks

| Check | Result | Evidence |
|---|---|---|
| Focused safe-sample, CLI, schema, route-file and architecture tests | PASS | Final focused pytest selection completed with exit code `0` |
| Change-aware validation selection | PASS | Categories: Airflow, CLI, connector route, docs, schema, packaging, Python, runtime state, workflow security |
| Ruff lint | PASS | `All checks passed!` |
| Ruff formatting | PASS | `3072 files already formatted` |
| Mypy | PASS | `Success: no issues found in 495 source files` |
| Import rules | PASS | No architectural import violations |
| Layer metrics | PASS | `cross_ratio=0.300`, no issues, max cross flow unchanged at `94` |
| Module-size budget | PASS | Ten pre-existing warnings; no new warning from this change |
| Full non-live regression | PASS | `4367 passed, 472 skipped in 274.71s` |
| Documentation links/contracts | PASS | `421 markdown files, 1745 local links checked` |
| Generated references | PASS | `2/2 in sync` |
| Compatibility registry | PASS | `19 registry entries`, documentation block in sync |
| Documentation language tests | PASS | `4 passed` |
| Strict MkDocs build | PASS | Documentation built successfully |
| Workflow security | PASS | `0 errors, 0 warnings` |
| Root sdist/wheel build | PASS | `dpone-0.72.2` artifacts built |
| Airflow pack sdist/wheel build | PASS | `dpone_airflow_pack-0.72.2` artifacts built |
| Native accel sdist/wheel build | PASS | `dpone_native_accel-0.72.2` artifacts built |
| Twine metadata validation | PASS | All artifacts in `dist/` passed |
| Whitespace/conflict markers | PASS | `git diff --check` exited `0` |

## Security and failure evidence

- Reserved `current`, traversal-like IDs, unsafe cache roots and symlink loops
  are rejected with structured errors before handoff artifacts are written.
- Live environment files are consumed through bounded, descriptor-pinned,
  no-follow reads; symlink swaps and files larger than 4 MiB fail closed.
- Route attestation, signature subject, source pin, environment fingerprints,
  schemas, route binding and policy remain prerequisites for resolver/client
  construction.
- Human beginner output hides platform-only commands. JSON retains the
  diagnostic handoff for platform automation and backward compatibility.
- `execution_mode` is additive and reports `live_copy`, `local_handoff`, or
  `blocked`; existing v1 runtime evidence remains schema-valid.
- Missing explicit ops inputs retain the existing
  `DPONE_SAFE_SAMPLE_LIVE_COPY_INPUT_INVALID` contract. Unsafe or unstable
  paths use `DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID`.

## Development findings resolved

- Fresh architecture and test reviews identified an unhandled unsafe overlay
  identity and a consume-time path race. Typed path errors, pre-write identity
  validation and no-follow bounded reads now cover both cases.
- A first broad architecture run exposed a cross-layer ratio regression. The
  final dependency direction uses a read-only structural plan view and a thin
  command orchestration helper; all final architecture gates pass.
- The command briefly crossed the module warning budget. Runtime-mode selection
  was extracted by responsibility; the final module-size warning count returned
  to the pre-change value of ten.
- The final fresh-context architecture re-review reported no remaining
  blocking, high, or medium findings. It reconfirmed discovery-state
  preservation, internal-error exit `5`, diagnostic-handoff ordering,
  `current` rejection, bounded no-follow reads and trust-before-credential
  ordering.

## Live certification

`UNVERIFIED`: this workspace did not provide an explicitly approved signed
Sigstore overlay plus Kubernetes identity, Vault, MSSQL and ClickHouse live
environment. No live route is claimed as passed. Required follow-up evidence is
a real five-command run whose runtime evidence proves signature verification,
Vault resolution at workload start, bounded MSSQL reads, temporary ClickHouse
target cleanup and row/count parity.

## Residual risk

The code is ready for review and merge as a backward-compatible, fail-closed
slice. It is not release-certified for the live production path until the live
environment procedure above is executed and attached to a frozen commit.
