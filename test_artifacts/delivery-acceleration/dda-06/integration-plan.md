# DDA-06 integration and validation plan

Status: preparation in progress; no implementation or performance PASS implied.

## Identity and ownership

- Audited production baseline: `d5ad9aaecc900c24df421b160ed36b4cfc726e45`.
- Immutable approved planning dependency: `f3682940f8864563cde0e6b6ecee60f746b49020`.
- Integration branch: `codex/dda-06-integration`.
- Contract: `../agent-task-contracts/dda-06-integration.yml`.
- `origin/master` was fetched before branch preparation and contained the baseline.
- The clean detached worktree was placed on the integration branch and imported
  the planning dependency with ordinary `git merge --no-edit` (fast-forward).
- DDA-01 through DDA-05 changes require reviewed handoffs and recorded commits.
  Audit imported dependency paths separately from DDA-06's own diff.
- Planning specification and task-plan documents remain immutable. Record current
  implementation status here and in the integration completion report.
- The contract's `integrator_owned_paths` explicitly assigns `CHANGELOG.md` and
  `mkdocs.yml` to DDA-06; it does not grant other shared paths.

## Compatible changes

1. Carry the frozen producer's encoded-byte reservation through the scheduler;
   keep frame/task serialization limits and worker byte equality checks.
2. Populate prepared business values and canonical framework metadata in one
   explicit INSERT SELECT, followed by shared validation and terminal evidence.
3. Compute the existing business and full digests from one full prepared iterator;
   keep a separate full prepublication scan and all four raw inspections.
4. Inject optional observations without changing journal/recovery serialization,
   source ownership, admission, retry policy, publication or state ordering.
5. Retain public native SWITCH rejection before I/O. The isolated component has
   no registry or production-runtime activation.

No manifest migration, version change, release, publication or provider change is
authorized. Live services and credentials are not authorized by this task.

## Proof matrix

| Invariant | Focused evidence |
|---|---|
| Four raw and two prepared typed scans | Count connector iterators in the integrated lifecycle; observer labels alone are insufficient |
| No preparation metadata UPDATE | Capture SQL from the actual preparer/normalizer; retain direct BCP and finalizer-clock regression checks |
| No scheduler sizing pass | Instrument parent sizing with real spawned encoders; assert frozen frame/file bytes and both IPC bounds |
| Mutable buffer safety | Mutate driver-owned buffers after yielding; compare actual retained encoded bytes |
| Recovery/cancellation | Reuse real worker, receipt-first, source-free, unknown-commit and owner/fencing fixtures |
| Failure boundaries | Raw and prepared/metadata tamper must fail before mutation; observer failures cannot authorize success |
| Diagnostic interoperability | Execute DDA-05 producer and DDA-01 consumer together; verify identities, artifacts and unavailable/live status |
| Native SWITCH remains unavailable | Existing `tests/test_runtime_partition_replace_native_contracts.py` before-I/O rejection |
| Documentation journey | Executable plan/example, focused tests, strict build and reference checks |

Run focused checks before the full non-live suite. Set
`PYTEST_XDIST_AUTO_NUM_WORKERS=2` for the full suite. Run ruff, format, mypy,
import/layer/module-size gates and documentation gates against the exact integrated
source commit. Record commands, exit codes and retained logs through their actual
execution; never edit a generated result into a PASS.

## Review and integration

Fresh-context read-only explorer, architect, test/certification and docs/UX agents
review the integration plan. A separate fresh-context reviewer inspects the final
implementation; findings are fixed and independently reviewed again. Push the
first DDA-06 implementation commit and immediately attempt an implementation PR.
Keep that PR open for review; this task does not merge it.

## Live evidence

Live SQL, BCP interoperability and measured speed: **SKIP / UNVERIFIED** until an
explicitly approved disposable environment is available. Structural scan counts
and hermetic timing do not establish real end-to-end acceleration.
