# B02 internal responsibility amendment

Status: APPROVED FOR IMPLEMENTATION by programme coordinator under the existing
user-approved B02 behavior and explicit resume instruction, 2026-09-14.
This is not merge, release, live-environment or certification approval.

Implementation starting commit: 5c9d8ff36bb51ab7764b347a7ebc9fb1817681cf.
Approved reconciled base: 1202d2cf652cf41594abdd21040378204653b656.
Sole writer: existing Stage02 task 01a09be4-b055-7730-889a-87c84fa8509d,
worktree /Users/paulkov007/.codex/worktrees/b02v1/dpone-public,
branch codex/b02-validated-file-staging-linear, draft PR68.

## Reason and independent design review

Current official architecture result is FAIL: 0.18292201222764504.
Independent architect /root/b02_structure_design reproduced the combined forecast
using collect_internal_deps and compute_coupling_stats: 3725 modules, 9619 edges,
0.18195837305022228 against the unchanged checked-in ceiling 0.182.
The forecast is not implementation evidence. Actual gate remains UNVERIFIED.
The reviewer approved these actual responsibility corrections, with no new
product decision required. docs/benchmarks/quality_budgets.yml is authoritative;
do not change budgets or baselines. Historical prose mentioning 0.180 is not
authorization to alter the current producer or its checked-in configuration.

## Exact changes

1. Move actual pure client command/redaction construction and credentials/options
   definitions to connectors/clickhouse_client_request.py; move actual HTTP URL
   construction and credentials/options to connectors/clickhouse_http_request.py.
   Legacy runners retain identity-preserving aliases and delegating methods.
   Preserve self._base_command and self._path dispatch, public constructors,
   defaults, from_bulk_wire_contract merging, class metadata and pickle paths.
   Controlled adapters use these pure functions rather than constructing legacy
   execution runners. Adapter factories construct options from resolved scalar
   settings; the sink drops direct dependencies on both legacy bulk modules.
2. Move actual finite policy/error/constants/admission definitions from
   sinks/clickhouse_validated_file_models.py into clickhouse_file_stage_contract.py.
   Keep the old module an identity-preserving facade; update internal consumers.
3. Move actual iter_wire_rows, decode_wire_value, iter_rows and
   BulkTextFileReadError implementations from support/bulk_text_file_reader.py
   into connectors/bulk_text_codec.py. Keep exact aliases at the reader path.
   Preserve codec identity, function signatures, defaults, UTF-8/error translation,
   binary whitespace, NULL/empty distinctions, row bounds and profile admission.
4. Put a narrow journal resource protocol and unchanged canonical_json in the
   shared stage contract. Preserve canonical_json at its old import path too.
   Port members are directory, attempt_id, context management, require_identity,
   reserve, open_partial, seal, release_spool, record and attach_failure only.
   Sink composes the concrete storage-bound journal factory; ingestion/preparation
   consume the port. Explicit internal DI signature amendment: replace service
   storage= plus default concrete journal_factory with a required storage-bound
   factory. The approved spec treats this service constructor as internal DI.
   Public sink constructor and stage_validated_file signature remain unchanged.
   Do not inject planner/preparer or move verification budgets.
5. Move actual _DeadlineRaw, _FramingReader, _ResponseSocket, _BoundedResponse to
   stdlib-only connectors/clickhouse_file_response.py. Inject the existing limit
   explicitly for independent metadata AND chunk-body counters, plus remaining
   deadline callback. Preserve outer body cap, malformed framing rejection,
   non-chunked completion, supplied connection factory, reader/socket ownership,
   closure and unknown-outcome behavior. Keep adapter public constructor intact.

All connector paths above are under src/dpone/runtime/. New pure request and
response modules must contain real implementations and no hidden internal imports.
No facade laundering, artificial leaves, deleted annotations or graph-only tricks.

## Ownership activation before source edits

Copy this amendment unchanged into Stage02 operational approval artifacts; append
its hash and scope to the approval receipt without changing the original approved
specification/appendix. Update and validate the active task contract first.
Add these owned paths:

- src/dpone/runtime/connectors/clickhouse_client_request.py
- src/dpone/runtime/connectors/clickhouse_http_request.py
- src/dpone/runtime/connectors/clickhouse_file_response.py
- src/dpone/runtime/connectors/bulk_text_codec.py
- src/dpone/runtime/connectors/clickhouse_http_bulk.py
- tests/test_b02_dependency_compatibility.py

Remove only the last two existing source paths from read_only_paths. Existing
owned source/test/docs paths remain owned. Update ADR0064 and developer composition
map within current ownership. No additional paths, shared fixture, dependency,
workflow, budget, baseline, package or public policy changes are authorized.

## Preserved behavior and validation

No changes to retry/checkpoint/receipt authority, wire admission, exact RowBinary
values, intent-before-mutation, one-submit execution, unknown retention, exact
COUNT/identity checks, zero-row branch, resource limits, spool cleanup or final
durable evidence before a handle. No monkeypatch/private-method replacement.

Characterize legacy identity, module/qualname, signatures/defaults, type hints,
synthetic legacy pickle loading and builder outputs before moving implementations.
Add meaningful compatibility tests in the new module, including public subclass
dispatch through real synthetic child/loopback peers. Preserve historical private
dispatch points by delegating in source; tests do not replace private methods.
Do not fix unrelated exception pickle behavior. Preserve HTTP positional
connection_factory and query=None/empty-string fallback behavior.

Run new compatibility tests, existing complete B02 tests and source validator/
integrity tests; preserve genuine HTTP framing/deadline/cleanup, journal fault
ordering and both-mode value oracle tests. Only necessary internal DI fixture
rewiring is allowed; do not weaken assertions or invent passing evidence.
Run change-aware plan and required static/docs/architecture checks, including
actual fitness and exact-commit module size. Commit and normal push to PR68 when
ready; update its concrete description immediately. No force/merge commits.

Coordinator then owns full non-live source validation with frozen all-extras,
regenerated installed inventory and installed acceptance on the final clean SHA.
Require fresh independent final exact-commit review and resolve blockers before
completion. Source/installed/final acceptance remain UNVERIFIED until executed.
