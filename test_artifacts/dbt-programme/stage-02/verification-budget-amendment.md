# B02 implementation amendment: enforce the approved verification budget

Decision: coordinator authorizes this narrow implementation/path amendment under
the maintainer's existing B02 approval. Independent read-only architect
`/root/b02_deadline_contract` approved the technical plan. The original reviewed
specification and appendix remain immutable. The public staging method, finite
policy, accepted types/transports, receipt formats and terminal authority do not
change. This amendment supplies the missing mechanism for the approved deadlines;
it does not weaken them or add user policy knobs.

Observed gap: existing source integrity verification hashes the full file without
budget checks, including a descriptor-authority path. Checking elapsed time only
after that traversal does not implement the approved finite scan. Preparation
must create its budget before entering the wrapper attempt.

Implement one immutable FileVerificationBudget in existing
`src/dpone/runtime/file_artifact_authority.py`, with injected monotonic clock,
absolute phase deadline and maximum source bytes. It checks time and observed
bytes and supplies remaining time; it cannot replace verification or issue proof.
Thread optional keyword-only verification_budget=None through existing verification
calls and the new attempt's entry/verify/complete operations. No-budget callers
retain existing behavior and call compatibility. A budgeted unsupported authority
fails closed, never falls back to unbounded verification.

Check before opening/reading and after each fixed-size read, including EOF.
Reject excessive initial file size and observed byte totals. Each repeated scan
has a fresh byte counter but shares the phase deadline. Preserve every existing
receipt/wire/path/descriptor/scope/contract check, reader position and authority
lock. Lock acquisition uses remaining time; only acquired locks are released.
A single stalled OS syscall remains a cooperative-I/O limitation; traversal
must stop between chunks after expiry.

Carry the preparation budget through entry, decoding, seal and pre-DDL checks.
Use one new post-transport verification budget through source/derived checks and
complete. Propagate typed original failures, reset summaries and preserve cleanup
or retained-unknown outcomes.

Endpoint proof uses the approved bounded runner: freeze explicit single-node
endpoint/database settings, probe actual identity, bind data/control to it and
reject drift before mutation. Do not add an unbounded generic connector.get_records
probe. This does not claim observation of an already-open finalizer socket; later
finalization retains its existing owner and contract.

Additional owned source paths (same single Stage02 integrator):

- src/dpone/runtime/file_artifact_authority.py
- src/dpone/runtime/file_artifacts.py
- src/dpone/runtime/artifact_integrity.py
- src/dpone/runtime/pinned_file_consumer.py
- src/dpone/runtime/pinned_file_integrity.py

Additional owned tests:

- tests/test_file_verification_budget.py
- tests/test_mssql_artifact_integrity.py

Use already-owned validator/attempt/preparation/ingestion and B02 test paths for
propagation. Add focused tests for pathname and pinned-descriptor scans, actual
bytes before expiry, pre-stat denial, lock expiry, legacy no-budget authority,
receipt equality, entry/seal/pre-DDL/post-transport/complete deadlines and no
generic get_records call. Keep all original full, installed and fresh-review gates.
ADR0064 and developer documentation must explain the cooperative guarantee.

Validation of the design: static trace PASS. Implementation/runtime behavior:
UNVERIFIED until the actual tests execute. No new release/live authorization.
