# Native workspace attempt lifecycle

Native delivery retains its physical workspace attempt after the dbt process
finishes. The same attempt must remain `RUNNING` while the protected native
completion and freeze operations validate ownership. A successful dbt build is
an execution result; it does not settle delivery or release capacity.

## Composition

`DbtExecutionService` accepts an optional `workspace_attempt_lifecycle` capability.
When omitted, ordinary execution retains its existing immediate durable
terminalization behavior. Supplying a lifecycle together with
`workspace_attempt_factory` or `workspace_attempt_admission` raises `ValueError`.
This prevents ambiguous ownership at the composition root.

The lifecycle interface has two operations: `admit(...)` and
`record_execution_outcome(...)`. The ordinary implementation records outcomes by
calling its existing durable terminalization operation. The native implementation
retains the execution outcome locally and never creates a terminal SQL receipt.

`NativeWorkspaceAttemptLifecycle` receives the exact previously admitted request
and `RUNNING` receipt, the existing request factory and bounded manifest reader,
and an injected `require_current_running` callable. It reconstructs the request
from the actual authenticated pack and preflight manifest, compares the complete
request, and requires the fresh receipt to equal the retained receipt, including
all guard identities and epochs. One instance permits only one admission attempt.
A retained `RUNNING` row does not authorize execution replay.

The application must supply
`MssqlDbtWorkspaceAttemptAdmission.require_current_running` through a bounded
control connection. That read validates the ACTIVE activation, current physical
guard ownership and epochs, complete write-subject coverage, the exact RUNNING
attempt and its recorded guards in one fresh serializable transaction. SQL errors
fail closed and close the connection. The existing `require` method retains its
historical exact-record read behavior. Neither read grants a lifetime lease;
protected native mutations must independently check fencing again.

## Failure and recovery

A failed build leaves the attempt owned by native delivery. An exception while
recording an outcome after dispatch produces `COMMIT_UNKNOWN` and follows the
existing execution-evidence path. Evidence publication failure after dispatch
still raises the existing `CommitUnknownError`. No local success value authorizes
freeze, replica publication or settlement. The outer owner must require a
successful service return and separately authenticated native completion.

Repeated identical local outcome recording is idempotent; conflicting outcomes
fail closed. Process loss discards the local outcome. Recovery must use the
retained SQL attempt and authenticated originals, without automatically replaying
dbt or inferring cleanup. This lifecycle has no settlement or quota-release API.
Final native settlement belongs to the outer delivery owner after the approved
three-replica success or positively proved cleanup conditions.

## Validation boundary

Focused tests cover ordinary execution compatibility, current-owner rejection,
retained ownership during evidence writing, failed builds, recording and evidence
failures, receipt mismatch and attempted replay. SQL-adapter unit tests use a
connection double. Live transaction/concurrency proof, installed runtime
qualification, complete native bootstrap and end-to-end delivery remain
**UNVERIFIED** until their separate evidence is recorded.
