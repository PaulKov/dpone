# Trusted native dbt invocation

`TrustedDbtInvocationRecorder` observes one admitted, finite dbt command sequence
and publishes an immutable positive completion original. It is an internal
composition capability. The native analyst CLI journey and the complete
source-ledger closure/freeze protocol are still under implementation.

For platform owners, [native generation admission](native-generation-admission.md)
explains reservation, capacity and once-only writer binding. A retained BUILDING
row is not permission to repeat a command. This recorder provides the next piece:
proof of the qualified synchronous runner's normal return for the entire locked
sequence. It does not release capacity or change SQL custody.

## Bootstrap responsibilities

Application composition must authenticate the selected deployment, complete
platform policy, exact runtime inventory and qualification evidence before
issuing generation-local invocation attestations. The existing three-version dbt
inspector alone does not prove the Python, driver, image, distribution or macro
identity needed for this qualification. The actual inventory and qualification
producers and their application wiring remain pending.

The application owns, authenticates and holds distinct project, output and
profile directories throughout execution. Project contents must match their
retained inventory. Writable roots must be isolated from other invocations and
untrusted writers. Metadata contains directory device/inode identity and a
logical root ID; an arbitrary serialized pathname never grants ownership.
Path checks in the recorder complement that lifetime isolation. They do not
replace it or prevent a concurrent privileged filesystem writer.

Inject the existing `SubprocessDbtCommandRunner` as the delegate. Its synchronous
contract waits for the child and joins bounded output collectors before normal
return. Qualification must additionally establish that exit zero completes the
admitted synchronous database effects for the exact supported runtime. The
recorder does not infer SQL session termination from a PID or invent a task
inventory.

## Invocation records

The existing `NativeOriginalKind` retains its three storage variants and adds
five explicit variants:

| Kind | Payload | Purpose |
| --- | --- | --- |
| `trusted_dbt_command_plan_v1` | `dpone.trusted-dbt-command-plan.v1` | Exact phase, ordered argv, roots and time allowances |
| `trusted_dbt_toolchain_v1` | `dpone.trusted-dbt-toolchain.v1` | Unchanged seven-field dbt contract and runtime-inventory reference |
| `trusted_dbt_qualification_v1` | `dpone.trusted-dbt-qualification.v1` | Admitted policy, profile, toolchain, command plan and retained provenance |
| `trusted_dbt_owned_root_v1` | `dpone.trusted-dbt-owned-root.v1` | Invocation-specific PROJECT, OUTPUT or PROFILE directory identity |
| `trusted_dbt_invocation_completion_v1` | `dpone.trusted-dbt-invocation-completion.v1` | Internally observed complete positive normal return |

Decoding an attestation or finding a nonempty evidence list does not qualify a
runtime. The authorized producer must verify upstream evidence under its original
subject. A generation-local attestation references that provenance; it cannot
relabel a platform original as a generation original.

Codecs reject extra/missing fields, noncanonical bytes and coerced primitive
values. Existing `DbtToolchainContract` fields and fingerprint remain unchanged.
The original SQL index stores exact kind bytes, so these Python variants do not
require a SQL enum migration or a new object-key version. Reservation originals
retain their existing `generation_stored_file_v1` binding.

## Execution and deadlines

1. Bind the exact executor, command, toolchain and qualification references,
   original reader/index, publisher and clocks in the recorder constructor.
   Bindings must belong to the admitted generation under its workspace authority.
2. Call `validate_before_credentials()` before profile rendering or credential
   resolution. It independently resolves full bindings and reads bounded,
   exact-version bytes. A mismatched kind, subject, locator or digest rejects.
3. Use the same recorder as the command runner for the entire phase. BUILD is
   exactly `parse`, `ls`, `build`; QUALITY is exactly `test`. Every call repeats
   original authentication before admitting its next slot.
4. Pass the locked argv and unchanged command timeout. Only whole `PROJECT_DIR`,
   `PROFILE_DIR`, `TARGET_DIR` and `LOG_DIR` arguments at their corresponding
   explicit path flags may be substituted. All selectors, variables and other
   arguments remain literal. Prefer locked literal target/log paths where the
   output layout is already known.
5. Project cwd must match the authenticated project root. Output directories
   must be below the owned output root. The fresh profile directory must be an
   immediate child of the owned profile root. Links, traversal and profile
   pathname or device/inode changes reject.
6. Each slot is consumed before calling the delegate. Exceptions, nonzero exit,
   invalid result, entry overrun or absolute deadline overrun close admission.
   No consumed slot reopens. The absolute allowance starts at first validation
   or execution and includes authentication and gaps between commands; it never
   resets between `parse`, `ls` and `build`.

The per-entry termination allowance includes time outside the runner's process
wait, such as collector joining and cleanup. Exact integer zero is required for
all internally observed results. UTC timestamps are correlation metadata;
monotonic time decides deadline compliance.

## Close and uncertain completion

`close(deadline_monotonic=...)` immediately prevents another admission, then
waits for an admitted synchronous call only until the earlier of the caller's
absolute deadline and the total invocation deadline. The shortest close deadline
is retained in shared state and checked before accepting a return. A close timeout
latches `UNKNOWN`; it does not claim cancellation. Late exit zero cannot clear it.
A valid sequence completed before close remains valid.

`require_completion()` accepts no caller-supplied result. After complete valid
membership, it publishes captured timestamps and bytes once and independently
checks their binding and readback. It copies no stdout, stderr, profile contents
or redaction strings. Concurrent or repeated calls use the same immutable identity.

After a publication exception, another `require_completion()` performs read-only
reconciliation of that exact original. It never publishes again or reruns dbt.
If publication cannot be proven, completion remains unavailable. Do not release
capacity or redispatch from that uncertainty.

## Validation and integration boundary

Focused tests cover exact wire fields, every pair of incompatible invocation
kinds, faulty provider bytes, once-only slots, concurrent publication, lost ACK,
root replacement, symlinks and deterministic close/run barriers. These synthetic
ports test the recorder contract; they do not certify an actual dbt/SQL route.

Required next integration includes authenticated application bootstrap, actual
runtime qualification, retained root ownership, the build bridge, durable SQL
writer closure and authenticated completion/freeze. Final quality and export
remain separate steps. Until those paths and live evidence exist, this component
must not be described as a complete native BUILD route or installed self-service
workflow.
