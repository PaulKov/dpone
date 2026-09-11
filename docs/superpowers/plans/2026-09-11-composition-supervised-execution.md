# Composition Supervised Execution Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Execute every admitted v3 composition workload through a protected Kubernetes supervisor, with exact parent fencing, issued credentials, durable evidence, and no native-v2 or generic fallback.

**Architecture:** The build plane seals an administrator-provisioned supervisor capability into the deployment projection. The provider translates that capability into a least-privilege root-supervisor pod, while the verified launcher derives v3 dispatch only from authenticated release bytes. Canonical app roots compose native dbt, PostgreSQL-to-MSSQL, and MSSQL-to-ClickHouse workers around the existing parent attempt, gate, transaction, capture, and evidence adapters.

**Tech Stack:** Python 3.12, argparse, KubernetesPodOperator pod dictionaries/models, SQL Server control ledger, PostgreSQL, ClickHouse HTTP, dbt 1.12, pytest, Ruff, mypy.

## Global Constraints

- Native-v2 and non-composition releases retain their existing command, identity, retry, and evidence behavior.
- V3 never falls back to native-v2 workspace admission or generic `dpone run`.
- The supervisor PVC is ReadWriteMany and retains immutable attempt/identity tombstones across pod retries.
- The supervisor container runs as UID 0 with a read-only root filesystem, `allowPrivilegeEscalation=false`, `seccompProfile=RuntimeDefault`, and only `CHOWN`, `FOWNER`, `DAC_READ_SEARCH`, `SETUID`, `SETGID`, and `KILL`.
- Child UID/GID ranges contain at least 1,000,000 identities, stay below `2^31`, and are reserved outside image/platform accounts.
- Missing PVC, root identity, capability, tmpfs, complete `/proc` visibility, authority, enrollment, or evidence is a rejection, never a degraded execution.
- A skipped or unavailable live check is `SKIP` or `UNVERIFIED`, never `PASS`.
- No credentials enter arguments, files, evidence, logs, XCom, or committed fixtures.

---

### Task 1: Seal the supervisor deployment capability

**Files:**
- Create: `src/dpone/contracts/composition_supervisor.py`
- Modify: `src/dpone/readiness/airflow_deployment_projection.py`
- Modify: `src/dpone/commands/airflow_deployment_build_cmd.py`
- Modify: `src/dpone/runtime/deployment_cache_projection_validator.py`
- Modify: `packages/dpone-airflow-pack/src/dpone_airflow_pack/deployment_index_contract.py`
- Test: `tests/test_composition_supervisor_contract.py`
- Test: `tests/test_airflow_self_service_cli.py`

**Interfaces:**
- Consumes: schema-valid `dpone.release-set.v3`.
- Produces: `CompositionSupervisorProjection.from_mapping(value)` and its canonical `to_dict()` projection.

- [ ] **Step 1: Write failing contract tests**

```python
def test_v3_requires_complete_supervisor_projection():
    value = CompositionSupervisorProjection.from_mapping(
        {
            "schema": "dpone.composition-supervisor.v1",
            "persistent_volume_claim": "dpone-composition-supervisor",
            "child_uid_start": 1_000_000_000,
            "child_gid_start": 1_000_000_000,
            "child_identity_count": 1_000_000,
        }
    )
    assert value.child_uid_stop == 1_001_000_000


@pytest.mark.parametrize("field", ["persistent_volume_claim", "child_uid_start", "child_gid_start"])
def test_v3_rejects_missing_supervisor_field(field):
    payload = valid_supervisor_payload()
    payload.pop(field)
    with pytest.raises(ValueError, match="composition_supervisor"):
        CompositionSupervisorProjection.from_mapping(payload)
```

- [ ] **Step 2: Run the tests and confirm the missing contract**

Run: `uv run pytest tests/test_composition_supervisor_contract.py -q`

Expected: FAIL because `dpone.contracts.composition_supervisor` does not exist.

- [ ] **Step 3: Implement the immutable contract and projection validation**

```python
@dataclass(frozen=True, slots=True)
class CompositionSupervisorProjection:
    persistent_volume_claim: str
    child_uid_start: int
    child_gid_start: int
    child_identity_count: int
    schema: str = "dpone.composition-supervisor.v1"

    @classmethod
    def from_mapping(cls, value: Mapping[str, object]) -> "CompositionSupervisorProjection":
        if set(value) != {
            "schema", "persistent_volume_claim", "child_uid_start",
            "child_gid_start", "child_identity_count",
        }:
            raise ValueError("composition_supervisor_shape")
        result = cls(**value)
        result.require_valid()
        return result

    def require_valid(self) -> None:
        if self.schema != "dpone.composition-supervisor.v1":
            raise ValueError("composition_supervisor_schema")
        require_kubernetes_dns_label(self.persistent_volume_claim)
        if type(self.child_identity_count) is not int or self.child_identity_count < 1_000_000:
            raise ValueError("composition_supervisor_identity_range")
        for start in (self.child_uid_start, self.child_gid_start):
            if type(start) is not int or start < 1_000_000 or start + self.child_identity_count >= 2**31:
                raise ValueError("composition_supervisor_identity_range")
```

Add `--composition-supervisor-pvc`, `--composition-child-uid-start`,
`--composition-child-gid-start`, and `--composition-child-identity-count` as
an all-or-none CLI group. Require the object for v3 projection and forbid it
for v1/v2.

- [ ] **Step 4: Verify contract, projection, and CLI behavior**

Run: `uv run pytest tests/test_composition_supervisor_contract.py tests/test_airflow_self_service_cli.py tests/test_gitops_schema_contracts.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dpone/contracts/composition_supervisor.py src/dpone/readiness/airflow_deployment_projection.py src/dpone/commands/airflow_deployment_build_cmd.py src/dpone/runtime/deployment_cache_projection_validator.py packages/dpone-airflow-pack/src/dpone_airflow_pack/deployment_index_contract.py tests/test_composition_supervisor_contract.py tests/test_airflow_self_service_cli.py
git commit -m "feat: seal composition supervisor capability"
```

### Task 2: Materialize the least-privilege supervisor pod

**Files:**
- Modify: `packages/dpone-airflow-pack/src/dpone_airflow_pack/init_fetch_contract.py`
- Modify: `packages/dpone-airflow-pack/src/dpone_airflow_pack/init_fetch_pod.py`
- Modify: `packages/dpone-airflow-pack/src/dpone_airflow_pack/run_identity.py`
- Test: `tests/test_airflow_init_fetch_pod.py`
- Test: `tests/test_airflow_pack_core_contract_parity.py`

**Interfaces:**
- Consumes: `composition_supervisor` from the verified deployment index.
- Produces: fixed PVC/tmpfs mounts and `DPONE_COMPOSITION_SUPERVISOR_B64` containing only the canonical non-secret contract.

- [ ] **Step 1: Write failing pod assertions**

```python
def test_v3_runtime_pod_has_exact_supervisor_boundary():
    pod = compose_v3_runtime_pod(supervisor_projection())
    base = pod["spec"]["containers"][0]
    assert base["securityContext"] == {
        "runAsUser": 0,
        "runAsGroup": 0,
        "allowPrivilegeEscalation": False,
        "readOnlyRootFilesystem": True,
        "seccompProfile": {"type": "RuntimeDefault"},
        "capabilities": {
            "drop": ["ALL"],
            "add": ["CHOWN", "FOWNER", "DAC_READ_SEARCH", "SETUID", "SETGID", "KILL"],
        },
    }
    assert volume(pod, "dpone-composition-supervisor") == {
        "name": "dpone-composition-supervisor",
        "persistentVolumeClaim": {"claimName": "dpone-composition-supervisor"},
    }
    assert volume(pod, "dpone-composition-profiles")["emptyDir"]["medium"] == "Memory"
```

- [ ] **Step 2: Run the focused provider tests**

Run: `uv run pytest tests/test_airflow_init_fetch_pod.py -q`

Expected: FAIL because v3 pods still use the ordinary `emptyDir` execution contract.

- [ ] **Step 3: Add v3-only pod composition**

Implement `apply_composition_supervisor(pod, projection)` so it:

```python
base["securityContext"] = composition_supervisor_security_context()
base["volumeMounts"].extend(
    [
        {"name": "dpone-composition-supervisor", "mountPath": "/var/lib/dpone/composition"},
        {"name": "dpone-composition-profiles", "mountPath": "/dev/shm/dpone-composition"},
    ]
)
pod["spec"]["volumes"].extend(
    [
        {"name": "dpone-composition-supervisor", "persistentVolumeClaim": {"claimName": projection.persistent_volume_claim}},
        {"name": "dpone-composition-profiles", "emptyDir": {"medium": "Memory", "sizeLimit": "64Mi"}},
    ]
)
```

Reject collisions with existing volume, mount, environment, container-security,
or pod-security fields. Do not apply these changes to v1/v2.

- [ ] **Step 4: Verify provider compatibility**

Run: `uv run pytest tests/test_airflow_init_fetch_pod.py tests/test_airflow_pack_core_contract_parity.py tests/test_airflow_resources_strict_delivery.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add packages/dpone-airflow-pack/src/dpone_airflow_pack/init_fetch_contract.py packages/dpone-airflow-pack/src/dpone_airflow_pack/init_fetch_pod.py packages/dpone-airflow-pack/src/dpone_airflow_pack/run_identity.py tests/test_airflow_init_fetch_pod.py tests/test_airflow_pack_core_contract_parity.py
git commit -m "feat: materialize protected composition supervisor pods"
```

### Task 3: Allocate persistent child identities

**Files:**
- Modify: `src/dpone/adapters/composition_dbt_process_boundary.py`
- Create: `src/dpone/adapters/composition_child_identity_allocator.py`
- Test: `tests/test_composition_dbt_process_boundary.py`
- Test: `tests/test_composition_child_identity_allocator.py`

**Interfaces:**
- Consumes: attempt SHA, reserved UID/GID ranges, root-owned PVC.
- Produces: `CompositionChildIdentity(uid: int, gid: int)` and immutable canonical tombstones.

- [ ] **Step 1: Write collision, replay, and durability tests**

```python
def test_allocator_probes_collision_without_reusing_foreign_identity(tmp_path):
    allocator = CompositionChildIdentityAllocator(tmp_path, uid_start=1_000_000_000, gid_start=1_000_000_000, count=1_000_000)
    first = allocator.allocate(attempt("sha256:" + "1" * 64))
    second = allocator.allocate(attempt("sha256:" + "2" * 64))
    assert first != second
    assert allocator.read(first.uid).attempt_sha256.endswith("1" * 64)


def test_allocator_rejects_same_attempt_replay(tmp_path):
    allocator = allocator_for(tmp_path)
    allocator.allocate(attempt())
    with pytest.raises(DbtCaptureError, match="child_identity_replay"):
        allocator.allocate(attempt())
```

- [ ] **Step 2: Confirm tests fail**

Run: `uv run pytest tests/test_composition_child_identity_allocator.py -q`

Expected: FAIL because the allocator does not exist.

- [ ] **Step 3: Implement locked, fsynced allocation**

Use `fcntl.flock(LOCK_EX)`, no-follow directory descriptors, canonical JSON,
`O_CREAT|O_EXCL`, and directory `fsync`. Derive candidate offsets with
`sha256(b"uid\\0" + attempt_digest)` and `sha256(b"gid\\0" + attempt_digest)`,
then probe at most 4096 slots. Persist both
`attempts/<attempt_sha256>.json` and `identities/<uid>-<gid>.json`; accept
neither replay nor partial/conflicting records.

- [ ] **Step 4: Inject the allocator into the process boundary**

Replace constructor-supplied child UID/GID with:

```python
identity = self._identities.allocate(attempt)
return self._allocate(attempt, identity=identity, runtime_attempt_id=runtime_attempt_id, target_path=target_path)
```

Keep all existing root, tmpfs, `/proc`, no-follow, owner, and mode checks.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/test_composition_child_identity_allocator.py tests/test_composition_dbt_process_boundary.py -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/dpone/adapters/composition_child_identity_allocator.py src/dpone/adapters/composition_dbt_process_boundary.py tests/test_composition_child_identity_allocator.py tests/test_composition_dbt_process_boundary.py
git commit -m "feat: allocate persistent composition child identities"
```

### Task 4: Select v3 only from verified release authority

**Files:**
- Modify: `src/dpone/runtime/verified_pack_launcher.py`
- Create: `src/dpone/runtime/composition_verified_dispatch.py`
- Modify: `src/dpone/runtime/verified_pack_execution.py`
- Test: `tests/test_release_composition_delivery.py`
- Test: `tests/test_verified_pack_execution.py`

**Interfaces:**
- Consumes: authenticated release bytes and existing `VerifiedPackCommand`.
- Produces: `DPONE_RUNTIME_RELEASE_ADMISSION=dpone.release-composition-admission.v1` and a typed v3 dispatcher.

- [ ] **Step 1: Retain the existing failing-then-passing release projection test**

```python
if original["schema"] == "dpone.release-set.v3":
    assert command.env[RUNTIME_RELEASE_ADMISSION_ENV] == COMPOSITION_ADMISSION
else:
    assert RUNTIME_RELEASE_ADMISSION_ENV not in command.env
```

- [ ] **Step 2: Add fallback-denial tests**

```python
def test_v3_never_starts_generic_or_native_child(monkeypatch, command):
    command = replace(command, env={RUNTIME_RELEASE_ADMISSION_ENV: COMPOSITION_ADMISSION})
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: pytest.fail("generic child started"))
    assert execute_verified_pack_command(command, composition_dispatcher=rejecting_dispatcher()) == 5
```

- [ ] **Step 3: Implement typed dispatch**

`execute_verified_pack_command()` must call `CompositionVerifiedDispatcher.run`
before `_run_pack_subprocess` when and only when the marker equals
`COMPOSITION_ADMISSION`. Unknown non-empty markers reject. The dispatcher
selects native dbt or ordinary transfer from the verified argv prefix and has no
shell/generic fallback.

- [ ] **Step 4: Verify release compatibility**

Run: `uv run pytest tests/test_release_composition_delivery.py tests/test_dbt_versioned_runtime_launcher.py tests/test_verified_pack_execution.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dpone/runtime/verified_pack_launcher.py src/dpone/runtime/composition_verified_dispatch.py src/dpone/runtime/verified_pack_execution.py tests/test_release_composition_delivery.py tests/test_verified_pack_execution.py
git commit -m "feat: dispatch verified v3 runtime without fallback"
```

### Task 5: Complete the native dbt composition worker

**Files:**
- Create: `src/dpone/app/composition_dbt_execution.py`
- Create: `src/dpone/services/composition_dbt_capture_authority.py`
- Create: `src/dpone/adapters/composition_mssql_dbt_outcome.py`
- Modify: `src/dpone/services/composition_dbt_outcome.py`
- Modify: `src/dpone/runtime/dbt_execution_bootstrap.py`
- Test: `tests/test_composition_dbt_execution_root.py`
- Test: `tests/test_composition_dbt_outcome.py`
- Test: `tests/test_composition_dbt_outcome_proof.py`

**Interfaces:**
- Consumes: active occurrence, execution pack, run/deployment identity, Airflow attempt, runtime resolver, supervisor allocation.
- Produces: `execute_composition_dbt_pack(...) -> DbtExecutionOutcome` with a durable terminal parent receipt.

- [ ] **Step 1: Add an offline full-sequence test**

```python
def test_native_root_orders_all_authority_boundaries(authority):
    outcome = authority.root.execute(authority.request)
    assert outcome.passed
    assert authority.events == [
        "read_active", "admit_attempt", "allocate_identity", "issue_login",
        "preflight", "register_dispatch", "run_child", "capture_originals",
        "close_gate", "prove_quiescence", "observe_materialization",
        "persist_outcome", "finalize_attempt",
    ]
```

- [ ] **Step 2: Confirm no production root exists**

Run: `uv run pytest tests/test_composition_dbt_execution_root.py -q`

Expected: FAIL because `dpone.app.composition_dbt_execution` does not exist.

- [ ] **Step 3: Implement source-derived capture authority**

`CompositionDbtCaptureAuthority` must derive `DbtDispatchIntent`,
`DbtOutcomeExpectation`, and `DbtMaterializationContract` from the verified
pack, preflight manifest, run identity, allocated paths, inspected toolchain,
issued SID, and exact commands. It must re-read and hash the root-owned
preflight manifest for every callback. No caller may supply intent, expected
status, materialization, or evidence digest.

- [ ] **Step 4: Assemble the worker**

Compose `MssqlCompositionAttemptStore`, `MssqlCompositionLoginGate`,
`CompositionDbtAttemptLifecycle`, `LinuxDbtProcessBoundary`,
`IssuedDbtProfileRenderer`, `ProtectedDbtCommandRunner`,
`MssqlDbtCaptureStore`, `ProtectedDbtCapture`,
`MssqlDbtMaterializationObserver`, `CompositionDbtOutcomeObserver`,
`CompositionDbtOutcomeProofProducer`, and `CompositionWorker`.
Call `capture.capture(attempt)` only after the execution service writes final
evidence. Never construct native-v2 workspace attempt dependencies for v3.

- [ ] **Step 5: Verify mismatch and unknown boundaries**

Run: `uv run pytest tests/test_composition_dbt_execution_root.py tests/test_composition_dbt_outcome.py tests/test_composition_dbt_outcome_proof.py tests/test_dbt_execution_runtime.py -q`

Expected: PASS, including lost dispatch ACK, child failure, missing run-results,
materialization mismatch, gate closure failure, and OUTCOME commit ambiguity.

- [ ] **Step 6: Commit**

```bash
git add src/dpone/app/composition_dbt_execution.py src/dpone/services/composition_dbt_capture_authority.py src/dpone/adapters/composition_mssql_dbt_outcome.py src/dpone/services/composition_dbt_outcome.py src/dpone/runtime/dbt_execution_bootstrap.py tests/test_composition_dbt_execution_root.py tests/test_composition_dbt_outcome.py tests/test_composition_dbt_outcome_proof.py
git commit -m "feat: execute native dbt through parent worker"
```

### Task 6: Complete PostgreSQL-to-MSSQL ordinary execution

**Files:**
- Create: `src/dpone/services/composition_transfer_attempt.py`
- Create: `src/dpone/app/composition_transfer_execution.py`
- Create: `src/dpone/adapters/composition_mssql_transfer_outcome.py`
- Modify: `src/dpone/runtime/bootstrap_runner.py`
- Modify: `src/dpone/runtime/bootstrap_hydrator.py`
- Modify: `src/dpone/runtime/etl/mssql_transaction_admission.py`
- Modify: `src/dpone/runtime/etl/mssql_transaction_finalizer.py`
- Test: `tests/test_composition_transfer_execution_root.py`
- Test: `tests/test_mssql_generic_transaction_governance.py`

**Interfaces:**
- Consumes: verified ordinary manifest/selector, active parent, issued MSSQL login, read-only PostgreSQL source.
- Produces: parent-fenced `full_refresh` and durable OUTCOME proof.

- [ ] **Step 1: Write execution-root and replay tests**

```python
def test_transfer_root_issues_sink_and_fences_actual_transaction(runtime):
    result = runtime.execute()
    assert result.rows_written == 3
    assert runtime.sink_credentials.resolver == "composition-issued-login"
    assert runtime.events.index("target_fence_before_mutation") < runtime.events.index("target_dml")
    assert runtime.events.index("target_fence_before_receipt") < runtime.events.index("receipt_insert")


def test_running_replay_never_reads_postgres(runtime):
    runtime.attempts.return_running_replay = True
    with pytest.raises(CompositionAdmissionError, match="attempt_replay"):
        runtime.execute()
    assert runtime.source_reads == 0
```

- [ ] **Step 2: Confirm the missing DI seams**

Run: `uv run pytest tests/test_composition_transfer_execution_root.py -q`

Expected: FAIL because the transfer root and invocation-scoped overrides do not exist.

- [ ] **Step 3: Add invocation-scoped runtime dependencies**

Add optional typed dependencies to `DefaultProcessRunner.run()` and
`DefaultRuntimeHydrator.build()` for resolved sink/state overlays,
`MssqlTransactionAdmissionService`, and the existing
`MssqlCompositionTransactionFence`. Defaults must preserve all non-v3 behavior.
Register the exact generic operation and mutation-plan digest before target DML;
apply the fence to replay as well as fresh execution.

- [ ] **Step 4: Implement independent transfer OUTCOME**

Observe the committed generic receipt and actual target/state identity on a
fresh connection. Persist `SUCCEEDED` only for exact receipt and row/content
evidence; persist `FAILED` only with protected rollback/no-mutation proof;
otherwise persist `COMMIT_UNKNOWN`.

- [ ] **Step 5: Verify**

Run: `uv run pytest tests/test_composition_transfer_execution_root.py tests/test_mssql_generic_transaction_governance.py tests/test_composition_mssql_transaction_binding.py -q`

Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add src/dpone/services/composition_transfer_attempt.py src/dpone/app/composition_transfer_execution.py src/dpone/adapters/composition_mssql_transfer_outcome.py src/dpone/runtime/bootstrap_runner.py src/dpone/runtime/bootstrap_hydrator.py src/dpone/runtime/etl/mssql_transaction_admission.py src/dpone/runtime/etl/mssql_transaction_finalizer.py tests/test_composition_transfer_execution_root.py tests/test_mssql_generic_transaction_governance.py
git commit -m "feat: fence composed postgres mssql execution"
```

### Task 7: Complete MSSQL-to-ClickHouse execution

**Files:**
- Create: `src/dpone/app/composition_clickhouse_execution.py`
- Modify: `src/dpone/adapters/composition_clickhouse_gate.py`
- Modify: `src/dpone/adapters/composition_clickhouse_supervisor_enrollment.py`
- Modify: `src/dpone/runtime/composition_snapshot.py`
- Test: `tests/test_composition_clickhouse_execution_root.py`
- Test: `tests/test_composition_clickhouse_gate.py`

**Interfaces:**
- Consumes: active attempt, enrolled supervisor original, read-only MSSQL source, protected ClickHouse principal and dispatch policy.
- Produces: Atomic full-refresh snapshot publication, disappearance deletion, terminal proofs, and exact OUTCOME.

- [ ] **Step 1: Write full-refresh and failure-order tests**

```python
def test_clickhouse_root_publishes_atomic_snapshot_and_deletes_disappeared_rows(runtime):
    runtime.seed_target([(1, "old"), (2, "disappeared")])
    runtime.seed_source([(1, "new"), (3, None)])
    runtime.execute()
    assert runtime.target_rows() == [(1, "new"), (3, None)]
    assert runtime.events[-4:] == ["close_dispatch", "close_principal", "outcome_proof", "attempt_terminal"]
```

- [ ] **Step 2: Confirm no app root exists**

Run: `uv run pytest tests/test_composition_clickhouse_execution_root.py -q`

Expected: FAIL because the ClickHouse execution root does not exist.

- [ ] **Step 3: Compose the existing gate, dispatcher, HTTP transport, and snapshot service**

Require the exact supervisor enrollment original before principal creation,
before dispatch, and before publication. Bind every query ID and request body to
the attempt. Close dispatch admission before principal closure. Treat malformed
HTTP, partial response, unknown publication, missing reconciliation, or
supervisor drift as `COMMIT_UNKNOWN`.

- [ ] **Step 4: Verify**

Run: `uv run pytest tests/test_composition_clickhouse_execution_root.py tests/test_composition_clickhouse_gate.py tests/test_composition_clickhouse_http.py tests/test_composition_snapshot.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dpone/app/composition_clickhouse_execution.py src/dpone/adapters/composition_clickhouse_gate.py src/dpone/adapters/composition_clickhouse_supervisor_enrollment.py src/dpone/runtime/composition_snapshot.py tests/test_composition_clickhouse_execution_root.py tests/test_composition_clickhouse_gate.py
git commit -m "feat: execute protected ClickHouse snapshots"
```

### Task 8: Enable the public activation and runtime composition roots

**Files:**
- Modify: `src/dpone/app/composition_activation.py`
- Modify: `src/dpone/readiness/airflow_self_service_cache_sync.py`
- Modify: `src/dpone/readiness/airflow_desired_state_reconcile.py`
- Modify: `src/dpone/commands/airflow_cache_sync_cmd.py`
- Test: `tests/test_composition_activation_factory.py`
- Test: `tests/test_composition_cache_activation.py`

**Interfaces:**
- Consumes: all three installed execution roots and protected enrollment callbacks.
- Produces: the exact public factory from the approved spec and `--workspace-authority-connection-ref`.

- [ ] **Step 1: Update factory tests to require the complete cell set**

```python
assert coordinator.execution_cells == frozenset(
    {
        "sqlserver_dbt_v1",
        "postgres_mssql_full_refresh_v1",
        "mssql_clickhouse_full_refresh_v1",
    }
)
```

- [ ] **Step 2: Run and confirm fail-closed incomplete wiring**

Run: `uv run pytest tests/test_composition_activation_factory.py tests/test_composition_cache_activation.py -q`

Expected: FAIL until all roots are injected.

- [ ] **Step 3: Replace provisional capability declarations**

Expose only:

```python
def build_composition_activation_coordinator(
    *,
    cache_root: Path,
    authority_connection_ref: str,
    control_schema: str = "dpone_control",
) -> CompositionActivationCoordinator:
    ...
```

The concrete capability object must own callable factories for all three
execution cells, not just string names. Cache-sync and desired-state use this
same app root. Invalid authority input returns structured self-service errors.

- [ ] **Step 4: Verify public and compatibility behavior**

Run: `uv run pytest tests/test_composition_activation_factory.py tests/test_composition_cache_activation.py tests/test_airflow_cache_materializer.py tests/test_airflow_desired_state_authority.py -q`

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/dpone/app/composition_activation.py src/dpone/readiness/airflow_self_service_cache_sync.py src/dpone/readiness/airflow_desired_state_reconcile.py src/dpone/commands/airflow_cache_sync_cmd.py tests/test_composition_activation_factory.py tests/test_composition_cache_activation.py
git commit -m "feat: enable public composition execution"
```

### Task 9: Document provisioning, recovery, and self-service UX

**Files:**
- Modify: `docs/composition-activation-contract.md`
- Modify: `docs/airflow-cache-sync-promotion.md`
- Modify: `docs/airflow-pack-provider.md`
- Modify: `docs/cli-reference.md`
- Create: `docs/guides/composition-supervisor-kubernetes.md`
- Modify: `docs/compatibility.md`
- Modify: `CHANGELOG.md`

**Interfaces:**
- Consumes: exact shipped CLI/schema/runtime behavior.
- Produces: first-time setup, operation, diagnosis, retry, and retention guidance.

- [ ] **Step 1: Add documentation contract tests**

Require examples for supervisor PVC/ranges, namespace security prerequisites,
cache-sync authority, all three cells, `COMMIT_UNKNOWN` recovery, and explicit
`UNVERIFIED` live status.

- [ ] **Step 2: Write the user and operator journey**

Document administrator provisioning, data-engineer compose/build flow, operator
cache-sync/desired-state flow, DAG triggering, evidence inspection, blocked
retry diagnosis, and no-automatic-tombstone-deletion policy. Never describe
offline tests as route certification.

- [ ] **Step 3: Verify docs**

Run: `uv run dpone docs check-docs && uv run pytest tests/test_docs_language_contracts.py -q && uv run mkdocs build --strict`

Expected: PASS.

- [ ] **Step 4: Commit**

```bash
git add docs CHANGELOG.md
git commit -m "docs: add composition supervisor operations guide"
```

### Task 10: Validate, certify, review, and update the draft PR

**Files:**
- Generated evidence only through its producer under the documented test output paths.
- Update: GitHub draft PR 42 description after validation.

**Interfaces:**
- Consumes: complete branch diff against `origin/master`.
- Produces: change-aware check evidence, explicit live status, independent reviews, and truthful PR readiness.

- [ ] **Step 1: Run focused and change-aware validation**

```bash
uv run python tools/agent_policy/select_checks.py --base-ref origin/master
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
uv run pytest tests/test_architecture_fitness_gate.py -q
uv run pytest -m "not integration_live" -n auto --dist loadfile
```

Expected: PASS with average clustering at or below `0.182`; do not change the budget or baseline to pass.

- [ ] **Step 2: Run the approved synthetic campaign**

Run the repository-produced Linux x86-64 Kubernetes profile for native dbt,
PostgreSQL-to-MSSQL, and MSSQL-to-ClickHouse. Require semantic/typed assertions,
all supported strategies, target reconciliation, durable terminal evidence,
retry/replay, stale parent, reconnect denial, and `COMMIT_UNKNOWN` recovery.

Expected: PASS only when the exact environment completes. Missing cluster,
credentials, billing, PVC class, Docker/ClickHouse supervisor, or SQL service is
recorded as `SKIP`/`UNVERIFIED`.

- [ ] **Step 3: Run independent review gates in parallel**

Launch Bugbot, Security Review, and fresh-context code review against
`origin/master...HEAD`. Fix all Critical and Important findings, record Minor
follow-ups, then rerun Bugbot and semantic review after wire/type changes.

- [ ] **Step 4: Push and update PR 42**

```bash
git push origin codex/composition-execution-on-079
gh pr edit 42 --title "Complete protected composition execution on 0.79.0" --body-file /tmp/pr-42-body.md
```

Keep the PR draft unless all required offline checks and the complete three-cell
live campaign pass. Never call `SKIP`, `N/A`, or `UNVERIFIED` ready for release.
