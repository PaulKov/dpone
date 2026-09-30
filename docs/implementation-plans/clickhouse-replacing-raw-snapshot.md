# ClickHouse Replacing Raw Snapshot Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Add an opt-in, exact query-visible non-`FINAL` snapshot for `ReplacingMergeTree` while preserving every legacy native-route byte and recovery invariant.

**Architecture:** A small contract/policy module owns canonical profile, EOF, marker, and manifest parsing. The ClickHouse source acquires and revalidates the profile and emits bounded provenance; the existing MSSQL runtime binds it through `NativeChunkPlan.source_query_id` and `completion_metadata`, without a new recovery-plan version. The integrator owns public schemas, docs, certification, and shared files.

**Tech Stack:** Python 3.12, frozen dataclasses, canonical SHA-256 JSON, ClickHouse native driver, existing MSSQL native journal/runtime, pytest, Ruff, mypy.

**Spec:** `docs/feature-specs/clickhouse-replacing-raw-snapshot.md`

## Global Constraints

- Status is APPROVED; implement exactly `source_snapshot.mode: exact_raw_rows` and `replica_scope: single_server|connected_replica`.
- Omission retains plain `MergeTree`, BCP defaults, recovery-plan v1/v2, `NativeVerificationIdentityV2`, completion metadata, and evidence bytes.
- Raw mode uses one SELECT with `final=0`, `use_query_cache=0`, `apply_patch_parts=1`, `apply_deleted_mask=1`, `apply_mutations_on_fly=0`, and no source writes or `FINAL`.
- Unknown custody remains held; post-EOF recovery is source-free; validation precedes runtime bindings and target connections.
- Shared schemas, registries, docs navigation, changelog, workflows, and common fixtures belong only to the root integrator.

## Review Focus

- A patch committed after metadata preflight is visible or the run fails; `test_patch_commit_between_preflight_and_select_is_not_silently_ignored` owns this.
- Business `_part`/`_part_offset` columns fail before SELECT; `test_raw_mode_rejects_virtual_provenance_name_shadowing` owns this.
- A raw pre-EOF marker without EOF metadata still permits custody settlement; `test_pre_eof_raw_marker_keeps_retire_and_reconcile_available` owns this.
- A sealed EOF marker with missing/tampered extension fails before target connection; `test_raw_recovery_validates_extension_before_target_binding` owns this.
- Equal endpoints never claim absence of ABA; `test_equal_physical_endpoints_report_only_endpoint_authority_agreement` owns this.

---

### Task 1: Canonical contracts and manifest policy

**Files:** Create `src/dpone/contracts/clickhouse_raw_snapshot.py`, `src/dpone/manifest/clickhouse_raw_snapshot_policy.py`, `tests/test_clickhouse_raw_snapshot_contracts.py`, and `tests/test_clickhouse_raw_snapshot_policy.py`.

**Interfaces:**
- Produces `ClickHouseSourceSnapshotPolicy(mode: Literal["query_visible", "exact_raw_rows"], replica_scope: Literal["single_server", "connected_replica"] | None)` and `native_source_snapshot_policy(config: Any) -> ClickHouseSourceSnapshotPolicy`.
- Produces frozen `ClickHouseRawSnapshotProfileV1` with fields `engine_signature`, `relation_uuid`, `database_engine`, `ordered_schema: tuple[tuple[str, str, str], ...]`, `partition_key_sha256`, `sorting_key_sha256`, `primary_key_sha256`, `window: tuple[str, str, str] | None`, `read_settings: tuple[tuple[str, str | int], ...]`, `server_revision`, `endpoint_authority_sha256`, `replica_scope`, `replica_identity_sha256: str | None`, `principal_authority_sha256`, `row_policy_sha256`, `policy_filtered`, `query_shape_sha256`, `projection_sha256`, and `typed_parameters_sha256`.
- Produces frozen `ClickHouseRawSourceEofV1(vendor_query_id_sha256: str, rows: int, touched_part_coverage_sha256: str, before_physical_profile_sha256: str, after_physical_profile_sha256: str, endpoint_authority_agreement: bool)`; both models provide `document()`, strict `from_document(value: object)`, and `sha256`.
- Produces `raw_source_query_binding(legacy_source_binding_sha256: str, profile: ClickHouseRawSnapshotProfileV1) -> str`, `raw_source_query_binding_version(value: str) -> int | None`, `raw_snapshot_extension(binding: str, profile: ClickHouseRawSnapshotProfileV1, eof: ClickHouseRawSourceEofV1) -> dict[str, object]`, and `restore_raw_snapshot_extension(value: object, *, binding: str, completed_rows: int) -> tuple[ClickHouseRawSnapshotProfileV1, ClickHouseRawSourceEofV1]`.

- [x] Write failing tests for closed selector fields, omission, exact constants, canonical round trips/digests, lowercase 64-hex validation, marker domain separation, unknown marker version, bounded extension preimages, row parity, and extra/missing/tampered fields.
- [x] Run `uv run pytest tests/test_clickhouse_raw_snapshot_contracts.py tests/test_clickhouse_raw_snapshot_policy.py -q`; require RED for missing interfaces.
- [x] Implement only these pure contracts and parser; use existing canonical JSON helpers and stable error codes from the spec.
- [x] Re-run focused tests, Ruff on owned files, mypy on owned production modules, import rules, layer metrics, and module-size gate; require PASS.
- [x] Freeze exported signatures and file hashes for Tasks 2 and 3; commit only owned paths.

### Task 2: ClickHouse admission, one-query extraction, and provenance

**Files:** Modify `src/dpone/runtime/sources/clickhouse_native_source.py`, `src/dpone/runtime/sources/clickhouse_native_guard.py`, and `tests/test_clickhouse_native_source.py`; create `src/dpone/runtime/sources/clickhouse_raw_snapshot.py` and `tests/test_clickhouse_raw_snapshot.py`.

**Interfaces:**
- Consumes all Task 1 types/functions.
- Adds `ClickHouseNativeSource.snapshot_profile(config: Any) -> ClickHouseRawSnapshotProfileV1 | None` and extends `extract(config: Any, *, query_id: str | None = None, expected_profile: ClickHouseRawSnapshotProfileV1 | None = None, source_query_binding: str | None = None) -> ExtractResult`.
- Exact-raw artifacts expose `raw_snapshot_profile: ClickHouseRawSnapshotProfileV1`, `raw_snapshot_eof: ClickHouseRawSourceEofV1` after EOF, and `source_query_binding: str`; legacy artifacts preserve current attributes and behavior.

- [x] Write RED tests for exact engine parsing, replica-scope combinations, schema/default/type gates, reserved virtual names, effective settings, policy/role/profile drift, hidden provenance stripping, duplicates/nulls/empty windows, and legacy byte/query behavior.
- [x] Add deterministic barrier tests for insert/merge/update/patch/delete-mask changes between preflight, query acquisition, and EOF; assert query-visible patch/mask behavior and endpoint agreement only when checksum profiles match.
- [x] Implement profile acquisition before business rows, exact query rendering from bound shape/projection/typed params, one native SELECT, bounded physical profiles, and cleanup/cancellation without source DDL or fallback.
- [x] Run `uv run pytest tests/test_clickhouse_native_source.py tests/test_clickhouse_raw_snapshot.py -q`, then Ruff, mypy, import/layer/module-size gates; require PASS.
- [x] Record live route certification as N/A for this writer because Task 4 owns it; commit only owned paths.

### Task 3: Runtime identity, EOF binding, and source-free recovery

**Files:** Modify `src/dpone/runtime/mssql_native_application_assembly.py`, `src/dpone/runtime/sinks/mssql_native_completed_payload.py`, `src/dpone/runtime/sinks/mssql_native_prepare.py`, `src/dpone/app/mssql_native_recovery_application.py`, `tests/test_mssql_native_application.py`, and `tests/test_mssql_native_recovery_inspection.py`; create `tests/test_mssql_native_raw_snapshot_recovery.py`.

**Interfaces:**
- Consumes Task 1 contracts and Task 2 source signatures/artifact attributes.
- `_chunk_plan(..., *, source_query_id: str | None = None) -> NativeChunkPlan` retains its legacy derivation when omitted; exact mode passes the versioned marker derived from the frozen pre-plan profile.
- `completion_metadata()` adds `source_snapshot_v1` only for exact raw EOF; recovery validation consumes marker, extension, phase/action, schema/window, and completed rows before target binding.
- Adds `require_raw_snapshot_row_count(payload: Any, complete: NativeStageComplete) -> None` and `validate_raw_snapshot_recovery(config: Any, *, identity: NativeVerificationIdentityV2, projection: Mapping[str, Any], action: str) -> None` in `mssql_native_completed_payload.py`.

- [x] Write RED golden tests proving omitted-mode identity/recovery-plan/completion bytes are unchanged and no recovery-plan v3 exists.
- [x] Write RED tests for profile-before-identity ordering, atomic bounded EOF extension, post-stage source/count parity, existing verified-sum workload digest, and missing/extra/unknown/tampered selector-marker-extension combinations.
- [x] Write RED recovery tests proving pre-EOF custody settlement stays available, sealed EOF resume rejects before `ensure_runtime_bindings`/target connection, and valid recovery performs zero source connections.
- [x] Implement marker injection, EOF metadata production, post-complete count check, and early recovery validation without changing journal serialization or publication ordering.
- [x] Run the three owned test modules plus `tests/test_mssql_native_persisted_recovery.py`, then Ruff, mypy, import/layer/module-size gates; require PASS and commit only owned paths.

### Task 4: Integrator schema, UX, documentation, and certification

**Files:** Root integrator applies the T4 contract after Tasks 1–3 are frozen and reviewed.

**Interfaces:** Consumes final Task 1 schema/constants and Tasks 2–3 behavior; no delegated writer changes shared semantic files.

- [x] Add RED schema/example/docs contract tests for closed `source_snapshot`, omission compatibility, credential-free plan/doctor, and connection/live-only observations.
- [x] Integrate schema, generated references, route matrix, ADR, user guide, runbook, errors, example, changelog, and navigation with runnable discovery→run→evidence→recovery CJM.
- [ ] Run narrow/wide BCP and SqlClient synthetic route/recovery matrices, including pinned replica/TLS when available; record exact-commit evidence and truthful SKIP/UNVERIFIED cells.
- [ ] Run change-aware checks, full non-live suite, docs strict build, packaging as applicable, and fresh independent correctness/recovery/data-loss review.
- [ ] Reconcile findings, update spec status/evidence only from producers, and prepare the single integration commit/PR.
