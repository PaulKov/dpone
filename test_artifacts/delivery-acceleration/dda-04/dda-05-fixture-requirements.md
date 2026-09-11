# DDA-05 disposable fixture requirements

Status: fixture contract handoff; no services or credentials used by DDA-04.

1. Use an explicitly approved disposable SQL Server 2022 (major 16) instance,
   EngineEdition 2 or 3. Record exact server, driver and tool versions. The
   connection needs database `VIEW DEFINITION`, server `VIEW ANY DEFINITION`,
   `SELECT ON OBJECT::sys.sql_expression_dependencies` in the target database,
   table reads, ALTER/SWITCH rights, plus the existing finalizer's receipt/fence
   rights. The fixture owner provisions these permissions; `db_owner` membership
   is not required by the component. Never log credentials.
2. Provision a fixture database and identical three partitioned rowstore tables
   with a shared RANGE RIGHT function/scheme and two finite boundaries. Minimal
   physical shape: a heap with a non-null date key and an int business column,
   no constraints/dependencies/LOB features, three partitions on one filegroup.
   Also certify a datetime2(6) key and an aligned rowstore index case. Catalog
   field and profile details are in the component guide.
3. Resolve exact database/object identities, write each disposable table's
   `dpone.native_switch.owner.v1` extended property using the binding's role tag,
   and retain the durable invocation resource record. The fixture owner alone
   provisions/stamps/cleans resources; the component never does this.
4. Inject `NativeSwitchSql`/`NativeSwitchTransaction` on the same real connection.
   Resolve the existing exact receipt first. Assert current invocation,
   generation, mutation and target fence. Use one SERIALIZABLE transaction with
   XACT_ABORT ON. Supply verify_prepared(plan) to prove complete typed prepared content after
   executor locks are held; retain protection through both SWITCH statements. Do not manufacture authority with a Boolean field.
5. Seed deterministic typed rows before/inside/after the interval; duplicate
   rows; lower-bound and exact upper-bound rows; empty prepared and initially
   empty target cases. Compare exact typed multisets and metadata after commit.
   Count replaced rows independently; verify outside-window sentinels unchanged.
6. Inject first-SWITCH and second-SWITCH failures, including mutation followed by
   response loss. Confirm complete caller rollback from a separate session and
   verify original target/prepared/switch-out multisets. No fallback DML.
7. Exercise concurrent catalog/content drift before lock acquisition, external
   writes during held locks, stale ownership, nonempty/foreign switch-out,
   out-of-window and NULL prepared rows. Test unknown metadata visibility and
   unsupported table/index/dependency features. Ensure no partial publication.
8. Lose commit ACK after both switches and exact receipt insertion. Probe on a
   fresh connection; recover before touching emptied prepared content. Repeat
   with initially empty target, wrong/missing/unavailable receipt and source
   unavailable. Unknown outcome retains objects and blocks further mutation.
9. Observe confirmed target visibility on a separate session. Cleanup only
   after known outcome and retention/evidence requirements are satisfied.
10. Use integration_live and integration_mssql markers. Add integration_clickhouse
    only if real ClickHouse participates. For the frozen measurement schema use
    mode isolated_switch and execution live only for actual live observations.
    Include typed_content, duplicate_multiplicity, metadata_parity,
    commit_receipt_binding, outside_window_unchanged, empty_input, rollback,
    receipt_first_recovery and source_free_resume. A component PASS is not
    certification of the still-rejected public native SWITCH route.

DDA-04 has not supplied production provisioning, an authenticated transaction
bridge or live fixture execution. These requirements describe DDA-05's explicit
fixture responsibilities, and do not grant permission to use live environments.
