### JUnit evidence: `mssql-target-local-p1-primitives`

- Status: `PASS`
- Totals: `17 passed`, `0 failed`, `0 errors`, `0 skipped`

| test | status |
|---|---|
| `tests.integration.mssql.test_mssql_target_local_digest_live::test_fixture_descriptor_is_bound_to_generated_schema[narrow]` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_digest_live::test_fixture_descriptor_is_bound_to_generated_schema[wide100]` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_digest_live::test_wide_compiler_materializes_only_fixed_hash_state` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_digest_live::test_python_and_sql_target_digest_are_identical[narrow]` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_digest_live::test_python_and_sql_target_digest_are_identical[wide100]` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_digest_live::test_digest_rejects_count_overflow_and_detects_value_mutation` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_digest_live::test_digest_is_stable_across_repeated_barrier_observations` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_real_bcp_positive_terminal_precedes_target_authority` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_real_bcp_lost_ack_is_ambiguous_even_when_rows_arrived` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_real_bcp_failure_never_becomes_positive_terminal` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_real_bcp_cleanup_failure_retains_ambiguity_after_rows_arrive` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_real_bcp_vendor_count_mismatch_cannot_create_authority` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_changed_sealed_file_is_rejected_before_writer_launch` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_real_bcp_lock_timeout_is_ambiguous_and_cannot_release_custody` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_stable_custody_blocks_v1_and_overlap_after_lease_expiry` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_empty_invocation_uses_distinct_custody_release_reason` | `passed` |
| `tests.integration.mssql.test_mssql_target_local_bcp_lifecycle_live::test_empty_executor_completes_without_bcp_or_target_stage` | `passed` |
### JUnit evidence: `mssql-target-local-p1-route`

- Status: `PASS`
- Totals: `1 passed`, `0 failed`, `0 errors`, `0 skipped`

| test | status |
|---|---|
| `tests.integration.mssql.test_clickhouse_mssql_target_local_route_live::test_clickhouse_rows_use_aggregate_only_verification_and_atomic_publication` | `passed` |
