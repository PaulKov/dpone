# B02 approval appendix: recommended ADR and exact writer contract

Status: **PROPOSED — no implementation authority**. This appendix is part of the
[RESEARCHED B02 specification](b02-codec-design-draft.md). Approval concerns the
explicit stage API only; release and live authorization remain separate.

## Recommended ADR

Proposed path: `docs/adr/0064-validated-file-clickhouse-staging.md`.
Title: **Explicit validated character-file staging in ClickHouse**.
Proposed decision status: Accepted only together with maintainer specification
approval. The observed integration candidate ends at ADR0063; if another accepted
ADR reserves0064 first, the coordinator explicitly updates this administrative path.

Context: a source receipt binds a logical source validation to a character wire.
Raw forwarding into a different consumer can preserve row counts while changing
values. The ordinary runtime can evolve schema before generic staging begins,
so it cannot inherit this feature's preparation-before-mutation guarantee.

Accept these decisions as one v1 contract:

1. Add only `ClickHouseSink.stage_validated_file(..., policy=...)`; do not add
   manifest/CLI dispatch, a generic sink capability, or a runtime pre-schema hook.
2. Keep source authority and the original receipt on the original artifact. A
   typed busy-guarded attempt owns fresh verification and latest-attempt summary;
   a prepared RowBinary resource is derived evidence, never a replacement receipt.
3. Share the existing default BulkTextCodec logical reader with the producer,
   preserving its grammar. Fully prepare and verify within explicit finite limits
   before this method's first target mutation. Admit only the specified finite
   text/binary/integer/bit/decimal matrix; binary `none` means raw bytes.
4. Permit only explicitly configured controlled client/HTTP RowBinary on one
   node. Inject the runtime runner factory at sink composition; require bounded
   I/O, complete acknowledgment, exact bytes and one observed stage count.
5. Own a fresh local staging table and separate CREATE/INSERT/DROP query IDs.
   Persist immutable intent before each mutation. A staged handle follows durable
   final evidence; no checkpoint, source commit or target promotion is added.
6. Separate local sender stop from remote completion. Unknown execution retains
   inventory for manual recovery; do not retry, automatically drop or treat an
   empty process/KILL result as completion. Exclude external concurrent DDL in the
   private namespace; UUID/comment verification and DROP are not atomic CAS.
7. Activate by explicit caller code after implementation proof; rollback stops
   new calls and moves caller/package together, never forwarding raw encoded bytes.

Consequences: full source scan and bounded local spool/journal cost; extra
same-node control/count queries; explicit unsupported combinations; manual
unknown-attempt recovery. Benefits are reviewable value fidelity and rejection
before this method's mutations, subject to the specification's tests and later
authorized readback. No general crash recovery or transport certification claim.

Rejected alternatives and rationale are fixed in the specification: raw forwarding,
marker-free/integer-only exceptions, new SQL codec, automatic fallback and a generic
plugin framework. Typed Python transport or broader runtime integration require a
revised feature scope and are not implicit implementation choices.

## Proposed implementation task contract

Recommend one writer because source binding, derived identity, transport outcomes
and evidence ordering are coupled. Independent read-only roles review algorithms,
test oracles and UX before writing, and a fresh non-author reviews the final commit.
This does not grant write authority now. After approval, materialize this contract
at `docs/agent-task-contracts/b02-validated-clickhouse-file-staging.yml` and validate
it through existing policy tooling before production changes.

Before materializing the contract, the coordinator copies the exact approved
specification and appendix into the new isolated implementation worktree at their
recorded paths and verifies both approved SHA256 values against the approval
receipt. Record that receipt and its explicit approval of the RESEARCHED package
before changing its lifecycle status to APPROVED; verify the recorded original
hashes first. Do not reconstruct, summarize or reinterpret these local-only files.
Any content change beyond the recorded status transition requires a revised
approval package. No production change may precede this transfer and verification.

The proposed base is the observed frozen integration candidate from PR55. A later
base change requires coordinator reconciliation and an updated exact base; it is
not an implementer's permission to expand paths or behavior.

```yaml
schema_version: 1
task_id: DPONE-B02-VALIDATED-CLICKHOUSE-FILE-STAGING
title: Explicit validated character-file staging in ClickHouse
goal: Implement exactly the approved finite Python stage API and its evidence boundaries.
specification: test_artifacts/dbt-programme/stage-02/b02-codec-design-draft.md
base_commit: a72e95710352ac96188c45c7fdb067526b86b9f0
integrator: programme-coordinator
shared_file_owner: programme-coordinator

acceptance_criteria:
  - Exact independently decoded text/binary/scalar values through both admitted modes.
  - Preparation and all admission denials precede this method's target mutations.
  - Preserve producer grammar, receipt authority, existing imports and generic stage behavior.
  - No successful handle before count, identities, spool cleanup and durable final evidence.
  - Controlled adapters prove bounded I/O and separate local/remote execution outcomes.
  - All specified journal/resource/cancellation/zero-row negative gates pass.
  - Independent fresh-context final exact-commit review resolves every blocking finding.

public_contract_impact:
  level: compatible
  surfaces: [python_api, artifacts, evidence, documentation]
  migration_required: false

owned_paths:
  - src/dpone/runtime/support/bulk_text_file_reader.py
  - src/dpone/runtime/etl/file_contract_validation.py
  - src/dpone/runtime/etl/validated_file_artifact.py
  - src/dpone/runtime/etl/contract_artifacts.py
  - src/dpone/runtime/sinks/clickhouse_sink.py
  - src/dpone/runtime/sinks/clickhouse_validated_file_models.py
  - src/dpone/runtime/sinks/clickhouse_validated_file_preparation.py
  - src/dpone/runtime/sinks/clickhouse_validated_file_ingestion.py
  - src/dpone/runtime/sinks/clickhouse_validated_file_journal.py
  - src/dpone/runtime/clickhouse_file_stage_contract.py
  - src/dpone/runtime/connectors/clickhouse_file_stage_client.py
  - src/dpone/runtime/connectors/clickhouse_file_stage_http.py
  - src/dpone/runtime/connectors/clickhouse_bulk.py
  - tests/test_file_contract_validation.py
  - tests/test_b02_bulk_text_file_reader.py
  - tests/test_b02_validated_file_attempt.py
  - tests/test_b02_clickhouse_file_policy.py
  - tests/test_b02_clickhouse_file_preparation.py
  - tests/test_b02_clickhouse_file_staging.py
  - tests/test_b02_clickhouse_file_journal.py
  - tests/test_b02_clickhouse_file_client_adapter.py
  - tests/test_b02_clickhouse_file_http_adapter.py
  - tests/test_b02_clickhouse_file_value_oracle.py
  - test_artifacts/b02-codec-certification
  - docs/validated-clickhouse-file-staging.md
  - docs/developer-validated-clickhouse-file-staging.md
  - docs/runtime-fast-path-contracts.md
  - docs/data-contract-runtime.md
  - docs/connector-sdk.md
  - docs/compatibility.md
  - docs/architecture.md
  - docs/quality-metrics.md
  - docs/source-sink/postgres-to-clickhouse.md
  - docs/adr/0064-validated-file-clickhouse-staging.md
  - docs/adr-index.md
  - docs/agent-task-contracts/b02-validated-clickhouse-file-staging.yml
  - CHANGELOG.md
  - mkdocs.yml
integrator_owned_paths: []

read_only_paths:
  - AGENTS.md
  - docs/agent-templates/agent-task-contract.yml
  - docs/engineering-standards.md
  - docs/compatibility_registry.yaml
  - docs/benchmarks/quality_budgets.yml
  - src/dpone/runtime/connectors/bulk_text_codec.py
  - src/dpone/runtime/connectors/clickhouse_http_bulk.py
  - src/dpone/runtime/clickhouse_binary_encoding.py
  - src/dpone/runtime/storage_policy.py
  - src/dpone/runtime/process_io.py
  - src/dpone/runtime/immutable_local_tree.py
  - src/dpone/runtime/sinks/clickhouse_payload_ingestion.py
  - src/dpone/runtime/sinks/clickhouse_staged_load.py
  - test_artifacts/dbt-programme/stage-02/b02-codec-design-draft.md
  - test_artifacts/dbt-programme/stage-02/b02-contract-appendix.md

forbidden_paths:
  - pyproject.toml
  - uv.lock
  - .github/workflows
  - packages
  - src/dpone/contracts
  - src/dpone/manifest
  - src/dpone/dag
  - src/dpone/ports
  - src/dpone/runtime/etl/payload_lifecycle.py
  - tests/conftest.py
  - docs/module_size_baseline.json
  - docs/layer_metrics_baseline.json
  - docs/benchmarks/quality_budgets.yml
  - test_artifacts/dbt-programme/stage-02/evidence
  - tests/test_native_bcp_implicit_time_fidelity.py

dependencies:
  - Maintainer marks the specification APPROVED and accepts the proposed ADR decisions.
  - Coordinator activates this exact path contract against its reconciled base.
required_checks:
  focused:
    - Red-green source compatibility, exact value oracles and public sink boundary tests.
    - Controlled local client/HTTP fixtures plus journal/resource/cancellation fault matrix.
  broad:
    - Change-aware select_checks.py plan and every applicable required check.
    - Ruff, format, mypy, import rules, layer metrics and exact-commit module-size gate.
    - Full non-live suite and clean installed-runtime checks on the final candidate.
    - Docs checks, language contracts, generated reference checks and strict MkDocs build.
    - Fresh independent exact-commit review after validation and follow-up on fixes.
  live:
    - SKIP unless a separate target environment and credentials are expressly authorized.

required_outputs:
  - implementation
  - tests
  - documentation_impact
  - completion_report
  - independent_review_receipt
completion_statuses:
  allowed: [PASS, FAIL, SKIP, N/A, UNVERIFIED]
stop_conditions:
  - Approved specification or active exact path contract is missing or contradicted.
  - Required edit falls outside owned_paths or another writer owns the path.
  - Scope would change generic stage dispatch, schema evolution, source route or terminal authority.
  - A shared reader change would alter existing source-validator admission or receipt grammar.
  - A live check needs unapproved environment or credentials.
  - Required owned module would grow debt or need a budget or baseline relaxation.
```

Existing owned files have additional semantic limits: `contract_artifacts.py`
permits the cohesive wrapper extraction/re-export only; `clickhouse_sink.py`
permits the new method and optional runner-factory composition only;
`clickhouse_bulk.py` permits the pure query-command builder only. Generic
`stage_payload`, old runner execution, source selection, finalization/checkpoints,
native B01 algorithms and unrelated cleanup remain forbidden even inside an owned
file. `docs/quality-metrics.md` may change only through its producer,
`dpone docs update-dev-metrics`; no hand edits or quality-budget/baseline relaxation
are allowed. Every unlisted path is read-only until an explicit contract revision.

Certification artifacts are generated only by their approved test/report producer;
they are not hand-authored passing evidence. The owned output directory is limited
to local non-live `<candidate>/<mode>/value-reconciliation.json`, validation logs,
completion reports and `<candidate>/independent-review.md` recording the actual
fresh review. Directory ownership does not authorize external live evidence or
changes to protected historical stage-02 evidence. External live fixtures and route
certification require separate scope. The programme coordinator remains the sole
shared-file owner and prepares the draft PR after committing authorized changes.
