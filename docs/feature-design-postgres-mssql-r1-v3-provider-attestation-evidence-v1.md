<!-- Private migration draft: public bindings and approval transfer are PENDING. -->

# Feature design: PostgreSQL → MSSQL R1 V3 provider attestation evidence V1

> Migration scope: this document is retained from source development. Historical approval, acceptance, exception, commit and evidence statements below apply to that source context; they do not establish current migration approval, activation, certification or passing validation. Current candidate status is tracked separately.

- Status: APPROVED
- Owner: dpone maintainers
- Issue: PostgreSQL → MSSQL Industrial Integration V7 / R1
- Target release: R1
- Last verified: 2026-09-12
- Implementation approval: 2026-09-12 local maintainer approval; evidence unbound; not IMPLEMENTED
- Depends on: [Provider Attestation foundation V2](feature-design-postgres-mssql-r1-v3-provider-attestation-foundation-v2.md)

## Outcome and boundary

This document owns only the hermetic RED/GREEN evidence protocol for Provider
Attestation V2. It does not authorize production contracts, SQL Server I/O,
vendor-live certification, public APIs or activation. Separating it lets the
evidence schema and producer evolve through their own reviewed task without
changing the canonical attestation ABI.

## Exact identities and paths

```yaml
schema_id: dpone-postgres-mssql-r1-v3-provider-attestation-v2-evidence-1
case_registry_id: dpone-postgres-mssql-r1-v3-provider-attestation-v2-cases-1
canonical_json: RFC8785_JCS_UTF8
case_registry_path: docs/schemas/evidence/postgres-mssql-r1-v3-provider-attestation-v2-cases.json
schema_path: docs/schemas/evidence/postgres-mssql-r1-v3-provider-attestation-v2.schema.json
producer_path: tools/evidence/postgres_mssql_r1_v3_provider_attestation_v2_evidence.py
red_probe_path: tests/support/postgres_mssql_r1_v3_provider_attestation_v2_red_probe.py
artifact_root: test_artifacts/postgres-mssql-r1-v3/provider-attestation-v2
artifact_name: attestation-inventory.json
publication: create_only_then_read_back
max_architecture_stdout_bytes: 4194304
max_architecture_stderr_bytes: 1048576
```

The RED-assets commit is the first commit containing the frozen tests, fixtures,
oracle, probe and case registry. A later reviewed task measurement amendment
pins that commit and its measurements. The evidence-protocol RED commit adds
only the protocol meta-test. Its direct GREEN child is the first commit
containing the schema and producer; both commits treat all RED assets as
read-only. `red_assets_commit`, both protocol commits, `schema_sha256`,
`case_registry_sha256`, `producer_commit`, `producer_sha256` and the protocol
tree digest bind those exact authorities. The task authority commit and
SHA-256 bind the final approved task measurement amendment.

These are two different RED/GREEN axes. `evidence_protocol_red_commit` and
`evidence_protocol_commit` prove implementation of the evidence machinery
itself. The artifact's `evidence_kind: red | green` reports absence or presence
of the separately owned Provider Attestation foundation behavior; protocol
GREEN is required before either artifact kind may be retained.

## Case registry

The case registry is JCS with exact top-level fields:

```text
case_registry_id, specification_sha256, ordered_cases
```

Each case has exact fields:

```text
case_id, partition, nodeid, parameter_id, owner_symbol, mutation_target,
mutation_kind, expected_behavior, expected_red_pytest_outcome,
expected_green_pytest_outcome, expected_reason, expected_diagnostic_class
```

Closed partitions:

```text
canonical_abi
query_arms
stable_schema
stable_catalog
mutation_bounds_compatibility
```

`mutation_kind` is `valid_distinct` or `must_reject`; `expected_behavior` is
`accept` or `reject`. The two pytest outcomes are `pass` or `fail`; GREEN is
always `pass`, including a correct model rejection. RED is `fail` for every
production-facing behavior case. Case IDs are ASCII lowercase dotted
tokens, bytewise ordered and unique. The registry has at least one
production-facing rejection case per partition and one valid case per owned
symbol. `nodeid` is exactly `<test_path>::test_case[<case_id>]`, `parameter_id`
equals `case_id`, and the mapping is bijective. The reviewed task measurement
amendment pins the exact registry count/SHA and node mapping after RED assets
exist but before retained RED evidence; no test discovers cases dynamically
from production.

The semantic arms are total:

```text
mutation_kind = valid_distinct  iff expected_behavior = accept
mutation_kind = must_reject     iff expected_behavior = reject
```

## Evidence object

Required top-level fields are exact and additional properties reject:

```text
schema_id, evidence_kind, exact_commit, parent_commit, generated_at,
specification_path, specification_sha256, specification_status,
specification_approval_commit,
evidence_specification_path, evidence_specification_sha256,
evidence_specification_status, evidence_specification_approval_commit,
red_task_contract_path, red_task_authority_commit,
red_task_authority_sha256, red_assets_commit,
red_task_measurement_commit, red_task_measurement_sha256,
evidence_task_contract_path, evidence_task_contract_commit,
evidence_task_contract_sha256,
evidence_protocol_red_commit, evidence_protocol_commit,
schema_sha256, case_registry_sha256,
producer_commit, producer_sha256, evidence_meta_test_sha256,
evidence_protocol_tree_sha256, test_tree_sha256,
ordered_nodeids, partition_node_counts, node_count, outcome_counts,
failing_nodeids, diagnostics_by_nodeid, case_results,
executed_case_ids_sha256, case_count,
implementation_status, certification_status, activation_status,
live_gate_status, live_behavior_status, architecture_metrics,
architecture_subject_commit, architecture_normalized_output,
architecture_normalized_output_sha256
```

Closed values:

```yaml
evidence_kind: red | green
specification_status: approved
implementation_status: absent | implemented
certification_status: unverified | local_pass
activation_status: blocked
live_gate_status: n_a
live_behavior_status: unverified
outcome_counts_exact_keys: [passed, failed, skipped, errors, total]
case_outcome_count_keys: [passed, failed, skipped, errors]
architecture_metric_keys:
  - raw_exit
  - avg_clustering
  - cross_layer_ratio
  - max_module_ce
  - class_finding_count
  - issue_count
```

`partition_node_counts` has exactly the five partition keys listed above.
Each architecture metric is a finite JSON number except `raw_exit`,
`max_module_ce`, `class_finding_count` and `issue_count`, which are nonnegative
JSON integers; JSON booleans and null reject.

All counts are JSON integers `>=0`; booleans reject. Node IDs and diagnostic
keys are bytewise sorted and unique. `diagnostics_by_nodeid` values are closed
redacted diagnostic-class tokens pinned by the task. Raw exception text is not
retained. `architecture_metrics.raw_exit` preserves a repository-level failure;
the evidence must never recode it as PASS.
`architecture_subject_commit` equals `exact_commit`.
`architecture_normalized_output_sha256` is the SHA-256 of the deterministic
normalized architecture JSON defined below. `architecture_normalized_output`
retains those exact RFC 8785 JCS UTF-8 bytes as a JSON string, so its digest
preimage remains independently auditable after the producing checkout is gone.
Stderr and environment-specific raw stdout are local diagnostics only.
`generated_at` is the exact UTC author timestamp of `exact_commit`, normalized
to `YYYY-MM-DDTHH:MM:SSZ`; it introduces no wall-clock nondeterminism. Together
with path-independent architecture normalization, the same exact commit and
dependency environment regenerate identical authoritative evidence bytes.

Text domains are exact: `owner_symbol` is one class/function identifier from
the foundation symbol map; `mutation_target` is an ASCII dotted field/arm path
`[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)*`, at most 256 bytes.
`expected_reason` is JSON null for `accept` and one closed foundation failure
reason for `reject`. `expected_diagnostic_class` is the task-pinned ASCII token
`missing_behavior` for RED. In case results, `reason` is null or that case's
exact expected reason; `diagnostic_class` is null or the exact expected RED
diagnostic. No empty-string sentinel is allowed.

Digest preimages are exact:

```text
test_tree_sha256 = SHA256(
  b"dpone-provider-attestation-v2-test-tree-v1\0" +
  JCS([{path, sha256} ... bytewise path order])
)

producer_sha256 = SHA256(exact producer file bytes)
schema_sha256 = SHA256(exact schema file bytes)
case_registry_sha256 = SHA256(exact case registry file bytes)
red_task_authority_sha256 = SHA256(task YAML blob at red_task_authority_commit)
red_task_measurement_sha256 = SHA256(task YAML blob at red_task_measurement_commit)
evidence_task_contract_sha256 = SHA256(
  evidence-task YAML blob at evidence_task_contract_commit
)
specification_sha256 = SHA256(exact approved foundation spec bytes)
evidence_specification_sha256 = SHA256(exact approved evidence spec bytes)
evidence_meta_test_sha256 = SHA256(exact evidence meta-test bytes)
evidence_protocol_tree_sha256 = SHA256(
  b"dpone-provider-attestation-v2-evidence-protocol-tree-v1\0" +
  JCS([{path, sha256}] for schema, producer and meta-test in path order)
)
architecture_normalized_output_sha256 = SHA256(
  UTF8(architecture_normalized_output)
)
architecture_normalized_output = UTF8_DECODE(
  JCS(complete architecture JSON with only package normalized to "src/dpone")
)
executed_case_ids_sha256 = SHA256(
  b"dpone-provider-attestation-v2-executed-cases-v1\0" +
  JCS(case IDs in case-registry order)
)
```

The exact test-tree paths are the eight foundation test/support paths in the
foundation specification plus the case-registry path. Each `{path, sha256}` is
read from `red_assets_commit`; later modification invalidates evidence.

Git equations are:

```text
parent_commit = exact_commit^
red_task_contract_path =
  docs/agent-task-contracts/postgres-mssql-r1-v3-provider-attestation-v2-red.yml
evidence_task_contract_path =
  docs/agent-task-contracts/postgres-mssql-r1-v3-provider-attestation-v2-evidence.yml
red_assets_commit^ = red_task_authority_commit
red_task_measurement_commit^ = red_assets_commit
red_task_measurement_commit is the last commit touching red_task_contract_path
  and changes only measurement/self-pin fields authorized by the initial task
evidence_protocol_red_commit^ = evidence_task_contract_commit
evidence_protocol_commit^ = evidence_protocol_red_commit
evidence_task_contract_commit is the last commit touching
  evidence_task_contract_path
evidence_protocol_commit is an ancestor of exact_commit and the first commit
  containing schema_path and producer_path
producer_commit = evidence_protocol_commit
architecture_subject_commit = exact_commit
red_task_authority_commit < red_assets_commit < red_task_measurement_commit
  < evidence_task_contract_commit < evidence_protocol_red_commit
  < evidence_protocol_commit <= exact_commit
for every frozen RED path, its blob at exact_commit equals its blob at
  red_assets_commit and red_assets_commit is the last commit touching that path
the evidence meta-test blob at exact_commit equals its blob at
  evidence_protocol_red_commit, which is its last-touch commit
the schema and producer blobs at exact_commit equal their blobs at
  evidence_protocol_commit, which is their last-touch commit
case_registry.specification_sha256 = evidence.specification_sha256
specification_path =
  docs/feature-design-postgres-mssql-r1-v3-provider-attestation-foundation-v2.md
evidence_specification_path =
  docs/feature-design-postgres-mssql-r1-v3-provider-attestation-evidence-v1.md
specification_status = approved
evidence_specification_status = approved
specification_approval_commit and evidence_specification_approval_commit are
  ancestors of red_task_authority_commit
the two specification blobs at exact_commit equal their blobs at their
  approval commits, which are their last-touch commits
```

## RED and GREEN equations

Common equations:

```text
node_count = len(ordered_nodeids)
outcome_counts.total = node_count
sum(passed, failed, skipped, errors) = total
for each outcome key in [passed, failed, skipped, errors], outcome_counts[key]
  = count(case_results whose pytest_outcome maps to that key)
sum(partition_node_counts.values) = node_count
for each exact partition key, partition_node_counts[key]
  = count(registry cases whose partition = key)
case_count = len(case registry ordered_cases)
len(case_results) = case_count
case_results case IDs = case registry case IDs in exact order
ordered_nodeids = tuple(registry case.nodeid in registry order)
for every position, case_result.case_id/nodeid equal registry case.id/nodeid
for every position, the case-result tuple
  (case_id, nodeid, pytest_outcome, observed_behavior, reason,
   diagnostic_class) equals the same registry case's evidence-kind-derived
  expected tuple
executed_case_ids_sha256 matches those IDs
skipped = 0
errors = 0
```

RED additionally requires:

```text
evidence_kind = red
implementation_status = absent
certification_status = unverified
failed = node_count
passed = 0
every partition has at least one collected failing node
failing_nodeids = tuple(result.nodeid for failed case results in registry order)
set(failing_nodeids) = diagnostic keys
diagnostics_by_nodeid = {result.nodeid: result.diagnostic_class
  for failing case results}
every case result pytest outcome = expected_red_pytest_outcome
every case result observed_behavior = not_observed
every case result reason = null
every case result diagnostic_class = expected_diagnostic_class
```

GREEN additionally requires:

```text
evidence_kind = green
implementation_status = implemented
certification_status = local_pass
failed = skipped = errors = 0
passed = total
failing_nodeids = []
diagnostics_by_nodeid = {}
every case result pytest outcome = expected_green_pytest_outcome
every case result observed_behavior = expected_behavior
accepted case result reason = null and diagnostic_class = null
rejected case result reason = expected_reason and diagnostic_class = null
```

Each case result has exact fields `case_id`, `nodeid`, `pytest_outcome`,
`observed_behavior`, `reason` and `diagnostic_class`; observed behavior is
`accept`, `reject` or `not_observed`. RED requires `not_observed` because the
owned behavior is absent; GREEN requires equality to `expected_behavior`.
Collection errors are forbidden. Each test module uses a collection-safe local
behavior probe: imports occur inside test/helper call boundaries and absence of
its owned production behavior becomes the exact typed RED signal defined below.
A shared top-level missing import may not prevent collection of the other four
partitions. The task pins exact expected node IDs, partition counts, failing IDs
and diagnostics before RED execution.

### Exact structured pytest oracle

The frozen RED probe owns these support-only types/constants:

```text
ProviderAttestationMissingBehavior(case_id, diagnostic_class)
PROVIDER_ATTESTATION_OBSERVATION_PROPERTY =
  "dpone.provider_attestation.v2.case_observation"
```

`ProviderAttestationMissingBehavior` is a final exception with exactly the two
ASCII string attributes shown. It is never a production exception. Every test
records exactly one built-in `record_property` pair whose key is the constant
and whose value is RFC 8785 JCS text with exact fields:

```text
case_id, observed_behavior, reason, diagnostic_class
```

For RED, the local probe first records the case's exact `not_observed`, null
reason and `missing_behavior` observation, then raises
`ProviderAttestationMissingBehavior` with the same case ID and diagnostic. No
other failure type is an expected RED result. For GREEN, the test derives the
observation from the actual factory/decoder return value or caught production
typed rejection, records `accept`/null/null or `reject`/exact-reason/null, and
returns normally. Tests never copy a GREEN observation from registry expected
fields; the registry is used only by the producer when comparing the captured
actual observation.

`ProviderAttestationStructuredEvidencePlugin` implements only these exact
pytest hooks:

```text
pytest_collectreport(report)
pytest_collection_modifyitems(session, config, items)
pytest_runtest_makereport(item, call)  # hookwrapper
```

Its state machine is fail closed:

1. Every collection report must pass. The final collected node IDs must equal
   `ordered_nodeids` exactly in order; missing, extra and duplicate items reject.
2. Each collected node must yield exactly one report for each ordered phase
   `setup`, `call`, `teardown`; a missing, duplicate or foreign phase rejects.
3. `setup` and `teardown` must pass. Any skipped report, any `wasxfail`
   attribute (xfail or xpass), or any setup/teardown failure rejects evidence.
4. At the call hook the plugin reads `item.user_properties` directly and
   requires exactly one pair, with the exact authority key and a canonical JCS
   string value. Missing, duplicate, malformed, noncanonical or extra
   properties reject. The decoded case ID must equal the current node's
   registry case.
5. RED requires a failed call report, non-null `call.excinfo`, and an exception
   whose concrete type is exactly the frozen
   `ProviderAttestationMissingBehavior`; both exception attributes must equal
   the captured observation and registry case. AssertionError, import errors,
   timeouts and every other exception reject rather than count as expected RED.
6. GREEN requires a passed call report, null `call.excinfo`, and the captured
   observation must equal the actual-result tuple expected by that registry
   arm. A failed GREEN call always rejects.
7. After `pytest.main` returns, RED accepts only pytest exit status `1` and
   GREEN only status `0`. Exit statuses `2`, `3`, `4`, `5`, negative statuses
   and every other value reject. The plugin's independently counted reports
   must equal the returned status and every evidence equation.

Thus a merely failing assertion cannot satisfy RED, and a merely passing test
without an actual structured observation cannot satisfy GREEN.

## Producer and publication algorithm

1. Require a clean worktree, exact `HEAD`, both approved feature specs, the
   approved RED measurement authority, the approved evidence-task authority,
   and pinned evidence-protocol/RED-assets commits.
2. Verify both specs, both task authorities, schema, registry, producer and
   test-tree digests.
3. Require `nodeid.split("::", 1)[0]` to equal exactly one of:

   ```text
   tests/test_postgres_mssql_r1_v3_provider_attestation_v2_abi.py
   tests/test_postgres_mssql_r1_v3_provider_attestation_v2_query_results.py
   tests/test_postgres_mssql_r1_v3_provider_attestation_v2_stable_schema.py
   tests/test_postgres_mssql_r1_v3_provider_attestation_v2_stable_catalog.py
   tests/test_postgres_mssql_r1_v3_provider_attestation_v2_mutations.py
   ```

   The producer imports pytest and invokes exactly:

   ```python
   pytest.main(
       [*ordered_nodeids, "-q"],
       plugins=[ProviderAttestationStructuredEvidencePlugin(...)]
   )
   ```

   `ProviderAttestationStructuredEvidencePlugin` is defined only in the pinned
   producer file. It records collection and report objects through pytest
   hooks; terminal prose is never parsed. The pytest return code, collected
   node IDs and reports must agree with every count, order and diagnostic
   equation before evidence can be published.
4. In the exact clean checkout of `architecture_subject_commit = exact_commit`,
   run the subprocess argv through `subprocess.Popen`
   `["uv", "run", "dpone", "docs", "check-architecture-fitness", "--format", "json"]`.
   Drain stdout and stderr concurrently into bounded byte buffers. If stdout
   exceeds 4,194,304 bytes or stderr exceeds 1,048,576 bytes, terminate the
   child, discard the partial observation and fail closed; truncation is never
   evidence. Require UTF-8 without replacement.
   Require a successfully parsed JSON object even when the command returns
   nonzero. Validate that its absolute `package` resolves to
   `<exact-clean-checkout>/src/dpone`, replace only that value with the literal
   repo-relative `src/dpone`, serialize the complete resulting object as RFC
   8785 JCS, retain its UTF-8 decoding verbatim in
   `architecture_normalized_output`, and hash the same UTF-8 bytes into
   `architecture_normalized_output_sha256`. Reparsing the retained string must
   reproduce the complete normalized object and its JCS bytes exactly. Map
   metrics from that same parsed object without recoding:

   ```text
   architecture_metrics.raw_exit = subprocess return code
   architecture_metrics.avg_clustering = output["avg_clustering"]
   architecture_metrics.cross_layer_ratio = output["cross_layer_ratio"]
   architecture_metrics.max_module_ce = output["max_module_ce"]
   architecture_metrics.class_finding_count = len(output["class_findings"])
   architecture_metrics.issue_count = output["issue_count"]
   ```

   No other output field may be removed or normalized. Missing keys, malformed
   JSON, a foreign package path or a normalized digest mismatch fail closed. A
   nonzero architecture exit remains visible and does not prevent truthful
   foundation RED/GREEN test evidence.
5. Validate all equations and status fields; serialize RFC 8785 JCS UTF-8 with
   no trailing newline.
6. Resolve the exact artifact path under `artifact_root/<exact_commit>`; reject
   traversal, symlinks and a mismatching directory commit.
7. Create a private temporary file with exclusive semantics, write/fsync, close,
   atomically publish without overwrite, fsync parent, reread and verify exact
   bytes, schema and SHA-256.
8. If the final path already exists, succeed only after exact byte equality;
   otherwise fail closed. Never update or hand-edit an artifact.

The RED-task-owned stdout-only probe runs before the evidence protocol is
committed. It emits the same JCS logical fields plus `retained=false`; it does
not write a file and is not certification evidence.

## Validation and status

The RED task exclusively owns tests, fixtures, oracle, probe and case registry.
The later protocol task exclusively owns these three paths and has all RED
assets read-only:

```text
docs/schemas/evidence/postgres-mssql-r1-v3-provider-attestation-v2.schema.json
tools/evidence/postgres_mssql_r1_v3_provider_attestation_v2_evidence.py
tests/test_postgres_mssql_r1_v3_provider_attestation_v2_evidence.py
```

Production, current shared fixtures and generated artifacts are forbidden to
both tasks. The protocol task first commits only the evidence meta-test in
`evidence_protocol_red_commit`; it must collect and fail because its owned
schema and producer do not yet exist. The direct child
`evidence_protocol_commit` adds only the schema and producer, leaves the
meta-test byte-identical, and makes that same test GREEN. The protocol task's
exact focused argv is
`["uv", "run", "pytest", "tests/test_postgres_mssql_r1_v3_provider_attestation_v2_evidence.py", "-q"]`.
Schema/producer meta-tests cover additional properties, every closed enum,
digest shape, all equations, path safety, collision/read-back, wrong commits,
collection errors, missing/duplicate/foreign phase reports, setup/teardown
failures, unrelated call exceptions, skip/xfail/xpass, all forbidden pytest exit
statuses, missing/duplicate/malformed observation properties and registry versus
observation mismatches. Architecture cases cover a foreign package path,
normalizing package and only package, mutation/removal of every other field,
retained-string/JCS/digest disagreement, every metric-versus-retained-object
mismatch, stdout/stderr limit and limit+1, malformed UTF-8/JSON and preservation
of a nonzero architecture result.

The live gate is `N/A` because this protocol is hermetic. Actual SQL Server
behavior/certification is `UNVERIFIED`; activation stays blocked. A skipped,
mocked, stale or unavailable check never becomes PASS.

## Market comparison and documentation impact

Market comparison is `N/A`: an internal evidence serialization protocol has no
external connector/user metric. The public manifest, CLI and CJM are unchanged.

## Definition of Done

1. This protocol is maintainer-`APPROVED` on an exact reviewed commit.
2. Schema and case-registry paths are distinct and both hashes are pinned.
3. Five RED partitions collect independently with no errors/skips.
4. Exact RED/GREEN equations and status truth are schema-tested.
5. Producer is create-only, path-safe, fsync/read-back verified and commit-bound.
6. No generated artifact is hand-edited; live remains unverified/blocked.

Back: [Provider Attestation foundation V2](feature-design-postgres-mssql-r1-v3-provider-attestation-foundation-v2.md).
Next after approval: the self-pinned RED task contract.
