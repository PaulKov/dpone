# Impact-aware acceptance planning

Maintainers can produce an acceptance plan bound to two exact commits:

```bash
uv run python tools/agent_policy/acceptance_plan.py \
  --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA" \
  --output test_artifacts/acceptance/plan.json
```

Supply the intended comparison base explicitly, such as the PR merge base, and
its candidate head. The tool resolves both references once to full commit IDs
and compares those trees with Git replacement objects disabled. It does not include staged, unstaged, or untracked
changes. Missing revisions or unreadable Git objects exit with code 2 and do not
produce a success plan. Consumers must check the exit code and exact identities;
never reuse an artifact from another run.

This is an additive planning tool. The existing
`tools/agent_policy/select_checks.py` interface and selection remain unchanged.
The plan preserves its complete result under `legacy_validation`. Every existing
mandatory workflow, non-live, security, packaging, and release check remains
required. The plan itself does not run those commands or establish that CI ran
them, and must not be used as a workflow skip matrix.

## Conservative classification

| Classification | Proof and additional acceptance |
|---|---|
| `schedule_only` | Every changed file is an existing regular YAML domain file under `examples/**/gitops/domains/`; only supported cron schedule values changed. Run parser, serialization, scheduling, and interval contracts. No full data transfer is added solely for a schedule change. |
| `broad` | At least one file cannot meet that proof. Run broad non-live contracts and review affected routes for a scoped synthetic Docker smoke. |
| `no_changes` | The two trees have no changed paths. Existing required gates remain authoritative. |

Version 1 supports domain documents with `domain`, simple manifest-only
`workloads`, and `dags`. The DAG field allowlist covers description, schedule,
start date, timezone, catchup, concurrency, tags, default arguments, operator
overrides, workload membership, and wiring. It accepts cron fields containing
single bounded numbers or `*`, and `@hourly`, `@daily`, `@weekly`, `@monthly`, and
`@yearly`. Other valid framework inputs deliberately select broader acceptance;
this classifier does not narrow framework authoring support.

At least one normalized schedule must change. Mapping order and string quoting
do not matter, while typed non-schedule fields must remain equal. Changing
interval derivation, timezone, connection options, runtime code, dependencies,
schemas, or any other file prevents schedule-only classification. Renames,
additions, deletions, mode changes, symlinks, unsupported fields, invalid input,
duplicate keys, aliases, custom tags, and excessive document size or nesting
also select broader checks. No Python modules, YAML constructors, includes, or
templates from compared commits are executed. Run candidate code only in unprivileged CI; classifier output cannot authorize
privileged jobs or weaken mandatory checks.

## Reading and completing the artifact

The JSON schema version is `1`; `classifier_version` identifies the semantic
policy. `base_sha` and `head_sha` bind the plan to commits. `files` contains Git
blob identities, paths, status, modes, and a reason for each classification.
`required_checks` includes stable check IDs, reasons, optional commands,
execution status, and artifact references. New records start `UNVERIFIED` with
empty artifact lists. These are work requirements, not passing receipts.

Run the listed focused contracts and retain their JUnit results. For a broad
change, use affected supported open-source source/sink pairs and seeded synthetic
data for the local Docker smoke. Record execution outcomes and artifact paths
in the execution report alongside the plan. Missing infrastructure is `SKIP` or
`UNVERIFIED`; a required smoke remains incomplete. A route smoke can be `N/A`
only when review establishes that no route is affected and records the reason.
Large synthetic performance and soak runs remain opt-in for PRs; existing
release evidence requirements do not change.

The synthetic history tests in `tests/test_acceptance_plan.py` exercise exact
revision binding, semantic comparisons, mixed diffs, filenames, malformed input,
unsafe YAML constructs, mode changes, and CLI failures.
`tests/test_agent_policy_select_checks.py` verifies that the complete legacy
selection remains present, including security, packaging, and release gates.
See the [approved design](feature-design-bounded-streaming-window-v1.md) for the
independent runtime workstream and its Docker acceptance requirements.

## CI execution

The main CI workflow now adds four jobs alongside the existing checks:

- `acceptance-plan` compares the PR merge base and exact candidate head, the
  previous push commit and head, or the dispatched commit's parent and head.
  It uploads `acceptance-plan/plan.json` and exposes the bound head and class.
- `acceptance-contracts` runs the selected scheduling contracts and policy tests
  at that head and uploads JUnit plus a verified execution receipt. These are
  scheduler-neutral contracts; real Airflow rendering remains in the existing
  `airflow-pack-compat` matrix with its dedicated dependencies.
- `bounded-window-smoke` runs for `broad` changes using disposable PostgreSQL 16
  and ClickHouse 24.8 Docker services. It executes source snapshot, target
  publication, combined route, and bounded execution/type/recovery cases with
  synthetic data. Service image digests accompany its JUnit and receipt.
- `acceptance` fails if planning or contracts fail, or if a required broad-change
  smoke fails or skips. The smoke is omitted only for a proven schedule-only or
  unchanged-tree plan. It does not run performance or soak workloads.

All existing jobs retain their original conditions and dependencies. Candidate
code executes with read-only repository permissions and no deployment secrets;
classification never authorizes privileged work or suppresses existing gates.
Missing comparison history fails planning, including an initial commit without a
parent when dispatch requires one.

The receipt verifier requires actual passing test cases for **every specified
test file**, verifies summary counts, rejects duplicate identities, and rejects
all skips, failures, errors, malformed XML, or empty collections. A successful
pytest process alone is insufficient. Receipts bind the candidate SHA and JUnit
SHA-256 and require the exact clean candidate checkout (new generated evidence
under `test_artifacts/` is allowed; tracked changes and other untracked files
are rejected). Uncommitted local runs
retain JUnit as test evidence but cannot issue a committed-candidate PASS receipt.
Unsuccessful runs retain available JUnit through the always-upload step
without creating a passing receipt. Local runs can apply the same verification:

```bash
uv run python tools/agent_policy/acceptance_plan.py --head-ref "$HEAD_SHA" \
  --verify-junit test_artifacts/acceptance-contracts/junit.xml \
  --require-test-file tests/test_airflow_dag_schedule.py \
  --output test_artifacts/acceptance-contracts/receipt.json
```
