# Feature design: Airflow self-service quieter CLI UX (P1)

- Status: IMPLEMENTED
- Evidence: quieter CLI renderers + Advanced selection help; tests in
  `tests/test_airflow_self_service_cli.py` / rendering / redaction; branch
  `docs/airflow-pipeline-first-ux-p1`
- Owner: data architecture / integrator
- Issue: follow-up to P0 pipeline-first UX docs (merged [#344](https://github.com/PaulKov/dpone/pull/344))
- Target release: TBD (patch/minor after P0)
- Last verified: 2026-07-17

## Maintainer decisions to lock at APPROVED

Defaults proposed below; override only if you disagree:

1. Quieting applies to **passed / success text paths only**. Failures keep current
   richer text (status, errors, codes, fix hints).
2. No new `--verbose` flag in P1; verbose channel is `--format json`.
3. Selection args use an argparse group titled `Advanced selection` (not
   `SUPPRESS`).
4. Preview success omits `airflow index` path (still in JSON).
5. Static check success **keeps** `- deprecated input: …` when
   `deprecated_aliases` is non-empty (compatibility signal).
6. Redaction tests: static quiet success may omit `- secrets: no`; connections
   renderer still asserts secrets non-leakage.

## Executive summary

P0 fixed docs progressive disclosure. Beginners still see verbose default
**text** stdout on init/check/preview/sample and primary `--help` presents
selector flags as ordinary options. P1 quiets beginner **success** text and
demotes selection help without changing JSON payloads, exit codes, selection
engine semantics, or the Airflow v1 public-contract freeze YAML.

**P1 success (contract):** quiet shapes below; Advanced selection help strings;
CLI + rendering tests updated; First DAG text fences + prose synced; freeze
YAML/tests empty diff; JSON assertions unchanged.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| New data engineer | Five-command path | Verbose text; node/selector noise | Short success stdout matches First DAG |
| Data engineer (advanced) | `--select` | Help looks day-1 | Help under Advanced selection |
| CI/automation | Stable JSON | Fear of churn | JSON assertions unchanged |
| Maintainer | No freeze churn | Schema rename risk | Freeze YAML empty diff |

Same beginner CJM as P0. `dpone airflow explain` stays verbose (out of scope).

## Scope

### In scope

- Quiet **success** text for:
  - `dpone init project --airflow` / `dpone init pipeline … --airflow`
  - `dpone check` static pass (1:1 target) via `self_service_check_text`
  - `dpone airflow preview` success via `self_service_preview_text`
  - `dpone run … --sample` via `safe_sample_text` / `safe_sample_markdown`
    (exact shapes below)
- Demote selection CLI help via `selection_arguments.py` (Advanced group + exact
  strings below).
- Align tests listed in Test plan; sync First Airflow DAG text fences **and**
  prose that claims init lists changed paths.
- CHANGELOG Unreleased note.

### Non-goals

- JSON schema/field/URI/`workload_id` changes.
- Selection engine behavior or default `--max-selected` values.
- Editing freeze YAML or `tests/test_airflow_v1_public_contract_freeze.py`.
- Quieting connections/live check modes, `airflow explain`, multi-select run
  report, or failure text (except incidental shared helpers).
- P2 Source→Sink framing.
- New `--verbose` flag.

### Assumptions and constraints

- Text stdout is a documented UX contract (First DAG) but **not** freeze-YAML
  frozen; help string changes are compatible per freeze design.
- Failed paths keep actionable codes; do not hide blockers.
- English-only CLI text.
- Forbidden: `docs/airflow-self-service-public-contracts-v1.yaml`,
  `tests/test_airflow_v1_public_contract_freeze.py`, `docs/schemas/**`,
  selection service logic under `src/dpone/**/selection*` beyond help wiring.

## Public contract

### CLI

| Surface | Change |
|---|---|
| Exit codes | Unchanged |
| `--format json` | Unchanged payloads |
| Default text **success** | Quieter shapes below |
| Default text **failure** | Unchanged richness |
| Selection help | Advanced group + strings below |
| Required options | Unchanged |

#### Quieting rule

```text
IF command text path reports success/pass THEN emit quiet shape
ELSE keep current failure/blocked richness (codes, messages, fix hints)
```

#### Init success (authoritative)

```text
dpone init project
- status: <status>
- changes: <n> changed
- next: dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental --airflow
```

```text
dpone init pipeline
- status: <status>
- changes: <n> changed
- next: dpone check pipelines/<id>
```

- **Must not** list `- files:` / per-path lines on success.
- **Must not** emit `- details: rerun with --format json…` on success (JSON is
  the detail channel). On failure, existing detail/error lines may remain.
- Line budget: ≤ 4 lines (title + status + changes + next).

#### Static check success (authoritative)

```text
dpone check: OK
- next: dpone airflow preview <pipeline_id>
```

If `deprecated_aliases` non-empty, insert before `next`:

```text
- deprecated input: <ALIAS>[, <ALIAS>…]
```

- Omit mode/target/network/secrets/source_queries on success.
- Line budget: ≤ 3 lines normally; ≤ 4 when deprecated aliases present.
- Connections/live check text: unchanged (out of scope).

#### Preview success (authoritative)

```text
dpone airflow preview: OK
- pipeline: <pipeline_id>
- next: dpone run pipelines/<pipeline_id> --sample 1000 --target temporary
```

- Omit deployment type, runnable, airflow index, per-node lines, visible-task
  budget on success.
- Line budget: ≤ 3 lines.
- Preview FAILED: keep current richer lines (out of quieting).

#### Safe sample text (authoritative)

Titles unchanged:
`dpone safe sample live copy` | `dpone safe sample handoff prepared` |
`dpone safe sample run blocked` | `dpone safe sample run unavailable`.

**`live_copy` success (no errors):**

```text
dpone safe sample live copy
OK: safe sample run passed
- runtime evidence: <path>
```

(Omit `runtime status` / `data outcome` on success.)

**`local_handoff` (typical beginner blocked-until-platform path):**

```text
dpone safe sample handoff prepared
<CODE>: <message>   # each error, same as today
- next: platform prepares the signed authorization overlay; rerun the same dpone run command
```

(Omit `runtime status` / `data outcome` / `runtime evidence` on handoff when
`- next:` is present. Keep error codes/messages.)

**`blocked` / unavailable:** keep title + error lines; omit status/outcome
matrix when errors exist; keep evidence path only if no `- next:` and evidence
exists (prefer error + next).

**Legacy call** `safe_sample_text(... execution_mode=None)` with handoff mapping:
preserve existing handoff/live-copy command lines (do not break
`test_legacy_renderer_call_without_mode_preserves_handoff_commands`).

Markdown variants (`safe_sample_markdown`) mirror the same field omissions.

#### Advanced selection help (authoritative)

Add argparse group `Advanced selection` containing `--select`, `--exclude`,
`--state`, `--selectors`, `--max-selected`.

Exact help strings:

| Option | Help |
|---|---|
| `--select` | `Advanced: filter workloads (see Workload selectors docs)` |
| `--exclude` | `Advanced: exclude workloads after selection expansion` |
| `--state` | `Advanced: local dpone.selection-state.v1 baseline for state:* selectors` |
| `--selectors` | `Advanced: named selector file (default: selectors.yaml)` |
| `--max-selected` | `Advanced: hard selected workload limit (default: {N})` |

Docs page title to reference: **Workload selectors**
(`docs/airflow-self-service-selectors.md`). Engine semantics unchanged.

### Python API / Manifest / Artifacts

No change.

### Compatibility and migration

Quieter success text is intentional UX tightening. CHANGELOG note. Rollback =
revert MR. Unsupported parsers of human text may break; JSON is the machine API.

## Detailed algorithm

1. Patch success branches in the three renderers; leave JSON writers untouched.
2. Group selection args; set exact help strings.
3. Red-green update tests in Test plan.
4. Sync First DAG text fences + init prose (“changed paths” → summary/next).
5. Prove freeze empty diff.

### Pseudocode

```text
self_service_init_text(...):
  if success: return title, status, changes_count, next  # no files, no details
  else: keep current failure rendering

self_service_check_text(...):
  if passed: return OK, optional deprecated, next
  else: unchanged failure path (caller)

self_service_preview_text(...):
  if OK: return OK, pipeline, next
  else: keep current rich failure lines

safe_sample_text(...):
  emit title + error_or_OK lines
  apply mode-specific quiet rules above
```

## Architecture

| Component | Change |
|---|---|
| `airflow_self_service_rendering.py` | Quiet init/check success |
| `airflow_preview_rendering.py` | Quiet preview success |
| `run_safe_sample_rendering.py` | Quiet sample field set |
| `selection_arguments.py` | Advanced group + help |
| CLI/docs tests | Contract sync |

### Alternatives

| Option | Decision |
|---|---|
| `--verbose` restore old text | Reject P1 |
| Suppress select from argparse | Reject |
| Docs-only | Reject (P0 done) |

### Quality-budget impact

Small renderer edits; stay within existing module budgets. No new packages.

### ADR

Not required.

## Market comparison

Primary sources 2026-07-17.

| System | Relevant | Adopt/reject | Source |
|---|---|---|---|
| dlt | Concise CLI default | Adopt quiet default | dlt CLI docs |
| Airbyte | Progressive disclosure | Adopt for help grouping | UX Handbook |
| Cosmos | select/exclude advanced | Adopt Advanced label | Selecting & Excluding |
| Others (Fivetran, Informatica, Pentaho, SSIS, Beam, gusty) | N/A | N/A | not OSS beginner CLI text contracts |

## Measurable differentiation

```yaml
axis: beginner CLI text brevity
scenario: hermetic success stdout for golden path
baseline: preview prints node/selector lines; check prints mode/network/secrets
metric: line count of success text blocks (non-blank)
target: init<=4; check<=3 (4 if deprecated); preview<=3; JSON fixtures unchanged
procedure: uv run pytest tests/test_airflow_self_service_cli.py -q -k "beginner_init or beginner_check_and_preview"
artifact: pytest log on P1 MR
limitations: not a human preference study
```

## Security, privacy, and operations

No secret-surface change. `write_text` redaction unchanged. Static success may
omit `- secrets: no`; redaction coverage remains on connections renderer +
payload redaction tests.

## Test and certification plan

| Layer | Env | Scenario | Expected |
|---|---|---|---|
| CLI | hermetic | `test_beginner_init_text_*` | no file paths; has next |
| CLI | hermetic | `test_beginner_check_and_preview_*` | quiet shapes |
| Unit | hermetic | `test_default_check_text_surfaces_legacy_authoring_alias` | deprecated line kept |
| Unit | hermetic | `test_run_safe_sample_rendering.py` | new field set |
| Unit | hermetic | redaction check renderer test | update static expectations; keep leak asserts |
| CLI | hermetic | new help contract test | Advanced selection strings present on `check --help` |
| Docs | hermetic | First DAG text fences + prose | match quiet shapes |
| Docs | hermetic | language golden bash commands | unchanged |
| Freeze | hermetic | freeze YAML + freeze tests | empty diff |
| Live | — | — | N/A |

Explicitly update/reconcile:

- `tests/test_airflow_self_service_cli.py`
- `tests/test_run_safe_sample_rendering.py`
- `tests/test_airflow_authoring_v1.py` (deprecated alias — keep assert)
- `tests/test_output_text_redaction.py` (static secrets line expectation)
- New focused help test (same file as selection args or CLI tests)

## Documentation plan

| Doc | Change |
|---|---|
| `docs/getting-started/first-airflow-dag.md` | Text fences + init prose |
| `docs/airflow-self-service-selectors.md` | Optional one-liner: CLI help groups Advanced |
| `CHANGELOG.md` | Unreleased |
| This spec | IMPLEMENTED after gates |

## Rollout and rollback

APPROVED → branch from master → review → merge → revert if needed.

## Agent execution plan

| Role | Owned paths | Forbidden |
|---|---|---|
| Renderer writer | three rendering modules + `selection_arguments.py` | freeze YAML, schemas |
| Test/docs writer | CLI/rendering/redaction/authoring text tests; First DAG; selectors one-liner | freeze tests |
| Integrator | CHANGELOG; shared conftest only if required | unrelated packages |

read_only: freeze YAML, freeze tests, P0 docs (unless First DAG text sync).

## Approval checklist

- [x] Quiet shapes exact (incl. safe sample + failure rule).
- [x] Selection help complete (5 args + Advanced group).
- [x] Line budget consistent (no success `details:`).
- [x] Deprecated-alias + redaction policy locked.
- [x] Freeze/JSON safety explicit.
- [x] Test contradictions listed.
- [ ] Maintainer status → `APPROVED`.
