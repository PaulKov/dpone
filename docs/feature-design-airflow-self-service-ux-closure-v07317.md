# Feature design: Airflow self-service UX closure v0.73.17

- Status: IMPLEMENTED
- Owner: dpone maintainers / Codex integrator
- Issue: follow-up to the v0.73.0 self-service review against master
- Target release: next patch after 0.73.16
- Last verified: 2026-07-23

## Approval

The maintainer explicitly requested implementation on 2026-07-23 after
reviewing the current-master gap analysis. This specification closes existing
beginner-journey contract gaps; it does not introduce another authoring or
runtime engine.

## Executive summary

The current master provides a parse-safe provider, canonical authoring compiler,
hermetic tests, and a successful four-command DAG preview. Four UX defects still
break the intended self-service contract:

1. scaffolding silently accepts pipeline names that the canonical schemas reject;
2. `init pipeline` ignores `airflow.enabled` written by `init project`;
3. safe-sample mode does not accept the same pipeline ID/path references as
   `check`, `preview`, and `test`;
4. the public README and First DAG journey do not lead with a completely
   successful five-command offline path.

This change adds one canonical pipeline identity value object, one project
configuration reader, and one source-reference/snapshot boundary shared by all
beginner commands and the canonical compiler. It keeps the platform-gated live
sample separate and adds a
cross-artifact quality-authority regression matrix for the false-success class
found after v0.73.0.

## Personas and customer journey

| Persona | Goal | Current pain | Success signal |
|---|---|---|---|
| New data engineer | First checked DAG without platform access | Redundant flags, mixed ID/path forms, optional sample blocker | Five offline commands exit `0` |
| Data engineer | Request a safe live sample | `run` alone requires a filesystem path | The same pipeline ID works across commands |
| Platform engineer | Override project Airflow policy | Pipeline flag is required but no explicit disable exists | `--airflow` / `--no-airflow` override deterministically |
| Maintainer | Prevent another quality false-success family | Row authority has route-specific tests only | One fail-closed artifact-authority matrix |

The canonical offline journey is:

```bash
dpone init project --airflow
dpone init pipeline orders_daily --recipe mssql-to-clickhouse-incremental
dpone check orders_daily
dpone airflow preview orders_daily
dpone test orders_daily
```

All commands must exit `0` without Airflow, Vault, Kubernetes, databases,
connector SDKs, or credentials. Live sample remains a separate optional action:

```bash
dpone run orders_daily --sample 1000 --target temporary
```

Without the signed platform overlay, that command remains fail-closed with exit
`3`; it is not part of offline success.

## Scope

### In scope

- Canonical `PipelineId` parser and safe suggestion.
- Compiler propagation of canonical logical pipeline identity.
- Fail-before-write scaffolding for invalid pipeline IDs.
- Project-level Airflow default with explicit pipeline-level override.
- Shared ID/path resolution for check, preview, test, and safe-sample mode.
- One bounded, no-follow, digest-bound safe-sample source snapshot.
- ID-based generated next commands.
- Five-command offline First DAG and root README quickstart.
- Documentation/version consistency touched by this journey.
- Cross-artifact source-row-authority contract tests.

### Non-goals

- Renaming schema `workload_id`, pack URIs, or provider APIs.
- Moving platform commands to a new breaking CLI namespace.
- Making live sample run without platform authorization.
- Expanding hermetic v1 into SQL, transform, quality, or DLQ execution.
- Adding `get_parsing_context()` without a measured parse regression.
- Changing ordinary `dpone run <manifest>` path semantics.
- Editing `docs/airflow-self-service-public-contracts-v1.yaml`; old explicit
  `--airflow` invocations remain valid.
- Reserving schema-valid logical IDs such as `current`; deployment aliases and
  vendor-specific physical names remain separate projections.

## Public contract

### Pipeline identity

Canonical syntax remains the existing schema contract:

```text
^[a-z0-9][a-z0-9_-]{1,127}$
```

`PipelineId.parse(value)` accepts only an exact canonical string. It never
lowercases or rewrites the accepted value. Invalid input returns:

```yaml
schema: dpone.error.v1
code: DPONE_PIPELINE_ID_INVALID
stage: init_pipeline
severity: error
exit_code: 2
fixes:
  - id: use_suggested_pipeline_id
    safety: safe
    command: dpone init pipeline <safe-suggestion> ...
```

The raw invalid value is not echoed. A suggestion is advisory and is never
applied automatically. Suggestion normalization is deterministic: trim,
Unicode NFKD to ASCII, lowercase, replace non-identifier runs with `_`, require
an alphanumeric first character, ensure at least two characters, and truncate
to 128 characters.

### Project Airflow default

`init pipeline` uses tri-state precedence:

```text
explicit --airflow
  > explicit --no-airflow
  > dpone.yaml airflow.enabled
  > false when dpone.yaml is absent
```

- Both explicit flags are mutually exclusive.
- Existing `--airflow` commands remain compatible.
- `init project` still requires explicit `--airflow` to enable Airflow.
- Missing `dpone.yaml` preserves legacy non-Airflow behavior.
- An explicit override does not read the project default and therefore
  preserves existing behavior even when unrelated project defaults are invalid.
- An existing malformed, unsafe, oversized, duplicate-key, or wrong-schema
  `dpone.yaml` fails before scaffold writes with exit `2` when the project
  default is needed.
- Only `schema`, `airflow.enabled`, and existing project fields are read; no
  network, secret, or environment lookup occurs.

### Pipeline references

The existing project-confined resolver is authoritative:

```text
orders_daily
pipelines/orders_daily
pipelines/orders_daily/pipeline.yaml
```

All resolve to `pipelines/orders_daily/pipeline.yaml`. Bare IDs are validated by
`PipelineId`. Explicit paths remain project-confined and no-follow; canonical
metadata validation remains the compiler's responsibility. A bare requested ID
must equal the compiled `metadata.id`; mismatch fails before artifact creation.

Safe-sample mode resolves and reads one bounded no-follow source snapshot,
computes its digest, compiles that snapshot once, and passes the resulting
identity/path/digest through policy, planning, deployment, handoff, and evidence
before creating release/deployment, handoff, evidence, or runtime directories.
Ordinary `dpone run` is unchanged.
Reference failures return the existing path/safety family and never fall back
to a basename.

### CLI and compatibility

- Add `--no-airflow` to `dpone init pipeline`.
- Make `--airflow` optional for a pipeline when project policy enables it.
- Keep `--airflow` accepted indefinitely under the v1 compatibility contract.
- Change beginner success/next text to use the pipeline ID.
- Provider contracts and `dpone.self-service-certification.v1` remain
  unchanged. Safe-sample plan/evidence v1 schemas gain only additive, nullable
  source-snapshot fields so old documents remain readable. The strict v1
  handoff envelope remains unchanged and binds the snapshot-bearing plan by
  path, SHA-256, and byte length. Live execution of an old plan without the
  snapshot fails closed and requires regeneration.

## Detailed algorithms

### Pipeline initialization

```text
parse CLI without side effects
parse PipelineId
if invalid:
    return DPONE_PIPELINE_ID_INVALID exit 2
resolve Airflow tri-state:
    if explicit flag: use it without reading dpone.yaml
    elif dpone.yaml missing: false
    else:
        confined bounded read
        strict unique-key YAML parse
        require dpone.project.v1 and boolean airflow.enabled
apply existing no-clobber scaffold transaction
```

No directory or temporary file may be created before identity and project
policy validation succeeds.

### Shared pipeline reference

```text
if target is project root:
    preserve command-specific project behavior
elif target is a bare segment:
    PipelineId.parse(target)
    relative = pipelines/<id>/pipeline.yaml
else:
    normalize explicit project-relative/absolute-in-root path
    append pipeline.yaml for a directory
confine path under project root
reject parent traversal, backslash ambiguity, symlinks, and basename fallback
```

`check`, `preview`, `test`, and safe-sample call this resolver rather than
maintaining command-specific candidate lists.

After compilation, a bare requested ID is compared with the canonical
`PipelineId` propagated by `AuthoringCompilation`. A mismatch returns
`DPONE_PIPELINE_ID_MISMATCH`.

### Safe-sample source snapshot

```text
resolve requested ID/path
bounded no-follow read of the primary source
compute sha256 over exact bytes
parse/compile once
verify requested ID against compiled metadata.id
pass immutable source snapshot to policy/planning/deployment/copy preparation
revalidate digest immediately before the first external side effect
record logical pipeline ID, relative source path, and source digest in handoff/evidence
```

The runtime must not infer identity from a temporary table name. Hyphen
replacement or other backend naming rules are physical-name projections and
must never change the logical `PipelineId`. The schema-valid logical ID
`current` remains valid; command-specific deployment aliases may reject or
escape that value without changing the logical grammar.

### Quality-authority matrix

For each supported extraction artifact family, construct a completed artifact,
project it through `source_snapshot`, and assert:

```text
completed exact rows -> exact QualityProbeSnapshot.row_count
estimate only         -> None
invalid authority     -> None or typed fail-closed error, never coercion
wrapper/enrichment    -> same live authority or explicit forwarded authority
```

Initial matrix covers in-memory rows, streaming rows, prepared-source wrapper,
file/native transfer, slice-evidence columnar windows, and SQL-query artifacts.
The matrix is an architectural regression test, not route certification.

## Architecture

| Component | Responsibility | Dependency direction |
|---|---|---|
| `PipelineId` | Validate one logical pipeline identity and suggest corrections | Pure manifest contract |
| Pipeline source resolver | Resolve ID or path under one project root | Depends on `PipelineId` and confined files |
| Project configuration reader | Bounded read of `dpone.yaml` and pure default projection | `dpone.manifest`; no readiness/CLI dependency |
| Authoring compiler | Validate and propagate optional canonical `PipelineId` | Depends on pure identity contract |
| Source snapshot | Bind canonical path, bytes digest, compilation, and requested ID | Application boundary over confined manifest I/O |
| CLI facades | Preserve explicit-option state and render results | Depend on application services |
| Quality-authority matrix | Verify cross-artifact invariant | Tests only |

No generic identifier framework is introduced. Connector IDs, DAG IDs, and
Kubernetes projections retain their own existing contracts.

## State, failure, and recovery

- Validation failure: no authoring files created.
- Scaffold conflict: existing compare-and-create rollback semantics unchanged.
- Project defaults are resolved from one stable descriptor snapshot; a
  concurrent later replacement affects only the next invocation.
- Missing project config: legacy `airflow=false`.
- Invalid project config needed for defaulting: fail closed; an explicit flag
  remains an independent compatibility override.
- Safe-sample reference failure: no cache/handoff/evidence side effect.
- Safe-sample source digest drift before its first external side effect: fail
  closed without runtime execution.
- Existing projects: no migration; explicit `--airflow` keeps old behavior.
- Noncanonical self-service metadata must be migrated explicitly across source,
  domain workload keys, tests, references, and cached artifacts; no dual
  mapping or automatic rename is provided.
- Rollback: revert this patch; all old explicit commands remain executable.

## Security and privacy

- Raw invalid identity and unsafe OS errors are not emitted.
- Project config uses bounded, unique-key, no-follow reads.
- Safe-sample source is bounded, no-follow, and digest-bound; it is not reread
  independently by policy and runtime-preparation services.
- Suggestions contain only canonical identifier characters.
- Reference resolution performs no network, DB, Vault, Airflow, or Kubernetes
  access.
- Quality tests never serialize row values.

## Market comparison

Checked 2026-07-23 against official primary documentation.

| System | Relevant observation | Decision |
|---|---|---|
| Apache Beam YAML | Whole-pipeline tests use mocked inputs and expected outputs; its beginner YAML flow separates local tests from external execution | Adopt a successful credential-free test in the default journey; do not add Python UDF execution to hermetic v1 |
| Astronomer Cosmos | `DbtDag` and `DbtTaskGroup` keep one project configuration mental model while allowing explicit overrides | Adopt project default plus explicit local override; retain dpone static build/scheduler separation |
| gusty | File-oriented references reduce day-one ceremony | Adopt consistent ID/path references; reject recursive implicit filesystem discovery |
| dlt, Informatica, Airbyte, Fivetran, Pentaho, SSIS | N/A for this compatibility bug: they do not define dpone's pipeline-ID/project-policy boundary | No pattern imported |

Sources:

- https://beam.apache.org/documentation/sdks/yaml-testing/
- https://astronomer.github.io/astronomer-cosmos/getting_started/core-concepts.html
- https://pipeline-tools.github.io/gusty-docs/

No superiority claim is introduced.

## Test and certification plan

| Layer | Scenarios |
|---|---|
| Unit | canonical/invalid IDs, deterministic safe suggestions, project-policy precedence |
| CLI | both option orders, mutual exclusion, no-write invalid cases, JSON/text/exit codes |
| Reference | ID/directory/file equivalence for check, preview, test, safe-sample |
| Security | traversal, symlink, duplicate YAML key, oversized config, control characters |
| Journey | five offline commands all exit `0` in a fresh temporary project |
| Quality | artifact-family row-authority matrix and estimate/invalid negatives |
| Docs | README, First DAG, hub, generated-reference compatibility |
| Broad | standard non-live, architecture, docs, package and Airflow matrix gates |

Live Airflow/Kubernetes/Vault/MSSQL/ClickHouse remains `UNVERIFIED` unless an
approved environment is supplied.

## Documentation plan

- Root README leads with the five-command offline self-service journey.
- First Airflow DAG uses the same commands and labels live sample optional.
- Airflow hub says five successful offline commands, not a platform-gated fifth.
- Hermetic-test page uses ID form consistently.
- Advanced/platform documentation remains available but outside the beginner
  quickstart.

## Agent execution plan

| Role | Owned paths | Shared owner |
|---|---|---|
| Identity implementer | pipeline identity/source resolver + focused tests | Integrator |
| CLI policy implementer | init CLI/project policy + focused tests | Integrator |
| Reference implementer | safe-sample/test resolver parity + focused tests | Integrator |
| Docs/UX implementer | README/First DAG/hub/docs tests | Integrator |
| Test certifier | quality-authority matrix | Integrator |
| Integrator | schemas, shared fixtures, CHANGELOG, nav, final reconciliation | Codex |

Architecture is frozen by
[`ADR 0029`](adr/0029-canonical-pipeline-identity-and-reference.md).

## Approval checklist

- [x] User problem and CJM are clear.
- [x] Algorithm and failure semantics are implementable.
- [x] Compatibility is explicit.
- [x] Architecture and alternatives are bounded.
- [x] Relevant market research uses primary sources.
- [x] No unmeasured superiority claim is made.
- [x] Tests, docs, rollout, and rollback are defined.
- [x] Maintainer requested implementation.

## Implementation evidence

The implementation is validated in
[`test_artifacts/airflow-self-service-ux-closure-v07317/validation-report.md`](../test_artifacts/airflow-self-service-ux-closure-v07317/validation-report.md).
The local non-live contract passes; Airflow/Kubernetes/Vault and live
MSSQL/ClickHouse certification remain explicitly `UNVERIFIED`.
