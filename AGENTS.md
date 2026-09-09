# dpone repository agent contract

## Mission

Build `dpone` as a declarative, connector-neutral, production-grade ETL/ELT
framework. Prefer correctness, deterministic behavior, backward compatibility,
auditability, recoverability, and understandable user experience over short-term
implementation speed.

Make the smallest defensible change that satisfies an approved contract. Never
claim that behavior is production-ready without current evidence from the exact
commit and environment being assessed.

## Instruction and source-of-truth order

Read only the material relevant to the task. Use this order when rules overlap:

1. the nearest applicable `AGENTS.md`;
2. approved feature specification or issue acceptance criteria;
3. architecture decisions in `docs/adr/` and `docs/adr-index.md`;
4. normative project standards listed below;
5. implementation documentation and tests;
6. current code behavior, when it does not contradict a higher-level contract.

Primary references:

- architecture: `docs/architecture.md` and `docs/import-rules.md`;
- engineering quality: `docs/engineering-standards.md`,
  `docs/quality-tooling.md`, and `docs/benchmarks/quality_budgets.yml`;
- feature planning: `docs/feature-design-standard.md`;
- testing: `docs/testing/index.md` and `docs/testing/overview.md`;
- documentation and user journeys: `docs/documentation-standard.md`;
- agent coordination: `docs/agent-development.md`;
- releases: `docs/release.md` and `docs/agent-release-protocol.md`;
- compatibility: `docs/compatibility.md`;
- connectors and routes: `docs/connector-sdk.md`,
  `docs/source-sink-matrix.md`, and `docs/connector-certification.md`;
- nested data: `docs/nested-normalization.md`;
- orchestration integrations: `docs/airflow-pack-provider.md` and `docs/dbt.md`.

Do not scan all documentation by default. Use the change-to-context table in
`docs/agent-development.md`.

## Non-negotiable invariants

- Silent data loss, duplication, corruption, false success, and false
  certification are release blockers.
- New code uses canonical packages under `dpone.runtime`, `dpone.manifest`,
  `dpone.dag`, `dpone.contracts`, `dpone.ports`, and `dpone.adapters`.
- Compatibility shims adapt or re-export only. Do not add new domain policy to
  legacy namespaces.
- CLI, Python API, manifest/schema, state/checkpoint, evidence, artifacts,
  public imports, and documented behavior are public contracts.
- Keep CLI commands, framework adapters, and connector adapters thin. Put
  reusable decisions in cohesive domain, policy, application, or runtime
  services behind capability-oriented interfaces.
- Dependencies are injected at composition roots. Avoid hidden global clients,
  service locators, import-time I/O, and vendor SDK imports on base import/help
  paths.
- Generated evidence is changed through its producer. Never hand-edit an
  artifact to manufacture a passing result.
- A skipped, mocked, stale, or unavailable live check is not a pass. Report it
  explicitly as `SKIP`, `N/A`, or `UNVERIFIED` with the reason.
- Live integration work requires an explicitly approved environment and
  credentials. Never print, persist, or commit credentials.

## Before implementation

### Release work

Use `docs/release.md` for current publication authority and
`docs/agent-release-protocol.md` for scoped evidence. Distinguish preparation,
authorized publication, and read-only retrospective verification before acting.
The external `dpone-release-controller` is the sole ordinary PyPI publisher;
source-repository tag workflows and historical broker designs are not an
alternative upload path. A readiness GO or documentation request does not
authorize publication, retagging, or provider changes. Do not republish an
existing version to obtain verification evidence. Classify R1–R9 applicability
without weakening source PR checks or promoting unrelated CI-shadow backlog
into a release blocker. Preserve unfinished backlog truthfully.

### Feature design

A written feature specification is mandatory before production-code changes
when work introduces or materially changes any public feature, contract,
connector capability, load strategy, state/evidence behavior, user journey,
Airflow/dbt integration, or cross-layer architecture.

Use `docs/agent-templates/feature-design-spec.md`. The plan must explain:

- the user problem, personas, and end-to-end journey;
- exact public behavior and non-goals;
- data flow and step-by-step algorithm;
- state machine, identity, ordering, transactions, retries, replay, rollback,
  concurrency, and failure semantics where relevant;
- components, interfaces, dependency direction, and migration strategy;
- test, certification, observability, documentation, and rollout plans;
- relevant market comparison and the measurable axis on which dpone intends to
  improve.

Do not begin implementation until the specification is marked `APPROVED`,
unless the task is an isolated bug fix, documentation correction, or internal
refactor with no public-contract change. Those still require a concise impact
and validation plan.

Market research must use current official primary sources. Compare only systems
relevant to the capability among dlt, Informatica, Airbyte, Fivetran, Pentaho,
SSIS, gusty, Astronomer Cosmos, and Apache Beam. Mark irrelevant systems `N/A`
with a reason. Record source date/version, adopted and rejected patterns, and
separate fact from inference. Do not make an unqualified claim that dpone is
"better"; define a scenario, metric, target, procedure, and evidence artifact.

## Implementation protocol

1. Classify the change and identify affected public contracts.
2. Trace the real execution path, existing tests, docs, ADRs, generated outputs,
   and compatibility shims before editing.
3. Use red-green-refactor at the lowest useful test layer.
4. Keep the diff scoped; do not mix unrelated cleanup.
5. Preserve backward compatibility unless a breaking change is explicitly
   approved and includes migration guidance, deprecation policy, tests, docs,
   and an ADR when architectural.
6. Update documentation, examples, schemas, generated references, and
   `CHANGELOG.md` when public behavior changes.
7. Run focused checks first, then the required broader gate.
8. Have a fresh-context reviewer inspect correctness, compatibility, data-loss
   risk, tests, docs, and evidence before integration.

## Architecture and quality

Apply SOLID, DRY, KISS, dependency injection, and clean-code principles as
operationalized in `docs/engineering-standards.md`; slogans alone are not
acceptance criteria.

`docs/benchmarks/quality_budgets.yml` is the single source of truth for module
size and graph budgets. Current global hard limits include `max_sloc: 400` and
`max_avg_clustering: 0.180`. Do not duplicate or reinterpret these metrics.
Use the existing `dpone docs check-module-size`, `check-layer-metrics`, and
benchmark tooling. Existing debt may not grow.

A file under a threshold can still be a god module. Split by responsibility and
stable variation point, not by arbitrary line count. Do not create speculative
abstractions, generic plugin systems for one implementation, or mechanical
`part_1.py`/`part_2.py` decompositions.

## Parallel-agent protocol

Parallelize read-heavy work first. For non-trivial changes, delegate independent
analysis to explorer, architect, test/certification, and docs/UX roles, then let
one integrator reconcile conclusions.

Parallel writers require separate worktrees and an explicit task contract from
`docs/agent-templates/agent-task-contract.yml`. Each writer receives disjoint
`owned_paths`, plus `read_only_paths` and `forbidden_paths`.

Only the integrator owns shared semantic files unless the task contract says
otherwise, including:

- `pyproject.toml`, `uv.lock`, `CHANGELOG.md`, and `mkdocs.yml`;
- shared schemas, registries, factories, and compatibility facades;
- `.github/workflows/**` and release/certification indexes;
- shared fixtures such as `tests/conftest.py`.

An agent must stop and report a path-ownership conflict rather than editing
outside its contract.

## Validation

Use `uv run python tools/agent_policy/select_checks.py --base-ref origin/master`
to produce a change-aware validation plan. At minimum for normal Python changes:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics --baseline docs/layer_metrics_baseline.json
HEAD_SHA="$(git rev-parse HEAD)"
BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"
if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size \
  --baseline docs/module_size_baseline.json \
  --base-ref "$BASE_SHA" \
  --head-ref "$HEAD_SHA"
uv run pytest -m "not integration_live" -n auto --dist loadfile
```

For documentation changes:

```bash
uv run dpone docs check-docs
uv run pytest tests/test_docs_language_contracts.py -q
uv run mkdocs build --strict
```

For packaging or release changes:

```bash
uv build
uv build packages/dpone-native-accel --out-dir dist
uv build packages/dpone-airflow-pack --out-dir dist
uv build packages/apache-airflow-providers-dpone --out-dir dist
uv tool run twine check dist/*
```

Run focused tests before these broad gates. Do not run live profiles unless the
scope and environment explicitly require them.

## Completion report

Every agent completion must state:

1. what changed and why;
2. public-contract and compatibility impact;
3. tests/checks executed with `PASS`, `FAIL`, `SKIP`, or `N/A`;
4. evidence/artifact paths;
5. documentation and CJM impact;
6. remaining risk, uncertainty, and follow-up work;
7. whether the result is ready for review, merge, or release.
