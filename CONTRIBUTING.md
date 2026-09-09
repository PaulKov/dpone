# Contributing to dpone

Thanks for helping make `dpone` better. The project values clean, documented,
reusable code with clear dependency boundaries and evidence-backed behavior.

## Local setup

```bash
uv sync --all-extras
pre-commit install
```

## Repository instructions

Before making changes, read the applicable `AGENTS.md` files. The root file
contains project-wide invariants; nested files add rules for production code,
tests, documentation, and the Airflow pack.

Material public features and contract changes require an approved specification
based on [the feature design template](docs/agent-templates/feature-design-spec.md).
See [Agent-assisted development](docs/agent-development.md),
[Engineering standards](docs/engineering-standards.md), and
[Feature design standard](docs/feature-design-standard.md).

## Development loop

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live" -n auto
```

Generate a risk-aware check plan for the current diff:

```bash
uv run python tools/agent_policy/select_checks.py --base-ref origin/master
```

## TDD workflow

`dpone` follows red-green-refactor across the test pyramid
(unit -> contract -> integration -> e2e); the full policy with the
layer-by-layer requirements lives in
[docs/testing/index.md](docs/testing/index.md#tdd-policy):

1. **Red** — write a failing test at the lowest layer that expresses the
   requirement (a bug fix starts with a regression test).
2. **Green** — implement the minimal change; run the focused test, then the
   affected suites.
3. **Refactor** — clean up under a green bar; architecture fitness,
   import-rules, and module-size gates must stay green.

Data-movement changes (sources, sinks, strategies, state) additionally require
an `integration_*` case that moves real rows against the local Docker stack
(`docker compose -f docker/docker-compose.integration.yml up -d`) and checks
parity — see [docs/testing/backfill-integration.md](docs/testing/backfill-integration.md)
for the reference pattern. The coverage gate is a ratchet: raise `fail_under`
in `pyproject.toml` as the baseline grows, never lower it.

A mocked integration check does not certify a live route. Record unavailable
live evidence as `SKIP` or `UNVERIFIED`, never `PASS`.

## Feature design and market research

A feature plan must explain the actual algorithm, data/control flow, public
contract, identity/state/evidence ordering, retries, failure recovery,
architecture, tests, documentation, rollout, and rollback. Plans that only list
files or classes are not approvable.

Compare only relevant capabilities with current official primary sources for dlt,
Informatica, Airbyte, Fivetran, Pentaho, SSIS, gusty, Astronomer Cosmos, and
Apache Beam. Mark irrelevant systems `N/A`; record versions and dates; separate
facts from inference. A claim that dpone is better needs a scenario, metric,
target, reproducible procedure, evidence artifact, and stated limitation.

## Pull requests

Before opening a PR:

- Keep changes focused and documented.
- Update user-facing docs when CLI, manifest, or runtime behavior changes.
- Add or update tests for behavior changes.
- Do not commit secrets, tokens, service-account JSON, or live credentials.
- Prefer canonical imports under `dpone.runtime`, `dpone.manifest`, `dpone.dag`,
  `dpone.contracts`, `dpone.ports`, and `dpone.adapters`.
- Link the approved feature specification when one is required.
- Report checks as `PASS`, `FAIL`, `SKIP`, `N/A`, or `UNVERIFIED` with artifact
  paths; skipped or mocked evidence is not a pass.
- Include documentation/CJM, compatibility, migration, and operational impact.

For parallel work, use separate worktrees and an explicit task contract from
`docs/agent-templates/agent-task-contract.yml`. Writers must have disjoint path
ownership and one integrator must own shared semantic files.

## Code style

- Follow [Engineering standards](docs/engineering-standards.md).
- Keep modules small and cohesive; the quality-budget source of truth is
  `docs/benchmarks/quality_budgets.yml` (`max_sloc` and graph budgets included).
- Prefer dependency injection over hard-coded global clients.
- Keep public UX self-service: helpful errors, examples, and docs matter as much
  as implementation.
- Preserve compatibility shims unless removal is documented in
  `docs/compatibility.md`.
- Do not create god modules, case-specific architecture, speculative frameworks,
  or mechanical file splits that only satisfy a metric.

## Release changes

Use [Release](docs/release.md) and
[Agent release protocol](docs/agent-release-protocol.md). A release decision is
bound to an exact frozen commit; a new commit invalidates affected evidence.
