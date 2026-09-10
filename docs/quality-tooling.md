# Quality tooling

`dpone` quality tooling protects code, documentation, packaging, and runtime contracts.

## Core commands

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -m "not integration_live"
```

## Documentation checks

```bash
uv run pytest tests/test_docs_language_contracts.py -q
uv run mkdocs build --strict
```

Public MkDocs pages must be English-only and should use consistent link labels.

## Import checks

Production modules must use ordinary definitions, explicit dependency injection
and owned composition rather than replace another module's classes/functions.
Run the finite source policy before integration:

```bash
uv run python tools/agent_policy/import_mutation_gate.py
```

The gate parses `src` and maintained `packages/*/src` without importing them.
It checks import-executed attribute mutations, simple and tuple aliases, local
initializer helpers and constructors, classmethod argument binding, eager
annotations, registry overlays and recognized dynamic installation. Direct helper
returns and global aliases retain provenance. Deferred annotations are not
treated as executed code. Nonregular source files, including FIFOs, are rejected
before reading. Output is bounded,
deterministic JSON with `status`, `files_scanned`, `findings` and
`omitted_findings`; findings contain relative path, line and stable code without
source payloads. Findings, unreadable/malformed inputs and analysis limits fail
with exit 1. CLI misuse returns exit 2. Instance data injection, subclass
definitions and normal own-module exports remain valid.

This is a syntactic gate, not a proof for arbitrary Python execution. Unknown
calls, cross-file call graphs, reflection, metaclasses and descriptors require
manual review. Complex indirect return values and argument unpacking are
additional provenance limits. Do not add directory allowlists for mutation
installers. Replace the mechanism with an owned adapter or an injected service and add a regression
test before removal. Test-only hostile startup fixtures remain outside production
inventory. Archive startup-hook hygiene is a separate metadata check; passing
this source policy does not attest an installed environment.

```bash
uv run python tools/check_import_rules.py
uv run pytest tests/test_lazy_imports.py -q
```

Use these checks whenever adding optional dependencies or new connectors.

## Generated metrics

```bash
uv run dpone docs update-dev-metrics --check
uv run dpone docs check-generated-references
uv run dpone docs check-compatibility
uv run dpone docs check-import-rules
uv run dpone docs check-layer-metrics
uv run dpone docs check-architecture-fitness
HEAD_SHA="$(git rev-parse HEAD)"
BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"
if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size \
  --baseline docs/module_size_baseline.json \
  --base-ref "$BASE_SHA" \
  --head-ref "$HEAD_SHA"
uv run dpone docs check-module-size --package packages/dpone-airflow-pack/src/dpone_airflow_pack --no-baseline
uv run dpone docs check-module-size --package packages/apache-airflow-providers-dpone/src/airflow/providers/dpone --no-baseline
uv run dpone docs check-airflow-public-contracts
uv run dpone docs update-airflow-public-contract-reference --check
```

Run without `--check` when intentionally refreshing generated sections.
When adding new Python files, stage or commit those files before running
`dpone docs update-dev-metrics`; the generator intentionally uses
`git ls-files` so local scratch files and generated artifacts do not affect CI
metrics.

`check-module-size` reports LOC and SLOC per Python module. In authoritative
baseline mode, the version-2 debt baseline is a no-headroom ratchet: every
entry records the exact reviewed LOC and SLOC, growth fails, and a smaller
module requires the checked-in cap to be lowered. New warning-level debt, stale
entries, expired target dates, and all hard-limit violations fail. The current
defaults warn above 450 LOC or 350 SLOC and fail above 600 LOC or 400 SLOC.

Baseline governance requires explicit full base and head commit SHAs. The
command verifies Git ancestry and the checked-out head; missing history fails
closed. The source refactor must be committed before the writer runs. To persist
a real improvement, rerun the same command with `--write-baseline`; review the
atomic candidate, amend or commit the baseline, recompute the head SHA, and
rerun without the writer. The write intentionally returns exit code `2` because
the old head cannot authorize its newly written bytes. New debt cannot be
created by the writer and requires a separate Accepted ADR. Follow the complete
[module-size ratchet runbook](module-size-ratchet.md) and see
[ADR 0047](adr/0047-module-size-debt-ratchet.md).

Use `--package <python-dir>` for focused guards outside `src/dpone`, such as
repository-local tooling and dedicated test scopes. Agent-control changes must
keep both `tools/agent_policy` and `tests/agent_policy` below their stricter
CI budget. The Airflow provider is a separate distribution, so CI checks its
package explicitly; adding it only to the root `src/dpone` baseline would leave
provider debt invisible.

The explicit package checks above currently use `--no-baseline`: they fail on
hard limits and report warning debt as advisory, but they do not authorize the
root ratchet and cannot be used with `src/dpone`. Their JSON report declares
`debt_model: none`. Extending exact-cap ledgers to those distributions is a
separate policy migration, not an implicit property of this root baseline.

## Package checks

```bash
uv build
twine check dist/*
```

For release candidates, also run fresh virtual-environment smoke installs for base, selected extras, and `dpone[full]`.
