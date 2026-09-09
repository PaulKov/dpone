# Developer workflow

This workflow keeps `dpone` changes small, reviewable, and production-safe.

## 1. Load repository instructions

Read the root and nearest applicable `AGENTS.md`. For agent-assisted work, see
[Agent-assisted development](agent-development.md). Material feature and public-
contract changes require an approved specification under
[Feature design standard](feature-design-standard.md).

## 2. Start from the contract

Before writing runtime code, define or update the user-facing contract:

- Manifest shape.
- CLI and Python API behavior.
- Connector capability flags and route/strategy support.
- State, identity, schema evolution, quality, reconciliation, and evidence.
- Documentation, examples, runbooks, and customer journey.
- Compatibility, migration, rollout, and rollback.

Use [the feature design template](agent-templates/feature-design-spec.md) when the
change is material. The plan must explain the step-by-step algorithm and failure
semantics, not only proposed files and classes.

## 3. Add tests first

Use a targeted failing test before changing implementation code.

Recommended sequence:

```bash
uv run pytest tests/test_<area>.py -q
```

Then implement the smallest change that passes the test.

## 4. Keep architecture and optional imports clean

Follow [Engineering standards](engineering-standards.md) and the checked-in
quality budgets. Connector SDKs must not be imported during `import dpone` or
`dpone --help` unless the connector is actually executed. Add optional import
tests when introducing a new extra.

Keep adapters thin, inject dependencies at composition roots, and avoid adding
domain policy to compatibility namespaces.

## 5. Update documentation with the code

Apply [Documentation and user-journey standard](documentation-standard.md).
Every public connector or strategy change should update:

- Connector docs.
- Source/sink guide when applicable.
- Load strategy docs.
- Type mapping matrix if schemas are affected.
- Integration-test and operations runbooks.
- CLI/Python examples, architecture/data-flow diagrams, and the first-time-user
  journey.

## 6. Run the focused gate

Generate a change-aware plan:

```bash
uv run python tools/agent_policy/select_checks.py --base-ref origin/master
```

Then run focused checks:

```bash
uv run ruff check <changed paths>
uv run pytest <targeted tests> -q
```

For broader changes, run the default regression gate from
[Testing](testing/overview.md). Record unavailable live checks as `SKIP` or
`UNVERIFIED`, not `PASS`.

## 7. Produce artifacts for large changes

Benchmarks, live certification, wide-table compatibility checks, and release
proof should create machine-readable and human-readable artifacts in
`test_artifacts/`. Artifacts must identify the exact commit and environment.

## 8. Parallelize safely

Run code-path, architecture, test, and docs/UX analysis in parallel before
writing. Parallel writers use separate worktrees and a task contract from
[agent-task-contract.yml](agent-templates/agent-task-contract.yml). Writers have
disjoint owned paths; one integrator owns shared schemas, registries, dependency
files, workflows, MkDocs navigation, changelog, and common fixtures.

## 9. Prepare a pull request

A good pull request includes:

- Problem and solution summary.
- Approved specification when required.
- User-facing and compatibility changes.
- Algorithm and failure behavior.
- Test/certification evidence with honest statuses.
- Documentation and CJM links.
- Market comparison and measured differentiation when feature design requires it.
- Known limitations, rollback, or follow-up work.

## 10. Prepare a release

Use [Release](release.md) and
[Agent release protocol](agent-release-protocol.md). Audit R1-R9 against one
frozen commit and make a strict GO/NO-GO decision.

## Related docs

- [Agent-assisted development](agent-development.md)
- [Engineering standards](engineering-standards.md)
- [Feature design standard](feature-design-standard.md)
- [Documentation standard](documentation-standard.md)
- [Agent release protocol](agent-release-protocol.md)
- [Developer integrations runbook](developer-integrations-runbook.md)
- [Developer CI/CD guide](developer-ci-cd.md)
- [Import rules](import-rules.md)
- [Quality tooling](quality-tooling.md)
