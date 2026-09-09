# Developer Release RC finalizer

`ReleaseRcFinalizerService` is the release-level control-plane gate above route
RC orchestration and route release finalization. It validates a whole stacked
merge train plus release evidence. It is not a route runtime, a GitHub client,
or a command executor.

## Module boundaries

| Module | Responsibility |
| --- | --- |
| `dpone.ops.release_rc_finalizer` | Reads `merge_train.json`, normalizes PR/check payloads, reads evidence artifacts, calls policy, and writes JSON/Markdown. |
| `dpone.ops.release_rc_models` | Stable public contracts: merge train, PR, check run, finalizer check, decision, and report models. |
| `dpone.ops.release_rc_payloads` | Shared payload normalization used by both the Release RC collector and finalizer. |
| `dpone.ops.release_rc_policy` | Pure scoring, blockers, warnings, and next actions for version, merge train, PR state, checks, and artifacts. |
| `dpone.ops.artifact_validation` | Shared artifact pass/fail, missing, checksum, and summary normalization. |
| `dpone.commands.ops_parsers_core` | Argument parsing only. |
| `dpone.services.ops.command_handlers_release` | Thin command handler that delegates to the service. |

Do not add release RC business logic to CLI handlers. CLI code parses
arguments, calls `ReleaseRcFinalizerService`, emits JSON/Markdown, and maps
`passed` to exit code.

## GitHub boundary

Do not add GitHub API calls to `dpone.ops.release_rc_finalizer`.

The service intentionally consumes `merge_train.json` instead of calling
GitHub. This keeps OSS CI deterministic, testable, and credential-free. A
workflow, operator shell, or external integration may use `gh pr view` or
GraphQL to export the file before invoking the finalizer.
Use [Release RC collector](release-rc-collector.md) when the export should also
produce `release_rc_inputs.json` and a generated finalizer command.

## Policy extension rules

Add checks in this order:

1. Extend `ReleaseRcFinalizerCheck` only if the current fields cannot describe
   the new evidence.
2. Add pure policy logic in `ReleaseRcFinalizerPolicy`.
3. Add service tests for green and blocked behavior.
4. Add CLI tests only when arguments or exit behavior change.
5. Update user docs, developer docs, architecture, CI/CD docs, generated CLI
   reference, and quality metrics in the same PR.

## Stable JSON contract

The public schema is `dpone.release_rc_finalizer.v1`. Keep these fields stable:

- `release`
- `previous_release`
- `package_version`
- `mode`
- `passed`
- `level`
- `score`
- `blockers`
- `warnings`
- `next_actions`
- `merge_train`
- `artifacts`
- `checks`
- `json_path`
- `markdown_path`

Additive fields are allowed when tests and docs are updated.

## Testing

Focused tests:

```bash
uv run pytest tests/test_release_rc_finalizer.py tests/test_cli_release_rc_finalizer_command.py -q
```

Docs contract:

```bash
uv run pytest tests/test_release_rc_finalizer_docs_contract.py -q
```

Full quality gates:

```bash
uv run ruff check .
uv run ruff format --check .
uv run mypy --config-file mypy.ini
uv run pytest -q
uv run dpone docs check-docs
uv run mkdocs build --strict
```
