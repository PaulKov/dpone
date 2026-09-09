# dpone pre-release checklist

- Release: `v0.17.0`
- Release type: `minor`
- Passed: `True`
- Generated at: `2026-06-19T06:32:07.775399+00:00`

| check | required | status |
|---|---:|---|
| `cli_help_surface` | `True` | pass |
| `cli_output_contracts` | `True` | pass |
| `run_cli_manifest` | `True` | pass |
| `run_python_api_manifest` | `True` | pass |
| `nested_hierarchical_identity` | `True` | pass |
| `nested_parent_child_integrity` | `True` | pass |
| `source_sink_strategy_matrix` | `True` | pass |
| `source_sink_artifacts` | `True` | pass |
| `docker_live_routes` | `True` | pass |
| `contracts_guardrails` | `True` | pass |
| `documentation_yaml_examples` | `True` | pass |
| `documentation_links` | `True` | pass |
| `documentation_mkdocs` | `True` | pass |
| `ci_cd_quality` | `True` | pass |
| `package` | `True` | pass |

## Blockers

- none

## Runbook

1. Run the missing or failing gate and regenerate this checklist.
2. Do not tag or publish a minor/major release while this report is red.
3. Attach this checklist to `release-evidence-pack` as `pre_release_checklist` when publishing.
