# Developer guide: manifest sparse paths

`dpone manifest sparse-paths` is a manifest control-plane feature. It helps
GitOps and scheduler integrations fetch the files required for a manifest while
keeping scheduler code unaware of dpone manifest internals.
The sparse checkout contract is deliberately shared by CLI, CI, and future
scheduler helpers.

## Taxonomy

- `SparsePathPolicy` owns path safety, workload-root scoping, repo-relative
  rendering, and trailing slash directory output.
- `ManifestSparsePathDiscovery` reads raw YAML through an injected reader and
  discovers only manifest-owned dependency fields.
- `SparsePathEntry`, `SparsePathIssue`, and `SparsePathReport` are the stable
  domain contract used by CLI, CI, docs, and future scheduler helpers.
- `ManifestSparsePathPlanner` composes policy, discovery, standard optional
  includes, and blocker/warning aggregation. It is deliberately separate from
  discovery so manifest graph traversal does not become a god module.
- `ManifestSparsePathsService` composes `AppContext`, the YAML codec, the
  filesystem port, and the planner.
- `ManifestSparsePathsView` is the service-layer JSON DTO. Renderers consume the
  view and never parse manifests.

Do not add Airflow-specific logic to discovery, services, or CLI handlers.
Airflow, `KubernetesPodOperator`, GitHub Actions, and local shell scripts should
all consume the same sparse path contract.

## Dependency rule catalog

Rules must stay explicit. Do not include arbitrary keys named `path`, because
manifest schemas also contain JSONPath and nested-normalization paths.

Current rules:

- `convention` and `conventions` include custom YAML paths when the value is not
  a known built-in convention.
- `registry` and `registries` include registry YAML paths relative to the
  manifest directory.
- `depends_on.path` includes the file part and strips `#selector`.
- `#selector` local references do not add a path.
- `depends_on.group` produces a warning because no file path is encoded.

When adding a first-class SQL/template/file field, add one rule to the catalog,
unit tests for discovery and path safety, CLI output tests, and user docs.

## Module boundaries

- `dpone.commands.manifest.sparse_paths_cmd` contains argparse and service
  invocation only.
- `dpone.services.manifest.sparse_paths_service` performs use-case orchestration
  and DI.
- `dpone.manifest.sparse_paths_policy` and
  `dpone.manifest.sparse_paths_discovery` contain reusable domain logic.
- `dpone.manifest.sparse_paths_planner` converts raw command/service inputs
  into a stable report without importing scheduler or runtime code.
- `dpone.cli_render.manifest.sparse_paths` renders sparse text only.

The command must remain credential-free and must not import runtime connectors,
Airflow, Git clients, or optional database SDKs.

## Runbook

1. Add a failing unit test in `tests/test_manifest_sparse_paths.py` before
   changing discovery or path policy.
2. Add or update CLI tests in `tests/test_cli_manifest_sparse_paths_command.py`.
3. Update this page, [Manifest sparse paths](manifest-sparse-paths.md), the CLI
   reference, and architecture docs in the same PR.
4. Run:

```bash
uv run pytest tests/test_manifest_sparse_paths.py tests/test_cli_manifest_sparse_paths_command.py tests/test_manifest_sparse_paths_docs_contract.py -q
uv run dpone docs update-cli-reference --check
uv run dpone docs check-import-rules
HEAD_SHA="$(git rev-parse HEAD)"; BASE_SHA="$(git merge-base "$HEAD_SHA" origin/master)"; if [ "$BASE_SHA" = "$HEAD_SHA" ]; then BASE_SHA="$(git rev-parse "${HEAD_SHA}^")"; fi
uv run dpone docs check-module-size --baseline docs/module_size_baseline.json --base-ref "$BASE_SHA" --head-ref "$HEAD_SHA"
uv run mkdocs build --strict
```

## Quality guardrails

Keep modules small and focused. If discovery grows beyond manifest-owned path
rules, split rule extraction from traversal rather than expanding the service or
planner.
The CI module-size, layer-metrics, import-rules, and generated developer metrics
checks must stay green for every sparse-paths change.
