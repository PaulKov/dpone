# VS Code setup

Recommended VS Code extensions:

- Python
- Ruff
- YAML
- Markdown All in One
- GitHub Pull Requests

## Recommended workflow

```bash
uv sync --all-extras
uv run pytest tests/test_docs_language_contracts.py -q
uv run ruff check .
```

Use the integrated terminal for targeted tests and keep generated reports open next to the manifest you are editing.

## Helpful files

- `pyproject.toml` - package metadata and optional extras.
- `mkdocs.yml` - public documentation navigation.
- `docs/` - OSS documentation source.
- `examples/` - runnable manifest examples.
- `tests/` - unit, contract, integration, and documentation quality gates.
