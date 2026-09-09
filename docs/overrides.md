# Overrides

Variant C manifests are resolved from multiple layers into a deterministic final process.

## Resolution layers

The final process is assembled in this order:

1. Registry defaults.
2. Convention defaults.
3. File-level `vars`.
4. File-level `defaults`.
5. Item-level configuration.

Later layers override earlier layers according to the merge rules below.

## Merge rules

- Dictionary plus dictionary uses recursive deep merge.
- Scalar values replace previous values.
- Lists replace previous lists by default.
- Top-level `depends_on` and `transforms` use append semantics.

Append semantics allow defaults to define shared dependencies while individual tables add local dependencies without rewriting the full list.

## Template rendering

Templates can reference resolved values from the manifest context.

If a value consists only of `{{ expr }}`, the original type is preserved. For example, booleans and lists remain booleans and lists instead of becoming strings.

## Naming policy

Prefer putting naming templates in `naming` or `convention` sections instead of copying string patterns into every table.

```yaml
naming:
  landing_table: "landing__{{ source.type }}__{{ table.name }}"
```

## Debugging commands

Inspect the resolved config:

```bash
dpone manifest explain manifest.yaml --format md
```

Inspect why a field has a specific value:

```bash
dpone manifest explain manifest.yaml --show-provenance
```

Inspect task-level provenance:

```bash
dpone dag explain-task manifest.yaml --task-id landing__orders
```

## Practical guidance

- Put repeated connection, strategy, and task-group settings in `defaults`.
- Put naming policy in conventions.
- Use templates for metadata such as labels and table descriptions.
- Use registry entries for source metadata such as host, type, owner, and team.
- Keep item-level overrides for true per-table differences.
