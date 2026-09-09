# Production profiles

Profiles provide safe defaults without hiding configuration. The first profile
is `production_safe`.

## `production_safe`

```yaml
profile: production_safe
```

Runtime defaults:

| Area | Default |
| --- | --- |
| Schema contract | `enforcement: strict` |
| Type inference | `enabled: true`, `conflict_policy: fail` |
| Schema evolution | `enabled: true` |
| Physical design | `enabled: true`, `apply: online` |
| Runtime evidence | `enabled: true`, `output_dir: .dpone/evidence/latest` |
| Lineage | `enabled: true`, `preset: standard` |

User-specified values win over profile defaults:

```yaml
profile: production_safe

sink:
  options:
    type_inference:
      sample_rows: 50000
```

`sample_rows` remains `50000`; profile defaults only fill missing values.

## Runtime integration

`LoadConfigRuntimeService.prepare()` applies profile defaults before
`ETLProcessor` runs lifecycle gates. This means CLI, Python API, and
orchestrated runs all see the same profile behavior.

## Runbook

| Need | Action |
| --- | --- |
| Certified production pipeline | Start with `profile: production_safe` and add explicit column contracts. |
| Dirty API source | Override `schema_contract.enforcement: quarantine` and monitor quarantine evidence. |
| Planned DDL window | Override `physical_design.apply: safe_window` only with approval evidence. |
| Experimental connector | Keep profile on, then relax one policy at a time with documented risk. |

Related docs:

- [Runtime lifecycle](run.md#runtime-lifecycle-gates)
- [Runtime data contracts](data-contract-runtime.md)
- [Physical DDL apply](physical-ddl-apply.md)
- [Certification suite](certification-suite.md)
