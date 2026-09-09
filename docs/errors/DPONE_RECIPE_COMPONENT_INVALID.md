# DPONE_RECIPE_COMPONENT_INVALID

A component is empty, too large, uses an unknown/non-whole-value placeholder,
or contains executable/template/credential-bearing content.

## Fix

Keep components declarative `processes` only. `$param` and `$context` mappings
must replace an entire value and reference a known safe name. Remove Python,
module, callable, shell, command, Jinja/template, and secret fields; publish a
new immutable component version and run `dpone recipe validate`.
