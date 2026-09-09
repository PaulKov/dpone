# dpone agent evaluations

Changes to `AGENTS.md`, `.codex/**`, `.agents/skills/**`, task templates, or agent
policy tooling must be evaluated like product changes.

A representative task set should include:

- a backward-compatible manifest field;
- a CLI output/file contract change;
- a nested identity replay defect;
- a new connector capability and unsupported combination;
- a state/evidence ordering bug;
- an Airflow provider compatibility change;
- a docs-only broken YAML/link/navigation fix;
- a release audit with unavailable live evidence.

Store each task as a versioned prompt plus allowed/forbidden paths, acceptance
criteria, required checks, and expected structured result. Use
`result.schema.json` for the final report.

Track at least first-pass CI success, accepted-without-rework, unrelated files,
required-check selection, contract violations, escaped defects, merge conflicts,
tokens, and wall time. Never optimize only for generated code volume or green CI.
