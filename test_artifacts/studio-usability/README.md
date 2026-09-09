# Studio v1 usability evidence

Status: `UNVERIFIED`.

This directory reserves the evidence location for the human usability gate. No
CI, mocked session, maintainer walkthrough, or API contract test counts as a
human usability pass.

Use the existing `dpone.self-service-usability-study.v1` protocol documented in
`docs/airflow-self-service-evidence.md`. The study must:

- target the exact commit under review;
- include at least five first-time dpone users;
- ask each participant to discover a scaffoldable route, create a pipeline,
  run static check, and preview the DAG without Airflow Python;
- record start/completion timestamps and facilitator interventions;
- contain no names, credentials, Vault paths, or production data;
- report `PASS` only when at least 80% finish within 15 minutes without help.

Store the validated study and derived certification under a dated,
commit-specific subdirectory:

```text
test_artifacts/studio-usability/<YYYY-MM-DD>-<commit>/
  usability-study.json
  certification.json
  certification.md
  summary.md
```

`summary.md` is the human-readable session summary and must identify the exact
commit, participants, completion timing, facilitator actions, failures, and
links to the validated JSON study and certification.

Until those artifacts exist and validate for the release commit,
`dpone-studio v0.1.0` and the Studio v0.2 authoring milestone remain `NO-GO`.
