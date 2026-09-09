# Snapshot promote

Snapshot promotion is a controlled way to test a build artifact before cutting or publishing a stable release.

## When to use it

Use snapshot promotion for:

- Review-environment validation.
- Connector certification candidates.
- Large integration or benchmark runs.
- Release-candidate smoke tests.

Do not use snapshot promotion as a substitute for stable PyPI releases in production.

## Recommended flow

1. Build a snapshot package.
2. Run unit and contract gates.
3. Install the snapshot in a clean environment.
4. Run package smoke tests.
5. Run targeted integration tests.
6. Store artifacts under `test_artifacts/`.
7. Promote to a stable release only after gates pass.

## Commands

```bash
uv build
twine check dist/*
python tools/package_smoke.py --project-root . --dpone-cmd dpone
```

For development overlays, see [Development install mode](dev-install-mode.md).

## Artifact expectations

A promotion artifact should include:

- Package version or snapshot identifier.
- Commit SHA.
- Runner and environment.
- Commands executed.
- Test result summary.
- Known limitations and follow-up actions.
