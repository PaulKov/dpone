# dpone Test Artifacts

This directory stores durable test evidence for OSS/release and production-readiness gates.

Artifact rules:

- Each artifact has a unique timestamped filename.
- Each artifact records who ran it, when it ran, command, result, and evidence summary.
- Failed and blocked attempts are kept intentionally because they explain real infrastructure or compatibility constraints.
- Large generated datasets are not committed; only structured evidence and reproducible commands are stored.
