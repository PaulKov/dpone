# DPONE_SAFE_SAMPLE_LIVE_INPUT_PATH_INVALID

A derived or local live-input path is missing, escapes the allowed root, or is a
symlink. Safe-sample assembly fails closed before credential resolution.

## Fix

Remove the unsafe path and restore inputs from the trusted platform materializer.
Re-run the same beginner command after the overlay is complete.

See [Airflow route attestation](../airflow-route-attestation.md).
