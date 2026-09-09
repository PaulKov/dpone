# DPONE_PIPELINE_ID_INVALID

The pipeline does not declare a canonical logical identity.

Use `metadata.id` with lowercase letters, digits, `_`, or `-`; the first
character must be a letter or digit and the total length must be 2–128
characters. For a new pipeline, rerun the safe fix emitted by
`dpone init pipeline`. For an existing source, edit its primary authoring file
and rerun `dpone check <pipeline-id>`.

The safe-sample path never derives a logical identity from a directory name.
No release, deployment, runtime handoff, credential lookup, or data-system I/O
is allowed until the identity is valid.
