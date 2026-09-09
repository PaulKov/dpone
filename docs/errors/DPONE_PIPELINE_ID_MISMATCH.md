# DPONE_PIPELINE_ID_MISMATCH

The canonical ID requested on the command line does not match `metadata.id` in
the resolved primary authoring source.

Choose the intended authority:

- rename the pipeline directory/reference to match `metadata.id`; or
- change `metadata.id`, update domain catalog references, and run the explicit
  authoring migration workflow.

Then rerun `dpone check <pipeline-id>`. dpone does not silently choose one ID,
because doing so could test, preview, or execute a different pipeline.
