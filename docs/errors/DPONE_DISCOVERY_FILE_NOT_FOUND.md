# DPONE_DISCOVERY_FILE_NOT_FOUND

**Audience:** pipeline authors and CI maintainers.

A domain-first pipeline directory does not contain its required
`pipeline.yaml`, or the file disappeared while a bounded discovery snapshot was
being built.

Restore the primary source or remove the incomplete pipeline directory, then
rerun `dpone check .`.

[Domain-first error overview](index.md) · [Return to the tutorial](../getting-started/domain-first-airflow.md)
