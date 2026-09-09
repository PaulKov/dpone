# DPONE_DOMAIN_CATALOG_MEMBERSHIP_CONFLICT

**Audience:** pipeline authors and repository maintainers.

`dpone init pipeline` in a domain-first project registers the new pipeline in
the domain workload catalog
(`<system_root>/config/domains/<domain>.yaml`, `workloads:` block) after the
pipeline scaffold is applied. This error means the membership entry could not
be written safely:

- the catalog already registers the same pipeline id with a **different**
  `manifest:` path;
- the catalog file is not safe YAML or its root is not a mapping;
- the catalog changed **concurrently** between the scaffold and the
  compare-and-swap membership write.

The pipeline scaffold files were already created and remain on disk only when
membership registration fails after a successful scaffold apply without
compensation (manual recovery required). When preflight detects a conflict or
unsafe catalog state, no pipeline files are written.

How to resolve:

1. Open `<system_root>/config/domains/<domain>.yaml` and inspect the
   `workloads:` block for the pipeline id from the error message.
2. If an entry with a stale `manifest:` path exists, fix or remove it
   manually, then rerun `dpone init pipeline` (the rerun is idempotent) or add
   the membership entry yourself:

   ```yaml
   workloads:
     <pipeline_id>:
       manifest: ../../../workloads/<domain>/pipelines/<pipeline_id>/pipeline.yaml
   ```

3. If the file was changed concurrently, stop the concurrent writer and rerun
   the command.

Do not add a `dags:` block to the domain catalog: colocated
`workloads/<domain>/dags/*.yaml` files are the only schedule/wiring source of
truth in domain-first projects. A pipeline left without catalog membership is
reported by reconcile as the `workload_catalog_membership_missing` warning and
is not packed.

[Domain-first error overview](index.md) ·
[Domain-first operations](../domain-first-operations.md)
