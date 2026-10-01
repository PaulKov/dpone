# Frozen unpublished ClickHouse prototype history

These synthetic records preserve the pre-consolidation formats from commit
`5a36601e26fb826ed4ea1b4a08aca722d366ffec`. They are not live evidence,
production stores, credentials, execution grants, or proof of server completion.
Do not open them through a mutation-capable authority implementation.

The generated bundle is in `frozen/`; filenames below are relative to it.
`provenance.json` lists the exact producer, imported source and artifact hashes.
The producer verifies its imported dpone/test source against the frozen commit.
Its archived text is `producer.py.txt`; it deliberately refuses an existing
destination. Reproduction requires an isolated checkout of that exact commit,
with that checkout as the working directory, and the original test dependencies:

```bash
uv run python /absolute/path/to/producer.py.txt /new/private/output-directory
```

Random invocation/grant hashes are intentionally retained in the database
snapshots. A new production of synthetic stores may have different physical
bytes and grant hashes; it must not replace these fixed vectors or their
provenance. Deterministic JSON record and completion digests remain test inputs.

## Contents and limits

- `publication-records.json`: exact encoded guarded-publication-v2 bytes for
  four selected methods and six state/claim combinations each.
- Three authority-v1 databases: PREPARED, possible send, and positive synthetic
  transport completion. The last still has publication state CLAIMED; transport
  closure and publication resolution are deliberately different facts.
- Two authority-v2 databases: completed candidate CREATE followed by an uncertain
  INSERT, and completed INSERT with source exhaustion/admission closure.
- Original diagnostic outputs and synthetic native/candidate completion vectors.

The five databases are quiesced snapshots without WAL/journal sidecars. Read-only
`immutable=1` is valid for these frozen fixtures only, never an instruction to
ignore the WAL of a real authority. Historical-reader tests must separately
cover coherent WAL handling and corruption using temporary copies.

No seal, v3 operation, source/transport I/O, live ClickHouse result or actual
EndOfStream is represented. Unknown/malformed versions and damaged schemas,
records, histories or digests must be tested with copies; never edit the vectors
to make a new implementation pass. Original-format readers may inspect them but
must never infer a fresh mutation, cleanup, retry or successor capability.

This README is explanatory and is not part of the generated artifact manifest.
