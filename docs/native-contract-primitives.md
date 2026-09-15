# Native contract primitives

This reference is for connector and contract authors who need deterministic,
bounded native evidence bytes. These primitives validate references and JSON;
they do not execute a route, authenticate storage, or authorize publication.
Analysts continue to use the existing [dbt workflow](dbt.md).

## First local example

With dpone installed, run this Python example without a database or credentials:

```python
from hashlib import sha256

from dpone.contracts.native_delivery_json import (
    decode_native_delivery_json,
    encode_native_delivery_json,
)
from dpone.contracts.native_identity import OriginalRef

payload = encode_native_delivery_json({"rows": 2, "label": "sample"})
reference = OriginalRef("generation/receipt.json", "sha256:" + sha256(payload).hexdigest())
assert payload == b'{"label":"sample","rows":2}'
assert decode_native_delivery_json(payload)["rows"] == 2
print(reference.locator)
```

The output is `generation/receipt.json`. No file is written and no client is
created. Repeating the encoding produces the same bytes.

## Reference identity

`OriginalRef(locator, sha256)` is an immutable pair. The locator must be a canonical
relative POSIX path with at most 4096 UTF-8 bytes. Absolute paths, dot/parent segments,
repeated or trailing separators, backslashes, Unicode control characters and invalid
Unicode scalars are rejected. The digest is `sha256:` followed by exactly 64 lowercase
hexadecimal characters. Invalid references raise `DbtPublishingError` with code
`DPONE_NATIVE_IDENTITY_INVALID`, without performing filesystem lookup.

A well-shaped reference does not prove that an object exists or belongs to the
caller. The consuming domain must independently resolve the exact version, subject,
kind and stored digest through its authenticated original binding.

## JSON limits

| Boundary | Limit and counting rule |
|---|---|
| Document | 1,048,576 encoded UTF-8 bytes |
| String or object key | 4096 decoded UTF-8 bytes |
| Nesting | 32 containers, including the root container |
| Lexical tokens | 65,536; each punctuation mark, string/key, number or keyword counts once |
| Integer | 128 decimal digits, excluding a minus sign |

These limits are fixed protocol boundaries. SQL revision/count codecs apply their
own narrower range checks; JSON integers are not globally restricted to SQL bigint.
Booleans remain distinct from integer values.

`encode_native_delivery_json(value)` accepts only exact built-in `None`, `bool`,
`int`, `str`, `list` and string-keyed `dict` values. It rejects floats, tuples,
sets, arbitrary mappings, subclasses and ancestor cycles. A shared acyclic child
may appear more than once. DTO codecs explicitly convert tuples to arrays first.
Any supported primitive root can be encoded. Inputs are not mutated.

Encoding validates sizes before whole-document serialization, sorts keys, uses
compact separators and emits UTF-8 without a BOM or trailing newline. Unicode is
not normalized: composed and decomposed strings remain different identities.

`decode_native_delivery_json(payload)` requires exact `bytes` and an object root.
It rejects duplicate decoded keys, floats/exponents/nonfinite numbers, invalid
UTF-8, a BOM, unpaired surrogates and malformed JSON. A nonrecursive lexical pass
bounds nesting, tokens and integer/string size before full grammar parsing.
Valid paired Unicode escapes, whitespace and unsorted keys may be decoded.

## Canonical originals and schema ownership

Decoding is not a canonical-identity check. For an immutable stored original, its
schema-owning codec must additionally require:

```python
decoded = decode_native_delivery_json(payload)
if encode_native_delivery_json(decoded) != payload:
    raise ValueError("stored original must use canonical JSON bytes")
```

That codec must validate its exact schema discriminator, keys, field types,
nullability and domain limits before constructing an accepted record. Original
binding authentication remains a separate operation. A parsed dictionary is
never a successful execution receipt or publication permission by itself.

## Failures and compatibility

Malformed or over-budget native JSON raises `NativeJsonError`, a `ValueError`.
Fix the producer or reject the document; do not trim data, coerce floats, normalize
Unicode or silently truncate an original to make it fit. Preserve rejected evidence
according to its owner's retention policy. Retrying unchanged invalid bytes does
not change the result and neither helper advances state or a checkpoint.

Existing generic `strict_json_object` behavior, older evidence readers, CLI commands
and route execution remain unchanged. In particular, the generic parser can still
accept finite floats; only these native primitives apply the new restrictions.
No full native generation/publication route is enabled by these two modules.

## Validation and next steps

Run `uv run pytest tests/test_native_identity.py tests/test_native_delivery_json.py`.
The tests exercise exact bounds, malformed/duplicate input, scalar fidelity,
cycles versus sharing, canonical output and generic-parser compatibility. These
are unit/contract checks, not database or three-replica certification.

Next, apply the owning schema's closed field validation and original resolution.
See [engineering standards](engineering-standards.md) for contract design and
[testing overview](testing/overview.md) for route qualification requirements.
