"""Strict JSON token ownership for schema field declarations, never payloads."""

import json

from tools.agent_policy.public_clean_receipts import GateError

SCHEMAS = frozenset({"https://json-schema.org/draft/2020-12/schema"})


def schema_context(text: str, start: int, end: int) -> tuple[int, int]:
    """Parse a duplicate-free document and walk only JSON Schema schema positions."""
    decoder = json.JSONDecoder()
    position = 0
    spans: dict[tuple[str | int, ...], tuple[int, int, int, int]] = {}

    def space():
        nonlocal position
        while position < len(text) and text[position] in " \r\n\t":
            position += 1

    def value(path):
        nonlocal position
        if len(path) > 64 or len(spans) > 10000:
            raise GateError("SYNTAX_PARSE_LIMIT")
        space()
        begin = position
        result: object
        if text[position] == "{":
            position += 1
            result = dict[str, object]()
            space()
            while text[position] != "}":
                key_start = position
                name, position = decoder.raw_decode(text, position)
                if type(name) is not str or name in result:
                    raise GateError("SYNTAX_JSON_DUPLICATE")
                key_end = position
                space()
                if text[position] != ":":
                    raise GateError("SYNTAX_JSON_INVALID")
                position += 1
                child = path + (name,)
                result[name] = value(child)
                spans[child] = (key_start, key_end, spans[child][2], spans[child][3])
                space()
                if text[position] != ",":
                    break
                position += 1
                space()
                if text[position] == "}":
                    raise GateError("SYNTAX_JSON_INVALID")
            if text[position] != "}":
                raise GateError("SYNTAX_JSON_INVALID")
            position += 1
        elif text[position] == "[":
            position += 1
            result = []
            space()
            while text[position] != "]":
                result.append(value(path + (len(result),)))
                space()
                if text[position] != ",":
                    break
                position += 1
                space()
                if text[position] == "]":
                    raise GateError("SYNTAX_JSON_INVALID")
            if text[position] != "]":
                raise GateError("SYNTAX_JSON_INVALID")
            position += 1
        else:
            result, position = decoder.raw_decode(text, position)
        spans[path] = (begin, begin, begin, position)
        return result

    root = value(())
    space()
    if position != len(text) or type(root) is not dict or root.get("$schema") not in SCHEMAS:
        raise GateError("SYNTAX_SCHEMA_ROOT_INVALID")
    # Independent strict syntax/nonfinite check; recursive parser owns exact positions.
    json.loads(text, parse_constant=lambda _: (_ for _ in ()).throw(GateError("SYNTAX_JSON_INVALID")))
    from jsonschema import Draft202012Validator
    from jsonschema.exceptions import SchemaError

    try:
        Draft202012Validator.check_schema(root, format_checker=None)
    except SchemaError as exc:
        raise GateError("SYNTAX_SCHEMA_INVALID") from exc
    candidates = []

    def walk(schema, path):
        if type(schema) is not dict:
            return
        if path and "$schema" in schema:
            raise GateError("SYNTAX_SCHEMA_DIALECT_TRANSITION")
        properties = schema.get("properties", {})
        if type(properties) is not dict:
            raise GateError("SYNTAX_SCHEMA_INVALID")
        for name, child in properties.items():
            target = path + ("properties", name)
            a, b, c, d = spans[target]
            if type(child) is bool and a < start < b and end == d:
                candidates.append((a, d))
            if type(child) is dict and not any(k in child for k in ("default", "examples", "example", "const", "enum")):
                # Full scanner span must begin within the literal field-name token
                # and end in schema punctuation only, before any schema value token.
                if a < start < b and b <= end <= c + 1 and text[b:end].strip() in {":", ": {", ":{"}:
                    candidates.append((a, d))
            walk(child, target)
        for keyword in ("$defs", "patternProperties", "dependentSchemas"):
            children = schema.get(keyword, {})
            if type(children) is dict:
                for name, child in children.items():
                    walk(child, path + (keyword, name))
        for keyword in ("items", "contains", "additionalProperties", "propertyNames", "not", "if", "then", "else"):
            walk(schema.get(keyword), path + (keyword,))
        for keyword in ("allOf", "anyOf", "oneOf", "prefixItems"):
            children = schema.get(keyword, [])
            if type(children) is list:
                for index, child in enumerate(children):
                    walk(child, path + (keyword, index))

    walk(root, ())
    if len(candidates) != 1:
        raise GateError("SYNTAX_SCHEMA_FIELD_UNPROVEN")
    return candidates[0]
