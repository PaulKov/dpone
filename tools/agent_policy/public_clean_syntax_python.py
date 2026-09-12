"""Conservative AST ownership proof for annotation-only credential shapes."""

import ast
import io
import tokenize

from tools.agent_policy.public_clean_receipts import GateError

BUILTINS = frozenset({"str", "bytes", "bool", "int", "float", "object"})


def annotation_context(text: str, start: int, end: int) -> tuple[int, int]:
    """Accept direct builtin annotations without defaults, values or shadowing."""
    depth = 0
    for token in tokenize.generate_tokens(io.StringIO(text).readline):
        if token.type == tokenize.OP:
            if token.string in {"(", "[", "{"}:
                depth += 1
            elif token.string in {")", "]", "}"}:
                depth -= 1
            if depth > 64:
                raise GateError("SYNTAX_PARSE_LIMIT")
    tree = ast.parse(text)
    lines = text.splitlines(keepends=True)
    starts = [0]
    for line in lines:
        starts.append(starts[-1] + len(line))

    def position(line: int, byte: int) -> int:
        return starts[line - 1] + len(lines[line - 1].encode()[:byte].decode())

    def span(node: ast.expr | ast.stmt | ast.arg) -> tuple[int, int]:
        if node.end_lineno is None or node.end_col_offset is None:
            raise GateError("SYNTAX_AST_SPAN_INVALID")
        return position(node.lineno, node.col_offset), position(node.end_lineno, node.end_col_offset)

    forbidden = (ast.Global, ast.Nonlocal, ast.Import, ast.ImportFrom)
    bound = set()
    for index, node in enumerate(ast.walk(tree)):
        if index >= 10000 or getattr(node, "type_params", None) or getattr(node, "decorator_list", None):
            raise GateError("SYNTAX_PARSE_LIMIT")
        if isinstance(node, ast.MatchMapping) and node.rest:
            bound.add(node.rest)
        if isinstance(node, forbidden):
            raise GateError("SYNTAX_ANNOTATION_AMBIGUOUS")
        if isinstance(node, ast.Name) and isinstance(node.ctx, (ast.Store, ast.Del)):
            bound.add(node.id)
        if isinstance(node, (ast.arg, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            bound.add(node.arg if isinstance(node, ast.arg) else node.name)
        if isinstance(node, ast.ExceptHandler) and node.name:
            bound.add(node.name)
        if isinstance(node, (ast.MatchAs, ast.MatchStar)) and node.name:
            bound.add(node.name)
        if isinstance(node, ast.Call):
            raise GateError("SYNTAX_ANNOTATION_AMBIGUOUS")
    for token in tokenize.generate_tokens(io.StringIO(text).readline):
        if token.type in (tokenize.STRING, tokenize.COMMENT):
            a = starts[token.start[0] - 1] + token.start[1]
            b = starts[token.end[0] - 1] + token.end[1]
            if start < b and a < end:
                raise GateError("SYNTAX_VALUE_OVERLAP")
    candidates: list[tuple[ast.arg | ast.AnnAssign, ast.expr | None]] = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.decorator_list:
                continue
            args = node.args.posonlyargs + node.args.args
            defaulted = args[len(args) - len(node.args.defaults) :] if node.args.defaults else []
            defaulted += [arg for arg, value in zip(node.args.kwonlyargs, node.args.kw_defaults) if value is not None]
            for arg in args + node.args.kwonlyargs:
                if arg not in defaulted:
                    candidates.append((arg, arg.annotation))
        if isinstance(node, ast.AnnAssign) and node.value is None and isinstance(node.target, ast.Name):
            candidates.append((node, node.annotation))
    for owner, annotation in candidates:
        if not isinstance(annotation, ast.Name) or annotation.id not in BUILTINS - bound:
            continue
        a, b = span(owner)
        c, d = span(annotation)
        # Scanner may start at a credential suffix of an identifier, never outside it.
        if a <= start < c and end == d and text[start:c].rstrip().endswith(":"):
            return a, b
    raise GateError("SYNTAX_ANNOTATION_UNPROVEN")
