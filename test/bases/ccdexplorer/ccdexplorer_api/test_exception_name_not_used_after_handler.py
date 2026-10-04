"""An except-bound name is gone once its handler ends.

Python deletes the name at the end of the except clause, so

    try:
        result = await thing()
    except Exception as error:
        result = None

    if result:
        return result
    raise HTTPException(404, detail=f"... {error}")   # UnboundLocalError, always

fails whether or not an exception was raised -- the del happens on the way out of
a handler that ran, and the name was never bound if it did not. Production saw it
on seven endpoints at once when Mongo went away, turning an intended 404 into a
500.

Those call sites were fixed in 0fef2d1 by capturing `error_message = str(error)`
inside the handler, and there are none left: this file is the guard that keeps it
that way, not a fix. `test_the_check_catches_the_pattern` is what shows the guard
works, since the codebase can no longer demonstrate it.
"""

import ast
from collections import defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parents[4]

BAD = """
def f():
    try:
        r = g()
    except Exception as error:
        r = None
    if r:
        return r
    raise RuntimeError(f"{error}")
"""

SIBLING_HANDLERS_ARE_FINE = """
def f():
    try:
        return g()
    except ValueError as e:
        log(e)
    except TypeError as e:
        log(e)
"""

CAPTURED_IS_FINE = """
def f():
    error_message = None
    try:
        r = g()
    except Exception as error:
        error_message = str(error)
        r = None
    if r:
        return r
    raise RuntimeError(f"{error_message}")
"""

REBOUND_IS_FINE = """
def f():
    error = None
    try:
        r = g()
    except Exception as error:
        r = None
    return f"{error}"
"""


def _uses_after_handler(tree: ast.AST) -> list[tuple[int, str]]:
    """Names read outside every handler that binds them.

    Two things that look like the bug but are not: sibling handlers reusing one
    name (each body is 'outside' the other), and a name also assigned in the
    function, which rebinds it independently of the handler.
    """
    found = []
    for fn in ast.walk(tree):
        if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue

        bound: dict[str, set[int]] = defaultdict(set)
        for handler in ast.walk(fn):
            if isinstance(handler, ast.ExceptHandler) and handler.name:
                for stmt in handler.body:
                    for node in ast.walk(stmt):
                        bound[handler.name].add(id(node))
        if not bound:
            continue

        assigned = {
            t.id
            for node in ast.walk(fn)
            if isinstance(node, ast.Assign)
            for t in node.targets
            if isinstance(t, ast.Name)
        }

        for node in ast.walk(fn):
            if (
                isinstance(node, ast.Name)
                and node.id in bound
                and isinstance(node.ctx, ast.Load)
                and id(node) not in bound[node.id]
                and node.id not in assigned
            ):
                found.append((node.lineno, node.id))
    return sorted(set(found))


def test_the_check_catches_the_pattern():
    """Without this the guard below could pass by doing nothing."""
    assert _uses_after_handler(ast.parse(BAD)) == [(9, "error")]


def test_sibling_handlers_sharing_a_name_are_not_flagged():
    assert _uses_after_handler(ast.parse(SIBLING_HANDLERS_ARE_FINE)) == []


def test_capturing_into_another_variable_is_not_flagged():
    """The shape 0fef2d1 moved these call sites to."""
    assert _uses_after_handler(ast.parse(CAPTURED_IS_FINE)) == []


def test_a_name_also_assigned_in_the_function_is_not_flagged():
    assert _uses_after_handler(ast.parse(REBOUND_IS_FINE)) == []


def test_nothing_in_the_codebase_reads_an_exception_name_after_its_handler():
    offenders = []
    for directory in ("bases", "components"):
        for path in sorted((REPO / directory).rglob("*.py")):
            for lineno, name in _uses_after_handler(ast.parse(path.read_text())):
                offenders.append(f"{path.relative_to(REPO)}:{lineno} reads '{name}'")

    assert not offenders, "these raise UnboundLocalError when reached:\n  " + "\n  ".join(offenders)
