"""An error response says what failed, never what the exception said.

31 handlers built their 4xx/5xx detail from the exception's own text, which
can name Mongo hosts, collection names, query fragments and file paths. The
caller gets a message about what could not be retrieved; the exception goes to
the log.
"""

import ast
import logging
from pathlib import Path

import pytest
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_api.app.utils import failure

ROUTERS = Path(__file__).resolve().parents[4] / "bases/ccdexplorer/ccdexplorer_api/app/routers"
EXCEPTION_NAMES = {"error", "e", "exc", "err", "error_message", "ex"}


def _leaks(path: Path):
    tree = ast.parse(path.read_text())
    for node in ast.walk(tree):
        if not (isinstance(node, ast.Call) and getattr(node.func, "id", "") == "HTTPException"):
            continue
        for kw in node.keywords:
            if kw.arg != "detail" or not isinstance(kw.value, ast.JoinedStr):
                continue
            for part in ast.walk(kw.value):
                if isinstance(part, ast.FormattedValue) and (
                    isinstance(part.value, ast.Name) and part.value.id in EXCEPTION_NAMES
                ):
                    yield f"{path.name}:{node.lineno}"


def test_no_response_detail_interpolates_an_exception():
    leaks = [hit for p in sorted(ROUTERS.rglob("*.py")) for hit in _leaks(p)]
    assert leaks == []


def test_failure_hides_the_exception_from_the_caller():
    e = failure(500, "Error retrieving blocks.", RuntimeError("mongodb://user:pw@10.0.0.5/db"))
    assert isinstance(e, HTTPException)
    assert e.status_code == 500
    assert e.detail == "Error retrieving blocks."
    assert "mongodb" not in str(e.detail)


def test_failure_logs_what_it_hid(caplog):
    with caplog.at_level(logging.ERROR):
        failure(500, "Error retrieving blocks.", RuntimeError("the real reason"))
    assert "the real reason" in caplog.text
    assert "Error retrieving blocks." in caplog.text


def test_failure_without_an_exception_just_answers():
    e = failure(404, "Not found.")
    assert (e.status_code, e.detail) == (404, "Not found.")
