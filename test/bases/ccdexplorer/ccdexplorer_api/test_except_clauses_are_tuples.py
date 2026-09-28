"""`except A | B` catches nothing -- it raises instead.

A union of two exception classes is a types.UnionType, not a tuple, and Python
refuses it:

    TypeError: catching classes that do not inherit from BaseException is not allowed

So the handler never runs. Production saw it on /v2/{net}/plt/list-token-ids the
moment the node became unreachable: instead of the intended 404, the route
raised TypeError and answered 500. The bug is invisible until the error path is
taken, which is exactly when it is least welcome.

Two sites had it -- plts_v2.py and heartbeat's block_loop.py -- so there is a
structural test as well as a behavioural one.
"""

import ast
from pathlib import Path
from unittest.mock import MagicMock

import pytest
from fastapi import HTTPException
from grpc._channel import _InactiveRpcError, _MultiThreadedRendezvous

REPO = Path(__file__).resolve().parents[4]


def _union_except_clauses() -> list[str]:
    offenders = []
    for directory in ("bases", "components"):
        for path in sorted((REPO / directory).rglob("*.py")):
            for node in ast.walk(ast.parse(path.read_text())):
                if not isinstance(node, ast.ExceptHandler) or node.type is None:
                    continue
                if isinstance(node.type, ast.BinOp) and isinstance(node.type.op, ast.BitOr):
                    offenders.append(
                        f"{path.relative_to(REPO)}:{node.lineno} except {ast.unparse(node.type)}"
                    )
    return offenders


def test_no_except_clause_catches_a_union():
    """A tuple is the only thing `except` accepts for more than one type."""
    assert not _union_except_clauses(), "these handlers can never run:\n  " + "\n  ".join(
        _union_except_clauses()
    )


def test_a_union_really_does_raise():
    """Pins the language behaviour the fix is for, so the reason is not lost."""
    with pytest.raises(TypeError, match="do not inherit from BaseException"):
        try:
            raise _InactiveRpcError(MagicMock())
        except _InactiveRpcError | _MultiThreadedRendezvous:  # noqa: B030
            pass


def _plt_route_args(raises: BaseException | None):
    client = MagicMock()
    if raises is not None:
        client.get_token_list.side_effect = raises
    else:
        client.get_token_list.return_value = None
    return client


async def test_an_unreachable_node_gives_404_not_500():
    from ccdexplorer.ccdexplorer_api.app.routers.v2.plts_v2 import (
        get_all_plt_tokens_from_node,
    )

    with pytest.raises(HTTPException) as exc:
        await get_all_plt_tokens_from_node(
            request=None,
            net="mainnet",
            grpcclient=_plt_route_args(_InactiveRpcError(MagicMock())),
            mongomotor=None,
            api_key="k",
        )

    assert exc.value.status_code == 404


async def test_the_other_rpc_error_is_caught_too():
    from ccdexplorer.ccdexplorer_api.app.routers.v2.plts_v2 import (
        get_all_plt_tokens_from_node,
    )

    with pytest.raises(HTTPException) as exc:
        await get_all_plt_tokens_from_node(
            request=None,
            net="mainnet",
            grpcclient=_plt_route_args(_MultiThreadedRendezvous(MagicMock(), None, None, None)),
            mongomotor=None,
            api_key="k",
        )

    assert exc.value.status_code == 404


async def test_an_unrelated_error_is_not_swallowed():
    """The handler must stay narrow -- a bug in the node call is not a 404."""
    from ccdexplorer.ccdexplorer_api.app.routers.v2.plts_v2 import (
        get_all_plt_tokens_from_node,
    )

    with pytest.raises(ZeroDivisionError):
        await get_all_plt_tokens_from_node(
            request=None,
            net="mainnet",
            grpcclient=_plt_route_args(ZeroDivisionError("not an rpc problem")),
            mongomotor=None,
            api_key="k",
        )
