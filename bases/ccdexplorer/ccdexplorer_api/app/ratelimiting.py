import json
from typing import Tuple

from fastapi.responses import JSONResponse
from ratelimit.auths import EmptyInformation
from ratelimit.types import ASGIApp, Receive, Scope, Send


async def handle_auth_error(exc: Exception) -> ASGIApp:
    return JSONResponse({"message": "Unauthorized access."}, status_code=401)


def handle_429(retry_after: int):
    async def inside_yourself_429(scope: Scope, receive: Receive, send: Send) -> None:
        body = json.dumps({"message": "Too many requests."}).encode("utf8")
        headers = [
            (b"content-length", str(len(body)).encode("utf8")),
            (b"content-type", b"application/json"),
        ]
        await send({"type": "http.response.start", "status": 429, "headers": headers})

        await send(
            {
                "type": "http.response.body",
                "body": body,
                "more_body": False,
            }
        )

    return inside_yourself_429


async def AUTH_FUNCTION(scope: Scope) -> Tuple[str, str]:
    """
    To gain access to v2 API
    """
    app = scope["app"]
    # if not app.api_keys:
    api_keys: dict = await app.state.get_api_keys_fn(
        motormongo=app.motormongo, app=app, for_="ratelimiting"
    )
    # else:
    #     api_keys = app.api_keys

    api_account_id, group_name = None, None

    headers: list[Tuple[bytes, bytes]] = scope["headers"]
    try:
        headers_decoded = {x[0].decode(): x[1].decode() for x in headers}
        api_key = headers_decoded.get("x-ccdexplorer-key")
    except:  # noqa: E722
        api_key = None

    if api_key:
        if api_keys.get(api_key):
            recognized_api_key_document = api_keys.get(api_key)
            assert recognized_api_key_document is not None
            api_account_id = recognized_api_key_document["api_account_id"]
            group_name = recognized_api_key_document["api_group"]
            scope["api_auth"] = (api_account_id, group_name)
        else:
            raise EmptyInformation(scope)
    else:
        raise EmptyInformation(scope)
    return api_account_id, group_name
