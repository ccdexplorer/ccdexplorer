"""`/search` must not build a redirect out of an unvalidated net.

`net` is the first path segment of every url the route builds, and it arrives
straight from the request body. Before this was validated, production answered

    POST /search {"selector":"all","value":"probe","net":"/evil.example.com"}
    location:    //evil.example.com/search_all/probe
    hx-redirect: //evil.example.com/search_all/probe

a protocol-relative url, so the browser leaves the origin -- from a link that
genuinely starts https://ccdexplorer.io/. On a block explorer that is worth
real money to whoever sends it.
"""

import pytest
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_site.app.routers.home import SearchRequest, search


def _request(net: str, selector: str = "all", value: str = "probe") -> SearchRequest:
    return SearchRequest(selector=selector, value=value, net=net)


@pytest.mark.parametrize(
    "net",
    [
        "/evil.example.com",  # the reported payload
        "//evil.example.com",
        "evil.example.com",
        "mainnet/../../evil.com",
        "MAINNET",  # the check is exact, not case-folded
        "",
    ],
)
async def test_a_net_that_is_not_a_known_network_is_refused(net):
    with pytest.raises(HTTPException) as exc:
        await search(request=None, search_request=_request(net))
    assert exc.value.status_code == 404


@pytest.mark.parametrize("net", ["mainnet", "testnet", "devnet"])
async def test_the_three_real_networks_still_redirect(net):
    response = await search(request=None, search_request=_request(net))

    assert response.headers["HX-Redirect"] == f"/{net}/search_all/probe"
    # Relative, single leading slash: it cannot leave the origin.
    assert not response.headers["HX-Redirect"].startswith("//")


async def test_an_unknown_selector_does_not_redirect_anywhere():
    """`url` is now initialised to None, so an unknown selector redirects nowhere.

    It returns None, which FastAPI renders as a null body -- untidy, but no
    redirect is issued, which is the part that matters here. Before `url = None`
    was added, an unrecognised selector left the name unbound.
    """
    response = await search(request=None, search_request=_request("mainnet", selector="nope"))

    assert response is None
