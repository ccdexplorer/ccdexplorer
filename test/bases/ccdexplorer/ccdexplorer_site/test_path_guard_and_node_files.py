"""Two ways a request could reach further than the page it was meant for.

* `{net}` went straight into urls for the api. Most handlers checked it and
  redirected; the ones that did not let `net=..` through, which httpx
  collapses -- turning /v2/../x into a top-level api path under the site's own
  unrestricted key. It is checked once, for every route, before any handler.
* /node served the whole node_modules tree: every package's package.json,
  README and source, which is a list of exactly what is installed.
"""

import ast
from pathlib import Path
from types import SimpleNamespace

import httpx2 as httpx
import pytest
from fastapi import Depends, FastAPI, HTTPException, Request

from ccdexplorer.ccdexplorer_site.app.node_files import NodeModulesFiles
from ccdexplorer.ccdexplorer_site.app.path_guard import guard_path_params

# --- path guard -----------------------------------------------------------


def _client():
    app = FastAPI(dependencies=[Depends(guard_path_params)])

    @app.get("/{net}/page")
    def page(net: str):
        return {"net": net}

    @app.get("/thing/{ident}")
    def thing(ident: str):
        return {"ident": ident}

    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://t", follow_redirects=False
    )


@pytest.mark.parametrize("net", ["mainnet", "testnet", "devnet"])
async def test_a_real_network_passes(net):
    async with _client() as c:
        r = await c.get(f"/{net}/page")
    assert r.status_code == 200 and r.json() == {"net": net}


@pytest.mark.parametrize("net", ["evilnet", "MAINNET", "main net", "mainnet2", "x"])
async def test_anything_else_is_sent_to_mainnet_like_the_handlers_already_do(net):
    async with _client() as c:
        r = await c.get(f"/{net}/page")
    assert r.status_code == 302
    assert r.headers["location"] == "/mainnet"


@pytest.mark.parametrize("value", [".", ".."])
async def test_a_dot_segment_is_refused_wherever_it_is_a_parameter(value):
    request = SimpleNamespace(path_params={"ident": value})
    with pytest.raises(HTTPException) as exc:
        await guard_path_params(request)
    assert exc.value.status_code == 404


async def test_the_net_check_covers_a_dot_segment_too():
    request = SimpleNamespace(path_params={"net": ".."})
    with pytest.raises(HTTPException):
        await guard_path_params(request)


async def test_a_route_without_a_net_is_untouched():
    async with _client() as c:
        r = await c.get("/thing/abc")
    assert r.status_code == 200


def test_the_site_applies_the_guard_to_every_route():
    tree = ast.parse(Path("bases/ccdexplorer/ccdexplorer_site/app/factory.py").read_text())
    for node in ast.walk(tree):
        if isinstance(node, ast.Call) and getattr(node.func, "id", "") == "FastAPI":
            kw = {k.arg: k.value for k in node.keywords}
            assert "dependencies" in kw
            assert "guard_path_params" in ast.dump(kw["dependencies"])
            return
    raise AssertionError("no FastAPI( in the site factory")


# --- /node ----------------------------------------------------------------


@pytest.fixture
def node_root(tmp_path):
    pkg = tmp_path / "bootstrap"
    (pkg / "dist" / "js").mkdir(parents=True)
    (pkg / "dist" / "css").mkdir(parents=True)
    (pkg / "dist" / "js" / "bootstrap.min.js").write_text("js")
    (pkg / "dist" / "css" / "bootstrap.min.css").write_text("css")
    (pkg / "dist" / "js" / "bootstrap.min.js.map").write_text("{}")
    (pkg / "package.json").write_text('{"version": "5.3.3"}')
    (pkg / "README.md").write_text("# readme")
    (pkg / "LICENSE").write_text("MIT")
    (pkg / "page.html").write_text("<script>alert(1)</script>")
    (pkg / "dist" / "x.ts").write_text("export {}")
    return tmp_path


def _node_client(root):
    app = FastAPI()
    app.mount("/node", NodeModulesFiles(directory=root), name="node_modules")
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://t")


@pytest.mark.parametrize(
    "path",
    [
        "bootstrap/dist/js/bootstrap.min.js",
        "bootstrap/dist/css/bootstrap.min.css",
        "bootstrap/dist/js/bootstrap.min.js.map",
    ],
)
async def test_what_the_pages_load_is_served(node_root, path):
    async with _node_client(node_root) as c:
        assert (await c.get(f"/node/{path}")).status_code == 200


@pytest.mark.parametrize(
    "path",
    [
        "bootstrap/package.json",
        "bootstrap/README.md",
        "bootstrap/LICENSE",
        "bootstrap/page.html",
        "bootstrap/dist/x.ts",
    ],
)
async def test_what_only_describes_the_install_is_not(node_root, path):
    async with _node_client(node_root) as c:
        assert (await c.get(f"/node/{path}")).status_code == 404


async def test_a_traversal_does_not_get_out(node_root):
    async with _node_client(node_root) as c:
        r = await c.get("/node/bootstrap/dist/../../../etc/passwd")
    assert r.status_code in (404, 400)


async def test_a_directory_listing_is_not_offered(node_root):
    async with _node_client(node_root) as c:
        assert (await c.get("/node/bootstrap/dist/js/")).status_code == 404


def test_the_site_mounts_it():
    src = Path("bases/ccdexplorer/ccdexplorer_site/app/factory.py").read_text()
    assert "NodeModulesFiles(" in src
    assert "StaticFiles(directory=app_settings.node_modules_dir)" not in src
