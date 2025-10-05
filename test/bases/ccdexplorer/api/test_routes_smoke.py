import pytest
from fastapi.routing import APIRoute
import httpx

from conftest import build_test_app
from .hand_picked_routes import (
    SAMPLE_PATH_VALUES,
    SAMPLE_QUERY_PARAMS,
    SAMPLE_JSON_BODIES,
)


ALLOWED_METHODS = {"GET", "POST", "PUT", "PATCH", "DELETE"}


def extract_path_param_names(route: APIRoute) -> list[str]:
    return [p.name for p in route.dependant.path_params]


def route_requires_body(route: APIRoute) -> bool:
    return bool(route.dependant.body_params)


def list_all_routes(app) -> list[dict]:
    """For diagnostics: list all routes with metadata."""
    rows = []
    for r in app.routes:
        if not isinstance(r, APIRoute):
            continue
        rows.append(
            {
                "path": r.path,
                "methods": sorted((r.methods or []) & ALLOWED_METHODS),  # type: ignore
                "path_params": extract_path_param_names(r),
                "requires_body": route_requires_body(r),
                "include_in_schema": bool(r.include_in_schema),
                "endpoint": f"{r.endpoint.__module__}:{r.endpoint.__name__}",
                "name": r.name,
            }
        )
    return rows


def fill_path(route: APIRoute) -> tuple[str | None, list[str]]:
    """Replace path params using SAMPLE_PATH_VALUES."""
    url = route.path
    missing = []
    for p in route.dependant.path_params:
        if p.name in SAMPLE_PATH_VALUES:
            url = url.replace(f"{{{p.name}}}", str(SAMPLE_PATH_VALUES[p.name]))
        else:
            missing.append(p.name)
    return (None if missing else url), missing


def collect_smoke_cases(app):
    cases = []
    for r in app.routes:
        if not isinstance(r, APIRoute):
            continue
        if r.include_in_schema is False:
            continue

        url, missing = fill_path(r)
        if url is None:
            continue  # skip until SAMPLE_PATH_VALUES has the missing param

        for method in r.methods or []:
            if method not in ALLOWED_METHODS:
                continue

            kwargs = {}

            # Attach query params if we have any
            if url in SAMPLE_QUERY_PARAMS:
                kwargs["params"] = SAMPLE_QUERY_PARAMS[url]

            # Attach body if defined
            if url in SAMPLE_JSON_BODIES:
                kwargs["json"] = SAMPLE_JSON_BODIES[url]

            # If it's a POST/PUT/PATCH with no explicit body, use empty {}
            elif method in {"POST", "PUT", "PATCH"}:
                kwargs["json"] = {}

            cases.append((method, url, r, kwargs))
    return cases


def pytest_generate_tests(metafunc):
    if {"method", "url", "kwargs"} <= set(metafunc.fixturenames):
        app = (
            build_test_app()
        )  # if this is a plain function; if it's a fixture, use a builder func instead
        cases = collect_smoke_cases(app)  # -> [(method, url, route, kwargs), ...]
        assert cases, "No testable routes found — extend SAMPLE_PATH_VALUES."

        params = [(m, u, kw) for (m, u, _r, kw) in cases]
        ids = [f"{m} {u}" for (m, u, _r, kw) in cases]

        metafunc.parametrize("method,url,kwargs", params, ids=ids)


@pytest.mark.asyncio
async def test_smoke(client: httpx.AsyncClient, method, url, kwargs):
    resp = await client.request(method, url, **(kwargs or {}))
    assert resp.status_code < 500, f"{method} {url} → {resp.status_code} ({resp.text[:200]})"


# @pytest.mark.asyncio
# async def test__print_routes_inventory(test_app):
#     rows = list_all_routes(test_app)
#     print("\n--- ROUTES INVENTORY ---")
#     for row in rows:
#         print(
#             f"{','.join(row['methods']):<18} {row['path']:<40} "
#             f"params={row['path_params']} body={row['requires_body']} "
#             # f"name={row['name']} endpoint={row['endpoint']} "
#             # f"schema={row['include_in_schema']}"
#         )
#     from ccdexplorer.api.app.state_getters import get_api_keys

#     print("Resolved module:", get_api_keys.__module__)  # use this in monkeypatch
