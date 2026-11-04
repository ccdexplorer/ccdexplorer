"""Both services answer /health directly, so a deploy can ask them.

The site had a /health handler that was never reached. `home.router`
registers `/{net}` for any single segment, and `health` is not a valid net,
so the request was answered 302 -> /mainnet. The container healthcheck passed
regardless, because urllib.request.urlopen follows redirects and got its 200
from the rendered homepage -- an expensive page render standing in for a
liveness probe, and nobody could tell from the compose file.

The api had no /health at all: it answered 404.

Both matter now, because the automated blue/green flip asks the new colour
whether it is serving before it moves traffic. A gate that follows a redirect
cannot tell a working container from one that redirects you somewhere that
works, and a gate on a 404 cannot be written at all.

The routes touch nothing on purpose -- no Mongo, no api, no app state. A
health route that calls a dependency reports that dependency's health, and
the flip would then wait on the wrong thing. It also means these tests need
no lifespan, which is why TestClient is never entered as a context manager.
"""

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

REPO = Path(__file__).resolve().parent.parent


def _site_app():
    from ccdexplorer.ccdexplorer_site.app.factory import AppSettings, create_app

    project = REPO / "projects" / "ccdexplorer_site"
    return create_app(
        AppSettings(
            static_dir=project / "static",
            templates_dir=project / "templates",
            node_modules_dir=project / "node_modules",
            addresses_dir=project / "addresses",
        )
    )


def _api_app():
    from ccdexplorer.ccdexplorer_api.app.factory import AppSettings, create_app

    project = REPO / "projects" / "ccdexplorer_api"
    return create_app(
        AppSettings(
            static_dir=project / "static",
            templates_dir=project / "templates",
            node_modules_dir=project / "node_modules",
        )
    )


@pytest.fixture(scope="module")
def site():
    return TestClient(_site_app(), follow_redirects=False)


@pytest.fixture(scope="module")
def api():
    return TestClient(_api_app(), follow_redirects=False)


# --- the gate the deploy actually uses -------------------------------------


@pytest.mark.parametrize("service", ["site", "api"])
def test_health_answers_200_without_redirecting(service, request):
    """follow_redirects=False is the whole point: a 302 has to fail here, the
    way it would not in a browser or under urllib."""
    client = request.getfixturevalue(service)

    response = client.get("/health")

    assert response.status_code == 200, f"{service} answered {response.status_code}"
    assert response.json() == {"status": "ok"}


@pytest.mark.parametrize("service", ["site", "api"])
def test_health_is_not_behind_a_redirect_chain(service, request):
    client = request.getfixturevalue(service)

    response = client.get("/health")

    assert response.history == [], f"{service} redirected: {response.history}"
    assert "location" not in {h.lower() for h in response.headers}


# --- the regression that put it there --------------------------------------


def test_the_site_registers_health_before_its_catch_all():
    """`/{net}` matches any single segment. Whichever of the two is
    registered first wins, and for two years it was not this one."""
    routes = [getattr(r, "path", None) for r in _site_app().routes]

    assert "/health" in routes, routes
    assert "/{net}" in routes, "the catch-all moved; this guard needs rewriting"
    assert routes.index("/health") < routes.index("/{net}")


def test_the_site_has_exactly_one_health_route():
    """The unreachable one in home.router was removed, not left to rot."""
    routes = [getattr(r, "path", None) for r in _site_app().routes]

    assert routes.count("/health") == 1, routes


def test_health_costs_nothing_to_serve(site):
    """No lifespan has run on these apps -- app.mongodb, app.api_url and the
    rest do not exist. If this passes, the route touched none of them."""
    assert not hasattr(site.app, "mongodb")

    assert site.get("/health").status_code == 200


# --- /version: which build is answering ------------------------------------
#
# /health cannot close this gap. Both colours answer it 200 -- that is what
# blue/green means -- so it proves a container is alive and says nothing about
# which image it is. The deploy gate could therefore confirm a stack was
# redeployed and that Traefik pointed at the new container while never once
# asking the running process what it was.


@pytest.mark.parametrize("service", ["site", "api"])
def test_version_answers_200_without_redirecting(service, request):
    client = request.getfixturevalue(service)

    response = client.get("/version")

    assert response.status_code == 200, f"{service} answered {response.status_code}"
    assert response.history == [], f"{service} redirected: {response.history}"


@pytest.mark.parametrize("service", ["site", "api"])
def test_version_reports_the_build_tag(service, request):
    """Whatever BUILD_TAG says. It is "unknown" in a test run and a seven-char
    sha in a built image; the gate compares it to the tag it deployed."""
    from ccdexplorer.env import BUILD_TAG

    client = request.getfixturevalue(service)

    assert client.get("/version").json() == {"build": BUILD_TAG}


def test_the_site_registers_version_before_its_catch_all():
    """Same trap as /health: `/{net}` matches any single segment, so a
    /version registered after home.router would answer 302 -> /mainnet."""
    routes = [getattr(r, "path", None) for r in _site_app().routes]

    assert "/version" in routes, routes
    assert routes.index("/version") < routes.index("/{net}")


@pytest.mark.parametrize("service", ["site", "api"])
def test_version_costs_nothing_to_serve(service, request):
    """No lifespan has run, so a route that touched app state would error."""
    client = request.getfixturevalue(service)

    assert not hasattr(client.app, "mongodb")
    assert client.get("/version").status_code == 200


def test_the_build_tag_is_baked_in_rather_than_deployed():
    """The point of the whole mechanism.

    An answer the deployer supplied would only repeat what the deployer
    already believed, so BUILD_TAG has to arrive from the image. Asserted on
    the Dockerfiles because that is where it is decided.
    """
    for project in ("ccdexplorer_site", "ccdexplorer_api"):
        dockerfile = (REPO / "projects" / project / "Dockerfile").read_text()

        assert "ARG BUILD_TAG" in dockerfile, project
        assert "ENV BUILD_TAG=${BUILD_TAG}" in dockerfile, project


def test_the_build_arg_is_declared_last():
    """An ARG that changes every commit invalidates every layer after it, so
    putting this near the top would silently cost the whole build cache."""
    for project in ("ccdexplorer_site", "ccdexplorer_api"):
        lines = (REPO / "projects" / project / "Dockerfile").read_text().splitlines()
        arg_at = next(i for i, line in enumerate(lines) if line.startswith("ARG BUILD_TAG"))
        expensive = [
            i
            for i, line in enumerate(lines)
            if line.startswith(("RUN ", "COPY ", "FROM "))
        ]

        assert arg_at > max(expensive), f"{project}: ARG BUILD_TAG precedes cached work"


def test_the_build_passes_the_tag_it_tags_the_image_with():
    """If these two ever name different values, the gate fails a build that is
    in fact correct -- which is worse than no gate, because it looks like a
    broken deploy."""
    workflow = (REPO / ".github" / "workflows" / "build-projects.yml").read_text()

    assert "short_sha=${SHORT_SHA}" in workflow
    assert "BUILD_TAG=${{ steps.tags.outputs.short_sha }}" in workflow
    assert 'TAGS="${REPO}:${SHORT_SHA}"' in workflow
