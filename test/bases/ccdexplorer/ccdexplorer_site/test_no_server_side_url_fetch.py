"""The site must not fetch a URL somebody else chose.

/fetch-submit took `api_url` straight off a form and GET it server-side, with
no authentication, no allowlist and no scheme check, then handed the response
body back as a CSV download. Host and protocol were both the caller's, which
makes it a read-SSRF with exfiltration rather than a path-confusion nuisance:
a POST with api_url=http://127.0.0.1:7010/openapi.json returned 887,032 bytes
of the internal API's own spec, and anything else the site's network could
reach -- Mongo's http interfaces, the grpc hosts, cloud instance metadata --
was readable the same way.

Nothing used it. The only references in the repo were the route, the page
that posted to it, and the form inside that page: no nav entry, no menu, no
javascript, and include_in_schema=False kept it out of /docs. Its label even
rendered the literal string "{API_KEY_HEADER_NAME}", an unformatted
placeholder. So it is removed rather than fenced -- an allowlist around a
feature nobody asked for is a fence around an empty field.

Worth recording why it mattered more than it looked: the page promised "Key
is only used for this request and isn't logged or stored", while the response
was written to /tmp/api_export_<timestamp>.csv -- and the /tmp/{filename}
route serves any name in that directory to anyone, with a timestamp accurate
to the second being the whole of the secret.
"""


def _site_paths():
    from ccdexplorer.ccdexplorer_site.app.routers import tools

    return {getattr(r, "path", None) for r in tools.router.routes}


def test_the_url_fetching_route_is_gone():
    assert "/fetch-submit" not in _site_paths()


def test_the_page_that_drove_it_is_gone():
    assert "/tools/api-direct" not in _site_paths()


def test_the_template_is_gone():
    import pathlib

    here = pathlib.Path(__file__).resolve()
    root = next(p for p in here.parents if (p / "justfile").is_file())
    assert not (root / "projects/ccdexplorer_site/templates/tools/api_direct.html").exists()


def test_no_route_in_the_site_fetches_a_caller_supplied_url():
    """The general property, not just the one route. A handler that takes a
    url or endpoint as a form field or query parameter is the shape to catch;
    every legitimate call in the site is built from request.app.api_url."""
    import pathlib

    import ccdexplorer.ccdexplorer_site.app.routers as routers

    # A namespace package, so __path__ rather than __file__.
    roots = [pathlib.Path(p) for p in routers.__path__]
    offenders = []
    for path in (f for root in roots for f in root.rglob("*.py")):
        source = path.read_text()
        for name in ("api_url: str = Form", "url: str = Form", "endpoint: str = Form"):
            if name in source:
                offenders.append((path.name, name))
    assert offenders == [], offenders


# --- the certificate authority bundle -------------------------------------
#
# Eight eMudhra intermediate CA keys were generated with single-byte private
# exponents and were compromised; reported 2026-10-17, revoked 2026-10-18.
# Mozilla removed the root that signed them -- emSign ECC Root CA - G3 -- in
# NSS 3.131.1, and certifi follows NSS. The pinned bundle was 2025.8.3, which
# still carried it, so every outbound https call in the repo (requests, httpx
# and aiohttp all default to certifi) would have accepted a certificate
# chaining to those keys.


def test_the_compromised_emudhra_root_is_not_trusted():
    import certifi

    bundle = open(certifi.where()).read()
    assert "emSign ECC Root CA - G3" not in bundle


def test_the_root_mozilla_kept_is_still_trusted():
    """Not a blanket purge of the issuer: Mozilla explicitly did not remove
    emSign Root CA - G1, so dropping it here would be our own decision to
    break whatever still chains to it."""
    import certifi

    bundle = open(certifi.where()).read()
    assert "emSign Root CA - G1" in bundle


def test_the_bundle_is_not_the_stale_one():
    """2025.8.3 was a year behind by the time this was found, and 27 roots
    adrift. Pinning the version here would fight every future refresh, so
    this asserts the floor that matters instead."""
    import certifi

    bundle = open(certifi.where()).read()
    assert bundle.count("BEGIN CERTIFICATE") <= 130, "looks like the pre-removal bundle"
