"""The token base.html puts in every page, and the key that sits beside it.

base/base.html renders `<script>api_key = '{{env.API_KEY}}'</script>`, and the
templates append that value to their ajax urls. Nothing validates it: the only
reference to app.env["API_KEY"] in the repo is the line that assigns it, and
the routes that take it as a path parameter ignore it. It is a cache buster,
not a credential.

It was uuid.uuid1(), whose node field is the host's MAC address and whose
time field is the moment of generation. Both were published in the source of
every page on the site. uuid4 is random, which is all a cache buster needs.

Separately, ten routes passed "API_KEY": CCDEXPLORER_API_KEY -- the real
backend key -- into template context, one character away from the
{{env.API_KEY}} that is already rendered. No template read it, so nothing
leaked, but a single careless {{API_KEY}} would have published the production
key. They are removed rather than renamed: nothing wanted them.
"""

import re


def test_the_token_is_not_a_version_1_uuid():
    """Version 1 encodes the MAC address. The version nibble is the first
    character of the third group."""
    import inspect

    from ccdexplorer.ccdexplorer_site.app import factory

    source = inspect.getsource(factory.create_app)
    assert "uuid.uuid1()" not in source, "the page token still leaks the host MAC"


def test_the_token_is_random():
    import inspect

    from ccdexplorer.ccdexplorer_site.app import factory

    source = inspect.getsource(factory.create_app)
    assert "uuid.uuid4()" in source


def test_a_version_4_uuid_carries_no_host_or_time():
    """What the change buys, asserted on the value's shape: version 4 and a
    variant nibble, with the other 122 bits random."""
    import uuid

    value = str(uuid.uuid4())
    assert re.match(
        r"^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$", value
    ), value
    assert uuid.UUID(value).version == 4


def test_two_tokens_differ():
    import uuid

    assert str(uuid.uuid4()) != str(uuid.uuid4())


# --- the real key must not travel next to the rendered one ----------------


def test_no_route_passes_the_backend_key_into_a_template():
    """The footgun: a context key one character from the one base.html
    renders, holding the production key."""
    import pathlib

    import ccdexplorer.ccdexplorer_site.app.routers as routers

    offenders = []
    for root in (pathlib.Path(p) for p in routers.__path__):
        for path in root.rglob("*.py"):
            for number, line in enumerate(path.read_text().splitlines(), 1):
                if '"API_KEY":' in line:
                    offenders.append(f"{path.name}:{number}")
    assert offenders == [], offenders


def test_no_template_renders_a_bare_api_key():
    """Nothing did, which is why nothing leaked. Asserted so it stays that
    way: base.html's {{env.API_KEY}} is the cache buster and is fine."""
    import pathlib

    here = pathlib.Path(__file__).resolve()
    root = next(p for p in here.parents if (p / "justfile").is_file())
    templates = root / "projects/ccdexplorer_site/templates"

    offenders = []
    for path in templates.rglob("*.html"):
        text = path.read_text(errors="replace")
        for match in re.finditer(r"\{\{\s*API_KEY\s*\}\}", text):
            offenders.append(f"{path.name}: {match.group(0)}")
    assert offenders == [], offenders


# --- the shared environment dict ------------------------------------------


def test_the_app_does_not_mutate_the_shared_environment_dict():
    """app.env = environment aliased the module-level dict, so assigning
    API_KEY into it added the token to ccdexplorer.env.environment for every
    component in the process that imports it."""
    import inspect

    from ccdexplorer.ccdexplorer_site.app import factory

    source = inspect.getsource(factory.create_app)
    assert "app.env = environment\n" not in source
    assert "dict(environment)" in source


def test_the_module_level_environment_has_no_api_key():
    from ccdexplorer.env import environment

    assert "API_KEY" not in environment
