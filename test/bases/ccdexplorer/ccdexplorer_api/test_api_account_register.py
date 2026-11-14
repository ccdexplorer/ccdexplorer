"""An api account is not created for something that is not an email address.

The api's own register form stored whatever was typed as the account's email,
and the forgot-password mail later goes to that stored value. The site's
register route makes the same check; see test_site_auth.py.
"""

from types import SimpleNamespace

import pytest

from ccdexplorer.ccdexplorer_api.app.routers.auth import auth


class _Result:
    def __init__(self, docs):
        self._docs = docs

    async def to_list(self, length=None):
        return list(self._docs)

    def __await__(self):
        async def me():
            return self

        return me().__await__()


class _Collection:
    def __init__(self, docs=None):
        self.docs = docs or []
        self.writes = []

    def find(self, query):
        return _Result([])

    def aggregate(self, pipeline):
        return _Result(self.docs)

    async def bulk_write(self, operations):
        self.writes.append(operations)


class _Db:
    """Stands in for MongoMotor, and for the factory that returns one."""

    def __init__(self):
        self.users = _Collection()
        self.utilities_db = {
            "api_users": self.users,
            "api_aliases": _Collection([{"alias": "3alias", "alias_id": 0}]),
        }

    def __call__(self):
        return self


class _Templates:
    def __init__(self):
        self.rendered = []

    def TemplateResponse(self, request, name, context):
        self.rendered.append((name, context))
        return SimpleNamespace(template=name, context=context)


def _request(templates):
    return SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(templates=templates)))


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "email",
    [
        "a@example.com&smtp=evil.example",
        "a@example.com,json://evil.example/x",
        "a@example.com other@evil.example",
        "nobody",
    ],
)
async def test_no_account_for_what_is_not_an_address(email):
    db = _Db()
    templates = _Templates()

    await auth.register(
        request=_request(templates),
        form_data=SimpleNamespace(username=email, password="whatever"),
        db=db,
    )

    assert db.users.writes == []
    name, context = templates.rendered[0]
    assert name == "auth/register.html"
    assert context["errors"] == ["please enter a valid email address."]


@pytest.mark.asyncio
async def test_an_ordinary_address_still_gets_an_account():
    db = _Db()

    response = await auth.register(
        request=_request(_Templates()),
        form_data=SimpleNamespace(username="user+api@example.com", password="whatever"),
        db=db,
    )

    assert len(db.users.writes) == 1
    assert response.status_code == 303
