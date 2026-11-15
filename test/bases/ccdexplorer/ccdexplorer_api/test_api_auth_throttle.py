"""The api's own login, reset and register forms are throttled.

/auth/* had no limit of any kind: a login could be guessed at without end,
the reset form mailed its owner as often as it was asked to, and accounts
could be registered without a ceiling -- which also defeats the per-account
free-tier limits. And a reset token, once issued, never stopped working.

The site's /site-auth routes already had all of this; these tests hold the
api's forms to the same numbers.
"""

import datetime as dt
from types import SimpleNamespace

from ccdexplorer.ccdexplorer_api.app.models import User
from ccdexplorer.ccdexplorer_api.app.routers.auth import auth
from ccdexplorer.ccdexplorer_api.app.routers.site_auth import site_auth
from ccdexplorer.ccdexplorer_api.app.security import hash_password

NOW = dt.datetime.now().astimezone(dt.UTC)


class FakeRedis:
    def __init__(self):
        self.store: dict[str, int] = {}

    async def get(self, key):
        value = self.store.get(key)
        return None if value is None else str(value).encode()

    async def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    async def expire(self, key, ttl):
        return True

    async def delete(self, key):
        self.store.pop(key, None)
        return 1


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
        hits = [d for d in self.docs if all(d.get(k) == v for k, v in query.items())]
        return _Result(hits)

    def aggregate(self, pipeline):
        return _Result([])

    async def find_one_and_update(self, query, update):
        return {"alias": "3alias", "alias_id": query["alias_id"]}

    async def find_one(self, query):
        return {"alias": "3alias"}

    async def bulk_write(self, operations):
        self.writes.append(operations)


class _Db:
    def __init__(self, users=()):
        self.users = _Collection([u.model_dump(exclude_none=True) for u in users])
        self.utilities_db = {
            "api_users": self.users,
            "api_aliases": _Collection([{"alias": "3alias", "alias_id": 0}]),
        }

    def __call__(self):
        return self


class _Templates:
    def __init__(self):
        self.rendered = []

    def TemplateResponse(self, request, name, context, status_code=200):
        self.rendered.append((name, context, status_code))
        return SimpleNamespace(template=name, context=context, status_code=status_code)


class _Tooter:
    def __init__(self):
        self.sent = []

    def email_api(self, **kwargs):
        self.sent.append(kwargs)


def _user(email="a@example.org", password="correct horse", **extra):
    return User(
        scope="http://x",
        token="tok",
        alias_id=0,
        alias_account_id="3alias",
        api_account_id=f"id-{email}",
        email=email,
        password=hash_password(password),
        plan_end_date=NOW,
        **extra,
    )


def _request(ip="203.0.113.7", body=b"", redis=None, templates=None, tooter=None):
    async def read_body():
        return body

    return SimpleNamespace(
        headers={"x-forwarded-for": ip},
        client=SimpleNamespace(host="172.18.0.2"),
        body=read_body,
        app=SimpleNamespace(
            r=redis if redis is not None else FakeRedis(),
            tooter=tooter or _Tooter(),
            state=SimpleNamespace(templates=templates or _Templates()),
        ),
    )


def _form(username, password="x"):
    return SimpleNamespace(username=username, password=password)


# --- login ----------------------------------------------------------------


async def test_ten_wrong_passwords_lock_that_email_out():
    db = _Db([_user()])
    redis = FakeRedis()
    req = _request(redis=redis)
    for _ in range(site_auth.LOGIN_MAX_ATTEMPTS):
        await auth.login(req, None, _form("a@example.org", "wrong"), db)

    # Now even the right password is refused, until the window ends.
    result = await auth.login(req, None, _form("a@example.org", "correct horse"), db)
    assert getattr(result, "status_code", None) == 429
    assert "Too many" in result.context["errors"][0]


async def test_a_good_login_is_not_refused_before_the_limit():
    db = _Db([_user()])
    req = _request()
    result = await auth.login(req, None, _form("a@example.org", "correct horse"), db)
    assert result.status_code == 303


async def test_a_good_login_clears_the_failures():
    db = _Db([_user()])
    redis = FakeRedis()
    req = _request(redis=redis)
    for _ in range(site_auth.LOGIN_MAX_ATTEMPTS - 1):
        await auth.login(req, None, _form("a@example.org", "wrong"), db)
    await auth.login(req, None, _form("a@example.org", "correct horse"), db)
    result = await auth.login(req, None, _form("a@example.org", "wrong"), db)
    assert getattr(result, "status_code", 200) != 429


async def test_one_address_trying_many_emails_is_stopped():
    """Ten attempts each across ten thousand addresses is the same attack."""
    db = _Db([])
    redis = FakeRedis()
    req = _request(redis=redis)
    for i in range(site_auth.LOGIN_MAX_ATTEMPTS_PER_IP):
        await auth.login(req, None, _form(f"u{i}@example.org", "x"), db)
    result = await auth.login(req, None, _form("fresh@example.org", "x"), db)
    assert result.status_code == 429


async def test_another_address_is_not_locked_out_by_the_first():
    db = _Db([_user()])
    redis = FakeRedis()
    for _ in range(site_auth.LOGIN_MAX_ATTEMPTS_PER_IP):
        await auth.login(_request("198.51.100.1", redis=redis), None, _form("x@example.org"), db)
    result = await auth.login(
        _request("198.51.100.2", redis=redis), None, _form("a@example.org", "correct horse"), db
    )
    assert result.status_code == 303


async def test_the_failure_message_is_the_same_for_unknown_and_wrong():
    db = _Db([_user()])
    req = _request()
    unknown = await auth.login(req, None, _form("nobody@example.org"), db)
    wrong = await auth.login(req, None, _form("a@example.org", "wrong"), db)
    assert unknown.context["errors"] == wrong.context["errors"]


async def test_a_broken_redis_does_not_lock_everyone_out():
    class Broken:
        async def get(self, k):
            raise RuntimeError

        incr = expire = delete = get

    db = _Db([_user()])
    result = await auth.login(
        _request(redis=Broken()), None, _form("a@example.org", "correct horse"), db
    )
    assert result.status_code == 303


# --- forgot password ------------------------------------------------------


async def test_the_reset_mail_stops_after_the_limit():
    db = _Db([_user()])
    tooter = _Tooter()
    redis = FakeRedis()
    for _ in range(site_auth.EMAIL_MAX_PER_WINDOW + 3):
        req = _request(redis=redis, tooter=tooter)
        await auth.forgot_password_action(req, None, db, username="a@example.org")
    assert len(tooter.sent) == site_auth.EMAIL_MAX_PER_WINDOW


async def test_a_throttled_reset_looks_the_same_as_an_unthrottled_one():
    """Otherwise the throttle tells a stranger which addresses have accounts."""
    db = _Db([_user()])
    redis = FakeRedis()
    first = await auth.forgot_password_action(
        _request(redis=redis), None, db, username="a@example.org"
    )
    for _ in range(site_auth.EMAIL_MAX_PER_WINDOW):
        last = await auth.forgot_password_action(
            _request(redis=redis), None, db, username="a@example.org"
        )
    assert first.template == last.template
    assert first.status_code == last.status_code == 200


async def test_one_address_cannot_mail_many_accounts():
    db = _Db([_user(f"u{i}@example.org") for i in range(site_auth.EMAIL_MAX_PER_IP + 5)])
    tooter = _Tooter()
    redis = FakeRedis()
    for i in range(site_auth.EMAIL_MAX_PER_IP + 5):
        req = _request(redis=redis, tooter=tooter)
        await auth.forgot_password_action(req, None, db, username=f"u{i}@example.org")
    assert len(tooter.sent) == site_auth.EMAIL_MAX_PER_IP


# --- reset tokens expire --------------------------------------------------


async def test_a_fresh_reset_token_is_issued_with_an_expiry():
    db = _Db([_user()])
    req = _request()
    await auth.forgot_password_action(req, None, db, username="a@example.org")
    replacement = db.users.writes[-1][0]._doc
    assert replacement["reset_password_token"]
    expires = replacement["reset_password_token_expires"]
    assert dt.timedelta(minutes=30) < expires - dt.datetime.now(dt.UTC) <= dt.timedelta(hours=1)


async def test_an_expired_token_does_not_find_its_user():
    stale = _user(
        reset_password_token="old",
        reset_password_token_expires=NOW - dt.timedelta(minutes=1),
    )
    assert await auth.get_user_by_reset_password_token("old", _Db([stale])) is None


async def test_a_live_token_finds_its_user():
    live = _user(
        reset_password_token="new",
        reset_password_token_expires=NOW + dt.timedelta(minutes=30),
    )
    found = await auth.get_user_by_reset_password_token("new", _Db([live]))
    assert found is not None and found.email == "a@example.org"


async def test_a_token_issued_before_expiries_existed_is_not_honoured():
    old = _user(reset_password_token="legacy")
    assert await auth.get_user_by_reset_password_token("legacy", _Db([old])) is None


# --- register -------------------------------------------------------------


async def test_registrations_from_one_address_are_capped():
    db = _Db([])
    redis = FakeRedis()
    results = []
    for i in range(site_auth.EMAIL_MAX_PER_IP + 3):
        req = _request(redis=redis)
        results.append(await auth.register(req, _form(f"new{i}@example.org", "pw"), db))
    created = [r for r in results if getattr(r, "status_code", None) == 303]
    assert len(created) == site_auth.EMAIL_MAX_PER_IP
    assert results[-1].status_code == 429


# --- which address is counted ---------------------------------------------


def test_the_forged_front_of_the_chain_is_not_the_caller():
    req = _request("6.6.6.6, 203.0.113.7")
    assert auth.caller_ip(req) == "203.0.113.7"


def test_our_own_hop_is_skipped():
    assert auth.caller_ip(_request("203.0.113.7, 10.0.0.1")) == "203.0.113.7"


def test_garbage_falls_back_to_the_peer():
    assert auth.caller_ip(_request("nonsense")) == "172.18.0.2"
