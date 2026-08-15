"""Tests for the security fixes on the ``/site-auth`` router.

These cover finding #3 from the security review:

* The endpoints now require a *valid* API key (``require_api_auth``), not just a
  present header. Bogus keys are rejected; the site's registered key passes.
* Login / forgot-password / register are throttled per email to stop brute force
  and verification/reset email bombing.

They follow the unit style of ``test_mcp_auth_headers.py`` (call the functions
directly with in-memory fakes) so they need no live Mongo/Redis.
"""

import datetime as dt
import json
from types import SimpleNamespace

import pytest
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_api.app.ratelimiting import require_api_auth
from ccdexplorer.ccdexplorer_api.app.routers.site_auth import site_auth
from ccdexplorer.ccdexplorer_api.app.security import hash_password
from ccdexplorer.mongodb import CollectionsUtilities


# --------------------------------------------------------------------------- #
# In-memory fakes
# --------------------------------------------------------------------------- #
class FakeRedis:
    """Minimal async stand-in for the bits of redis the throttler uses."""

    def __init__(self):
        self.store: dict[str, int] = {}

    async def get(self, key):
        value = self.store.get(key)
        return None if value is None else str(value).encode()  # real client returns bytes

    async def incr(self, key):
        self.store[key] = int(self.store.get(key, 0)) + 1
        return self.store[key]

    async def expire(self, key, ttl):
        return True

    async def delete(self, key):
        self.store.pop(key, None)
        return 1


def _matches(doc: dict, query: dict) -> bool:
    for key, cond in query.items():
        value = doc.get(key)
        if isinstance(cond, dict) and "$ne" in cond:
            if value == cond["$ne"]:
                return False
        elif value != cond:
            return False
    return True


class FakeCursor:
    """Minimal async stand-in for a Mongo cursor: supports .sort()/.limit()
    chaining (mutating the pending result list) and `async for`."""

    def __init__(self, docs):
        self._docs = list(docs)

    def sort(self, key, direction=-1):
        self._docs.sort(key=lambda d: d.get(key), reverse=(direction == -1))
        return self

    def limit(self, n):
        self._docs = self._docs[:n]
        return self

    def __aiter__(self):
        return self._aiter()

    async def _aiter(self):
        for doc in self._docs:
            yield dict(doc)


class FakeCollection:
    def __init__(self, docs=None):
        self.docs = list(docs or [])

    async def find_one(self, query, projection=None):
        for doc in self.docs:
            if _matches(doc, query):
                return dict(doc)
        return None

    def find(self, query=None):
        query = query or {}
        return FakeCursor([doc for doc in self.docs if _matches(doc, query)])

    async def insert_one(self, doc):
        self.docs.append(dict(doc))
        return SimpleNamespace(inserted_id=doc.get("_id"))

    async def update_one(self, query, update):
        for doc in self.docs:
            if _matches(doc, query):
                doc.update(update.get("$set", {}))
                return SimpleNamespace(matched_count=1)
        return SimpleNamespace(matched_count=0)

    async def update_many(self, query, update):
        count = 0
        for doc in self.docs:
            if _matches(doc, query):
                doc.update(update.get("$set", {}))
                count += 1
        return SimpleNamespace(matched_count=count)

    async def delete_one(self, query):
        for i, doc in enumerate(self.docs):
            if _matches(doc, query):
                del self.docs[i]
                return SimpleNamespace(deleted_count=1)
        return SimpleNamespace(deleted_count=0)

    async def delete_many(self, query):
        before = len(self.docs)
        self.docs = [doc for doc in self.docs if not _matches(doc, query)]
        return SimpleNamespace(deleted_count=before - len(self.docs))

    async def bulk_write(self, operations):  # save_user() writes through this
        return None


class FakeMongo:
    def __init__(self, users=None, sessions=None, audit_log=None):
        users = users if users is not None else FakeCollection([])
        self.utilities = {
            # Both keys point at the same collection so tests don't depend on
            # the developer's local ENVIRONMENT setting -- see
            # state_getters.site_users_collection_name().
            CollectionsUtilities.users_v2_prod: users,
            CollectionsUtilities.users_v2_dev: users,
            CollectionsUtilities.user_sessions: (
                sessions if sessions is not None else FakeCollection([])
            ),
            CollectionsUtilities.login_audit_log: (
                audit_log if audit_log is not None else FakeCollection([])
            ),
        }


class FakeTooter:
    def __init__(self):
        self.emails = []

    def email_api(self, **kwargs):
        self.emails.append(kwargs)


def make_request(redis=None, tooter=None):
    app = SimpleNamespace(r=redis or FakeRedis(), tooter=tooter or FakeTooter())
    return SimpleNamespace(app=app)


# --------------------------------------------------------------------------- #
# Authentication: a valid API key is required (finding #3, part A)
# --------------------------------------------------------------------------- #
def test_site_auth_router_enforces_valid_api_key():
    """The router carries the validating dependency, not just presence checks."""
    dep_names = [d.dependency.__name__ for d in site_auth.router.dependencies]
    assert "require_api_auth" in dep_names


def _scope_for(api_key: str, keys: dict):
    async def fake_get_api_keys(*args, **kwargs):
        return keys

    app = SimpleNamespace(
        state=SimpleNamespace(get_api_keys_fn=fake_get_api_keys),
        motormongo=None,
    )
    return {"app": app, "headers": [(b"x-ccdexplorer-key", api_key.encode())]}


@pytest.mark.asyncio
async def test_require_api_auth_rejects_bogus_key():
    request = SimpleNamespace(scope=_scope_for("bogus-key", {}))
    with pytest.raises(HTTPException) as exc:
        await require_api_auth(request)
    assert exc.value.status_code == 401


@pytest.mark.asyncio
async def test_require_api_auth_accepts_registered_key():
    keys = {"site-key": {"api_account_id": "acct", "api_group": "grp"}}
    request = SimpleNamespace(scope=_scope_for("site-key", keys))
    assert await require_api_auth(request) == ("acct", "grp")


# --------------------------------------------------------------------------- #
# Throttling (finding #3, part B)
# --------------------------------------------------------------------------- #
@pytest.mark.asyncio
async def test_login_throttles_after_max_attempts():
    """After LOGIN_MAX_ATTEMPTS failures the next attempt is 429, not 401."""
    email = "victim@example.com"
    # User has no password set, so each attempt fails fast (no bcrypt) but still
    # increments the throttle counter.
    users = FakeCollection([{"token": "tok", "email_address": email}])
    mongo = FakeMongo(users)
    request = make_request()
    body = site_auth.LoginRequest(email=email, password="guess")

    for _ in range(site_auth.LOGIN_MAX_ATTEMPTS):
        with pytest.raises(HTTPException) as exc:
            await site_auth.login(request=request, body=body, mongomotor=mongo)
        assert exc.value.status_code == 401

    with pytest.raises(HTTPException) as exc:
        await site_auth.login(request=request, body=body, mongomotor=mongo)
    assert exc.value.status_code == 429


@pytest.mark.asyncio
async def test_login_valid_key_reaches_credentials_check():
    """A request that passed auth but has no matching user gets the app-level
    401 ('Invalid email or password'), proving the valid-key path runs."""
    mongo = FakeMongo(FakeCollection([]))  # no users
    request = make_request()
    body = site_auth.LoginRequest(email="nobody@example.com", password="x")

    with pytest.raises(HTTPException) as exc:
        await site_auth.login(request=request, body=body, mongomotor=mongo)
    assert exc.value.status_code == 401
    assert exc.value.detail == "Invalid email or password."


@pytest.mark.asyncio
async def test_login_success_returns_token_and_clears_throttle():
    email = "user@example.com"
    users = FakeCollection(
        [
            {
                "token": "the-token",
                "email_address": email,
                "password": hash_password("correct-horse"),
                "email_verified": True,
            }
        ]
    )
    redis = FakeRedis()
    # Pre-load some prior failures; a successful login must wipe them.
    redis.store[site_auth._throttle_key("login", email)] = 3
    request = make_request(redis=redis)

    bad = site_auth.LoginRequest(email=email, password="wrong")
    with pytest.raises(HTTPException):
        await site_auth.login(request=request, body=bad, mongomotor=FakeMongo(users))

    good = site_auth.LoginRequest(email=email, password="correct-horse")
    result = await site_auth.login(request=request, body=good, mongomotor=FakeMongo(users))
    assert result == {"token": "the-token"}
    assert site_auth._throttle_key("login", email) not in redis.store


@pytest.mark.asyncio
async def test_forgot_password_stops_emailing_when_throttled_but_stays_silent():
    email = "reset@example.com"
    users = FakeCollection([{"token": "tok", "email_address": email}])
    tooter = FakeTooter()
    request = make_request(tooter=tooter)
    body = site_auth.ForgotPasswordRequest(email=email)

    # Calls up to the limit each send one email...
    for _ in range(site_auth.EMAIL_MAX_PER_WINDOW):
        assert await site_auth.forgot_password(
            request=request, body=body, mongomotor=FakeMongo(users)
        ) == {"ok": True}

    # ...the next is throttled: still 'ok' (no enumeration), but no extra email.
    assert await site_auth.forgot_password(
        request=request, body=body, mongomotor=FakeMongo(users)
    ) == {"ok": True}
    assert len(tooter.emails) == site_auth.EMAIL_MAX_PER_WINDOW


@pytest.mark.asyncio
async def test_register_throttled_returns_429():
    email = "spam-target@example.com"
    redis = FakeRedis()
    redis.store[site_auth._throttle_key("register", email)] = site_auth.EMAIL_MAX_PER_WINDOW
    request = make_request(redis=redis)
    body = site_auth.RegisterRequest(email=email, password="whatever")

    with pytest.raises(HTTPException) as exc:
        await site_auth.register(request=request, body=body, mongomotor=FakeMongo(FakeCollection([])))
    assert exc.value.status_code == 429


# --------------------------------------------------------------------------- #
# Reset / verification token expiry (finding #5)
# --------------------------------------------------------------------------- #
def _future():
    return dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)


def _past():
    return dt.datetime.now(dt.timezone.utc) - dt.timedelta(hours=1)


def _user_doc(**overrides):
    doc = {"token": "tok", "email_address": "u@example.com"}
    doc.update(overrides)
    return doc


@pytest.mark.asyncio
async def test_reset_password_accepts_unexpired_token():
    mongo = FakeMongo(
        FakeCollection([_user_doc(reset_password_token="rt", reset_password_token_expires=_future())])
    )
    result = await site_auth.reset_password(
        body=site_auth.ResetPasswordRequest(reset_password_token="rt", password="new-pw"),
        mongomotor=mongo,
    )
    assert result == {"token": "tok"}


@pytest.mark.asyncio
async def test_reset_password_rejects_expired_token():
    mongo = FakeMongo(
        FakeCollection([_user_doc(reset_password_token="rt", reset_password_token_expires=_past())])
    )
    with pytest.raises(HTTPException) as exc:
        await site_auth.reset_password(
            body=site_auth.ResetPasswordRequest(reset_password_token="rt", password="new-pw"),
            mongomotor=mongo,
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_reset_password_rejects_token_without_expiry():
    """Legacy tokens stored before expiry existed must not be usable."""
    mongo = FakeMongo(FakeCollection([_user_doc(reset_password_token="rt")]))
    with pytest.raises(HTTPException) as exc:
        await site_auth.reset_password(
            body=site_auth.ResetPasswordRequest(reset_password_token="rt", password="x"),
            mongomotor=mongo,
        )
    assert exc.value.status_code == 404


@pytest.mark.asyncio
async def test_verify_email_accepts_unexpired_token():
    mongo = FakeMongo(
        FakeCollection(
            [_user_doc(verification_token="vt", verification_token_expires=_future(), email_verified=False)]
        )
    )
    result = await site_auth.verify_email(verification_token="vt", mongomotor=mongo)
    assert result == {"token": "tok"}


@pytest.mark.asyncio
async def test_verify_email_rejects_expired_token():
    mongo = FakeMongo(
        FakeCollection(
            [_user_doc(verification_token="vt", verification_token_expires=_past(), email_verified=False)]
        )
    )
    with pytest.raises(HTTPException) as exc:
        await site_auth.verify_email(verification_token="vt", mongomotor=mongo)
    assert exc.value.status_code == 404


# --------------------------------------------------------------------------- #
# Sessions: rotate-on-use with reuse detection, no PII stored
# --------------------------------------------------------------------------- #
def _utc_now():
    return dt.datetime.now(dt.timezone.utc)


def _session_doc(**overrides):
    now = _utc_now()
    doc = {
        "_id": "sess-1",
        "token": "current-token",
        "previous_token": None,
        "previous_token_expires": None,
        "user_token": "tok",
        "created_at": now,
        "last_seen_at": now,
        "last_rotated_at": now,
        "revoked": False,
        "revoked_reason": None,
    }
    doc.update(overrides)
    return doc


@pytest.mark.asyncio
async def test_create_session_mints_rotating_token_with_no_pii():
    mongo = FakeMongo(FakeCollection([_user_doc()]))
    session = await site_auth.create_session("tok", mongo)
    assert session["user_token"] == "tok"
    assert session["token"]
    assert set(session.keys()) == {
        "_id",
        "token",
        "previous_token",
        "previous_token_expires",
        "user_token",
        "created_at",
        "last_seen_at",
        "last_rotated_at",
        "revoked",
        "revoked_reason",
    }


@pytest.mark.asyncio
async def test_resolve_session_within_throttle_window_does_not_rotate():
    sessions = FakeCollection([_session_doc(last_rotated_at=_utc_now())])
    mongo = FakeMongo(FakeCollection([_user_doc()]), sessions)
    result = await site_auth.resolve_session("current-token", mongo)
    assert result["ok"] is True
    assert result["session_token"] == "current-token"


@pytest.mark.asyncio
async def test_resolve_session_rotates_after_throttle_window():
    stale = _utc_now() - site_auth.ROTATE_THROTTLE - dt.timedelta(seconds=1)
    sessions = FakeCollection([_session_doc(last_rotated_at=stale, created_at=stale)])
    mongo = FakeMongo(FakeCollection([_user_doc()]), sessions)
    result = await site_auth.resolve_session("current-token", mongo)
    assert result["ok"] is True
    assert result["session_token"] != "current-token"
    stored = sessions.docs[0]
    assert stored["token"] == result["session_token"]
    assert stored["previous_token"] == "current-token"


@pytest.mark.asyncio
async def test_resolve_session_grace_window_converges_to_current_token():
    """A request racing a rotation (using the just-superseded token) gets the
    new current token back, and does *not* trigger another rotation."""
    sessions = FakeCollection(
        [
            _session_doc(
                token="new-token",
                previous_token="old-token",
                previous_token_expires=_utc_now() + dt.timedelta(seconds=5),
            )
        ]
    )
    mongo = FakeMongo(FakeCollection([_user_doc()]), sessions)
    result = await site_auth.resolve_session("old-token", mongo)
    assert result["ok"] is True
    assert result["session_token"] == "new-token"
    assert sessions.docs[0]["token"] == "new-token"  # unchanged: no re-rotation


@pytest.mark.asyncio
async def test_resolve_session_reuse_past_grace_window_is_revoked_and_logged():
    sessions = FakeCollection(
        [
            _session_doc(
                token="new-token",
                previous_token="old-token",
                previous_token_expires=_utc_now() - dt.timedelta(seconds=1),
            )
        ]
    )
    audit = FakeCollection([])
    mongo = FakeMongo(FakeCollection([_user_doc()]), sessions, audit)
    result = await site_auth.resolve_session("old-token", mongo)
    assert result == {"ok": False, "reason": "revoked"}
    assert sessions.docs[0]["revoked"] is True
    assert sessions.docs[0]["revoked_reason"] == "reuse_detected"
    assert audit.docs[0]["event"] == "session_reuse_detected"
    assert audit.docs[0]["user_token"] == "tok"
    assert "ip" not in audit.docs[0]
    assert "user_agent" not in audit.docs[0]


@pytest.mark.asyncio
async def test_resolve_session_unknown_token_fails():
    mongo = FakeMongo(FakeCollection([_user_doc()]), FakeCollection([]))
    result = await site_auth.resolve_session("nonexistent", mongo)
    assert result == {"ok": False, "reason": "invalid"}


@pytest.mark.asyncio
async def test_resolve_session_migrates_legacy_account_token_cookie():
    """Pre-session-layer cookies held the raw SiteUser.token directly; the
    resolver treats that as an implicit login instead of forcing a logout."""
    mongo = FakeMongo(FakeCollection([_user_doc()]), FakeCollection([]))
    result = await site_auth.resolve_session("tok", mongo)
    assert result["ok"] is True
    assert result["session_token"] != "tok"  # cookie gets upgraded to a real session


@pytest.mark.asyncio
async def test_revoke_other_sessions_keeps_the_given_session():
    sessions = FakeCollection(
        [
            _session_doc(_id="keep", token="t1", user_token="tok"),
            _session_doc(_id="drop", token="t2", user_token="tok"),
        ]
    )
    mongo = FakeMongo(FakeCollection([_user_doc()]), sessions)
    await site_auth.revoke_other_sessions("tok", "keep", "logout_others", mongo)
    by_id = {d["_id"]: d for d in sessions.docs}
    assert by_id["keep"]["revoked"] is False
    assert by_id["drop"]["revoked"] is True
    assert by_id["drop"]["revoked_reason"] == "logout_others"


@pytest.mark.asyncio
async def test_revoke_other_sessions_with_no_keep_id_revokes_all():
    """Used by password reset, which has no "current device" to preserve."""
    sessions = FakeCollection(
        [
            _session_doc(_id="a", token="t1", user_token="tok"),
            _session_doc(_id="b", token="t2", user_token="tok"),
        ]
    )
    mongo = FakeMongo(FakeCollection([_user_doc()]), sessions)
    await site_auth.revoke_other_sessions("tok", None, "password_changed", mongo)
    assert all(d["revoked"] for d in sessions.docs)


def test_session_public_timestamps_are_unambiguous_utc():
    """Real Mongo drivers return naive datetimes for UTC-stored values. If the
    serialized ISO string doesn't carry an explicit UTC offset, downstream
    ``.astimezone()`` calls (e.g. the site's `datetime_delta_format_since`
    filter) silently reinterpret it as the *server's local time* -- a real
    bug, not just cosmetic. Reproduces it against a naive doc field."""
    import dateutil.parser

    naive_created_at = dt.datetime(2026, 8, 9, 14, 32, 10)  # no tzinfo, as real pymongo returns
    assert naive_created_at.tzinfo is None

    public = site_auth._session_public(
        {"_id": "s1", "created_at": naive_created_at, "last_seen_at": naive_created_at}
    )
    parsed = dateutil.parser.parse(public["created_at"])
    assert parsed.tzinfo is not None
    assert parsed.astimezone(dt.timezone.utc) == naive_created_at.replace(tzinfo=dt.timezone.utc)


@pytest.mark.asyncio
async def test_list_user_sessions_excludes_revoked():
    sessions = FakeCollection(
        [
            _session_doc(_id="active", user_token="tok", revoked=False),
            _session_doc(_id="gone", user_token="tok", revoked=True),
        ]
    )
    mongo = FakeMongo(FakeCollection([_user_doc()]), sessions)
    result = await site_auth.list_user_sessions("tok", mongo)
    assert [s["_id"] for s in result] == ["active"]


@pytest.mark.asyncio
async def test_export_account_strips_secrets():
    users = FakeCollection(
        [
            {
                "token": "tok",
                "email_address": "u@example.com",
                "password": "hashed",
                "reset_password_token": "rt",
                "verification_token": "vt",
            }
        ]
    )
    mongo = FakeMongo(users)
    export = await site_auth.export_account(token="tok", mongomotor=mongo)
    body = json.loads(export.body)
    assert "password" not in body
    assert "reset_password_token" not in body
    assert "verification_token" not in body
    assert body["email_address"] == "u@example.com"


@pytest.mark.asyncio
async def test_export_account_prunes_disabled_notification_preferences():
    users = FakeCollection(
        [
            {
                "token": "tok",
                "email_address": "u@example.com",
                "accounts": {
                    "123": {
                        "account_index": 123,
                        "label": "my wallet",
                        "account_notification_preferences": {
                            "payday_account_reward": {
                                "telegram": {"enabled": True, "limit": 1_000_000},
                                "email": {"enabled": False},
                            },
                            "account_transfer": {
                                "telegram": {"enabled": False},
                                "email": {"enabled": False},
                            },
                            "data_registered": None,
                        },
                        "validator_notification_preferences": None,
                    }
                },
                "other_notification_preferences": {
                    "protocol_update": {
                        "telegram": None,
                        "email": {"enabled": True},
                    },
                    "module_deployed": {
                        "telegram": {"enabled": False},
                        "email": None,
                    },
                },
            }
        ]
    )
    mongo = FakeMongo(users)
    export = await site_auth.export_account(token="tok", mongomotor=mongo)
    body = json.loads(export.body)

    # Only the actually-enabled event/channel combination survives.
    other_prefs = body["other_notification_preferences"]
    assert set(other_prefs.keys()) == {"protocol_update"}
    assert set(other_prefs["protocol_update"].keys()) == {"email"}
    assert other_prefs["protocol_update"]["email"]["enabled"] is True

    account_prefs = body["accounts"]["123"]["account_notification_preferences"]
    assert set(account_prefs.keys()) == {"payday_account_reward"}
    assert set(account_prefs["payday_account_reward"].keys()) == {"telegram"}
    assert account_prefs["payday_account_reward"]["telegram"]["enabled"] is True
    assert account_prefs["payday_account_reward"]["telegram"]["limit"] == 1_000_000

    assert body["accounts"]["123"]["validator_notification_preferences"] == {}
    # Non-preference fields are untouched.
    assert body["accounts"]["123"]["label"] == "my wallet"


@pytest.mark.asyncio
async def test_delete_account_cascades_sessions():
    users = FakeCollection([_user_doc()])
    sessions = FakeCollection([_session_doc(user_token="tok")])
    mongo = FakeMongo(users, sessions)
    await site_auth.delete_account(token="tok", mongomotor=mongo)
    assert users.docs == []
    assert sessions.docs == []


@pytest.mark.asyncio
async def test_login_success_writes_audit_entry_without_pii():
    email = "audited@example.com"
    users = FakeCollection(
        [
            {
                "token": "tok",
                "email_address": email,
                "password": hash_password("correct-horse"),
                "email_verified": True,
            }
        ]
    )
    audit = FakeCollection([])
    mongo = FakeMongo(users, audit_log=audit)
    body = site_auth.LoginRequest(email=email, password="correct-horse")
    await site_auth.login(request=make_request(), body=body, mongomotor=mongo)
    assert audit.docs[0]["event"] == "login_success"
    assert audit.docs[0]["user_token"] == "tok"
    assert set(audit.docs[0].keys()) == {"user_token", "event", "at"}
