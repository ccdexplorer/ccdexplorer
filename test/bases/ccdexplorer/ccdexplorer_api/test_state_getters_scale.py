"""Per-request lookups that did more work, or the wrong work, than asked.

* get_user_details loaded every api user, synchronously, on every page view
  and then picked one out of the dict.
* the blocks-per-day and memos caches were one value for the whole app, so a
  testnet request within a minute of a mainnet one was answered with mainnet's
  data.
* get_memos on a route without {net} reached `db_to_use` without assigning it.
"""

import datetime as dt
from types import SimpleNamespace

from ccdexplorer.ccdexplorer_api.app import state_getters
from ccdexplorer.ccdexplorer_api.app.models import User
from ccdexplorer.mongodb import Collections

NOW = dt.datetime.now().astimezone(dt.UTC)


def _user_doc(token="tok"):
    return {
        "scope": "http://x",
        "token": token,
        "alias_id": 0,
        "alias_account_id": "3alias",
        "api_account_id": "acct",
        "email": "a@example.org",
        "password": "hash",
        "plan_end_date": NOW,
    }


class _Users:
    def __init__(self, docs):
        self.docs = docs
        self.queries = []

    def find(self, *a, **k):
        raise AssertionError("the whole collection must not be read for one user")

    def find_one(self, query):
        self.queries.append(query)
        return next((d for d in self.docs if all(d.get(k) == v for k, v in query.items())), None)


def _request(users, cookie="tok"):
    app = SimpleNamespace(mongodb=SimpleNamespace(utilities_db={"api_users": users}))
    return SimpleNamespace(app=app, cookies={"api.ccdexplorer.io": cookie} if cookie else {})


# --- users ----------------------------------------------------------------


def test_one_user_is_looked_up_not_all_of_them():
    users = _Users([_user_doc("other"), _user_doc("tok")])
    got = state_getters.get_user_details(_request(users))
    assert isinstance(got, User) and got.token == "tok"
    assert users.queries == [{"token": "tok"}]


def test_an_unknown_token_is_no_user():
    assert state_getters.get_user_details(_request(_Users([_user_doc("x")]))) is None


def test_no_cookie_is_no_user_and_asks_nothing():
    users = _Users([_user_doc()])
    assert state_getters.get_user_details(_request(users, cookie=None)) is None
    assert users.queries == []


def test_an_explicit_token_is_looked_up_instead_of_the_cookie():
    users = _Users([_user_doc("explicit")])
    got = state_getters.get_user_details(_request(users, cookie="other"), token="explicit")
    assert got.token == "explicit"


def test_an_app_without_a_database_is_no_user_not_a_crash():
    req = SimpleNamespace(app=SimpleNamespace(), cookies={"api.ccdexplorer.io": "tok"})
    assert state_getters.get_user_details(req) is None


# --- the per-net caches ---------------------------------------------------


class _Coll:
    def __init__(self, rows):
        self.rows = rows
        self.reads = 0

    def find(self, query):
        self.reads += 1
        return list(self.rows)


def _app_with_two_nets():
    mainnet = {
        Collections.blocks_per_day: _Coll(
            [
                {
                    "_id": "2026-01-01",
                    "height_for_first_block": 1,
                    "height_for_last_block": 2,
                    "slot_time_for_first_block": NOW,
                    "slot_time_for_last_block": NOW,
                    "hash_for_first_block": "h1",
                    "hash_for_last_block": "h2",
                    "date": "2026-01-01",
                }
            ]
        ),
        Collections.memos_to_hashes: _Coll([{"_id": "m-main", "tx_hashes": ["a"]}]),
    }
    testnet = {
        Collections.blocks_per_day: _Coll(
            [
                {
                    "_id": "2026-02-02",
                    "height_for_first_block": 3,
                    "height_for_last_block": 4,
                    "slot_time_for_first_block": NOW,
                    "slot_time_for_last_block": NOW,
                    "hash_for_first_block": "h3",
                    "hash_for_last_block": "h4",
                    "date": "2026-02-02",
                }
            ]
        ),
        Collections.memos_to_hashes: _Coll([{"_id": "m-test", "tx_hashes": ["b"]}]),
    }
    return (
        SimpleNamespace(
            mongodb=SimpleNamespace(mainnet=mainnet, testnet=testnet),
            blocks_per_day=None,
            blocks_per_day_last_requested=NOW - dt.timedelta(days=1),
            memos=None,
            memos_last_requested=NOW - dt.timedelta(days=1),
        ),
        mainnet,
        testnet,
    )


def _patch_net_db(monkeypatch):
    monkeypatch.setattr(state_getters, "net_db", lambda client, net: getattr(client, net))


def test_a_testnet_request_is_not_answered_with_mainnets_memos(monkeypatch):
    _patch_net_db(monkeypatch)
    app, *_ = _app_with_two_nets()
    main = state_getters.get_memos(SimpleNamespace(app=app, path_params={"net": "mainnet"}))
    test = state_getters.get_memos(SimpleNamespace(app=app, path_params={"net": "testnet"}))
    assert list(main) == ["m-main"]
    assert list(test) == ["m-test"]


def test_the_same_net_is_still_cached(monkeypatch):
    _patch_net_db(monkeypatch)
    app, mainnet, _ = _app_with_two_nets()
    req = SimpleNamespace(app=app, path_params={"net": "mainnet"})
    state_getters.get_memos(req)
    state_getters.get_memos(req)
    assert mainnet[Collections.memos_to_hashes].reads == 1


def test_memos_on_a_route_without_a_net_are_mainnets(monkeypatch):
    _patch_net_db(monkeypatch)
    app, *_ = _app_with_two_nets()
    got = state_getters.get_memos(SimpleNamespace(app=app, path_params={}))
    assert list(got) == ["m-main"]


def test_a_testnet_request_is_not_answered_with_mainnets_blocks_per_day(monkeypatch):
    _patch_net_db(monkeypatch)
    app, *_ = _app_with_two_nets()
    main = state_getters.get_blocks_per_day(
        SimpleNamespace(app=app, path_params={"net": "mainnet"})
    )
    test = state_getters.get_blocks_per_day(
        SimpleNamespace(app=app, path_params={"net": "testnet"})
    )
    assert list(main) == ["2026-01-01"]
    assert list(test) == ["2026-02-02"]
