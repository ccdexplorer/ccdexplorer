"""/v 72723 -- what the site's validator tab says, in a chat."""

from ccdexplorer.ccdexplorer_chart_bot import validator

POOL = {
    "all_pool_total_capital": 10_012_849_592_303_895,
    "equity_capital": 15_597_471_902_276,
    "delegated_capital": 57_959_976_930_657,
    "delegated_capital_cap": 77_987_359_511_380,
    "is_suspended": False,
    "pool_info": {
        "commission_rates": {"baking": 0.1, "finalization": 1.0, "transaction": 0.1},
        "open_status": "open_for_all",
    },
    "current_payday_info": {"missed_rounds": 0, "is_primed_for_suspension": False},
}
NODE = {"nodeName": "ccdexplorer.io 2", "client": "10.0.12", "uptime": "770205394",
        "averagePing": 265.4, "peersCount": "7"}
TALLY = {"data": [{"date": "2026-10-11", "missed": 0}, {"date": "2026-10-10", "missed": 5}]}
SITE = "https://ccdexplorer.io"


def _text(**kw):
    args = {"pool": POOL, "node": NODE, "payday": "68 / 360 blocks",
            "win": "2026-10-11T13:11:43.139000Z", "tally": TALLY}
    args.update(kw)
    return validator.summary(72723, args["pool"], args["node"], args["payday"],
                             args["win"], args["tally"], SITE)


def test_an_index_is_digits_only():
    assert validator.index_or_none(["72723"]) == 72723
    assert validator.index_or_none(["3BFChzvx3783jGUK"]) is None
    assert validator.index_or_none([]) is None
    assert validator.index_or_none(["-5"]) is None


def test_it_reads_the_routes_the_site_reads():
    """account_tab_validator.py calls pool-info, node, current-payday-stats
    and earliest-win-time; the tally is its own route on the same page."""
    paths = validator.api_paths(72723)

    assert set(paths) == {"pool", "node", "payday", "win", "tally"}
    assert paths["pool"].endswith("/pool-info")
    assert paths["payday"].endswith("/current-payday-stats")
    assert paths["win"].endswith("/earliest-win-time")
    assert paths["tally"].endswith("/validator-tally/0/180")


def test_an_account_that_bakes_nothing_is_not_a_validator():
    assert not validator.is_validator({})
    assert not validator.is_validator(None)
    assert validator.is_validator(POOL)


def test_the_name_and_index_lead():
    assert _text().startswith("<b>ccdexplorer.io 2</b> — validator 72723")


def test_delegation_space_is_a_number():
    assert "20,027,383 CCD" in _text()


def test_a_closed_pool_says_so_instead_of_a_number():
    pool = {**POOL, "pool_info": {**POOL["pool_info"], "open_status": "closed_for_all"}}

    assert "closed to delegation" in _text(pool=pool)


def test_a_full_pool_says_so():
    pool = {**POOL, "delegated_capital_cap": POOL["delegated_capital"]}

    assert "full" in _text(pool=pool)


def test_todays_blocks_are_whatever_the_api_said():
    """current-payday-stats returns "68 / 360 blocks" because that is what
    the site shows; recomputing it here would be a second answer."""
    assert "68 / 360 blocks" in _text()


def test_missed_is_given_for_today_and_for_the_window():
    text = _text()

    assert "<b>Missed today</b> 0" in text
    assert "<b>Missed last 2 days</b> 5" in text


def test_the_three_commissions_are_quoted():
    text = _text()

    assert "10.0% baking" in text and "100.0% finalization" in text


def test_the_next_win_is_readable():
    assert "11 Oct 13:11 UTC" in _text()


def test_an_unparseable_win_time_is_passed_through_rather_than_crashing():
    assert "soon" in _text(win="soon")


def test_suspension_is_shouted():
    assert "SUSPENDED" in _text(pool={**POOL, "is_suspended": True})


def test_primed_for_suspension_is_warned_about():
    pool = {**POOL, "current_payday_info": {"is_primed_for_suspension": True}}

    assert "primed for suspension" in _text(pool=pool)


def test_a_node_not_reporting_says_so():
    assert "not reporting" in _text(node={})


def test_a_pool_name_cannot_break_the_message():
    """Operator-supplied, and this is HTML."""
    text = _text(node={**NODE, "nodeName": "<b>pwn</b>"})

    assert "&lt;b&gt;pwn&lt;/b&gt;" in text


def test_the_account_link_is_there():
    assert "https://ccdexplorer.io/mainnet/account/72723" in _text()


def test_it_fits_a_telegram_message():
    assert len(_text()) < 4096
