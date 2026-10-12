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
NODE = {
    "nodeName": "ccdexplorer.io 2",
    "client": "10.0.12",
    "uptime": "770205394",
    "averagePing": 265.4,
    "peersCount": "7",
}
TALLY = {"data": [{"date": "2026-10-11", "missed": 0}, {"date": "2026-10-10", "missed": 5}]}
SITE = "https://ccdexplorer.io"


def _text(**kw):
    args = {
        "pool": POOL,
        "node": NODE,
        "payday": "68 / 360 blocks",
        "win": "2026-10-11T13:11:43.139000Z",
        "tally": TALLY,
    }
    args.update(kw)
    return validator.summary(
        72723, args["pool"], args["node"], args["payday"], args["win"], args["tally"], SITE
    )


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


def test_the_index_leads_not_the_node_name():
    """The node name is the operator's machine, not the validator;
    leading with it labelled the whole message with one of its own
    footnotes."""
    text = _text()

    assert text.startswith("<b>Validator 72723</b>")
    assert "ccdexplorer.io 2" in text.split("<b>Node</b>")[1], "it belongs under Node"


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

    assert "Missed today 0" in text
    assert "Missed last 2 days 5" in text


def test_the_three_commissions_are_quoted():
    text = _text()

    assert "10.0% baking" in text and "100.0% finalization" in text


def test_the_next_win_keeps_the_absolute_time():
    assert "13:11 UTC" in _text()


def test_an_unparseable_win_time_is_passed_through_rather_than_crashing():
    assert "soon" in _text(win="soon")


def test_suspension_is_shouted():
    assert "SUSPENDED" in _text(pool={**POOL, "is_suspended": True})


def test_primed_for_suspension_is_warned_about():
    pool = {**POOL, "current_payday_info": {"is_primed_for_suspension": True}}

    assert "primed for suspension" in _text(pool=pool)


def test_a_node_not_reporting_says_so():
    assert "not reporting" in _text(node={})


def test_a_node_name_cannot_break_the_message():
    """Operator-supplied, and this is HTML: a tag would make Telegram
    reject the message and nothing would arrive."""
    text = _text(node={**NODE, "nodeName": "<b>pwn</b>"})

    assert "&lt;b&gt;pwn&lt;/b&gt;" in text


def test_the_account_link_is_there():
    assert "https://ccdexplorer.io/mainnet/account/72723" in _text()


def test_it_fits_a_telegram_message():
    assert len(_text()) < 4096


# --- sections -------------------------------------------------------------


def test_the_three_sections_are_headed():
    """Eleven lines of facts with no headings is a wall. What is fixed
    about a validator, what is true right now, and what its node is doing
    are three different questions."""
    text = _text()

    assert "<b>Validator</b>" in text
    assert "<b>Live</b>" in text
    assert "<b>Node</b>" in text


def test_the_statics_are_under_validator():
    text = _text()
    section = text.split("<b>Validator</b>")[1].split("<b>Live</b>")[0]

    assert "Stake" in section and "Commission" in section and "Delegation space" in section


def test_what_is_true_now_is_under_live():
    text = _text()
    section = text.split("<b>Live</b>")[1].split("<b>Node</b>")[0]

    assert "Today" in section and "Missed today" in section and "Next win" in section
    assert "Missed last" not in section, "the six-month total is a standing fact, not a live one"


# --- the next win ---------------------------------------------------------


def test_the_next_win_is_relative_because_telegram_gives_no_timezone():
    """The Bot API's User carries language_code and no timezone, so there
    is nothing to localise to. "in 23 min" is right in every zone."""
    import datetime as dt

    # Half a minute of slack: the clock moves between building this and
    # formatting it, and 23 minutes exactly rounds down to 22.
    soon = dt.datetime.now(dt.UTC) + dt.timedelta(minutes=23, seconds=30)

    assert "in 23 min" in _text(win=soon.isoformat().replace("+00:00", "Z"))


def test_a_win_due_within_the_minute_says_so():
    import datetime as dt

    # Not exactly now: by the time it is formatted that is already past.
    soon = dt.datetime.now(dt.UTC) + dt.timedelta(seconds=20)

    assert "any moment" in _text(win=soon.isoformat().replace("+00:00", "Z"))


def test_a_win_already_past_is_not_reported_as_the_future():
    import datetime as dt

    gone = dt.datetime.now(dt.UTC) - dt.timedelta(minutes=5)

    assert "in " not in _text(win=gone.isoformat().replace("+00:00", "Z")).split("Next win")[1][:20]


def test_the_absolute_time_is_still_given_for_anyone_who_wants_it():
    import datetime as dt

    soon = dt.datetime.now(dt.UTC) + dt.timedelta(minutes=23)

    assert "UTC)" in _text(win=soon.isoformat().replace("+00:00", "Z"))


# --- the three charts -----------------------------------------------------


def test_the_three_views_are_the_three_boxes_on_the_validator_page():
    assert validator.VIEWS == ("apy", "performance", "rewards")


def test_apy_is_the_default():
    assert validator.VIEWS[0] == "apy"


def test_each_view_points_at_the_page_s_own_graph_route():
    """The same figures the validator page draws, asked for with
    /image.png on the end because Telegram will not post to fetch an
    image -- and because the site renders a png only for that name."""
    urls = {
        v: validator.image_url(SITE, 72723, v, address="3BFChz", now=0) for v in validator.VIEWS
    }

    assert "/account/apy-graph/72723/validator/image.png" in urls["apy"]
    assert "/account/baker-performance/72723/3000/image.png" in urls["performance"]
    assert "/account/rewards-bucketed/3BFChz/72723/image.png" in urls["rewards"]


def test_rewards_is_keyed_by_address_because_the_index_answers_nothing():
    """staking-rewards-bucketed takes an address; asked by index it comes
    back as an empty list rather than an error, so the chart would be
    blank for every validator."""
    assert validator.address_of(POOL_WITH_ADDRESS) == "3BFChzvx3783jGUK"
    assert validator.address_of({}) == ""
    assert validator.address_of(None) == ""


POOL_WITH_ADDRESS = {**POOL, "address": "3BFChzvx3783jGUK"}


def test_there_is_one_button_per_chart_and_the_current_one_is_marked():
    labels = [b.text for row in validator.keyboard(72723, "rewards").inline_keyboard for b in row]

    assert labels == ["APY", "Performance", "· Rewards ·"]


def test_a_press_resolves_to_an_index_and_a_view():
    assert validator.parse_callback("v:72723:performance") == (72723, "performance")


def test_a_press_for_an_unknown_view_is_refused():
    assert validator.parse_callback("v:72723:nonsense") is None


def test_a_chart_catalogue_press_is_not_ours():
    assert validator.parse_callback("c:staking_validator_count") is None


def test_the_six_month_total_closes_the_validator_section():
    """It describes what this validator is like, not what is happening
    now, so it belongs with the stake and the commission."""
    section = _text().split("<b>Validator</b>")[1].split("<b>Live</b>")[0]

    assert "Missed last" in section
    assert section.strip().endswith("Missed last 2 days 5")
