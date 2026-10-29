"""One line break per intended break in Telegram notifications.

Apprise collapsed the whitespace around a `<br>` until 2.1.0 and stopped, so
bodies that carry both `<br/>` and real newlines -- which most of the bot's do,
being readable multi-line f-strings full of inline tags -- grew blank lines
throughout (reported 2026-10-29). Normalising at the transport rather than
rewriting ~270 break sites across six message files.
"""


def _f():
    from ccdexplorer.tooter.core import for_telegram

    return for_telegram


# --- the reported case ----------------------------------------------------


def test_a_tag_followed_by_a_newline_is_one_break():
    """The payday message: `...(Tx fees)<br/>` + `"\\nDistribution..."`."""
    out = _f()("<code>1.5 CCD</code> (Tx fees)<br/>\nDistribution to validators:")

    assert out == "<code>1.5 CCD</code> (Tx fees)\nDistribution to validators:"
    assert "\n\n" not in out


def test_a_deliberate_blank_line_survives():
    out = _f()("Reward payout:<br/><br/>1.5 CCD")

    assert out == "Reward payout:\n\n1.5 CCD"


def test_no_run_is_ever_more_than_one_blank_line():
    """Two deliberate blank lines in different places are two blank lines --
    four newlines. What must never appear is a run of three."""
    out = _f()("block)\n\nReward payout: <br/><br/>next")

    assert out == "block)\n\nReward payout:\n\nnext"
    assert "\n\n\n" not in out


def test_a_sloppy_run_is_capped():
    out = _f()("a<br/><br/><br/><br/>b")

    assert out == "a\n\nb"


# --- the pieces it must not damage ---------------------------------------


def test_telegram_markup_is_left_alone():
    body = "<code>1.5 CCD</code> for <a href='https://x/y'>label</a>"

    assert _f()(body) == body


def test_tags_are_matched_in_any_spelling():
    for tag in ("<br>", "<br/>", "<br />", "<BR/>", "<br  />"):
        assert _f()(f"a{tag}b") == "a\nb", tag


def test_trailing_whitespace_before_a_break_goes():
    assert _f()("a   \nb") == "a\nb"


def test_surrounding_whitespace_is_trimmed():
    assert _f()("\n\nbody<br/>") == "body"


def test_an_empty_body_stays_empty():
    assert _f()("") == ""
    assert _f()("<br/><br/>") == ""


# --- the real message shape ----------------------------------------------


def test_the_payday_message_has_no_blank_line_runs():
    body = (
        "Payday: <code>2026-10-29</code> (<code>3</code> blocks)\n\n"
        "Reward payout of <code>1.234567 CCD</code> for validator "
        "<a href='https://ccdexplorer.io/mainnet/account/1'>v</a>: <br/><br/>"
        "<code>     1.000000 CCD</code> (Blocks)<br/>"
        "<code>     0.234567 CCD</code> (Tx fees)<br/>"
        "\nDistribution to validator and delegators:\n\n"
        "<code>     1.100000 CCD</code> (Validator)<br/>"
        "<code>     0.134567 CCD</code> (Delegators)<br/>"
    )

    out = _f()(body)

    assert "\n\n\n" not in out
    # the four reward lines each stand alone, rather than asserting a total
    # newline count, which is brittle and which I got wrong twice
    lines = out.split("\n")
    assert "<code>     1.000000 CCD</code> (Blocks)" in lines
    assert "<code>     0.234567 CCD</code> (Tx fees)" in lines
    assert "<code>     1.100000 CCD</code> (Validator)" in lines
    assert "<code>     0.134567 CCD</code> (Delegators)" in lines
    assert out.endswith("(Delegators)")
