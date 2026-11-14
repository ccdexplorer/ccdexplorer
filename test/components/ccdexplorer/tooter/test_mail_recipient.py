"""The address a mail goes to is data, and the url it travels in is not.

Mail leaves through apprise, which takes one url carrying the server, the
login and the recipient together. The recipient was pasted into that url as
it arrived, so whatever followed the address was read as more of the url.

These tests ask apprise itself what a url means rather than looking for
characters in a string, because the question is what the parser does with it.
Nothing is sent: `requests.post` is replaced, and the server and login are
placeholders.
"""

import string

import apprise
import pytest
from apprise.utils.parse import parse_urls

SERVER = "smtp.example.org"
LOGIN = "bot@example.org"


@pytest.fixture
def sent(monkeypatch):
    """A Tooter whose posts are recorded instead of made."""
    from ccdexplorer.tooter import core

    monkeypatch.setattr(core, "MAILTO_LINK", f"mailto://{SERVER}/?to=")
    monkeypatch.setattr(core, "MAILTO_USER", LOGIN)
    monkeypatch.setattr(core, "FASTMAIL_TOKEN", "placeholder")
    posts = []
    monkeypatch.setattr(core.requests, "post", lambda url, json=None, **kw: posts.append(json))
    return core.Tooter(), posts


def _read(urls: str) -> list:
    """What apprise makes of the `urls` field, the way its api reads it."""
    return [apprise.Apprise.instantiate(u, suppress_exceptions=True) for u in parse_urls(urls)]


def _goes_only_to(urls: str, address: str) -> bool:
    plugins = _read(urls)
    if len(plugins) != 1 or type(plugins[0]).__name__ != "NotifyEmail":
        return False
    mail = plugins[0]
    return (
        mail.smtp_host == SERVER
        and mail.user == LOGIN
        and mail.targets == [(False, address)]
        and not mail.cc
        and not mail.bcc
    )


ORDINARY = [
    "user@example.com",
    "first.last@example.com",
    "user+ccd@example.com",
    "user_name@mail.example.co.uk",
    "u-1@sub-domain.example.org",
    "UPPER@Example.COM",
]

HOSTILE = [
    "a@example.com&smtp=evil.example",
    "a&smtp=evil.example@example.com",
    "a@example.com,json://evil.example/x",
    "a@example.com&bcc=other@evil.example",
    "a@example.com&cc=other@evil.example",
    "a@example.com,other@evil.example",
    "a@example.com other@evil.example",
    "a@example.com;other@evil.example",
    "a@example.com\nother@evil.example",
    "a@example.com%26smtp%3Devil.example",
    "a@example.com%2Cother@evil.example",
    "a@example.com?smtp=evil.example",
    "a@example.com#x",
    "a@example.com/x",
    "json://evil.example/x",
    "Name <a@example.com>",
    # apprise reads this one as a mail to brien@example.com from someone
    # called "o": a different mailbox, belonging to somebody else.
    "o'brien@example.com",
]

NOT_AN_ADDRESS = ["", " ", "nobody", "@example.com", "a@", "a@b", "a@@example.com", None, 7, {}, []]


@pytest.mark.parametrize("address", ORDINARY)
def test_an_ordinary_address_gets_its_mail(sent, address):
    tooter, posts = sent

    assert tooter.email_api("t", "b", address) is True

    assert len(posts) == 1
    assert _goes_only_to(posts[0]["urls"], address)


@pytest.mark.parametrize("address", HOSTILE + NOT_AN_ADDRESS)
def test_anything_else_is_not_sent_at_all(sent, address):
    tooter, posts = sent

    assert tooter.email_api("t", "b", address) is False
    assert tooter.email("t", "b", address) is False

    assert posts == []


def test_the_notification_mail_takes_the_same_route(sent):
    """email() and email_api() built the url separately, so fixing one would
    have left the other -- and the bot's mails go through email()."""
    tooter, posts = sent

    assert tooter.email("t", "b", "user+ccd@example.com") is True

    assert _goes_only_to(posts[0]["urls"], "user+ccd@example.com")


@pytest.mark.parametrize("position", ["local", "domain", "end"])
def test_no_single_character_changes_where_the_mail_goes(sent, position):
    """Every printable character, in every part of the address: the mail is
    either refused or goes to exactly the address given, to that server, with
    that login. There is no third outcome."""
    tooter, posts = sent
    for char in string.printable:
        address = {
            "local": f"a{char}b@example.com",
            "domain": f"ab@exam{char}ple.com",
            "end": f"ab@example.com{char}",
        }[position]
        posts.clear()

        accepted = tooter.email_api("t", "b", address)

        if accepted:
            assert _goes_only_to(posts[0]["urls"], address), repr(address)
        else:
            assert posts == [], repr(address)


def test_the_check_is_available_without_sending():
    """The api refuses a bad address at the door, before it stores it."""
    from ccdexplorer.tooter import is_acceptable_email_address

    assert all(is_acceptable_email_address(a) for a in ORDINARY)
    assert not any(is_acceptable_email_address(a) for a in HOSTILE + NOT_AN_ADDRESS)


def test_an_overlong_address_is_refused():
    from ccdexplorer.tooter import is_acceptable_email_address

    assert not is_acceptable_email_address("a" * 250 + "@example.com")


# --- accepted has to mean delivered ---------------------------------------
#
# The other direction. apprise has its own idea of an address, and where it is
# narrower than ours the mail is dropped without a word: it takes one `+tag`
# and no more, so `a+b+c@example.com` passed the check here and reached nobody.
# An address the form accepts and the mailer discards is a verification mail
# that never arrives, with nothing on the page to say why.
#
# Every arrangement of the characters the check allows, short enough to try
# them all.


def _delivered_to(tooter, posts, address) -> bool:
    posts.clear()
    return tooter.email_api("t", "b", address) and _goes_only_to(posts[0]["urls"], address)


def test_a_second_plus_tag_is_refused(sent):
    tooter, posts = sent

    assert tooter.email_api("t", "b", "a+b+c@example.com") is False
    assert tooter.email_api("t", "b", "a+@example.com") is False
    assert posts == []


def test_every_accepted_local_part_is_one_apprise_delivers(sent):
    import itertools

    from ccdexplorer.tooter import is_acceptable_email_address

    tooter, posts = sent
    accepted = 0
    for length in range(1, 5):
        for chars in itertools.product("a1._+-", repeat=length):
            address = f"{''.join(chars)}@example.com"
            if is_acceptable_email_address(address):
                accepted += 1
                assert _delivered_to(tooter, posts, address), address
    # Guards the loop itself: a check that refused everything would pass.
    assert accepted > 100


def test_every_accepted_domain_is_one_apprise_delivers(sent):
    import itertools

    from ccdexplorer.tooter import is_acceptable_email_address

    tooter, posts = sent
    accepted = 0
    for length in range(1, 6):
        for chars in itertools.product("a1-.", repeat=length):
            for address in (f"user@{''.join(chars)}", f"user@{''.join(chars)}.com"):
                if is_acceptable_email_address(address):
                    accepted += 1
                    assert _delivered_to(tooter, posts, address), address
    assert accepted > 100
