"""The bot must not ship Telegram PII to Sentry.

It was the only one of the three services with send_default_pii=True, which
for a process whose whole input is people's chat messages means chat
identifiers and message content leaving for a third party. The site and the
API both attach an opaque id instead.
"""

import pathlib

BOT_MAIN = (
    pathlib.Path(__file__).resolve().parents[4]
    / "bases"
    / "ccdexplorer"
    / "ccdexplorer_bot"
    / "__main__.py"
)


def test_the_bot_entrypoint_exists_where_this_test_expects_it():
    assert BOT_MAIN.is_file(), BOT_MAIN


def test_send_default_pii_is_not_enabled():
    source = BOT_MAIN.read_text()

    assert "send_default_pii=True" not in source
