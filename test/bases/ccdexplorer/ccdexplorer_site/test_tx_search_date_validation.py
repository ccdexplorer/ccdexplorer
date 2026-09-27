"""The transfer search must not turn an unreadable date into a 500.

`start_date` and `end_date` arrive in the POST body of

    /{net}/ajax_tools/transactions-search/transfer

from a datepicker, but nothing stops a client sending anything at all. On
2026-09-25 production was sent "Septembre 2026" -- a French month name, which
`dateutil` cannot read -- and answered with an unhandled ParserError:

    ParserError: Unknown string format: Septembre 2026
      routers/tools.py:563 in ajax_tx_search_transfers
      dateutil/parser/_parser.py:1368 in parse

A date the user typed wrong is a bad request, not a server error, and the site
has an established way of saying so: HTTPException, as /search does for a net
it does not recognise.
"""

import datetime as dt

import pytest
from fastapi import HTTPException

from ccdexplorer.ccdexplorer_site.app.routers.tools import (
    PostDataTransfer,
    ajax_tx_search_transfers,
)


def _post_data(start_date: str = "2026-09-01", end_date: str = "2026-09-30") -> PostDataTransfer:
    return PostDataTransfer(
        theme="light",
        gte="0",
        lte="1000",
        start_date=start_date,
        end_date=end_date,
        page=1,
        size=20,
        # A dict, as the real JSON body sends: `tools.py` defines SortItem twice,
        # so the name the module exports is not the class this model validates
        # against.
        sort=[{"field": "amount", "dir": "desc"}],
        memo="",
    )


@pytest.mark.parametrize(
    "bad_date",
    [
        "Septembre 2026",  # the reported payload
        "Janvier 2026",
        "not a date at all",
        "",
        "2026-13-45",  # shaped like a date, is not one
    ],
)
async def test_an_unreadable_start_date_is_a_bad_request(bad_date):
    with pytest.raises(HTTPException) as exc:
        await ajax_tx_search_transfers(
            request=None,
            net="mainnet",
            post_data=_post_data(start_date=bad_date),
            tags={},
            httpx_client=None,
        )

    assert exc.value.status_code == 422


@pytest.mark.parametrize("bad_date", ["Septembre 2026", "not a date at all", ""])
async def test_an_unreadable_end_date_is_a_bad_request(bad_date):
    with pytest.raises(HTTPException) as exc:
        await ajax_tx_search_transfers(
            request=None,
            net="mainnet",
            post_data=_post_data(end_date=bad_date),
            tags={},
            httpx_client=None,
        )

    assert exc.value.status_code == 422


async def test_the_rejection_names_which_field_was_unreadable():
    """So the client knows which of the two dates to fix."""
    with pytest.raises(HTTPException) as exc:
        await ajax_tx_search_transfers(
            request=None,
            net="mainnet",
            post_data=_post_data(end_date="Septembre 2026"),
            tags={},
            httpx_client=None,
        )

    assert "end_date" in str(exc.value.detail)


@pytest.mark.parametrize(
    ("given", "month_start", "month_end"),
    [
        ("2026-09-14", "2026-09-01", "2026-09-30"),
        ("September 2026", "2026-09-01", "2026-09-30"),
        ("2026-02-03", "2026-02-01", "2026-02-28"),  # a short month
        ("2024-02-03", "2024-02-01", "2024-02-29"),  # a leap February
        ("2026-12-31", "2026-12-01", "2026-12-31"),  # crossing the year
    ],
)
def test_a_readable_date_is_still_widened_to_its_whole_month(given, month_start, month_end):
    """The guard must not change what a date that does parse means.

    The route searches whole months: the start date is pulled back to the first
    of its month and the end date pushed out to the last.
    """
    from ccdexplorer.ccdexplorer_site.app.routers.tools import _parse_month_boundary

    parsed = _parse_month_boundary(given, "start_date")

    assert dt.datetime(parsed.year, parsed.month, 1).strftime("%Y-%m-%d") == month_start

    from dateutil.relativedelta import relativedelta

    next_month = dt.datetime(parsed.year, parsed.month, 1) + relativedelta(months=1)
    assert (next_month - relativedelta(days=1)).strftime("%Y-%m-%d") == month_end
