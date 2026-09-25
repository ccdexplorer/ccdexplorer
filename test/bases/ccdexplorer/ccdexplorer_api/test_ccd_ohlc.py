"""CCD candles from Kraken.

This is the only third party anything in the API calls. The chain price series
owes nothing to anyone; this is one exchange's order book -- what people
actually paid, with volume -- and it is only as available as Kraken is.
"""

import pytest

from ccdexplorer.ccdexplorer_api.app.routers.v2.misc_v2 import (
    KRAKEN_CCD_PAIR,
    KRAKEN_INTERVALS,
    KRAKEN_MAX_BARS,
    KRAKEN_STALE_SECONDS,
)


def test_the_intervals_are_the_ones_kraken_serves():
    """Kraken takes minutes, and only these. Anything else comes back empty
    rather than as an error, which would look like no trades."""
    assert KRAKEN_INTERVALS == {"15m": 15, "30m": 30, "1h": 60, "4h": 240, "1d": 1440}


def test_the_pair_is_usd_not_usdt():
    """Kraken quotes CCD against dollars directly, so there is no stablecoin
    leg to explain away."""
    assert KRAKEN_CCD_PAIR == "CCDUSD"


def test_a_stale_response_outlives_the_refresh():
    """Kraken returns 720 candles and we keep the last good answer for an
    hour. A chart an hour old beats no chart, and this is the only part of the
    site that depends on somebody else's uptime."""
    assert KRAKEN_STALE_SECONDS >= 3600
    assert KRAKEN_MAX_BARS <= 720
