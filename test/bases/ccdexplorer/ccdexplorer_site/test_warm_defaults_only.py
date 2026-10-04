"""What the warmer redraws every fifty minutes.

Warming every combination would be 25 charts x 4 windows x 3 groupings x 2
themes = 600 renders at roughly two seconds each: twenty minutes of kaleido per
fifty-minute cycle, for combinations nobody asked for. Only the state a chart
opens in is warmed; the rest render on tap and then cache for the hour.
"""

from types import SimpleNamespace

from ccdexplorer.charts.registry import ALL_SPECS
from ccdexplorer.ccdexplorer_site.app.utils import plot_warm_targets


def _app(paths):
    return SimpleNamespace(routes=[SimpleNamespace(path=p) for p in paths])


def test_a_spec_is_warmed_only_in_its_default_state():
    """Two urls, one state. The path form is what the bot's buttons ask
    for; the stateless /image.png is the og:image of the shareable plot
    page, so it is what Telegram fetches. They are separate cache entries
    of the same picture, so both are warmed -- and the state travels in
    the path, because asked as a query the stateless route redirects and
    the warmer, which does not follow, warms nothing.
    """
    app = _app(["/plots/{net}/transactions_count/image.png"])
    targets = plot_warm_targets(app)
    assert len(targets) == 2

    paths = [path for path, _ in targets]
    assert "/plots/mainnet/transactions_count/image.png" in paths
    assert any(p.startswith("/plots/mainnet/transactions_count/weekly/") for p in paths)
    assert all(params == {} for _, params in targets)


def test_a_chart_with_no_spec_is_still_warmed_plainly():
    """The twenty-five not yet migrated have no window or grouping."""
    app = _app(["/plots/{net}/ccd_kraken_1h/image.png"])
    assert plot_warm_targets(app) == [("/plots/mainnet/ccd_kraken_1h/image.png", {})]


def test_the_budget_does_not_grow_with_the_number_of_combinations():
    """Two renders a chart, not one per window times grouping."""
    paths = [f"/plots/{{net}}/{spec.name}/image.png" for spec in ALL_SPECS]
    configurable = [spec for spec in ALL_SPECS if spec.groupings]
    assert len(plot_warm_targets(_app(paths))) == len(ALL_SPECS) + len(configurable)
