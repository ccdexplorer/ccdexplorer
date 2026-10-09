"""What a chart is, as data.

Deliberately free of plotly, telegram, fastapi and pymongo: the site, the chart
bot and the API all import this, and a component that dragged any of those in
would make every one of them heavier for nothing.

The important field is `Series.agg`. Some series are flows -- a count
accumulated during that day -- and summing them over a week is right. Others
are levels, a snapshot of the world at day's end, and summing those produces a
number seven times too large on a chart that looks entirely normal. It has no
default for that reason.
"""

import datetime as dt
import re
from enum import Enum

from pydantic import BaseModel, ConfigDict, model_validator


class Agg(str, Enum):
    """How one series collapses from days into a bucket."""

    SUM = "sum"  # flows: fees, transaction counts
    LAST = "last"  # levels: validator count, balances
    MEAN = "mean"  # ratios and averages
    DELTA_OF_LAST = "delta_of_last"  # growth derived from a cumulative level


class Grouping(str, Enum):
    DAILY = "daily"
    WEEKLY = "weekly"
    MONTHLY = "monthly"


class Axis(str, Enum):
    """What the reader configures on this chart.

    Every chart used to be the first of these: a grouping and a date range
    over a date-keyed collection. The two that are not had no page at all,
    because the page only knew how to offer a grouping and a range.
    """

    #: A grouping and a date range over daily documents.
    CALENDAR = "calendar"
    #: A candle interval, which is also the chart's reach -- Kraken serves a
    #: fixed number of bars, so 1m is two hours and 1d is four months. There
    #: is no date range to offer.
    INTERVAL = "interval"
    #: A fixed forward span, where what the reader configures is the lookback
    #: some reference figure is measured over rather than the x-axis.
    LOOKBACK = "lookback"


class Interval(str, Enum):
    """A candle interval.

    Not a Grouping: these are not calendar buckets, and the calendar
    groupings stop at a day in both directions.
    """

    M1 = "1m"
    M5 = "5m"
    M15 = "15m"
    M30 = "30m"
    H1 = "1h"
    H4 = "4h"
    D1 = "1d"


class Window(str, Enum):
    D30 = "30d"
    D90 = "90d"
    Y1 = "1y"
    ALL = "all"

    def days(self) -> int | None:
        """How far back this window reaches, or None for all of history."""
        return {"30d": 30, "90d": 90, "1y": 365, "all": None}[self.value]


class Kind(str, Enum):
    BAR = "bar"
    STACKED_BAR = "stacked_bar"
    LINE = "line"
    #: Stacked bands making up a total, where the band heights are the point.
    AREA = "area"
    CANDLE = "candle"


class Series(BaseModel):
    """One line or bar stack, and how it survives being grouped."""

    model_config = ConfigDict(frozen=True)

    key: str
    label: str
    colour: str
    agg: Agg
    #: What the stored number is divided by to reach the unit the chart
    #: shows. fee_for_day is microCCD; the handwritten chart divided by a
    #: million before drawing and the generic one has to as well.
    scale: float = 1.0
    #: Drawn differently from the rest of the chart. TPS is a line against
    #: bars because 0.6 transactions a second beside fourteen million CCD is
    #: an invisible bar, and on its own axis for the same reason.
    kind: "Kind | None" = None
    secondary_y: bool = False
    #: What this trace is called in a url. The mongo field will not do: two
    #: of them have spaces in, and amount_to_make_top_100 is not something to
    #: put in an address. Derived from the key unless given, and never
    #: containing the hyphen that separates traces from each other.
    url_name: str = ""
    #: Fields that roll up into this series, e.g. the five source columns
    #: behind "Transfer". Empty means `key` is itself the mongo field.
    source_fields: tuple[str, ...] = ()
    #: Mongo stores statistics_microccd's four fields as strings; everything
    #: else is a BSON number.
    cast_to_double: bool = False
    #: True when source_fields names a top-level field whose NAME contains a
    #: dot, rather than a path into a nested document. "$gate.io" means the
    #: `io` subfield of `gate`, so the real field reads as missing and
    #: $ifNull quietly substitutes zero -- an exchange with a wallet drawn as
    #: having none. The two cases look identical in the string, so the series
    #: has to say which it means.
    literal_field: bool = False
    #: What an empty bucket means for this series, when that does not follow
    #: from `agg`. A flow fills with zero -- nothing happened -- and a level
    #: carries forward, because the validator count did not drop to zero, the
    #: job simply did not write. Usually `agg` says which: SUM is a flow,
    #: LAST is a level. Active addresses is the exception that forced this
    #: field: it collapses with LAST for a mechanical reason (its source is
    #: pre-grouped, one document per bucket) while being a per-period count.
    #: Carrying that forward would draw a missed week at the previous week's
    #: value, indistinguishable from real activity.
    fills_with_zero: bool | None = None

    @property
    def short_period_understates(self) -> bool:
        """Whether fewer days in a period makes this number smaller.

        A total does: six days of fees in a seven-day bar really is less
        than a week's worth, and the bar should say so. A snapshot does not
        -- the week's closing validator count is Sunday's number whether or
        not Monday was recorded -- and nor does an average.
        """
        return self.empty_bucket_is_zero or self.agg is Agg.DELTA_OF_LAST

    @property
    def empty_bucket_is_zero(self) -> bool:
        if self.fills_with_zero is not None:
            return self.fills_with_zero
        return self.agg is Agg.SUM

    @model_validator(mode="after")
    def _default_url_name(self):
        if not self.url_name:
            object.__setattr__(self, "url_name", _URL_SAFE.sub("", self.key.lower()))
        return self


_URL_SAFE = re.compile(r"[^a-z0-9]")


class ChartSpec(BaseModel):
    """One chart, everywhere it appears."""

    model_config = ConfigDict(frozen=True)

    name: str  # "transaction_fees" -- the /plots route name
    slug: str  # "transaction-fees" -- the /charts page path
    title: str
    description: str  # the paragraph the site shows
    blurb: str  # the one line an inline bot result shows
    category: str  # chain | staking | accounts | exchanges | plt | agents
    source: str  # the mongo `type`
    series: tuple[Series, ...]
    chain_start: dt.date

    keywords: tuple[str, ...] = ()
    claims: tuple[str, ...] = ()
    #: Route names this chart supersedes. Three dashboard tiles name a route
    #: that became a spec under a different name, and without saying so they
    #: cannot find the page that replaced them.
    aliases: tuple[str, ...] = ()
    groupings: tuple[Grouping, ...] = (
        Grouping.DAILY,
        Grouping.WEEKLY,
        Grouping.MONTHLY,
    )
    default_grouping: Grouping = Grouping.WEEKLY
    windows: tuple[Window, ...] = (Window.D30, Window.D90, Window.Y1, Window.ALL)
    default_window: Window = Window.Y1
    kind: Kind = Kind.BAR
    docs_path: str | None = None
    mainnet_only: bool = True
    #: Named transform the renderer runs before drawing. Three of these
    #: charts plot a computation of their fields rather than the fields --
    #: a percentage, a cost, a difference -- and drawing the raw columns
    #: gives a chart of nothing: fee stabilization came out as GTU_numerator
    #: at 1.2e19 against NRG_numerator at 1.
    derived: str | None = None
    #: A second collection merged in on the date. Network activity needs one:
    #: the CCD transferred is in statistics_network_activity and the
    #: transaction count behind its TPS line is in another collection
    #: entirely. Its fields are named by extra_series.
    extra_source: str | None = None
    extra_series: tuple[Series, ...] = ()
    #: What a derived chart draws, which is not what it reads. The reader
    #: sees "Activity" and "TPS"; network_activity and account_transaction
    #: are an implementation detail they never chose.
    derived_series: tuple[Series, ...] = ()
    #: Some of those only make sense on a log axis.
    log_y: bool = False
    #: Whether /{net}/charts/<slug> exists. A chart gets a spec before it gets
    #: a page: registering it already buys the bot its buttons and the API its
    #: aggregation rules. False keeps the gallery from linking to a 404.
    has_page: bool = False
    #: Whether /plots/{net}/<name>/image.png exists. False keeps the gallery
    #: from rendering a broken thumbnail.
    has_image: bool = False
    #: Whether the gallery lists this chart. False for the six Kraken
    #: intervals that are not the one it opens at: they are one chart seven
    #: ways, and seven tiles of the same picture is not seven charts.
    listed: bool = True
    #: Charts whose source collection is already pre-grouped, so the grouping
    #: picks the `type` instead of driving a $group. Active addresses are
    #: written daily, weekly and monthly by the nightly job.
    source_by_grouping: dict[Grouping, str] = {}

    axis: Axis = Axis.CALENDAR
    #: The intervals an INTERVAL chart offers, and which it opens at.
    intervals: tuple[Interval, ...] = ()
    default_interval: Interval | None = None
    #: How many days forward a LOOKBACK chart draws. Fixed: the cooldown
    #: schedule is the next seven days, including the ones on which nothing
    #: is released -- which, absent, read as a schedule that ends early.
    horizon_days: int = 0
    #: Names the figure builder the site registers for this chart, for the
    #: charts whose data is live rather than a Mongo collection. A name
    #: rather than a callable, the way `derived` is, so this module stays
    #: free of plotly and the bot can import it.
    live_source: str | None = None
    #: The page this chart is configured on, where that is not its own slug.
    #: The seven Kraken specs are one chart seven ways and share a page; they
    #: cannot share a `slug`, which keys BY_SLUG.
    page_slug: str = ""

    @property
    def automatic_grouping(self) -> bool:
        """Whether this chart picks its own resolution rather than asking.

        Grouping earns a control where it changes what the number is: a
        week of fees is not a day of fees, a week of growth is not a day of
        growth, and a week's distinct addresses cannot be built out of
        seven daily counts -- which is what source_by_grouping exists for.

        For a closing value it changes nothing. The same measurement,
        fewer points, so the only question is resolution, and the span
        answers that better than the reader can. Asked, they could pick
        monthly over thirty days and get a two-point chart.

        A mean is left asking on purpose: "average over the week" is a
        smoothing a reader may genuinely want, and the panel says so.
        """
        if not self.display_series or self.source_by_grouping:
            return False
        return all(series.agg is Agg.LAST for series in self.display_series)

    @property
    def display_series(self) -> tuple[Series, ...]:
        """The traces the reader sees, selects and shares."""
        return self.derived_series or self.series

    def source_for(self, grouping: Grouping) -> str:
        """The mongo `type` to read for this grouping."""
        return self.source_by_grouping.get(grouping, self.source)

    @model_validator(mode="after")
    def _default_page_slug(self):
        if not self.page_slug:
            object.__setattr__(self, "page_slug", self.slug)
        return self

    @model_validator(mode="after")
    def _check_axis(self):
        """That an off-calendar chart declares enough to be drawable.

        Refused at import rather than at request time: a spec missing its
        interval list or its provider registers a page that answers 500 to
        every caller, and the registry is built once at startup where a
        failure is loud.
        """
        if self.axis is Axis.CALENDAR:
            return self
        if not self.live_source:
            raise ValueError(f"{self.name}: a {self.axis.value} chart must name a live_source")
        if self.axis is Axis.INTERVAL:
            if not self.intervals:
                raise ValueError(f"{self.name}: an interval chart must offer intervals")
            if self.default_interval is None:
                object.__setattr__(self, "default_interval", self.intervals[0])
            elif self.default_interval not in self.intervals:
                raise ValueError(
                    f"{self.name}: {self.default_interval.value} is not one of its intervals"
                )
        if self.axis is Axis.LOOKBACK:
            if self.horizon_days <= 0:
                raise ValueError(f"{self.name}: a lookback chart needs a horizon")
            if len(self.groupings) > 1:
                raise ValueError(
                    f"{self.name}: a {self.horizon_days}-day span has nothing to group"
                )
        return self
