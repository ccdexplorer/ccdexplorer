"""/v 72723 -- what the site's validator tab says, in a chat.

Reads the same api routes account_tab_validator.py reads, and prints the
answers. Nothing is computed here that the api does not already give:
current-payday-stats hands back "68 / 360 blocks" as a string because
that is what the site puts on the page, so that is what goes in the
message.
"""

import datetime as dt
import html
import time

NET = "mainnet"

#: How far back the missed-rounds total reaches.
PAYDAYS = 180

#: The three charts the validator page shows, in the order the buttons
#: read. APY first: it is what a delegator opens the page for.
VIEWS = ("apy", "performance", "rewards")

VIEW_LABELS = {"apy": "APY", "performance": "Performance", "rewards": "Rewards"}

#: Callback payload prefix, kept clear of the chart catalogue's "c:".
CALLBACK_PREFIX = "v:"

#: What the page asks baker-performance for, so the picture matches it.
PERFORMANCE_DAYS = 3000

#: How often the image url changes, so Telegram refetches rather than
#: serving its stored copy of a chart that has moved on.
REFRESH_SECONDS = 3600

#: microCCD to CCD.
MICRO = 1_000_000


def index_or_none(args: list[str]) -> int | None:
    """The account index `/v <index>` names, or None. Digits only."""
    if not args:
        return None
    raw = args[0].strip()
    return int(raw) if raw.isdigit() else None


def api_paths(index: int) -> dict[str, str]:
    """The routes the site's validator tab uses, for one validator."""
    return {
        "pool": f"{NET}/account/{index}/pool-info",
        "node": f"{NET}/account/{index}/node",
        "payday": f"{NET}/account/{index}/current-payday-stats",
        "win": f"{NET}/account/{index}/earliest-win-time",
        "tally": f"{NET}/account/{index}/validator-tally/0/{PAYDAYS}",
    }


def _ccd(value) -> float:
    try:
        return float(value) / MICRO
    except (TypeError, ValueError):
        return 0.0


def is_validator(pool) -> bool:
    """Whether this account bakes. An account that does not is told so."""
    return bool(pool) and pool.get("equity_capital") is not None


def summary(index: int, pool, node, payday, win, tally, site_url: str) -> str:
    """The message. One line per thing somebody came to find out."""
    pool = pool or {}
    node = node or {}
    info = pool.get("pool_info") or {}
    rates = info.get("commission_rates") or {}
    current = pool.get("current_payday_info") or {}

    equity = _ccd(pool.get("equity_capital"))
    delegated = _ccd(pool.get("delegated_capital"))
    cap = _ccd(pool.get("delegated_capital_cap"))
    total_network = _ccd(pool.get("all_pool_total_capital"))
    room = max(0.0, cap - delegated)
    # Not the node name: that is the operator's machine, not the
    # validator, and leading with it labelled the whole message with one
    # of its own footnotes.
    lines = [f"<b>Validator {index}</b>"]

    if pool.get("is_suspended"):
        lines.append("⚠ <b>SUSPENDED</b>")
    if current.get("is_primed_for_suspension"):
        lines.append("⚠ <b>primed for suspension</b>")

    # Three sections, because they answer three different questions: what
    # is fixed about this validator, how it is performing, and what its
    # node is doing. Eleven lines of facts in a row is a wall.
    share = (equity + delegated) / total_network * 100 if total_network else 0
    status = info.get("open_status") or ""
    if status == "closed_for_all":
        space = "closed to delegation"
    elif status == "closed_for_new":
        space = "closed to new delegators"
    elif room <= 0:
        space = "none — full"
    else:
        space = f"{room:,.0f} CCD"

    lines += [
        "",
        "<b>Validator</b>",
        f"Stake {equity + delegated:,.0f} CCD · {share:.2f}% of network",
        f"Owner {equity:,.0f} · delegated {delegated:,.0f}",
        f"Delegation space {space}",
        f"Commission {(rates.get('baking') or 0) * 100:.1f}% baking · "
        f"{(rates.get('transaction') or 0) * 100:.1f}% transaction · "
        f"{(rates.get('finalization') or 0) * 100:.1f}% finalization",
    ]

    # Six months of missed rounds is a standing characteristic of this
    # validator, not something happening now, so it closes the section
    # that describes it rather than opening the one that does not.
    rows = (tally or {}).get("data") or []
    if rows:
        lines.append(f"Missed last {len(rows)} days {sum(r.get('missed') or 0 for r in rows):,}")

    lines += ["", "<b>Live</b>"]

    # The api already formats this the way the site shows it.
    if payday:
        lines.append(f"Today {html.escape(str(payday))}")
    lines.append(f"Missed today {current.get('missed_rounds') or 0}")
    if win:
        lines.append(f"Next win {html.escape(_when(win))}")

    lines += ["", "<b>Node</b>"]
    if node.get("client"):
        # The node's own name belongs here, not in the header: it names
        # the operator's machine, not the validator.
        bits = []
        if node.get("nodeName"):
            bits.append(html.escape(str(node["nodeName"])))
        bits.append(str(node["client"]))
        if node.get("uptime"):
            bits.append(f"up {int(node['uptime']) // 86_400_000}d")
        if node.get("averagePing") is not None:
            bits.append(f"{float(node['averagePing']):.0f}ms ping")
        if node.get("peersCount"):
            bits.append(f"{node['peersCount']} peers")
        lines.append(" · ".join(bits))
    else:
        lines.append("not reporting to the dashboard")

    lines += ["", f"{site_url.rstrip('/')}/{NET}/account/{index}"]
    return "\n".join(lines)


def _when(timestamp: str) -> str:
    """An earliest-win-time, relative and absolute.

    Relative because there is nothing to localise to: the Bot API's User
    carries a language_code and no timezone, so the bot cannot know what
    "15:30" means to whoever is reading. "in 23 min" is right in every
    zone, and the UTC time is kept beside it for anyone comparing against
    something else.
    """
    try:
        when = dt.datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return str(timestamp)
    if when.tzinfo is None:
        when = when.replace(tzinfo=dt.UTC)

    absolute = when.strftime("%H:%M UTC")
    seconds = (when - dt.datetime.now(dt.UTC)).total_seconds()
    if seconds < 0:
        # The node is past its slot; the api has not caught up yet.
        return f"due now ({absolute})"
    if seconds < 60:
        return f"any moment ({absolute})"
    minutes = int(seconds // 60)
    if minutes < 60:
        return f"in {minutes} min ({absolute})"
    return f"in {minutes // 60}h {minutes % 60}m ({absolute})"


def address_of(pool) -> str:
    """The validator's account address.

    staking-rewards-bucketed is keyed by address, not index -- asked for
    by index it answers with an empty list rather than an error, so the
    rewards chart would come back blank for every validator.
    """
    return str((pool or {}).get("address") or "")


def image_url(
    site_url: str, index: int, view: str, address: str = "", now: float | None = None
) -> str:
    """The png the site already draws for this box on the validator page.

    Each is the same figure that page builds, asked for over GET with
    /png on the end, because Telegram fetches an image url itself and
    will not post to get one.
    """
    bucket = int((time.time() if now is None else now) // REFRESH_SECONDS)
    if view == "performance":
        path = f"{NET}/account/baker-performance/{index}/{PERFORMANCE_DAYS}/png"
    elif view == "rewards":
        path = f"{NET}/account/rewards-bucketed/{address}/{index}/png"
    else:
        path = f"{NET}/account/apy-graph/{index}/validator/png"
    return f"{site_url.rstrip('/')}/{path}?t={bucket}"


def keyboard(index: int, view: str):
    """One button per chart on the validator page, the current one marked."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    f"· {VIEW_LABELS[v]} ·" if v == view else VIEW_LABELS[v],
                    callback_data=f"{CALLBACK_PREFIX}{index}:{v}",
                )
                for v in VIEWS
            ]
        ]
    )


def parse_callback(data: str) -> tuple[int, str] | None:
    """(index, view) for a button press, or None if it is not one of ours."""
    if not data or not data.startswith(CALLBACK_PREFIX):
        return None
    raw_index, _, view = data[len(CALLBACK_PREFIX) :].partition(":")
    if not raw_index.isdigit() or view not in VIEWS:
        return None
    return int(raw_index), view
