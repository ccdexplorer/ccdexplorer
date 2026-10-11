"""/v 72723 -- what the site's validator tab says, in a chat.

Reads the same api routes account_tab_validator.py reads, and prints the
answers. Nothing is computed here that the api does not already give:
current-payday-stats hands back "68 / 360 blocks" as a string because
that is what the site puts on the page, so that is what goes in the
message.
"""

import datetime as dt
import html

NET = "mainnet"

#: How far back the missed-rounds total reaches.
PAYDAYS = 180

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
    name = html.escape(str(node.get("nodeName") or f"Validator {index}"))

    lines = [f"<b>{name}</b> — validator {index}"]

    if pool.get("is_suspended"):
        lines.append("⚠ <b>SUSPENDED</b>")
    if current.get("is_primed_for_suspension"):
        lines.append("⚠ <b>primed for suspension</b>")

    share = (equity + delegated) / total_network * 100 if total_network else 0
    lines.append(f"<b>Stake</b> {equity + delegated:,.0f} CCD · {share:.2f}% of network")
    lines.append(f"owner {equity:,.0f} · delegated {delegated:,.0f}")

    status = info.get("open_status") or ""
    if status == "closed_for_all":
        space = "closed to delegation"
    elif status == "closed_for_new":
        space = "closed to new delegators"
    elif room <= 0:
        space = "none — full"
    else:
        space = f"{room:,.0f} CCD"
    lines.append(f"<b>Delegation space</b> {space}")

    lines.append(
        f"<b>Commission</b> {(rates.get('baking') or 0) * 100:.1f}% baking · "
        f"{(rates.get('transaction') or 0) * 100:.1f}% transaction · "
        f"{(rates.get('finalization') or 0) * 100:.1f}% finalization"
    )

    # The api already formats this the way the site shows it.
    if payday:
        lines.append(f"<b>Today</b> {html.escape(str(payday))}")
    lines.append(f"<b>Missed today</b> {current.get('missed_rounds') or 0}")

    rows = (tally or {}).get("data") or []
    if rows:
        lines.append(
            f"<b>Missed last {len(rows)} days</b> {sum(r.get('missed') or 0 for r in rows):,}"
        )

    if win:
        lines.append(f"<b>Next win</b> {html.escape(_when(win))}")

    if node.get("client"):
        bits = [str(node["client"])]
        if node.get("uptime"):
            bits.append(f"up {int(node['uptime']) // 86_400_000}d")
        if node.get("averagePing") is not None:
            bits.append(f"{float(node['averagePing']):.0f}ms ping")
        if node.get("peersCount"):
            bits.append(f"{node['peersCount']} peers")
        lines.append(f"<b>Node</b> {' · '.join(bits)}")
    else:
        lines.append("<b>Node</b> not reporting to the dashboard")

    lines.append(f"{site_url.rstrip('/')}/{NET}/account/{index}")
    return "\n".join(lines)


def _when(timestamp: str) -> str:
    """An earliest-win-time as something readable in a chat."""
    try:
        when = dt.datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
    except (TypeError, ValueError):
        return str(timestamp)
    return when.strftime("%d %b %H:%M UTC")
