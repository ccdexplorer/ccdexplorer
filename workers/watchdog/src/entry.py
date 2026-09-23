"""An outside view of ccdexplorer, hosted where ccdexplorer is not.

live-everything watches the estate from inside it: the collectors run in a
container on ccd-1, reach Mongo over WireGuard and read the Docker socket.
That makes it very good at saying which part is unwell and no good at all at
saying the whole thing is gone -- when ccd-1 goes, the monitoring goes with
it, and the first report comes from a user.

Tooter has the mirror-image problem. Every alert in the estate is posted to
https://tooter.ccdexplorer.io, which is another container on the same network,
so the one message that matters most -- "everything is down" -- is the one
message that cannot be sent. This Worker therefore talks to Telegram's HTTP
API directly and shares nothing with the thing it watches: not a host, not a
network, not a DNS zone, not a notification path.

It answers three questions on every tick:

    Is the site answering?      GET https://ccdexplorer.io/mainnet
    Is the API answering?       GET .../v2/<net>/blocks/last/1
    Is the indexer keeping up?  that block's height, against the network's own

The third is the one the other two cannot cover. Site and API can both return
200 while the newest block in Mongo is an hour old, which from inside the
estate looks perfectly healthy -- every container is up, every probe is green,
and the data is stale. Catching that needs a height that does not come from
Sander's infrastructure at all, and Concordium's own network dashboard is
exactly that: ~140 nodes, each reporting the head it has seen.

The median of those heights is used, not the maximum and not the minimum. The
maximum is whichever node is a block ahead on a fork, so it alarms on noise.
The minimum is a node that has been stuck at height 3.2M since forever, so it
never alarms at all. The median only moves when the network moves.

Alerts fire on transitions, not on every tick, so a long outage is one message
and one all-clear rather than an hourly wall of them. A reminder is repeated
while a check stays bad, because a silent Worker and a healthy estate look
identical from a phone.
"""

import json
import time
from urllib.parse import urlsplit

from workers import Response, WorkerEntrypoint, fetch

try:
    from js import AbortSignal
except ImportError:  # Only absent outside workerd, e.g. when linting locally.
    AbortSignal = None

#: Concordium publishes a live summary of every node that reports in. There is
#: no smaller endpoint for "what height is the network at", and paying ~265KB
#: a minute for an answer that owes nothing to our own infrastructure is a
#: bargain -- that independence is the whole point of this Worker.
DASHBOARDS = {
    "mainnet": "https://dashboard.mainnet.concordium.software/nodesSummary",
    "testnet": "https://dashboard.testnet.concordium.com/nodesSummary",
}

#: One key, not two. Workers KV allows 1,000 writes a day on the free plan and
#: a tick a minute is 1,440 of them before anything else is counted, so the
#: per-check state and the /status snapshot share a document and a write.
STATE_KEY = "watchdog:v1"

#: Blocks arrive about every two seconds, so the default threshold is roughly
#: a hundred seconds of indexing lag. Low enough to catch a stopped indexer
#: quickly, high enough to ride out a Mongo election or a deploy.
DEFAULT_LAG_BLOCKS = 50

#: How long a check may stay bad before it is mentioned again.
DEFAULT_REMINDER_MINUTES = 60

#: A hung site is a down site. Without this, a target that accepts the
#: connection and then never answers would stall the tick instead of failing
#: it, which is the failure this Worker most needs to report.
FETCH_TIMEOUT_MS = 10_000

#: Nothing is written unless a check changed state or the stored document has
#: gone this stale, which puts a quiet day at around 150 writes instead of
#: 2,880. The cost is that /status can be up to this far behind; it carries a
#: checked_at so a reader can see that for themselves.
MIN_WRITE_INTERVAL_SECONDS = 600

#: The stored document outlives a few ticks, so a reader can tell "the last
#: check said everything was fine" from "there has been no check at all".
STATE_TTL_SECONDS = 3600


def _timeout_signal(timeout_ms):
    """An AbortSignal that fires after ``timeout_ms``, or None if unavailable.

    Returning None rather than raising keeps a missing AbortSignal from turning
    a monitoring Worker into another thing that needs monitoring; the checks
    simply fall back to the runtime's own subrequest limits.
    """
    if AbortSignal is None:
        return None
    try:
        return AbortSignal.timeout(timeout_ms)
    except Exception:  # never let the alarm clock break the alarm
        return None


async def _get(url, headers=None, timeout_ms=FETCH_TIMEOUT_MS):
    """GET ``url``, returning (response, elapsed_ms). Raises on network error."""
    options = {}
    if headers:
        options["headers"] = headers
    signal = _timeout_signal(timeout_ms)
    if signal is not None:
        options["signal"] = signal
    started = time.time()
    response = await fetch(url, **options)
    return response, int((time.time() - started) * 1000)


def _ok_status(status):
    return 200 <= int(status) < 300


async def check_site(url):
    """Can the outside world load a page of the site at all?"""
    try:
        response, elapsed_ms = await _get(url)
    except Exception as error:  # any failure here is "site down"
        return {"id": "site", "ok": False, "detail": f"unreachable ({error})", "url": url}

    ok = _ok_status(response.status)
    return {
        "id": "site",
        "ok": ok,
        "detail": f"HTTP {response.status} in {elapsed_ms} ms",
        "status": int(response.status),
        "elapsed_ms": elapsed_ms,
        "url": url,
    }


async def check_api(api_url, net, api_key):
    """Is the API answering, and what is the newest block it knows about?

    The height comes back alongside the verdict because the lag check needs it
    and there is no reason to ask twice.
    """
    url = f"{api_url}/v2/{net}/blocks/last/1"
    headers = {"x-ccdexplorer-key": api_key} if api_key else None
    result = {"id": f"api:{net}", "net": net, "url": url, "height": None}

    try:
        response, elapsed_ms = await _get(url, headers=headers)
    except Exception as error:  # any failure here is "API down"
        result.update(ok=False, detail=f"unreachable ({error})")
        return result

    result["status"] = int(response.status)
    result["elapsed_ms"] = elapsed_ms

    if not _ok_status(response.status):
        # 401 deserves its own words: the Worker is fine, the key is not, and
        # "API down" would send Sander looking at the wrong thing.
        if int(response.status) == 401:
            result.update(ok=False, detail="HTTP 401 -- the Worker's API key is not valid")
        else:
            result.update(ok=False, detail=f"HTTP {response.status} in {elapsed_ms} ms")
        return result

    try:
        blocks = await response.json()
        result["height"] = int(blocks[0]["height"])
    except Exception as error:  # a 200 with no block is still broken
        result.update(ok=False, detail=f"HTTP 200 but no block in the body ({error})")
        return result

    result.update(ok=True, detail=f"HTTP 200 in {elapsed_ms} ms, height {result['height']:,}")
    return result


async def network_height(net):
    """The height the Concordium network itself is at.

    Returns (height, note). ``height`` is None when the dashboard could not be
    read, which is not an alert: it means this tick cannot judge lag, not that
    anything of Sander's is wrong.
    """
    url = DASHBOARDS.get(net)
    if not url:
        return None, f"no network dashboard is configured for {net}"

    try:
        response, _ = await _get(url)
        if not _ok_status(response.status):
            return None, f"network dashboard returned HTTP {response.status}"
        nodes = await response.json()
    except Exception as error:  # an unreadable dashboard is not our outage
        return None, f"network dashboard unreachable ({error})"

    heights = sorted(
        int(node["bestBlockHeight"])
        for node in nodes
        if isinstance(node, dict) and isinstance(node.get("bestBlockHeight"), (int, float))
    )
    if not heights:
        return None, "network dashboard reported no heights"
    return heights[len(heights) // 2], f"median of {len(heights)} nodes"


def check_lag(net, explorer_height, network, threshold):
    """Is the indexer keeping up with the chain?

    Deliberately silent when either height is missing. A missing explorer
    height is already being reported by the API check, and a missing network
    height means we have nothing to compare against -- inventing a verdict from
    one number would just be the inside view again, wearing a disguise.
    """
    reference, note = network
    result = {"id": f"lag:{net}", "net": net, "threshold": threshold}

    if explorer_height is None:
        result.update(ok=True, skipped=True, detail="not checked -- the API returned no height")
        return result
    if reference is None:
        result.update(ok=True, skipped=True, detail=f"not checked -- {note}")
        return result

    lag = reference - explorer_height
    result.update(lag=lag, explorer_height=explorer_height, network_height=reference)
    behind = f"{lag:,} blocks behind the network ({reference:,} vs {explorer_height:,})"

    if lag > threshold:
        result.update(ok=False, detail=f"{behind}, over the threshold of {threshold:,}")
    else:
        # A negative lag means our height is ahead of the median, which is
        # normal: we follow the tip, the median trails it slightly.
        result.update(ok=True, detail=f"{behind}" if lag > 0 else f"at the tip ({reference:,})")
    return result


def label_for(check_id):
    """A human name for a check id, for the body of an alert."""
    kind, _, net = check_id.partition(":")
    if kind == "site":
        return "The site"
    if kind == "api":
        return f"The API ({net})"
    if kind == "lag":
        return f"Indexing ({net})"
    return check_id


def _escape(text):
    """Telegram's HTML parse mode rejects stray angle brackets and ampersands."""
    return str(text).replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def send_telegram(token, chat_id, text):
    """Post straight to Telegram, on purpose.

    Everything else in the estate notifies through tooter.ccdexplorer.io. That
    is one more container on the network this Worker exists to distrust, so it
    is the one path an alert from here must not take.
    """
    if not token or not chat_id:
        print("watchdog: no Telegram credentials configured, alert not sent")
        return False
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        response = await fetch(
            url,
            method="POST",
            headers={"content-type": "application/json"},
            body=json.dumps(payload),
        )
    except Exception as error:
        print(f"watchdog: Telegram unreachable ({error})")
        return False
    if not _ok_status(response.status):
        print(f"watchdog: Telegram returned HTTP {response.status}")
        return False
    return True


def reconcile(previous, checks, now, reminder_s):
    """Fold this tick into the stored state.

    Returns (transitions, state). Each transition is (check, kind, seconds),
    where kind is "down", "recovered" or "still-down". Kept free of I/O so the
    part most easily got wrong -- when an alert fires and when it does not --
    can be exercised without a KV namespace or a Telegram token.
    """
    transitions = []
    state = {}
    for check in checks:
        check_id = check["id"]
        before = previous.get(check_id) or {}
        was_ok = before.get("ok")
        since = before.get("since", now)
        notified_at = before.get("notified_at", 0)

        if was_ok is None or bool(was_ok) != check["ok"]:
            # First sighting counts as a transition only when it is bad; a
            # Worker's first tick should not announce that all is well.
            since = now
            if not check["ok"]:
                transitions.append((check, "down", 0))
                notified_at = now
            elif was_ok is False:
                transitions.append((check, "recovered", now - before.get("since", now)))
                notified_at = 0
        elif not check["ok"] and now - notified_at >= reminder_s:
            transitions.append((check, "still-down", now - since))
            notified_at = now

        state[check_id] = {"ok": check["ok"], "since": since, "notified_at": notified_at}

    return transitions, state


class Default(WorkerEntrypoint):
    """The Cron-triggered watchdog, plus a /status endpoint for the dashboard."""

    async def scheduled(self, controller, env, ctx):
        await self.run_checks()

    async def fetch(self, request):
        """Expose the last verdict so live-everything can show the outside view.

        This closes a loop worth closing: the dashboard on ccd-1 gets to
        display what someone who is not on ccd-1 currently thinks of it.
        """
        path = urlsplit(request.url).path
        if path in ("/status", "/"):
            snapshot = (await self._load()).get("snapshot")
            if not snapshot:
                return Response.json(
                    {"ok": None, "detail": "no check has run yet"},
                    status=503,
                    headers={"cache-control": "no-store"},
                )
            return Response.json(snapshot, headers={"cache-control": "no-store"})
        if path == "/check":
            # Useful once, after deploying, to prove the whole path works
            # without waiting for the cron to come round.
            return Response.json(await self.run_checks(), headers={"cache-control": "no-store"})
        return Response.json({"error": "not found"}, status=404)

    def _var(self, name, default):
        """Read a Worker var or secret, falling back when it is not bound."""
        value = getattr(self.env, name, None)
        return default if value in (None, "") else value

    async def run_checks(self):
        site_url = self._var("SITE_URL", "https://ccdexplorer.io/mainnet")
        api_url = str(self._var("API_URL", "https://api.ccdexplorer.io")).rstrip("/")
        api_key = self._var("CCDEXPLORER_API_KEY", "")
        nets = [n.strip() for n in str(self._var("NETS", "mainnet")).split(",") if n.strip()]
        threshold = int(self._var("LAG_BLOCKS", DEFAULT_LAG_BLOCKS))
        reminder_s = int(self._var("REMINDER_MINUTES", DEFAULT_REMINDER_MINUTES)) * 60

        checks = [await check_site(site_url)]
        for net in nets:
            api = await check_api(api_url, net, api_key)
            checks.append(api)
            checks.append(check_lag(net, api.get("height"), await network_height(net), threshold))

        now = int(time.time())
        stored = await self._load()
        previous = stored.get("checks") or {}
        transitions, current = reconcile(previous, checks, now, reminder_s)

        if transitions:
            await self._announce(transitions, now)

        snapshot = {
            "ok": all(check["ok"] for check in checks),
            "checked_at": now,
            "checks": checks,
            "source": "cloudflare-worker",
        }
        # A tick where nothing moved is the overwhelmingly common case, and
        # rewriting an identical document 1,440 times a day is how a watchdog
        # runs itself out of KV budget and then cannot record the one tick
        # that mattered.
        stale = now - int(stored.get("written_at") or 0) >= MIN_WRITE_INTERVAL_SECONDS
        if transitions or current != previous or stale:
            await self._save({"checks": current, "snapshot": snapshot, "written_at": now})
        return snapshot

    async def _load(self):
        raw = await self.env.WATCHDOG_STATE.get(STATE_KEY)
        try:
            return json.loads(raw) if raw else {}
        except ValueError:
            # A corrupt document is not worth a crash: start again and the
            # next tick re-establishes everything except the outage clock.
            return {}

    async def _save(self, document):
        await self.env.WATCHDOG_STATE.put(
            STATE_KEY, json.dumps(document), expirationTtl=STATE_TTL_SECONDS
        )

    async def _announce(self, transitions, now):
        token = self._var("TELEGRAM_BOT_TOKEN", "")
        chat_id = self._var("TELEGRAM_CHAT_ID", "")

        lines = []
        for check, kind, seconds in transitions:
            label = label_for(check["id"])
            detail = _escape(check["detail"])
            if kind == "recovered":
                lines.append(f"✅ <b>{label} is back</b> after {_duration(seconds)} — {detail}")
            elif kind == "still-down":
                lines.append(
                    f"🔴 <b>{label} is still down</b> after {_duration(seconds)} — {detail}"
                )
            else:
                lines.append(f"🔴 <b>{label} is down</b> — {detail}")

        body = "\n".join(lines)
        footer = "\n\n<i>Reported from a Cloudflare Worker, outside the estate.</i>"
        await send_telegram(token, chat_id, body + footer)


def _duration(seconds):
    """A rough, readable span -- alerts want '2h 5m', not '7523 seconds'."""
    seconds = max(int(seconds), 0)
    if seconds < 60:
        return f"{seconds}s"
    minutes, seconds = divmod(seconds, 60)
    if minutes < 60:
        return f"{minutes}m"
    hours, minutes = divmod(minutes, 60)
    if hours < 24:
        return f"{hours}h {minutes}m"
    days, hours = divmod(hours, 24)
    return f"{days}d {hours}h"
