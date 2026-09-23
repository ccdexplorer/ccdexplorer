# ccdexplorer-watchdog

An outside view of ccdexplorer, hosted where ccdexplorer is not.

## Why this exists

`live-everything` watches the estate from inside it. Its collectors run in a
container on ccd-1, reach Mongo over WireGuard and read the Docker socket, so
it can tell you exactly which piece is unwell — and nothing at all when the
whole thing is gone. When ccd-1 goes, the monitoring goes with it.

Tooter has the same problem from the other end. Every alert in the estate is
posted to `https://tooter.ccdexplorer.io`, another container on the same
network. The one message that matters most — *everything is down* — is the one
message that cannot be sent.

This Worker shares nothing with what it watches: not a host, not a network,
not a DNS zone, not a notification path. It posts directly to Telegram's HTTP
API.

## What it checks, every minute

| Check | Question | Source |
|---|---|---|
| `site` | Does the site answer? | `GET https://ccdexplorer.io/mainnet` |
| `api:<net>` | Does the API answer, and with what block? | `GET .../v2/<net>/blocks/last/1` |
| `lag:<net>` | Is the indexer keeping up? | that height vs. the network's own |

The lag check is the one the other two cannot cover. Site and API can both
return 200 while the newest block in Mongo is an hour old, and from inside the
estate that looks perfectly healthy — every container up, every probe green,
the data stale.

Judging it needs a height that owes nothing to our infrastructure, which is
what Concordium's network dashboard gives: ~140 nodes, each reporting the head
it has seen. The **median** of those is used. The maximum is whichever node is
a block ahead on a fork, so it alarms on noise; the minimum is a node stuck at
height 3.2M since forever, so it never alarms at all. The median moves only
when the network moves.

## Alerting

Alerts are edge-triggered: one message when a check goes bad, one when it
recovers, and a reminder every `REMINDER_MINUTES` while it stays bad. State
lives in KV, so a long outage is two messages and not one per minute.

A 401 from the API is reported in its own words — the Worker is fine, the key
is not — because "API down" would send you looking at the wrong thing.

## Setup

```sh
cd workers/watchdog
npm install

# 1. State store. Put the printed id into wrangler.jsonc.
npx wrangler kv namespace create WATCHDOG_STATE

# 2. Secrets. The Telegram pair is the same tgram://<token>/<chat_id> that
#    tooter already uses: NOTIFIER_API_TOKEN and ADMIN_CHAT_ID in the estate's
#    .env. The API key must be one scoped for api.ccdexplorer.io.
npx wrangler secret put CCDEXPLORER_API_KEY
npx wrangler secret put TELEGRAM_BOT_TOKEN
npx wrangler secret put TELEGRAM_CHAT_ID

# 3. Ship it.
uv run pywrangler deploy
```

Then prove the whole path works without waiting for the cron:

```sh
curl https://ccdexplorer-watchdog.<subdomain>.workers.dev/check
```

That runs the checks, writes state and sends any alerts, exactly as the cron
would. Break it on purpose once — point `SITE_URL` at a hostname that does not
resolve, confirm the Telegram message arrives, put it back. An alerting path
that has never fired is not an alerting path.

## Endpoints

- `GET /status` — the last verdict as JSON. `live-everything` can scrape this,
  which closes a loop worth closing: the dashboard on ccd-1 gets to display
  what someone who is *not* on ccd-1 currently thinks of it.
- `GET /check` — run the checks now.

## Configuration

Vars live in `wrangler.jsonc`; secrets are set with `wrangler secret put`.

| Name | Default | Meaning |
|---|---|---|
| `SITE_URL` | `https://ccdexplorer.io/mainnet` | page fetched for the site check |
| `API_URL` | `https://api.ccdexplorer.io` | API base |
| `NETS` | `mainnet` | comma-separated nets to check |
| `LAG_BLOCKS` | `50` | blocks behind before the indexer is "down" (~100s) |
| `REMINDER_MINUTES` | `60` | how often to repeat a standing alert |
| `CCDEXPLORER_API_KEY` | *(secret)* | sent as `x-ccdexplorer-key` |
| `TELEGRAM_BOT_TOKEN` | *(secret)* | the bot half of `tgram://token/chat` |
| `TELEGRAM_CHAT_ID` | *(secret)* | the chat half |

## Things worth knowing

- **Keep it on `workers.dev`.** Putting it on a route under `ccdexplorer.io`
  would give it a dependency on the zone it is meant to judge, and a `fetch`
  to the site could loop back into the Worker.
- **Budget.** A tick a minute is 1,440 requests a day against a free-plan
  allowance of 100,000, so requests are never the constraint. Two things are:

  - **KV writes**, capped at 1,000/day on the free plan. Writing the state on
    every tick would have been 1,440 before anything else — so nothing is
    written unless a check changed or the stored document is 10 minutes old,
    which measures at **144 writes a day**. The cost is that `/status` can be
    up to 10 minutes behind; it carries a `checked_at` so a reader can see so.
  - **CPU**, capped at 10 ms per cron invocation on the free plan. The work
    here is ~2 ms in native CPython, almost all of it parsing the 259 KB
    dashboard. It should fit under Pyodide; it is not comfortable. See the
    plan table in `../README.md`.

  The dashboard is ~259 KB per tick. There is no smaller endpoint for "what
  height is the network at", and independence is worth the bytes.
- **It cannot see inside.** This Worker answers "is it up and is it current",
  not "why". That is still `live-everything`'s job; the two are complementary
  and neither replaces the other.
