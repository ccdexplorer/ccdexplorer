#!/usr/bin/env bash
#
# Run the chart bot against a local site, over a Cloudflare quick tunnel.
#
# The bot does not upload chart images. It hands Telegram a URL and Telegram's
# own servers fetch it, so a site on 127.0.0.1 produces a bot whose every
# answer is a broken image. The tunnel is what makes the local site reachable
# from Telegram.
#
# A quick tunnel needs no Cloudflare account and no DNS, and costs a random
# hostname per run. That hostname has to exist before either process starts:
# the site bakes it into every og:image and download link, and the bot builds
# every chart URL from it. So the order is tunnel, read the hostname, then
# start the other two with SITE_URL pointing at it.
#
#   just bot-tunnel
#
# Ctrl-C stops all three.

set -euo pipefail

cd "$(dirname "$0")/.."

SITE_PORT="${SITE_PORT:-8000}"
API_PORT="${API_PORT:-7000}"
LOG_DIR="${TMPDIR:-/tmp}/ccdexplorer-bot-tunnel"
mkdir -p "$LOG_DIR"

# A chart whose data the site could not fetch still renders, as a png saying
# "No data" -- and still answers 200. So the range below is asked for by name
# at the end of startup and the rows counted: a year that has data on every
# chart, fixed rather than derived, so the check does not drift with today.
PROBE_CHART="transaction-fees"
PROBE_RANGE="weekly/202401/202412"

command -v cloudflared >/dev/null || {
    echo "cloudflared is not installed. brew install cloudflared" >&2
    exit 1
}

# Read from .env the same way the app does, rather than asking the user to
# export anything. Only the two keys this script decides on are taken.
token="$(sed -n 's/^[[:space:]]*CHART_BOT_TOKEN[[:space:]]*=[[:space:]]*//p' .env 2>/dev/null \
         | tail -1 | tr -d '"'"'"' \r')"
[ -n "$token" ] || {
    echo "CHART_BOT_TOKEN is not set in .env; the bot has nothing to connect with." >&2
    exit 1
}

pids=()
cleanup() {
    trap - INT TERM HUP EXIT
    # Negative pid: uv and cloudflared both spawn a child, and killing only
    # the parent leaves the port held and the tunnel up.
    for pid in "${pids[@]:-}"; do
        [ -n "$pid" ] && kill -- "-$pid" 2>/dev/null || true
    done

    # Then insist. The site takes the signal and logs "Waiting for
    # application shutdown" without ever finishing: its lifespan owns the
    # scheduler, and the plot warmer's kaleido renders sit in an executor
    # that shutdown waits on. A dev script that leaves :8000 held is a dev
    # script you have to clean up after by hand, so after a grace period the
    # survivors are killed outright.
    for _ in $(seq 1 10); do
        still_running=""
        for pid in "${pids[@]:-}"; do
            [ -n "$pid" ] && kill -0 "$pid" 2>/dev/null && still_running=1
        done
        [ -z "$still_running" ] && break
        sleep 0.5
    done
    for pid in "${pids[@]:-}"; do
        [ -n "$pid" ] && kill -9 -- "-$pid" 2>/dev/null || true
    done

    wait 2>/dev/null || true
    echo
    echo "stopped. logs in $LOG_DIR"
}
trap cleanup INT TERM HUP EXIT

echo "opening a quick tunnel to :$SITE_PORT ..."
set -m  # each child gets its own process group, so cleanup can take the group
cloudflared tunnel --url "http://localhost:$SITE_PORT" \
    > "$LOG_DIR/tunnel.log" 2>&1 &
pids+=("$!")
set +m

# cloudflared prints the assigned hostname a few seconds after start, inside a
# box of pipes and spaces -- hence the match on the URL itself rather than on
# any particular line.
site_url=""
for _ in $(seq 1 60); do
    site_url="$(grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' "$LOG_DIR/tunnel.log" \
                | head -1 || true)"
    [ -n "$site_url" ] && break
    sleep 1
done
[ -n "$site_url" ] || {
    echo "the tunnel never reported a hostname. $LOG_DIR/tunnel.log:" >&2
    tail -20 "$LOG_DIR/tunnel.log" >&2
    exit 1
}

echo "  tunnel  $site_url"

api_url="http://127.0.0.1:$API_PORT"
set -m
uv run uvicorn projects.ccdexplorer_api.asgi:app \
    --loop asyncio --port "$API_PORT" > "$LOG_DIR/api.log" 2>&1 &
pids+=("$!")
set +m

echo -n "  api     starting"
for _ in $(seq 1 60); do
    curl -sfo /dev/null "$api_url/openapi.json" && break
    echo -n "."
    sleep 2
done
curl -sfo /dev/null "$api_url/openapi.json" || {
    echo
    echo "the api did not come up. $LOG_DIR/api.log:" >&2
    tail -20 "$LOG_DIR/api.log" >&2
    exit 1
}
echo $'\r  api     :'"$API_PORT                                        "

# Passed explicitly rather than left to .env, so the site talks to the api
# this script started and not to whatever a stale API_URL points at.
set -m
SITE_URL="$site_url" API_URL="$api_url" uv run uvicorn projects.ccdexplorer_site.asgi:app \
    --loop asyncio --port "$SITE_PORT" > "$LOG_DIR/site.log" 2>&1 &
pids+=("$!")
set +m

# Through the tunnel, not on localhost: a site that answers locally but 502s
# from outside is the failure this script exists to prevent, and it is worth
# finding out now rather than from a bot that silently shows no charts.
echo -n "  site    starting"
for _ in $(seq 1 60); do
    if curl -sfo /dev/null "$site_url/mainnet"; then
        echo $'\r  site    :'"$SITE_PORT reachable at $site_url  "
        break
    fi
    echo -n "."
    sleep 2
done
curl -sfo /dev/null "$site_url/mainnet" || {
    echo
    echo "the site did not come up through the tunnel. $LOG_DIR/site.log:" >&2
    tail -20 "$LOG_DIR/site.log" >&2
    exit 1
}

# Reachable is not the same as working. With no api behind it the site serves
# every chart as a png reading "No data", under a 200 -- which is what this
# script reported as success until it started the api itself.
rows=$(curl -sf "$site_url/mainnet/charts/$PROBE_CHART/$PROBE_RANGE/data.csv" \
       | tail -n +2 | grep -c . || true)
if [ "${rows:-0}" -lt 2 ]; then
    echo "  charts  NO DATA — the site reached the api but got nothing back." >&2
    echo "          check $LOG_DIR/api.log and MONGO_URI in .env." >&2
else
    echo "  charts  $rows rows for $PROBE_CHART $PROBE_RANGE"
fi

set -m
SITE_URL="$site_url" uv run python -m ccdexplorer.ccdexplorer_chart_bot \
    > "$LOG_DIR/bot.log" 2>&1 &
pids+=("$!")
set +m

echo "  bot     started — message it on Telegram, or try /ccd fees"
echo
echo "tail -f $LOG_DIR/{tunnel,site,bot}.log"
echo "Ctrl-C to stop all three."
wait
