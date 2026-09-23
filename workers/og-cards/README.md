# ccdexplorer-og-cards

Social preview cards for ccdexplorer links, drawn at the edge.

## Why this exists

Paste a ccdexplorer link into Telegram, Slack or X today and the preview shows
a title and nothing else. The templates set `og:title` and `og:url` and stop
there — no `og:description`, no `og:image`.

This Worker fills the gap: one PNG per entity, generated on demand from the
same API the site uses, so a shared block link previews as that block's
height, hash, time and transaction count. It is the sort of thing people
screenshot, which is free distribution for the explorer.

## Why a Worker and not the site

- The site is a single-worker uvicorn process. Rasterising a 1200×630 PNG
  inside it would block the event loop for every other request, and crawler
  traffic arrives in bursts exactly when a link is spreading.
- Cards are the definition of cacheable. A finalized block's card is true
  forever, so the edge can serve it without ever asking us twice.
- It fails harmlessly. If the Worker is down the preview goes back to plain,
  which is where we are today.

Note what this deliberately does *not* do. Cloudflare's own OpenGraph example
puts a Worker in front of the whole site and rewrites the HTML on the way
past. That would place a Worker in the critical path of every page load to
gain some meta tags. Here the site keeps serving itself; only the image URL
points at the edge.

## URLs

```
/<net>/block/<height-or-hash>.png
/<net>/account/<index-or-address>.png
/<net>/transaction/<hash>.png
```

`<net>` is `mainnet`, `testnet` or `devnet`, and it is the most prominent
thing on the card — a testnet link that previews like a mainnet one is
actively misleading, so the net gets a colour as well as a word.

Anything unrecognised returns a plain branded card with **200**, not a 404. A
preview consumer that gets an error falls back to showing the bare link, which
looks worse than a generic card; a stale or mistyped link is not a reason to
make the whole message look broken.

## Caching

Two layers, for two different reasons:

| Layer | What it protects |
|---|---|
| KV, keyed `net:kind:ident` | the API. A spreading link is a burst of crawlers asking for the same entity; without this, each is a Mongo query — or, for accounts, a gRPC call to a node. |
| `Cache-Control` on the PNG | the Worker. Blocks and transactions get 24h, accounts 5 minutes, fallbacks 60s. |

The KV binding is optional. Without it the Worker still works and simply asks
the API every time.

## This one needs the paid plan

Rendering and encoding a card measures at ~12 ms of CPU in native CPython,
warm. The free plan allows 10 ms per request, and Pyodide's WebAssembly is
slower than native, so this Worker wants **Workers Paid** ($5/month, where the
default is 30 s).

The KV cache does not change that: it saves the API round-trip, not the
drawing, and every request still renders. Caching the finished PNG rather than
the JSON would — it is the obvious optimisation if the paid plan is not wanted,
at the cost of a 25 MiB-per-value store filling up with images.

## Setup

```sh
cd workers/og-cards
npm install

npx wrangler kv namespace create OG_CACHE     # put the id into wrangler.jsonc
npx wrangler secret put CCDEXPLORER_API_KEY   # scoped for api.ccdexplorer.io

uv run pywrangler deploy
```

Check a card before wiring it into the site:

```sh
curl -o card.png https://ccdexplorer-og-cards.<subdomain>.workers.dev/mainnet/block/52029165.png
open card.png
```

## Wiring it into the site

The site emits the tags itself; this Worker only supplies the image. Set
`OG_IMAGE_BASE` in the site's stack to the Worker's origin:

```
OG_IMAGE_BASE=https://ccdexplorer-og-cards.<subdomain>.workers.dev
```

When it is unset the templates fall back to the previous behaviour and emit no
`og:image` at all, so the site can be deployed before the Worker exists — and
keeps working if the Worker is ever removed.

## Rendering notes

- **Fonts.** Pillow 10.1 gave `ImageFont.load_default` a size argument, which
  returns the bundled Aileron TrueType instead of the old 10px bitmap. That is
  what lets the card look like this without shipping a font file. The upgrade
  path, if the type ever needs to match the site's Outfit, is to bundle a TTF.
- **Identifiers are attacker-controlled.** They arrive from a crawlable URL,
  so they are length-capped and `quote(..., safe="")`d before going anywhere
  near an API path.
- **Previewing changes locally** does not need Cloudflare. The rendering
  functions are plain Pillow and take no runtime objects; stub `workers` and
  `pyodide.ffi`, call `block_card` with a fixture and save the result.
