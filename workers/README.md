# workers/

Cloudflare Python Workers. These are the only things in this repository that
are not Python-in-a-container deployed by Komodo, so they need a paragraph of
explanation before the first surprise.

## What they are

Python Workers run CPython compiled to WebAssembly (Pyodide) inside V8
isolates, on Cloudflare's edge. They went GA in 2025. In practice:

- The language is Python 3.12, but the runtime is not CPython on Linux. There
  is no filesystem, no threads, no sockets, and no MongoDB driver — anything
  that opens a TCP connection is out.
- That last point is why nothing in the explorer's core can move here. Every
  base in `bases/` talks to Mongo, and Cloudflare's Hyperdrive covers
  PostgreSQL and MySQL only.
- Outbound HTTP is `fetch` from the `workers` module. `httpx` and `requests`
  work too, via a patched transport, but `fetch` is the one with no surprises.
- Dependencies come from `pyproject.toml` and are resolved at deploy time.

So the useful shape for us is: small things at the edge that read our public
HTTP API, or that need to be somewhere our own machines are not.

## What is here

| Worker | What it does |
|---|---|
| [`watchdog/`](watchdog/) | Checks that the site and API answer and that the indexer is current, from outside the estate, and alerts Telegram directly. The one monitor that survives ccd-1 going down. |
| [`og-cards/`](og-cards/) | Renders a PNG social preview per block, account and transaction, so a shared link previews with live chain data. |

## Which Cloudflare plan you need

Short answer: the **Workers Paid** plan, $5/month, for `og-cards`. The free
plan's ceiling is not the request count — it is CPU time.

| Free plan limit | What it means here |
|---|---|
| 10 ms CPU per request *and* per cron invocation | the binding constraint |
| 100,000 requests/day | never close; a tick a minute is 1,440 |
| 1,000 KV writes/day | `watchdog` was over this and was rewritten to fit |
| 5 cron triggers per account | one is used |

Measured on this machine, in native CPython, warm:

- `og-cards` spends **~12 ms** rendering and encoding one card. That is over
  the free ceiling before any of Pyodide's WebAssembly overhead is counted, and
  every request pays it — the KV cache saves the API call, not the drawing.
  This Worker needs the paid plan.
- `watchdog` spends **~2 ms**, nearly all of it parsing the 259 KB network
  dashboard. That should fit inside 10 ms, but not by much, so treat the free
  plan as something to try rather than something to rely on.

On the paid plan CPU defaults to 30 s and KV writes are unmetered, and neither
Worker comes anywhere near either.

## Deploying

Not Komodo, not Docker, not `just`. Each directory is its own project:

```sh
cd workers/<name>
npm install
uv run pywrangler deploy
```

`npm install` pulls in exactly one thing: **wrangler**, Cloudflare's CLI. It is
what actually talks to Cloudflare — uploading code, creating KV namespaces,
storing secrets, tailing logs. `pywrangler` is a thin Python wrapper around it,
from the `workers-py` dev dependency, that resolves the Python packages into
the Pyodide bundle first. Node is only ever a build tool here; none of it ships.

First time on a machine, authenticate once:

```sh
npx wrangler login
```

`pywrangler` is `wrangler` with the Python dependency handling wrapped around
it; `npx wrangler` on its own is fine for the subcommands that do not build
(`kv namespace create`, `secret put`, `tail`).

Secrets never live in `wrangler.jsonc` — that file is committed. Use
`npx wrangler secret put NAME`. The committed file carries only the vars that
are safe to read, and placeholder KV namespace ids that have to be replaced
with the real ones on first deploy.

## Conventions

- `compatibility_date` is **pinned**, not set to today. It selects the Pyodide
  build, so changing it is a runtime upgrade and belongs in its own commit
  with its own deploy.
- Rendering and formatting live in module-level functions that take no runtime
  objects, so they can be exercised locally with `workers` and `pyodide.ffi`
  stubbed. See the note at the bottom of `og-cards/README.md`.
- Anything reaching the API sends `x-ccdexplorer-key`, from a secret, using a
  key scoped for `api.ccdexplorer.io`.

## These are not part of `just test`

There is no CPython on this machine that can import `workers` or `js`, so
pytest cannot collect them and does not try. Treat a deploy plus a real
request as the test — both READMEs say which request to make.
