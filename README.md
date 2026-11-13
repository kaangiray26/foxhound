# foxhound 🦊

An async, segmented download manager with a CLI (`fxd`) and a local web UI.

## Features

- **Segmented downloads** — files are split into up to 16 parts fetched
  concurrently over pooled HTTP connections to maximize bandwidth.
- **Resume & repair** — every download keeps a `<file>.fxd` sidecar with exact
  byte offsets per segment. Interrupted or broken segment connections are
  retried with exponential backoff and resume from the last written byte, like
  IDM. Survives process restarts (`fxd resume`).
- **HTTP/2 + persistent connections** — powered by [httpx2] with `http2=True`,
  connection pooling and long keep-alives; HTTP/2 multiplexes segments over a
  single stream where the server supports it, and falls back to HTTP/1.1
  transparently.
- **Local web UI** — React + Tailwind + lucide, served by FastAPI with
  real-time updates over SSE.

## Install

```sh
uv sync
```

## CLI usage

```sh
# add one or more downloads and watch them live
uv run foxhound add https://example.com/big.iso -o ~/Downloads -j 16
uv run foxhound add URL1 URL2 URL3

# list known downloads (in-flight / paused)
uv run foxhound list

# control by ID (prefix match works)
uv run foxhound pause <id>
uv run foxhound resume <id>
uv run foxhound remove <id>

# launch the web UI (http://127.0.0.1:8000)
uv run foxhound serve
```

After installation (`uv pip install -e .`) the CLI is available as `foxhound`.

## UI usage

Run `uv run foxhound serve`, open <http://127.0.0.1:8000>, paste a URL, and
watch progress, speed and per-download connection count update live via SSE.
Pause, resume, retry and remove from the list.

## HTTP support

Only `http://` and `https://` are supported for now. Servers without
`Accept-Ranges` support are downloaded in a single stream (still resumable
in-process with retries).

## State

Sidecar files (`*.fxd`) live in `~/.foxhound/` and are removed automatically
when a download completes.

[httpx2]: https://github.com/encode/httpx2
