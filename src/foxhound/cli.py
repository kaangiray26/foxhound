"""Foxhound CLI — `fxd`."""

from __future__ import annotations

import asyncio

import typer

from .engine import DownloadManager, State, ensure_started

app = typer.Typer(
    name="fxd",
    help="Foxhound — an async segmented download manager.",
    no_args_is_help=True,
)


def _fmt_size(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return f"{int(n)} B" if unit == "B" else f"{n:,.1f} {unit}"
        n /= 1024
    return f"{n:,.1f} PiB"


def _colored(state: State) -> str:
    colors = {
        State.QUEUED: "yellow",
        State.PROBING: "yellow",
        State.DOWNLOADING: "cyan",
        State.PAUSED: "magenta",
        State.COMPLETED: "green",
        State.FAILED: "red",
    }
    return typer.style(state.value, fg=colors[state])


@app.command()
def add(
    urls: list[str] = typer.Argument(..., help="HTTP(S) URLs to download."),
    output: str = typer.Option(
        None, "--output", "-o", help="Destination directory or file path."
    ),
    jobs: int = typer.Option(
        8, "--jobs", "-j", min=1, max=32, help="Concurrent connections per download."
    ),
) -> None:
    """Add one or more downloads and track them live."""
    asyncio.run(_add(urls, output, jobs))


async def _add(urls: list[str], output: str | None, jobs: int) -> None:
    from rich.progress import (
        BarColumn,
        DownloadColumn,
        Progress,
        TaskID,
        TimeRemainingColumn,
        TransferSpeedColumn,
    )

    mgr = await ensure_started()
    mgr.connections = jobs
    for url in urls:
        await mgr.add(url, output)

    progress = Progress(
        "[bold]{task.fields[name]}",
        BarColumn(bar_width=None),
        "[progress.percentage]{task.percentage:>3.0f}%",
        DownloadColumn(),
        TransferSpeedColumn(),
        TimeRemainingColumn(),
    )
    task_ids: dict[str, TaskID] = {}
    with progress:
        try:
            while True:
                await asyncio.sleep(0.25)
                pending = 0
                for job in list(mgr.jobs.values()):
                    if job.state in (State.QUEUED, State.PROBING, State.DOWNLOADING):
                        pending += 1
                    tid = task_ids.get(job.id)
                    if tid is None and job.state != State.COMPLETED:
                        task_ids[job.id] = progress.add_task(
                            "download",
                            name=job.filename or job.url,
                            total=job.size or 1,
                            start=False,
                        )
                        tid = task_ids[job.id]
                    if tid is not None:
                        progress.update(
                            tid,
                            completed=job.downloaded or 0,
                            total=job.size or 1,
                            name=job.filename or job.url,
                        )
                if pending == 0 and task_ids:
                    break
        finally:
            for job in mgr.jobs.values():
                if job.state == State.FAILED and job.error:
                    typer.secho(f"[{job.id[:8]}] failed: {job.error}", fg="red")


@app.command("list")
def list_downloads() -> None:
    """List known downloads from the state directory."""
    import json

    from .engine import STATE_DIR

    found = False
    for f in sorted(STATE_DIR.glob("*.fxd")):
        found = True
        data = json.loads(f.read_text())
        typer.echo(
            f"{data['id'][:8]}  {_fmt_size(data.get('size', 0)):>12}  "
            f"{data.get('filename', '?')}  {data['url']}"
        )
    if not found:
        typer.echo("No downloads yet.")


@app.command()
def pause(job_id: str = typer.Argument(..., help="Download ID (or prefix).")) -> None:
    """Pause a running download."""
    asyncio.run(_simple("pause", job_id))


@app.command()
def resume(job_id: str = typer.Argument(..., help="Download ID (or prefix).")) -> None:
    """Resume a paused or failed download."""
    asyncio.run(_simple("resume", job_id))


@app.command()
def remove(job_id: str = typer.Argument(..., help="Download ID (or prefix).")) -> None:
    """Remove a download from the list."""
    asyncio.run(_simple("remove", job_id))


async def _simple(action: str, job_id: str) -> None:
    mgr = await ensure_started()
    full = _resolve_id(mgr, job_id)
    if full is None:
        typer.secho(f"No download matching '{job_id}'", fg="red")
        raise typer.Exit(1)
    fn = getattr(mgr, action)
    ok = await fn(full)
    typer.echo(f"{'OK' if ok else 'could not ' + action}: {full[:8]}")


def _resolve_id(mgr: DownloadManager, prefix: str) -> str | None:
    if prefix in mgr.jobs:
        return prefix
    matches = [j for j in mgr.jobs if j.startswith(prefix)]
    return matches[0] if len(matches) == 1 else None


@app.command()
def serve(
    host: str = typer.Option("127.0.0.1", help="Bind host."),
    port: int = typer.Option(8000, help="Bind port."),
) -> None:
    """Start the FastAPI server with the web UI."""
    import uvicorn

    uvicorn.run("foxhound.api:app", host=host, port=port, log_level="info")


def main() -> None:
    app()
