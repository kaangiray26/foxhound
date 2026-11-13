"""FastAPI server for Foxhound: REST API + SSE + mounted frontend."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from .engine import ensure_started, manager

# UI build (emitted by `frontend` vite build into this package) — ships with
# the wheel so `fxd serve` works from anywhere.
FRONTEND_DIR = Path(__file__).resolve().parent / "dist"


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncGenerator[None]:
    await ensure_started()
    yield
    await manager.aclose()


app = FastAPI(title="foxhound", version="0.1.0", lifespan=lifespan)
app.frontend("/", directory=str(FRONTEND_DIR), check_dir=False)


class AddRequest(BaseModel):
    url: str
    destination: str | None = None


@app.get("/api/downloads")
async def list_downloads() -> dict:
    mgr = await ensure_started()
    return {"downloads": [j.to_public() for j in mgr.jobs.values()]}


@app.post("/api/downloads", status_code=201)
async def add_download(req: AddRequest) -> dict:
    if not req.url.lower().startswith(("http://", "https://")):
        raise HTTPException(400, "Only http/https URLs are supported.")
    mgr = await ensure_started()
    job = await mgr.add(req.url, req.destination)
    return job.to_public()


@app.post("/api/downloads/{job_id}/pause")
async def pause_download(job_id: str) -> dict:
    mgr = await ensure_started()
    if not await mgr.pause(job_id):
        raise HTTPException(409, "Download cannot be paused.")
    return {"ok": True}


@app.post("/api/downloads/{job_id}/resume")
async def resume_download(job_id: str) -> dict:
    mgr = await ensure_started()
    if not await mgr.resume(job_id):
        raise HTTPException(409, "Download cannot be resumed.")
    return {"ok": True}


@app.delete("/api/downloads/{job_id}")
async def remove_download(job_id: str) -> dict:
    mgr = await ensure_started()
    if not await mgr.remove(job_id):
        raise HTTPException(404, "Download not found.")
    return {"ok": True}


@app.post("/api/downloads/{job_id}/retry")
async def retry_download(job_id: str) -> dict:
    mgr = await ensure_started()
    if not await mgr.retry(job_id):
        raise HTTPException(409, "Download cannot be retried.")
    return {"ok": True}


@app.get("/api/events")
async def events() -> StreamingResponse:
    mgr = await ensure_started()

    async def gen():
        queue = mgr.bus.subscribe()
        try:
            # initial snapshot so late subscribers get full state
            yield _sse(
                {
                    "type": "snapshot",
                    "downloads": [j.to_public() for j in mgr.jobs.values()],
                }
            )
            while True:
                try:
                    event = await asyncio.wait_for(queue.get(), timeout=15.0)
                except TimeoutError:
                    yield ": keep-alive\n\n"
                    continue
                yield _sse(event)
        finally:
            mgr.bus.unsubscribe(queue)

    return StreamingResponse(
        gen(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )


def _sse(event: dict) -> str:
    return f"data: {json.dumps(event)}\n\n"
