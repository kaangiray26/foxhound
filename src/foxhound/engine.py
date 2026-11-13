"""Foxhound download engine.

Async, segmented, resumable download manager built on httpx2 (HTTP/2,
persistent connections, connection pooling).
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import time
import uuid
from dataclasses import dataclass, field
from enum import StrEnum
from pathlib import Path
from urllib.parse import unquote, urlsplit

import httpx2 as httpx

# ---- tunables ---------------------------------------------------------------

MAX_SEGMENTS = 16  # split target for large files
MIN_SEGMENT_SIZE = 4 * 1024 * 1024  # don't over-segment small files
CHUNK = 256 * 1024  # read chunk size
MAX_RETRIES = 10  # per-segment retries (exponential backoff)
BACKOFF_BASE = 0.5
BACKOFF_CAP = 30.0
STATE_DIR = Path.home() / ".foxhound"
FLUSH_INTERVAL = 1.0  # seconds between sidecar flushes


class State(StrEnum):
    QUEUED = "queued"
    PROBING = "probing"
    DOWNLOADING = "downloading"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class Segment:
    start: int
    end: int
    done: int = 0  # bytes already written for this segment

    @property
    def total(self) -> int:
        return self.end - self.start + 1

    @property
    def remaining(self) -> int:
        return self.total - self.done


@dataclass
class Job:
    id: str
    url: str
    destination: Path
    state: State = State.QUEUED
    filename: str = ""
    size: int = 0
    downloaded: int = 0
    supports_ranges: bool = False
    error: str = ""
    speed_bps: float = 0.0
    segments: list[Segment] = field(default_factory=list)
    etag: str = ""
    last_modified: str = ""
    created_at: float = field(default_factory=time.time)

    @property
    def progress(self) -> float:
        if not self.size:
            return 0.0
        return min(100.0, self.downloaded / self.size * 100)

    def to_public(self) -> dict:
        return {
            "id": self.id,
            "url": self.url,
            "filename": self.filename,
            "state": self.state.value,
            "size": self.size,
            "downloaded": self.downloaded,
            "progress": round(self.progress, 2),
            "speed_bps": round(self.speed_bps, 1),
            "error": self.error,
            "segments": len(self.segments),
            "created_at": self.created_at,
        }


class EventBus:
    """Fan-out broadcast of job events to SSE / CLI subscribers."""

    def __init__(self) -> None:
        self._subscribers: set[asyncio.Queue] = set()

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=256)
        self._subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self._subscribers.discard(q)

    def publish(self, event: dict) -> None:
        for q in list(self._subscribers):
            try:
                q.put_nowait(event)
            except asyncio.QueueFull:
                pass  # drop for slow consumers


def _filename_from_response(url: str, headers: httpx.Headers) -> str:
    cd = headers.get("content-disposition", "")
    if "filename*=" in cd:
        raw = cd.split("filename*=", 1)[1].split(";")[0].strip().strip("'\"")
        _, _, encoded = raw.partition("''")
        name = unquote(encoded or raw)
        if name:
            return name
    if "filename=" in cd:
        name = cd.split("filename=", 1)[1].split(";")[0].strip().strip("'\"")
        if name:
            return name
    path = unquote(urlsplit(url).path)
    return os.path.basename(path) or "download.bin"


class DownloadManager:
    def __init__(self, connections_per_job: int = 8, max_concurrent_jobs: int = 4):
        self.connections = connections_per_job
        self.max_concurrent = max_concurrent_jobs
        self.jobs: dict[str, Job] = {}
        self.bus = EventBus()
        self.client: httpx.AsyncClient | None = None
        self._tasks: dict[str, asyncio.Task] = {}
        self._slots = asyncio.Semaphore(max_concurrent_jobs)
        self._speed_checkpoint: dict[str, tuple[int, float]] = {}
        self._start_lock = asyncio.Lock()
        STATE_DIR.mkdir(parents=True, exist_ok=True)

    # -- lifecycle ------------------------------------------------------------

    async def start(self) -> None:
        if self.client is None:
            self.client = httpx.AsyncClient(
                http2=True,
                follow_redirects=True,
                limits=httpx.Limits(
                    max_connections=200,
                    max_keepalive_connections=64,
                    keepalive_expiry=120.0,
                ),
                timeout=httpx.Timeout(connect=15.0, read=30.0, write=30.0, pool=30.0),
                headers={
                    "user-agent": "foxhound/0.1",
                    "accept-encoding": "identity",  # we need exact byte ranges
                },
            )
        self.restore_jobs()

    async def aclose(self) -> None:
        for task in self._tasks.values():
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
        self._tasks.clear()
        if self.client:
            await self.client.aclose()
            self.client = None

    # -- persistence ----------------------------------------------------------

    def _sidecar(self, job: Job) -> Path:
        # Sidecars live in the state dir so restarts can find them; never
        # clutter the user's download directory.
        return STATE_DIR / f"{job.id}.fxd"

    def save_state(self, job: Job) -> None:
        if not job.url or not job.id:
            return
        data = {
            "id": job.id,
            "url": job.url,
            "destination": str(job.destination),
            "filename": job.filename,
            "size": job.size,
            "supports_ranges": job.supports_ranges,
            "etag": job.etag,
            "last_modified": job.last_modified,
            "segments": [
                {"start": s.start, "end": s.end, "done": s.done} for s in job.segments
            ],
        }
        self._sidecar(job).write_text(json.dumps(data))

    def restore_jobs(self) -> None:
        for f in sorted(STATE_DIR.glob("*.fxd")):
            try:
                data = json.loads(f.read_text())
            except json.JSONDecodeError, OSError:
                continue
            job = self._job_from_sidecar(data)
            if job is None:
                continue
            self.jobs[job.id] = job
            if job.state in (State.QUEUED, State.DOWNLOADING, State.PROBING):
                job.state = State.PAUSED
            elif job.state == State.COMPLETED:
                # done and restored: sidecar served its purpose
                f.unlink(missing_ok=True)

    def _job_from_sidecar(self, data: dict) -> Job | None:
        try:
            dest = Path(data["destination"])
        except KeyError, TypeError:
            return None
        job = Job(
            id=data.get("id", str(uuid.uuid4())),
            url=data.get("url", ""),
            destination=dest,
            filename=data.get("filename", dest.name),
            size=data.get("size", 0),
            supports_ranges=data.get("supports_ranges", False),
            etag=data.get("etag", ""),
            last_modified=data.get("last_modified", ""),
        )
        segs = data.get("segments") or []
        job.segments = [Segment(s["start"], s["end"], s.get("done", 0)) for s in segs]
        total_done = sum(s.done for s in job.segments)
        job.downloaded = total_done
        if job.size and total_done >= job.size and dest.exists():
            job.state = State.COMPLETED
        elif dest.exists() or segs:
            job.state = State.PAUSED
        return job

    # -- public API -----------------------------------------------------------

    async def add(self, url: str, destination: str | None = None) -> Job:
        dest = Path(destination or ".")
        if dest.exists() and dest.is_dir():
            # directory: filename comes from the probe response
            job = Job(id=str(uuid.uuid4()), url=url, destination=dest)
        else:
            # explicit target path (may not exist yet); use it as-is. If the
            # probe later finds no name we still have a sane target.
            name = dest.name if dest.suffix else ""
            if not name:
                dest.mkdir(parents=True, exist_ok=True)
            job = Job(id=str(uuid.uuid4()), url=url, destination=dest, filename=name)
        self.jobs[job.id] = job
        self._emit(job)
        task = asyncio.create_task(self._run(job))
        self._tasks[job.id] = task
        return job

    async def pause(self, job_id: str) -> bool:
        job = self.jobs.get(job_id)
        if not job or job.state not in (State.QUEUED, State.PROBING, State.DOWNLOADING):
            return False
        task = self._tasks.pop(job_id, None)
        if task:
            task.cancel()
            if task.done() and not task.cancelled():
                # raced with completion; don't clobber the final state
                return False
        job.state = State.PAUSED
        self.save_state(job)
        self._emit(job)
        return True

    async def resume(self, job_id: str) -> bool:
        job = self.jobs.get(job_id)
        if not job or job.state not in (State.PAUSED, State.FAILED):
            return False
        job.error = ""
        self._emit(job)
        task = asyncio.create_task(self._run(job))
        self._tasks[job.id] = task
        return True

    async def remove(self, job_id: str) -> bool:
        job = self.jobs.get(job_id)
        if not job:
            return False
        task = self._tasks.pop(job_id, None)
        if task:
            task.cancel()
        self.jobs.pop(job_id, None)
        self._sidecar(job).unlink(missing_ok=True)
        self.bus.publish({"type": "removed", "id": job_id})
        return True

    async def retry(self, job_id: str) -> bool:
        job = self.jobs.get(job_id)
        if not job or job.state != State.FAILED:
            return False
        # keep partial data so we resume where we left off
        return await self.resume(job_id)

    # -- internals ------------------------------------------------------------

    def _emit(self, job: Job) -> None:
        self.bus.publish({"type": "job", "job": job.to_public()})

    async def _run(self, job: Job) -> None:
        assert self.client is not None
        async with self._slots:
            try:
                job.state = State.PROBING
                self._emit(job)
                await self._probe(job)

                if not job.segments:
                    self._plan_segments(job)

                job.state = State.DOWNLOADING
                self._emit(job)
                self.save_state(job)

                if job.supports_ranges:
                    await self._download_segmented(job)
                else:
                    await self._download_single(job)

                job.state = State.COMPLETED
                job.speed_bps = 0.0
                self._speed_checkpoint.pop(job.id, None)
                self._sidecar(job).unlink(missing_ok=True)
                self._emit(job)
            except asyncio.CancelledError:
                job.state = State.PAUSED
                self.save_state(job)
                self._emit(job)
                raise
            except Exception as exc:  # noqa: BLE001
                job.state = State.FAILED
                job.error = f"{type(exc).__name__}: {exc}"
                job.speed_bps = 0.0
                self._speed_checkpoint.pop(job.id, None)
                self.save_state(job)
                self._emit(job)

    async def _probe(self, job: Job) -> None:
        client = self.client
        assert client is not None
        # If we have sidecar state, validate it still matches the server.
        if job.segments and job.size:
            try:
                resp = await client.head(job.url)
                if (
                    resp.status_code == 200
                    and job.etag
                    and resp.headers.get("etag", job.etag) == job.etag
                ):
                    return  # metadata still valid, resume
                if (
                    resp.status_code == 200
                    and job.size
                    and resp.headers.get("content-length") == str(job.size)
                ):
                    return
            except httpx.HTTPError:
                return  # offline; resume anyway when connection returns
            job.segments = []
            job.downloaded = 0

        resp = await client.head(job.url)
        if resp.status_code in (405, 501):
            # HEAD not allowed; use a streamed GET and inspect headers
            async with client.stream("GET", job.url) as r:
                self._apply_probe(job, r.headers, r.status_code)
            return
        resp.raise_for_status()
        self._apply_probe(job, resp.headers, resp.status_code)

        # Some servers omit content-length/accept-ranges on HEAD. Refine with a
        # 1-byte ranged GET probe.
        if not job.size or not job.supports_ranges:
            try:
                async with client.stream(
                    "GET", job.url, headers={"range": "bytes=0-0"}
                ) as r:
                    if r.status_code == 206:
                        content_range = r.headers.get("content-range", "")
                        if "/" in content_range:
                            total = content_range.rsplit("/", 1)[1]
                            if total.isdigit():
                                job.size = int(total)
                                job.supports_ranges = True
                    elif r.status_code == 200 and not job.size:
                        cl = r.headers.get("content-length")
                        if cl and cl.isdigit():
                            job.size = int(cl)
            except httpx.HTTPError:
                pass

    def _apply_probe(self, job: Job, headers: httpx.Headers, status: int) -> None:
        if status >= 400:
            raise httpx.HTTPStatusError(
                f"probe failed with status {status}",
                request=httpx.Request("HEAD", job.url),
                response=httpx.Response(status),
            )
        cl = headers.get("content-length")
        job.size = int(cl) if cl and cl.isdigit() else 0
        job.supports_ranges = "bytes" in headers.get("accept-ranges", "")
        job.etag = headers.get("etag", "")
        job.last_modified = headers.get("last-modified", "")
        if not job.filename:
            job.filename = _filename_from_response(job.url, headers)
            target = job.destination / job.filename
            # destination may point at an explicit file path
            if not job.destination.is_dir() and job.destination.suffix:
                target = job.destination
            job.destination = target

    def _plan_segments(self, job: Job) -> None:
        if not job.supports_ranges or not job.size:
            job.segments = []
            return
        # never split finer than MIN_SEGMENT_SIZE, and never more pieces than
        # available connections
        n = min(MAX_SEGMENTS, self.connections, max(1, job.size // MIN_SEGMENT_SIZE))
        chunk = job.size // n
        remaining = job.size % n
        pos = 0
        segs: list[Segment] = []
        for i in range(n):
            size = chunk + (1 if i < remaining else 0)
            if size <= 0:
                continue
            segs.append(Segment(pos, pos + size - 1))
            pos += size
        job.segments = segs
        job.downloaded = 0

    async def _update_speed(self, job: Job) -> float:
        now = time.monotonic()
        prev_bytes, prev_ts = self._speed_checkpoint.get(job.id, (job.downloaded, now))
        dt = now - prev_ts
        speed = (job.downloaded - prev_bytes) / dt if dt > 0 else job.speed_bps
        job.speed_bps = max(0.0, speed)
        self._speed_checkpoint[job.id] = (job.downloaded, now)
        return job.speed_bps

    async def _download_segmented(self, job: Job) -> None:
        job.destination.parent.mkdir(parents=True, exist_ok=True)
        if not job.destination.exists():
            with open(job.destination, "wb") as f:  # noqa: ASYNC230 - small alloc, sync fine
                f.truncate(job.size)

        worker = self._segment_worker
        tasks = [asyncio.create_task(worker(job, s)) for s in job.segments]
        flusher = asyncio.create_task(self._flush_loop(job))
        try:
            await asyncio.gather(*tasks)
        finally:
            flusher.cancel()
            await asyncio.gather(flusher, return_exceptions=True)
        self.save_state(job)

    async def _segment_worker(self, job: Job, seg: Segment) -> None:
        attempts = 0
        while seg.remaining > 0:
            try:
                await self._fetch_segment(job, seg)
                attempts = 0  # made progress, reset backoff
            except asyncio.CancelledError:
                raise
            except httpx.HTTPError, OSError:
                attempts += 1
                if attempts > MAX_RETRIES:
                    raise RuntimeError(
                        f"segment {seg.start}-{seg.end}: exhausted {MAX_RETRIES} retries"
                    )
                wait = min(BACKOFF_CAP, BACKOFF_BASE * (2**attempts))
                wait *= 0.5 + random.random()
                await asyncio.sleep(wait)

    async def _fetch_segment(self, job: Job, seg: Segment) -> None:
        client = self.client
        assert client is not None
        pos = seg.start + seg.done
        headers = {"range": f"bytes={pos}-{seg.end}"}
        async with client.stream("GET", job.url, headers=headers) as resp:
            if resp.status_code == 206:
                content_range = resp.headers.get("content-range", "")
                if content_range and f"bytes {pos}-" not in content_range:
                    # server returned the wrong range: rewind and refetch
                    job.downloaded -= seg.done
                    seg.done = 0
                    pos = seg.start
            elif resp.status_code == 200:
                # server ignored the Range header: restart segment from zero
                job.downloaded -= seg.done
                seg.done = 0
                pos = seg.start
            with open(job.destination, "r+b") as f:  # noqa: ASYNC230
                f.seek(pos)
                async for chunk in resp.aiter_bytes(CHUNK):
                    f.write(chunk)
                    got = len(chunk)
                    seg.done += got
                    job.downloaded += got
                    if seg.start + seg.done > seg.end + 1:
                        overflow = seg.start + seg.done - (seg.end + 1)
                        seg.done -= overflow
                        job.downloaded -= overflow
                        break

    async def _download_single(self, job: Job) -> None:
        client = self.client
        assert client is not None
        # Small or range-hostile target: single streamed GET, restart on error.
        for attempt in range(MAX_RETRIES):
            try:
                mode = "wb"
                pos = 0
                if job.destination.exists() and job.downloaded:
                    mode = "ab"
                    pos = job.downloaded
                async with client.stream("GET", job.url) as resp:
                    resp.raise_for_status()
                    with open(job.destination, mode) as f:  # noqa: ASYNC230
                        f.seek(pos)
                        async for chunk in resp.aiter_bytes(CHUNK):
                            f.write(chunk)
                            job.downloaded += len(chunk)
                            if job.size:
                                job.downloaded = min(job.downloaded, job.size)
                if not job.size or job.downloaded >= job.size:
                    if not job.size:
                        job.size = job.downloaded
                    return
            except asyncio.CancelledError:
                raise
            except httpx.HTTPError, OSError:
                await asyncio.sleep(
                    min(BACKOFF_CAP, BACKOFF_BASE * (2**attempt))
                    * (0.5 + random.random())
                )
        raise RuntimeError("exhausted retries on single-stream download")

    async def _flush_loop(self, job: Job) -> None:
        try:
            while True:
                await asyncio.sleep(FLUSH_INTERVAL)
                self.save_state(job)
                await self._update_speed(job)
                self._emit(job)
        except asyncio.CancelledError:
            self.save_state(job)
            raise


# shared singleton for CLI / server -------------------------------------------

manager = DownloadManager()


async def ensure_started() -> DownloadManager:
    if manager.client is None:
        async with manager._start_lock:
            if manager.client is None:
                await manager.start()
    return manager
