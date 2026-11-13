import { useEffect, useRef, useState } from 'react'
import type { LucideIcon } from 'lucide-react'
import {
  CheckCircle2,
  Clock,
  Dog,
  Download,
  Globe,
  Loader2,
  Pause,
  Play,
  Plus,
  RotateCcw,
  Trash2,
  XCircle,
} from 'lucide-react'
import { api, fmtSize, fmtSpeed } from './types'
import type { Download as DownloadJob, DownloadState, ServerEvent } from './types'

interface StateStyle {
  label: string
  cls: string
  icon: LucideIcon
}

const STATE_STYLES: Record<DownloadState, StateStyle> = {
  queued: { label: 'Queued', cls: 'text-amber-400 bg-amber-400/10', icon: Clock },
  probing: { label: 'Probing', cls: 'text-amber-400 bg-amber-400/10', icon: Loader2 },
  downloading: {
    label: 'Downloading',
    cls: 'text-cyan-400 bg-cyan-400/10',
    icon: Loader2,
  },
  paused: { label: 'Paused', cls: 'text-fuchsia-400 bg-fuchsia-400/10', icon: Pause },
  completed: {
    label: 'Completed',
    cls: 'text-emerald-400 bg-emerald-400/10',
    icon: CheckCircle2,
  },
  failed: { label: 'Failed', cls: 'text-red-400 bg-red-400/10', icon: XCircle },
}

function isBusy(state: DownloadState): boolean {
  return state === 'queued' || state === 'probing' || state === 'downloading'
}

export default function App() {
  const [downloads, setDownloads] = useState<DownloadJob[]>([])
  const [url, setUrl] = useState('')
  const [error, setError] = useState('')
  const [connected, setConnected] = useState(false)
  const esRef = useRef<EventSource | null>(null)

  useEffect(() => {
    let cancelled = false
    let timer: ReturnType<typeof setTimeout> | undefined
    const connect = () => {
      if (cancelled) return
      const es = new EventSource('/api/events')
      esRef.current = es
      es.onopen = () => setConnected(true)
      es.onmessage = (e) => {
        const event: ServerEvent = JSON.parse(e.data)
        if (event.type === 'snapshot') {
          setDownloads(event.downloads)
        } else if (event.type === 'job') {
          setDownloads((prev) => {
            const idx = prev.findIndex((d) => d.id === event.job.id)
            if (idx === -1) return [...prev, event.job]
            const next = [...prev]
            next[idx] = event.job
            return next
          })
        } else if (event.type === 'removed') {
          setDownloads((prev) => prev.filter((d) => d.id !== event.id))
        }
      }
      es.onerror = () => {
        setConnected(false)
        es.close()
        timer = setTimeout(connect, 2000)
      }
    }
    connect()
    return () => {
      cancelled = true
      if (timer) clearTimeout(timer)
      esRef.current?.close()
    }
  }, [])

  const addDownload = async (e: React.FormEvent) => {
    e.preventDefault()
    setError('')
    try {
      await api<DownloadJob>('/api/downloads', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ url }),
      })
      setUrl('')
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const act = async (id: string, action: string) => {
    try {
      await api(`/api/downloads/${id}/${action}`, { method: 'POST' })
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  const remove = async (id: string) => {
    try {
      await api(`/api/downloads/${id}`, { method: 'DELETE' })
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    }
  }

  return (
    <div className="min-h-screen bg-zinc-950 text-zinc-100">
      <div className="mx-auto max-w-3xl px-4 py-10">
        <header className="mb-8 flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="flex h-10 w-10 items-center justify-center rounded-xl bg-zinc-800">
              <Dog className="h-5 w-5 text-cyan-400" />
            </div>
            <div>
              <h1 className="text-xl font-semibold tracking-tight">foxhound</h1>
              <p className="text-sm text-zinc-500">segmented download manager</p>
            </div>
          </div>
          <div
            className={`flex items-center gap-2 rounded-full px-3 py-1 text-xs font-medium ${
              connected
                ? 'bg-emerald-400/10 text-emerald-400'
                : 'bg-red-400/10 text-red-400'
            }`}
          >
            <span
              className={`h-1.5 w-1.5 rounded-full ${
                connected ? 'bg-emerald-400' : 'bg-red-400'
              }`}
            />
            {connected ? 'live' : 'reconnecting'}
          </div>
        </header>

        <form onSubmit={addDownload} className="mb-8">
          <div className="flex gap-2">
            <div className="relative flex-1">
              <Globe className="pointer-events-none absolute top-1/2 left-3 h-4 w-4 -translate-y-1/2 text-zinc-500" />
              <input
                value={url}
                onChange={(e) => setUrl(e.target.value)}
                placeholder="https://example.com/file.zip"
                className="w-full rounded-xl border border-zinc-800 bg-zinc-900 py-2.5 pr-3 pl-9 text-sm outline-none placeholder:text-zinc-600 focus:border-cyan-500/50"
              />
            </div>
            <button
              type="submit"
              disabled={!url.trim()}
              className="flex items-center gap-2 rounded-xl bg-cyan-500 px-4 py-2.5 text-sm font-medium text-zinc-950 transition hover:bg-cyan-400 disabled:opacity-40"
            >
              <Plus className="h-4 w-4" />
              Add
            </button>
          </div>
          {error && <p className="mt-2 text-sm text-red-400">{error}</p>}
        </form>

        {downloads.length === 0 ? (
          <div className="flex flex-col items-center gap-3 rounded-2xl border border-dashed border-zinc-800 py-20 text-zinc-600">
            <Download className="h-8 w-8" />
            <p className="text-sm">No downloads yet — add a URL above.</p>
          </div>
        ) : (
          <ul className="space-y-3">
            {downloads.map((d) => {
              const style = STATE_STYLES[d.state]
              const Icon = style.icon
              return (
                <li
                  key={d.id}
                  className="rounded-2xl border border-zinc-800 bg-zinc-900/60 p-4"
                >
                  <div className="mb-2 flex items-start justify-between gap-3">
                    <div className="min-w-0">
                      <p className="truncate text-sm font-medium">
                        {d.filename || d.url}
                      </p>
                      <p className="mt-0.5 truncate text-xs text-zinc-500">
                        {d.url}
                      </p>
                    </div>
                    <span
                      className={`flex shrink-0 items-center gap-1.5 rounded-full px-2.5 py-1 text-xs font-medium ${style.cls}`}
                    >
                      <Icon
                        className={`h-3 w-3${d.state === 'downloading' ? ' animate-spin' : ''}`}
                      />
                      {style.label}
                    </span>
                  </div>

                  {d.state === 'failed' && d.error && (
                    <p className="mb-2 truncate text-xs text-red-400">{d.error}</p>
                  )}

                  <div className="mb-2 h-1.5 overflow-hidden rounded-full bg-zinc-800">
                    <div
                      className={`h-full rounded-full transition-all duration-300 ${
                        d.state === 'failed'
                          ? 'bg-red-400'
                          : d.state === 'completed'
                            ? 'bg-emerald-400'
                            : 'bg-cyan-400'
                      }`}
                      style={{ width: `${Math.max(2, d.progress || 0)}%` }}
                    />
                  </div>

                  <div className="flex items-center justify-between">
                    <p className="text-xs text-zinc-500">
                      {fmtSize(d.downloaded)} / {fmtSize(d.size)}
                      {d.speed_bps ? ` · ${fmtSpeed(d.speed_bps)}` : ''}
                      {d.segments ? ` · ${d.segments} connections` : ''}
                    </p>
                    <div className="flex items-center gap-1">
                      {isBusy(d.state) && (
                        <IconBtn title="Pause" onClick={() => act(d.id, 'pause')}>
                          <Pause className="h-4 w-4" />
                        </IconBtn>
                      )}
                      {(d.state === 'paused' || d.state === 'failed') && (
                        <IconBtn
                          title="Resume"
                          onClick={() =>
                            act(d.id, d.state === 'failed' ? 'retry' : 'resume')
                          }
                        >
                          {d.state === 'failed' ? (
                            <RotateCcw className="h-4 w-4" />
                          ) : (
                            <Play className="h-4 w-4" />
                          )}
                        </IconBtn>
                      )}
                      <IconBtn title="Remove" onClick={() => remove(d.id)}>
                        <Trash2 className="h-4 w-4" />
                      </IconBtn>
                    </div>
                  </div>
                </li>
              )
            })}
          </ul>
        )}
      </div>
    </div>
  )
}

function IconBtn({
  title,
  onClick,
  children,
}: {
  title: string
  onClick: () => void
  children: React.ReactNode
}) {
  return (
    <button
      title={title}
      onClick={onClick}
      className="rounded-lg p-2 text-zinc-400 transition hover:bg-zinc-800 hover:text-zinc-100"
    >
      {children}
    </button>
  )
}
