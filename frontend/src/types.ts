export type DownloadState =
  | 'queued'
  | 'probing'
  | 'downloading'
  | 'paused'
  | 'completed'
  | 'failed'

export interface Download {
  id: string
  url: string
  filename: string
  state: DownloadState
  size: number
  downloaded: number
  progress: number
  speed_bps: number
  error: string
  segments: number
  created_at: number
}

export type ServerEvent =
  | { type: 'snapshot'; downloads: Download[] }
  | { type: 'job'; job: Download }
  | { type: 'removed'; id: string }

export async function api<T>(path: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(path, opts)
  if (!res.ok) {
    const body = await res.json().catch(() => ({}))
    throw new Error(body.detail || res.statusText)
  }
  return res.json() as Promise<T>
}

export function fmtSize(n: number | undefined): string {
  if (n === undefined || n === null) return '—'
  const units = ['B', 'KiB', 'MiB', 'GiB', 'TiB']
  let i = 0
  while (Math.abs(n) >= 1024 && i < units.length - 1) {
    n /= 1024
    i++
  }
  return i === 0 ? `${n} B` : `${n.toFixed(1)} ${units[i]}`
}

export function fmtSpeed(bps: number): string {
  if (!bps) return ''
  return `${fmtSize(bps)}/s`
}
