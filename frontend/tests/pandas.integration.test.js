import { describe, it, expect } from 'vitest'

// This is a lightweight integration test that hits the backend /api/packages
// and verifies that the frontend packages folder is being served.
// It will be skipped automatically if BACKEND_URL is not reachable.

const BASE = process.env.BACKEND_URL || 'http://127.0.0.1:8000'

async function ping(url) {
  try {
    const res = await fetch(url)
    return res.ok
  } catch {
    return false
  }
}

describe('integration: backend endpoints', () => {
  it('GET /api/packages returns list', async () => {
    const ok = await ping(`${BASE}/api/packages`)
    if (!ok) {
      console.warn('Skipping integration test; backend not reachable at', BASE)
      return
    }
    const res = await fetch(`${BASE}/api/packages`)
    expect(res.ok).toBe(true)
    const data = await res.json()
    expect(Array.isArray(data)).toBe(true)
    const names = data.map(x => x.name)
    expect(names).toContain('pandas')
  })

  it('GET /health returns kernel status', async () => {
    const ok = await ping(`${BASE}/health`)
    if (!ok) {
      console.warn('Skipping health integration; backend not reachable at', BASE)
      return
    }
    const res = await fetch(`${BASE}/health`)
    expect(res.ok).toBe(true)
    const data = await res.json()
    expect(typeof data).toBe('object')
    expect('kernel' in data).toBe(true)
  })
})
