import { describe, it, expect } from 'vitest'
import WebSocket from 'ws'

const BASE = process.env.BACKEND_URL || 'http://127.0.0.1:8000'
const WS = BASE.replace('http', 'ws') + '/ws'

async function ping(url) {
  try {
    const res = await fetch(url)
    return res.ok
  } catch {
    return false
  }
}

function sleep(ms){ return new Promise(r => setTimeout(r, ms)) }

describe('E2E /run executes code and streams output over WS', () => {
  it('executes simple pandas flow and receives head()', async () => {
    const ok = await ping(`${BASE}/health`)
    if (!ok) {
      console.warn('Skipping /run e2e; backend not reachable at', BASE)
      return
    }
    // require kernel ok
    const hres = await fetch(`${BASE}/health`)
    const h = await hres.json().catch(() => ({}))
    if (!h || h.kernel !== 'ok') {
      console.warn('Skipping /run e2e; kernel not ok:', h)
      return
    }
    // optional auth support
    const headers = {}
    if (process.env.PYFLOWS_API_TOKEN) {
      headers['Authorization'] = `Bearer ${process.env.PYFLOWS_API_TOKEN}`
    }

    // Prepare code that prints a unique marker
    const marker = `[[E2E:${Date.now()}]]`
    const code = [
      'import pandas as pd',
      "df = pd.DataFrame({'city':['Tokyo','Osaka'],'temp':[30,31]})",
      'print(df.head().to_string())',
      `print('${marker}')`
    ].join('\n')

    // Connect WS first to capture stream
    const ws = new WebSocket(WS, { headers })
    const logs = []
    let done = false
    ws.on('message', (buf) => {
      try {
        const msg = JSON.parse(String(buf))
        if (msg.type === 'stream' && msg.content?.text) {
          logs.push(msg.content.text)
          if (msg.content.text.includes(marker)) {
            done = true
          }
        }
      } catch {}
    })
    await new Promise(resolve => ws.once('open', resolve))

    // POST /run
  const res = await fetch(`${BASE}/run`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', ...headers },
      body: JSON.stringify({ code })
    })
    expect(res.ok).toBe(true)

    // Wait for marker
    const deadline = Date.now() + 4000
    while (!done && Date.now() < deadline) {
      await sleep(50)
    }
    ws.close()
    expect(done).toBe(true)
    // optionally assert that DataFrame head appeared
    const joined = logs.join('')
    expect(joined).toContain('city')
    expect(joined).toContain('temp')
  })
})
