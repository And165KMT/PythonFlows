import { describe, it, expect } from 'vitest'

const BASE = process.env.BACKEND_URL || 'http://127.0.0.1:8000'
const WS = BASE.replace('http', 'ws') + '/ws'

async function ping(url) {
  try { const res = await fetch(url); return res.ok } catch (e) { return false }
}

function sleep(ms){ return new Promise(r => setTimeout(r, ms)) }

const runE2E = !!process.env.BACKEND_URL

describe('E2E JsonParse/JsonStringify execution', () => {
  (runE2E ? it : it.skip)('parses large array and mixed timestamps, then stringify with ensure_ascii=False', async () => {
    const ok = await ping(`${BASE}/health`)
    if(!ok) { console.warn('Skipping json e2e; backend not reachable'); return }
    const hres = await fetch(`${BASE}/health`)
    const h = await hres.json().catch(()=>({}))
    if(!h || h.kernel !== 'ok') { console.warn('Skipping json e2e; kernel not ok'); return }

    const headers = {}
    if (process.env.PYFLOWS_API_TOKEN) headers['Authorization'] = `Bearer ${process.env.PYFLOWS_API_TOKEN}`

    const bigArr = '[' + Array.from({length:2000}, (_,i)=>`{"i":${i},"t":${Date.now()/1000 + i}}`).join(',') + ']'
    const nonUtf8 = '\\xFF' // deliberately odd byte escape; should not crash
    const marker = `[[JSONE2E:${Date.now()}]]`

    const code = [
      'import json as _json',
      // JsonParse equivalent core: load large list and object
      `v = ${JSON.stringify(bigArr)}`,
      '_obj = _json.loads(v)',
      'df = None',
      'try:\n  import pandas as pd\n  df = pd.DataFrame(_obj)\nexcept Exception:\n  df = _obj',
      // ParseDate-like fallback on timestamps when pandas present
      'try:\n  from datetime import datetime as _dt\n  if hasattr(df, "__getitem__") and hasattr(df, "apply") and "t" in getattr(df, "columns", []):\n    try:\n      import pandas as _pd\n      df["t"] = _pd.to_datetime(df["t"], unit="s", errors="coerce")\n    except Exception:\n      try:\n        df["t"] = df["t"].apply(lambda _x: _dt.fromtimestamp(float(_x)))\n      except Exception: pass\nexcept Exception: pass',
      // JsonStringify equivalent: ensure_ascii False
      '_rows = df.to_dict(orient="records") if hasattr(df, "to_dict") else df',
      '_s = _json.dumps(_rows, ensure_ascii=False)',
      'print(len(_rows))',
      'print(_s[:120])',
      `print(${JSON.stringify(nonUtf8)})`,
      `print(${JSON.stringify(marker)})`
    ].join('\n')

    const ws = new (await import('ws')).default(WS, { headers })
    const logs = []
    let found = false
    ws.on('message', (buf) => {
  try { const msg = JSON.parse(String(buf)); if(msg.type==='stream' && msg.content?.text){ const t=msg.content.text; logs.push(t); if(t.includes(marker)) found = true } } catch (e) {}
    })
    await new Promise(resolve => ws.once('open', resolve))

    const res = await fetch(`${BASE}/run`, { method:'POST', headers:{ 'Content-Type':'application/json', ...headers }, body: JSON.stringify({ code }) })
    expect(res.ok).toBe(true)

    const deadline = Date.now() + 6000
    while(!found && Date.now() < deadline) await sleep(50)
    ws.close()
    const out = logs.join('')
    expect(found).toBe(true)
    expect(out).toMatch(/\b2000\b/) // row count printed
    expect(out).toContain('"i":0')
  })
})
