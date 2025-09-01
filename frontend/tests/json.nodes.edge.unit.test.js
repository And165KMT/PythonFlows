import { describe, it, expect, beforeAll } from 'vitest'

// Minimal registry mock that collects nodes
function makeRegistry() {
  const nodes = []
  return { node(def){ nodes.push(def) }, get(){ return nodes } }
}

// Minimal ctx mock for code generation
const ctx = {
  srcVar(node){ return node?.inputVar || 'df' },
  setLastPlotNode(){}
}

function toStr(code){ return Array.isArray(code)? code.join('\n') : String(code||'') }

let py = null
beforeAll(async () => {
  py = await import('../packages/python/index.js')
})

describe('python JSON nodes edge cases', () => {
  it('JsonStringify uses ensure_ascii=False and list assignment (no pd.Series dependency)', () => {
    const reg = makeRegistry(); py.register(reg)
    const list = reg.get()
    const n = list.find(x => x.id === 'python.JsonStringify')
    expect(n).toBeTruthy()
    const node = { id:'N1', params:{ column:'', out:'json', orient:'records', indent:'2' }, inputVar:'df' }
    const s = toStr(n.code(node, ctx))
    expect(s).toContain('ensure_ascii=False')
    expect(s).toContain("[_val] * (len(")
    expect(s).not.toContain('pd.Series(')
  })

  it('JsonParse loads from str or bytes and handles list/object branches', () => {
    const reg = makeRegistry(); py.register(reg)
    const list = reg.get()
    const n = list.find(x => x.id === 'python.JsonParse')
    const node = { id:'N2', params:{ mode:'auto' }, inputVar:'v' }
    const s = toStr(n.code(node, ctx))
    expect(s).toContain('isinstance(_val, (str, bytes))')
    expect(s).toContain('if isinstance(_obj, list):')
    expect(s).toContain("_pd.DataFrame(")
    expect(s).toContain("[{'value': _x} for _x in (")
    expect(s).toContain('elif isinstance(_obj, dict):')
  })

  it('ParseDate includes python fallback fromisoformat and fromtimestamp', () => {
    const reg = makeRegistry(); py.register(reg)
    const list = reg.get()
    const n = list.find(x => x.id === 'python.ParseDate')
    const node = { id:'N3', params:{ column:'created_at', errors:'coerce', utc:'false', applyAll:'false' }, inputVar:'df' }
    const s = toStr(n.code(node, ctx))
    expect(s).toContain('from datetime import datetime as _dt')
    expect(s).toContain('fromisoformat')
    expect(s).toContain('fromtimestamp')
  })
})
