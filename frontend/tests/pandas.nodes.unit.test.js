import { describe, it, expect } from 'vitest'

// Minimal registry mock that collects nodes
function makeRegistry() {
  const nodes = []
  return {
    node(def) { nodes.push(def) },
    get() { return nodes }
  }
}

// Minimal ctx mock for code generation
const ctx = {
  srcVar(node) { return node?.inputVar || 'df' },
  setLastPlotNode() {}
}

// helper to turn code output (array or string) into a string
function toStr(code) {
  if (Array.isArray(code)) return code.join('\n')
  return String(code || '')
}

// Load pandas package module
import * as pandas from '../packages/pandas/index.js'

describe('pandas package nodes - unit', () => {
  it('registers nodes', () => {
    const reg = makeRegistry()
    pandas.register(reg)
    const list = reg.get()
    expect(Array.isArray(list)).toBe(true)
    expect(list.length).toBeGreaterThan(5)
    // spot check a couple
    const rcsv = list.find(n => n.id === 'pandas.ReadCSV')
    expect(rcsv).toBeTruthy()
    expect(rcsv.inputType).toBe('None')
    expect(rcsv.outputType).toBe('DataFrame')
  })

  it('ReadCSV inline generates pd.read_csv code', () => {
    const reg = makeRegistry()
    pandas.register(reg)
    const list = reg.get()
    const rcsv = list.find(n => n.id === 'pandas.ReadCSV')
    const node = { id: 'A', params: { mode: 'inline', inline: 'a,b\n1,2\n' } }
    const code = rcsv.code(node, ctx)
    const s = toStr(code)
    expect(s).toContain('pd.read_csv')
    expect(s).toContain("io.StringIO(")
  })

  it('SelectColumns emits selection slice', () => {
    const reg = makeRegistry()
    pandas.register(reg)
    const list = reg.get()
    const sel = list.find(n => n.id === 'pandas.SelectColumns')
    const node = { id: 'B', params: { columns: 'x,y' }, inputVar: 'df0' }
    const code = sel.code(node, ctx)
    const s = toStr(code)
    expect(s).toContain("df0[[" )
  })
})
