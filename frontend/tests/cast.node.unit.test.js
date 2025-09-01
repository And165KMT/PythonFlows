import { describe, it, expect, beforeAll } from 'vitest'

function makeRegistry(){ const nodes=[]; return { node(def){ nodes.push(def) }, get(){ return nodes } } }
const ctx = { srcVar(n){ return n?.inputVar || 'x' } }
function toStr(code){ return Array.isArray(code)? code.join('\n') : String(code||'') }

let py = null
beforeAll(async () => { py = await import('../packages/python/index.js') })

describe('python.Cast non-DataFrame branch', () => {
  it('emits simple python casting without relying on pandas', () => {
    const reg = makeRegistry(); py.register(reg)
    const list = reg.get()
    const n = list.find(x => x.id === 'python.Cast')
    const node = { id:'C1', params:{ target:'int', column:'', applyAll:'false' }, inputVar:'value' }
    const s = toStr(n.code(node, ctx))
    expect(s).toContain("if _tgt == 'int':")
  expect(s).toContain("elif _tgt == 'float':")
  expect(s).toContain("elif _tgt == 'str':")
  expect(s).toContain("elif _tgt == 'bool':")
    // pandas is optional; just ensure header _is_df check is guarded
    expect(s).toContain('try:\n  _is_df = isinstance(')
  })
})
