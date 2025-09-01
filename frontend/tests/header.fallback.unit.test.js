import { describe, it, expect } from 'vitest'
import { readFile } from 'node:fs/promises'
import { fileURLToPath } from 'node:url'
import path from 'node:path'

describe('codegen header fallbacks', () => {
  it('contains preview fallbacks for list-of-dicts, dict, and list/tuple/set', async () => {
    const __filename = fileURLToPath(import.meta.url)
    const __dirname = path.dirname(__filename)
    const nodesPath = path.resolve(__dirname, '../app/nodes.js')
    const src = await readFile(nodesPath, 'utf-8')
    expect(src).toContain('def _fp_preview(x, nid):')
    // list-of-dicts table branch
    expect(src).toContain('if isinstance(x, list) and (len(x)==0 or isinstance(x[0], dict))')
    // dict branch
    expect(src).toContain('if isinstance(x, dict):')
    // list/tuple/set branch
    expect(src).toContain('if isinstance(x, (list, tuple, set)):')
  })

  it('contains _fp_as_scalar extraction heuristics (dict/list/text)', async () => {
    const __filename = fileURLToPath(import.meta.url)
    const __dirname = path.dirname(__filename)
    const nodesPath = path.resolve(__dirname, '../app/nodes.js')
    const src = await readFile(nodesPath, 'utf-8')
    expect(src).toContain('def _fp_as_scalar(x):')
    expect(src).toContain("if 'text' in x and isinstance(x['text'], (str, bytes))")
    expect(src).toContain('if isinstance(x, (list, tuple)) and len(x)==1:')
  })
})
