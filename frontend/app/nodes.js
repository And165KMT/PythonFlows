// Node graph, registry, and code generation

export const state = {
  nodes: [],
  edges: [],
  nextId: 1,
  pendingSrc: null,
  lastPlotNodeId: null,
  activePkg: null,
  selectedNodeId: null,
  preview: { head: new Map(), desc: new Map(), headHtml: new Map(), descHtml: new Map() },
  stream: { currentNodeId: null, buffers: new Map() }
};

export const registry = { packages: [], nodes: new Map(), byPackage: new Map() };

export function uid(){ return 'n' + (state.nextId++); }
export function getNode(id){ return state.nodes.find(n => n.id === id); }

export function upstreamOf(node){ const e = state.edges.find(x=>x.to===node.id); if(!e) return null; return getNode(e.from); }

export function computeUpstreamColumns(n){
  const seen = new Set();
  function uniq(arr){ const s=new Set(); const out=[]; for(const x of arr){ if(!s.has(x)){ s.add(x); out.push(x); } } return out; }
  function walk(cur){
    if(!cur || seen.has(cur.id)) return [];
    seen.add(cur.id);
    const up = upstreamOf(cur);
    if(cur.type==='pandas.ReadCSV'){
      if(cur.params?.mode==='inline' && cur.params?.inline){
        const first = String(cur.params.inline).split(/\r?\n/)[0]||'';
        return first.split(',').map(s=>s.trim()).filter(Boolean);
      }
      return [];
    }
    if(cur.type==='pandas.SelectColumns'){
      return String(cur.params?.columns||'').split(',').map(s=>s.trim()).filter(Boolean);
    }
    if(cur.type==='pandas.GroupByAggregate'){
      const by = String(cur.params?.by||'group'); const val = String(cur.params?.value||'value');
      return uniq([by, val]);
    }
    if(cur.type==='pandas.FilterRows' || cur.type==='pandas.DropNA' || cur.type==='pandas.FillNA' || cur.type==='pandas.HeadTail' || cur.type==='pandas.SortValues'){
      return walk(up);
    }
    if(cur.type==='pandas.AddColumn'){
      const cols = walk(up);
      const newcol = String(cur.params?.newcol||'new').trim();
      return uniq(newcol ? [...cols, newcol] : cols);
    }
    if(cur.type==='pandas.RenameColumns'){
      const cols = walk(up);
      const mappingStr = String(cur.params?.mapping||'');
      const map = new Map();
      mappingStr.split(/\r?\n|,/).map(s=>s.trim()).filter(Boolean).forEach(s=>{
        const [oldN, newN] = s.split(':');
        if(oldN){ map.set(oldN.trim(), (newN||'').trim()); }
      });
      return cols.map(c=> map.has(c) && map.get(c) ? map.get(c) : c);
    }
    if(cur.type==='pandas.ValueCounts'){
      const col = String(cur.params?.column||'').trim();
      return col? [col, 'count'] : walk(up);
    }
    if(cur.type==='pandas.Melt'){
      const idv = String(cur.params?.id_vars||'').split(',').map(s=>s.trim()).filter(Boolean);
      const varName = (cur.params?.var_name||'variable').trim();
      const valueName = (cur.params?.value_name||'value').trim();
      return uniq([...idv, varName, valueName]);
    }
    if(cur.type==='pandas.PivotTable'){
      const cols = []; const count = parseInt(cur.params?.cols||'2')||2; const prefix = String(cur.params?.prefix||'x'); for(let i=1;i<=count;i++) cols.push(prefix + i); return cols;
    }
    if(cur.type==='sklearn.TrainTestSplit'){
      const cols = walk(up); return uniq([...cols, 'split']);
    }
    if(cur.type==='sklearn.StandardScaler'){
      const cols = walk(up);
      const inplace = String(cur.params?.inplace) !== 'false';
      if(inplace) return cols;
      const suffix = String(cur.params?.suffix||'_scaled');
      const suff = cols.map(c=> c + suffix);
      return uniq([...cols, ...suff]);
    }
    if(cur.type && cur.type.startsWith('python.')){
      return walk(up);
    }
    return walk(up);
  }
  const up = upstreamOf(n); return walk(up);
}

export function allNodeTypes(){ const out=[]; registry.byPackage.forEach((types)=>{ types.forEach(t=> out.push(t)); }); return out; }
export function nodeLabelOf(type){ const def = registry.nodes.get(type); return def?.title || type.split('.').slice(-1)[0] || type; }

export function suggestionsForNode(fromId){
  const n = getNode(fromId); const t = n?.type || '';
  const has = (id)=> registry.nodes.has(id);
  const acc = [];
  if(t.startsWith('pandas.')){
    ['pandas.SelectColumns','pandas.FilterRows','pandas.SortValues','pandas.GroupByAggregate','pandas.ValueCounts','pandas.PivotTable','pandas.Melt','pandas.AddColumn','pandas.DropNA','pandas.FillNA','pandas.RenameColumns','pandas.HeadTail','pandas.Plot','pandas.CorrHeatmap','python.Exec','python.If','python.For','python.While','python.FileWriteCSV','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
  } else if(t==='numpy.RandomNormal'){
    ['pandas.Plot','sklearn.StandardScaler','sklearn.KMeans','sklearn.ClusterPlot','python.Exec','python.For','python.While','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
  } else if(t.startsWith('sklearn.')){
    ['sklearn.KMeans','sklearn.ClusterPlot','pandas.Plot','sklearn.StandardScaler','sklearn.TrainTestSplit','python.Exec','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
  } else {
    ['pandas.Plot','pandas.FilterRows','pandas.SelectColumns','python.Exec','python.FileReadText','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
  }
  const seen=new Set(); const out=[]; for(const x of acc){ if(!seen.has(x)){ seen.add(x); out.push(x); if(out.length>=8) break; } }
  return out;
}

export function addNode(type, x=80, y=80){
  const def = registry.nodes.get(type);
  const n = { id: uid(), type, x, y, params: JSON.parse(JSON.stringify(def?.defaultParams||{})) };
  state.nodes.push(n);
  return n;
}

export function selectNode(id){ state.selectedNodeId = id; }
export function clearSelection(){ state.selectedNodeId = null; }
export function deleteNodeById(id){
  state.nodes = state.nodes.filter(n=> n.id !== id);
  state.edges = state.edges.filter(e=> e.from !== id && e.to !== id);
  if(state.lastPlotNodeId === id) state.lastPlotNodeId = null;
  if(state.selectedNodeId === id) state.selectedNodeId = null;
}

export function topoSort(){
  const indeg = Object.fromEntries(state.nodes.map(n=>[n.id,0]));
  state.edges.forEach(e=> indeg[e.to]++ );
  const q = state.nodes.filter(n=> indeg[n.id]===0).map(n=>n.id);
  const out = [];
  const adj = {};
  state.edges.forEach(e=>{ (adj[e.from] ||= []).push(e.to); });
  while(q.length){ const u = q.shift(); out.push(u); (adj[u]||[]).forEach(v=>{ if(--indeg[v]===0) q.push(v); }); }
  return out.map(id=> getNode(id));
}

let previewModeProvider = ()=> 'plots';
export function setPreviewModeProvider(fn){ if(typeof fn==='function') previewModeProvider = fn; }

export function genCode(){
  const pmode = previewModeProvider();
  const order = topoSort();
  const header = [ 'import pandas as pd', 'import matplotlib.pyplot as plt', 'import io', 'import glob', 'plt.close("all")',
    '# --- FlowPython helpers (shared) ---',
    'def _fp_env():',
    "    _safe_builtins = {'abs': abs, 'round': round, 'min': min, 'max': max, 'pow': pow}",
    "    return {'__builtins__': _safe_builtins, 'math': __import__('math'), 'PI': __import__('math').pi}",
    '',
    'def _fp_render(text, local=None):',
    '    try:',
    '        import re',
    '        s = str(text)',
    '        env = {}',
    '        env.update(globals())',
    '        if local: env.update(local)',
    "        def _rep(m): return str(env.get(m.group(1), ''))",
    "        return re.sub(r'\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}', _rep, s)",
    '    except Exception:',
    '        return str(text)',
    '',
    'def _fp_eval(expr, local=None):',
    '    try:',
    '        env = _fp_env()',
    '        if local is None: local = {}',
    '        loc = {}',
    '        loc.update(globals())',
    '        loc.update(local)',
    '        return eval(expr, env, loc)',
    '    except Exception:',
    '        return None',
    '',
    'def _fp_set_globals(text):',
    '    lines = str(text).splitlines()',
    '    env = _fp_env()',
    '    for __ln in lines:',
    '        __ln = __ln.strip()',
    '        if not __ln or __ln.startswith("#"): continue',
    '        __name, __eq, __expr = __ln.partition("=")',
    '        __name = __name.strip(); __expr = __expr.strip()',
    '        if not __name or not __expr: continue',
    '        try:',
    '            globals()[__name] = eval(__expr, env, globals())',
    '        except Exception:',
    '            try:',
    '                exec(f"{__name} = (" + __expr + ")", globals())',
    '            except Exception:',
    '                pass',
    '    rows = []',
    '    for __ln in lines:',
    '        __name, __eq, __expr = __ln.partition("=")',
    '        __name = __name.strip()',
    '        if not __name: continue',
    '        try:',
    '            __val = globals().get(__name, None)',
    '            rows.append((__name, type(__val).__name__, repr(__val)[:200]))',
    '        except Exception:',
    "            rows.append((__name, 'unknown', '<unrepr>'))",
    "    return pd.DataFrame(rows, columns=['name','type','repr'])",
    '',
    'def _fp_preview(df, nid):',
    '    try:',
    '        print(f"[[PREVIEW:{nid}:HEAD]]" + df.head().to_string())',
    '        print(f"[[PREVIEW:{nid}:HEADHTML]]" + df.head().to_html())',
    '    except Exception:',
    '        pass',
    '    try:',
    '        print(f"[[PREVIEW:{nid}:DESC]]" + df.describe().to_string())',
    '        print(f"[[PREVIEW:{nid}:DESCHTML]]" + df.describe().to_html())',
    '    except Exception:',
    '        print(f"[[PREVIEW:{nid}:DESC]]N/A")'
  ];
  const lines = [...header]; const varOf = {}; const ctx = { srcVar: (node)=> varOf[upstreamOf(node)?.id], varOfId: (id)=> varOf[id], setLastPlotNode: (id)=> state.lastPlotNodeId=id };
  order.forEach(n=>{ const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; const srcName = varOf[upstreamOf(n)?.id]; lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); if(def && typeof def.code==='function'){ const seg = def.code(n, ctx) || []; seg.forEach(s=> lines.push(s)); }
    const isPlot = n.type==='pandas.Plot' || n.type==='sklearn.ClusterPlot';
    const allowPreview = (pmode==='all');
    if(allowPreview){
      lines.push('try:'); lines.push(`    _fp_preview(${v}, '${n.id}')`); lines.push('except Exception:'); if(srcName){ lines.push('    try:'); lines.push(`        _fp_preview(${srcName}, '${n.id}')`); lines.push('    except Exception:'); lines.push('        pass'); } else { lines.push('    pass'); }
    }
    lines.push(`print("[[NODE:${n.id}:END]]")`); });
  return lines.join('\n');
}

export function genCodeUpTo(targetId){
  const pmode = previewModeProvider();
  const order = topoSort(); const keep = new Set(); const backAdj = {}; state.edges.forEach(e=>{ (backAdj[e.to] ||= []).push(e.from); }); const stack = [targetId]; while(stack.length){ const u = stack.pop(); if(!u || keep.has(u)) continue; keep.add(u); (backAdj[u]||[]).forEach(v=> stack.push(v)); }
  const header = [ 'import pandas as pd', 'import matplotlib.pyplot as plt', 'import io', 'import glob', 'plt.close("all")',
    '# --- FlowPython helpers (shared) ---',
    'def _fp_env():',
    "    _safe_builtins = {'abs': abs, 'round': round, 'min': min, 'max': max, 'pow': pow}",
    "    return {'__builtins__': _safe_builtins, 'math': __import__('math'), 'PI': __import__('math').pi}",
    '',
    'def _fp_render(text, local=None):',
    '    try:',
    '        import re',
    '        s = str(text)',
    '        env = {}',
    '        env.update(globals())',
    '        if local: env.update(local)',
    "        def _rep(m): return str(env.get(m.group(1), ''))",
    "        return re.sub(r'\\$\\{([A-Za-z_][A-Za-z0-9_]*)\\}', _rep, s)",
    '    except Exception:',
    '        return str(text)',
    '',
    'def _fp_eval(expr, local=None):',
    '    try:',
    '        env = _fp_env()',
    '        if local is None: local = {}',
    '        loc = {}',
    '        loc.update(globals())',
    '        loc.update(local)',
    '        return eval(expr, env, loc)',
    '    except Exception:',
    '        return None',
    '',
    'def _fp_set_globals(text):',
    '    lines = str(text).splitlines()',
    '    env = _fp_env()',
    '    for __ln in lines:',
    '        __ln = __ln.strip()',
    '        if not __ln or __ln.startswith("#"): continue',
    '        __name, __eq, __expr = __ln.partition("=")',
    '        __name = __name.strip(); __expr = __expr.strip()',
    '        if not __name or not __expr: continue',
    '        try:',
    '            globals()[__name] = eval(__expr, env, globals())',
    '        except Exception:',
    '            try:',
    '                exec(f"{__name} = (" + __expr + ")", globals())',
    '            except Exception:',
    '                pass',
    '    rows = []',
    '    for __ln in lines:',
    '        __name, __eq, __expr = __ln.partition("=")',
    '        __name = __name.strip()',
    '        if not __name: continue',
    '        try:',
    '            __val = globals().get(__name, None)',
    '            rows.append((__name, type(__val).__name__, repr(__val)[:200]))',
    '        except Exception:',
    "            rows.append((__name, 'unknown', '<unrepr>'))",
    "    return pd.DataFrame(rows, columns=['name','type','repr'])",
    '',
    'def _fp_preview(df, nid):',
    '    try:',
    '        print(f"[[PREVIEW:{nid}:HEAD]]" + df.head().to_string())',
    '        print(f"[[PREVIEW:{nid}:HEADHTML]]" + df.head().to_html())',
    '    except Exception:',
    '        pass',
    '    try:',
    '        print(f"[[PREVIEW:{nid}:DESC]]" + df.describe().to_string())',
    '        print(f"[[PREVIEW:{nid}:DESCHTML]]" + df.describe().to_html())',
    '    except Exception:',
    '        print(f"[[PREVIEW:{nid}:DESC]]N/A")'
  ];
  const lines = [...header]; const varOf = {}; const ctx = { srcVar: (node)=> varOf[upstreamOf(node)?.id], varOfId: (id)=> varOf[id], setLastPlotNode: (id)=> state.lastPlotNodeId=id };
  order.forEach(n=>{ if(!keep.has(n.id)) return; const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; const srcName = varOf[upstreamOf(n)?.id]; lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); if(def && typeof def.code==='function'){ const seg = def.code(n, ctx) || []; seg.forEach(s=> lines.push(s)); }
    const allowPreview = (pmode==='all');
    if(allowPreview){
      lines.push('try:'); lines.push(`    _fp_preview(${v}, '${n.id}')`); lines.push('except Exception:'); if(srcName){ lines.push('    try:'); lines.push(`        _fp_preview(${srcName}, '${n.id}')`); lines.push('    except Exception:'); lines.push('        pass'); } else { lines.push('    pass'); }
    }
    lines.push(`print("[[NODE:${n.id}:END]]")`); });
  return lines.join('\n');
}

export async function loadPackages(){
  const res = await fetch('/api/packages');
  const list = await res.json();
  registry.packages = list.map(x=> ({name:x.name, label:x.label, entry:x.entry}));
  for(const p of list){
    try{
      const mod = await import(`/pkg/${p.name}/${p.entry}`);
      if(mod && typeof mod.register==='function'){
        const reg = { node(def){ if(!def || !def.id) return; registry.nodes.set(def.id, def); const pkgName = p.name; if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []); registry.byPackage.get(pkgName).push(def.id); } };
        mod.register(reg);
      }
    }catch(e){ /* swallow */ }
  }
  state.activePkg = registry.packages[0]?.name || null;
}
