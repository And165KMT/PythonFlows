// Node graph, registry, and code generation

export const state = {
  nodes: [],
  edges: [],
  nextId: 1,
  pendingSrc: null,
  lastPlotNodeId: null,
  activePkg: null,
  // selection support
  selectedNodeId: null, // last-focused node (kept for compatibility)
  selection: new Set(), // multiple selection
  clipboard: null, // { nodes:[], edges:[], anchor:{x,y} }
  groups: [], // [{id, name, nodeIds:[] }]
  // viewport transform for zoom/pan
  view: { scale: 1, tx: 0, ty: 0 },
  preview: { head: new Map(), desc: new Map(), headHtml: new Map(), descHtml: new Map() },
  stream: { currentNodeId: null, buffers: new Map() }
};

export const registry = { packages: [], nodes: new Map(), byPackage: new Map() };

export function uid(){ return 'n' + (state.nextId++); }
export function getNode(id){ return state.nodes.find(n => n.id === id); }
export function getNodes(ids){ const S = new Set(ids||[]); return state.nodes.filter(n=> S.has(n.id)); }

export function upstreamOf(node){ const e = state.edges.find(x=>x.to===node.id); if(!e) return null; return getNode(e.from); }

// For nodes that can accept multiple inputs (e.g., pandas.Merge), return all upstream nodes in edge order
export function upstreamsOf(node){ return state.edges.filter(x=> x.to===node.id).map(e=> getNode(e.from)).filter(Boolean); }
export function incomingCount(node){ return state.edges.filter(e=> e.to===node.id).length; }

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
    if(cur.type==='pandas.Merge'){
      // If two inputs, union columns from both; else fallback to upstream walk
      const ups = upstreamsOf(cur);
      if(ups.length>=2){
        const a = walk(ups[0]);
        const b = walk(ups[1]);
        return uniq([...(a||[]), ...(b||[])]);
      }
      return walk(up);
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
      // For python.Math, include its output column name alongside upstream columns
      if(cur.type==='python.Math'){
        const cols = walk(up);
        const out = String(cur.params?.out||'result').trim();
        const uniqSet = new Set(cols);
        if(out && !uniqSet.has(out)) cols.push(out);
        return cols;
      }
      return walk(up);
    }
    return walk(up);
  }
  const up = upstreamOf(n); return walk(up);
}

export function allNodeTypes(){ const out=[]; registry.byPackage.forEach((types)=>{ types.forEach(t=> out.push(t)); }); return out; }
export function nodeLabelOf(type){ const def = registry.nodes.get(type); return def?.title || type.split('.').slice(-1)[0] || type; }

// ---- Selection helpers ----
export function setSelection(ids){
  state.selection = new Set(ids||[]);
  state.selectedNodeId = ids && ids[0] ? ids[0] : null;
}
export function addToSelection(id){ const s = new Set(state.selection); s.add(id); state.selection = s; state.selectedNodeId = id; }
export function removeFromSelection(id){ const s = new Set(state.selection); s.delete(id); state.selection = s; if(state.selectedNodeId===id) state.selectedNodeId = Array.from(s)[0]||null; }
export function clearSelection(){ state.selection = new Set(); state.selectedNodeId = null; }
export function isSelected(id){ return state.selection.has(id); }

// ---- Persistence ----
const LS_KEY = 'pythonflows_state_v1';
export function saveToLocal(){
  try{
    const data = {
      nodes: state.nodes,
      edges: state.edges,
      nextId: state.nextId,
      view: state.view,
      activePkg: state.activePkg,
      lastPlotNodeId: state.lastPlotNodeId,
      groups: state.groups
    };
    localStorage.setItem(LS_KEY, JSON.stringify(data));
  }catch{}
}
export function restoreFromLocal(){
  try{
    const raw = localStorage.getItem(LS_KEY);
    if(!raw) return false;
    const obj = JSON.parse(raw);
    if(obj && Array.isArray(obj.nodes) && Array.isArray(obj.edges)){
      state.nodes = obj.nodes;
      state.edges = obj.edges;
      state.nextId = obj.nextId || 1;
      state.view = obj.view || { scale:1, tx:0, ty:0 };
      state.activePkg = obj.activePkg || state.activePkg;
      state.lastPlotNodeId = obj.lastPlotNodeId || null;
      state.groups = Array.isArray(obj.groups) ? obj.groups : [];
      state.selection = new Set();
      return true;
    }
  }catch{}
  return false;
}

// ---- Clipboard helpers (data only; UI manages when to call) ----
export function makeSubgraph(ids){
  const S = new Set(ids||[]);
  const nodes = state.nodes.filter(n=> S.has(n.id)).map(n=> ({...n, params: JSON.parse(JSON.stringify(n.params||{})) }));
  const edges = state.edges.filter(e=> S.has(e.from) && S.has(e.to)).map(e=> ({...e}));
  // anchor top-left for paste offset convenience
  const xs = nodes.map(n=> n.x||0); const ys = nodes.map(n=> n.y||0);
  const anchor = { x: xs.length? Math.min(...xs): 0, y: ys.length? Math.min(...ys): 0 };
  return { nodes, edges, anchor };
}
export function pasteSubgraph(data, at){
  if(!data || !Array.isArray(data.nodes)) return [];
  const idMap = new Map();
  const dx = (at?.x ?? (data.anchor?.x||0)) - (data.anchor?.x||0);
  const dy = (at?.y ?? (data.anchor?.y||0)) - (data.anchor?.y||0);
  const newIds = [];
  for(const n of data.nodes){ const nid = uid(); idMap.set(n.id, nid); newIds.push(nid); state.nodes.push({ ...n, id: nid, x: (n.x||0)+dx+20, y: (n.y||0)+dy+20 }); }
  for(const e of (data.edges||[])){ const from = idMap.get(e.from); const to = idMap.get(e.to); if(from && to){ state.edges.push({ from, to }); } }
  return newIds;
}
export function deleteNodes(ids){
  const S = new Set(ids||[]);
  state.nodes = state.nodes.filter(n=> !S.has(n.id));
  state.edges = state.edges.filter(e=> !S.has(e.from) && !S.has(e.to));
  if(S.has(state.lastPlotNodeId)) state.lastPlotNodeId = null;
  if(S.has(state.selectedNodeId)) state.selectedNodeId = null;
}

// ---- Groups (Subsystems) ----
export function createGroup(name, ids){
  const id = 'g' + Math.random().toString(36).slice(2,8);
  const nodeIds = Array.from(new Set(ids||[]));
  state.groups.push({ id, name: name||('Subsystem '+(state.groups.length+1)), nodeIds });
  return id;
}
export function deleteGroup(id){ state.groups = state.groups.filter(g=> g.id!==id); }
export function renameGroup(id, name){ const g = state.groups.find(x=> x.id===id); if(g){ g.name = String(name||'').trim() || g.name; } }
export function getGroup(id){ return state.groups.find(g=> g.id===id); }

export function suggestionsForNode(fromId){
  const n = getNode(fromId); const t = n?.type || '';
  const has = (id)=> registry.nodes.has(id);
  const acc = [];
  if(t.startsWith('pandas.')){
    ['pandas.SelectColumns','pandas.FilterRows','pandas.SortValues','pandas.GroupByAggregate','pandas.ValueCounts','pandas.PivotTable','pandas.Melt','pandas.AddColumn','pandas.DropNA','pandas.FillNA','pandas.RenameColumns','pandas.HeadTail','pandas.Merge','pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','pandas.CorrHeatmap','python.Exec','python.If','python.For','python.While','python.FileWriteCSV','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
  } else if(t==='numpy.RandomNormal'){
    ['pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','sklearn.StandardScaler','sklearn.KMeans','sklearn.ClusterPlot','python.Exec','python.For','python.While','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
  } else if(t.startsWith('sklearn.')){
    const extras = ['sklearn.KMeans','sklearn.ClusterPlot','pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','sklearn.StandardScaler','sklearn.TrainTestSplit','python.Exec','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'];
    if(t==='sklearn.TrainTestSplit') extras.unshift('sklearn.SplitSelect');
    extras.forEach(x=> has(x)&&acc.push(x));
  } else {
    ['pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','pandas.FilterRows','pandas.SelectColumns','python.Exec','python.FileReadText','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
  }
  const seen=new Set(); const out=[]; for(const x of acc){ if(!seen.has(x)){ seen.add(x); out.push(x); if(out.length>=8) break; } }
  return out;
}

export function addNode(type, x=80, y=80){
  const def = registry.nodes.get(type);
  const n = {
    id: uid(),
    type,
    x,
    y,
    // default node size (can be changed via UI)
    w: 220,
    prevH: 140,
    params: JSON.parse(JSON.stringify(def?.defaultParams||{}))
  };
  state.nodes.push(n);
  return n;
}

export function selectNode(id){ state.selectedNodeId = id; }
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
  const lines = [...header]; const varOf = {}; const ctx = {
    srcVar: (node)=> varOf[upstreamOf(node)?.id],
    srcVars: (node)=> upstreamsOf(node).map(n=> varOf[n?.id]).filter(Boolean),
    varOfId: (id)=> varOf[id],
    setLastPlotNode: (id)=> state.lastPlotNodeId=id,
    incomingCount: (node)=> incomingCount(node)
  };
  order.forEach(n=>{ const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; const srcName = varOf[upstreamOf(n)?.id]; lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); if(def && typeof def.code==='function'){ const seg = def.code(n, ctx) || []; seg.forEach(s=> lines.push(s)); }
     const allowPreview = (pmode==='all');
     if(allowPreview){
       lines.push('try:'); lines.push(`    _fp_preview(${v}, '${n.id}')`); lines.push('except Exception:'); if(srcName){ lines.push('    try:'); lines.push(`        _fp_preview(${srcName}, '${n.id}')`); lines.push('    except Exception:'); lines.push('        pass'); } else { lines.push('    pass'); }
     }
     lines.push(`print("[[NODE:${n.id}:END]]")`); });
  return lines.join('\n');
}

// Generate code only for a set of nodes. If includeUpstream is true, include all upstream dependencies as well.
export function genCodeForNodes(ids, includeUpstream=true){
  const targets = new Set(ids||[]);
  if(targets.size===0) return genCode();
  const pmode = previewModeProvider();
  const order = topoSort();
  const keep = new Set();
  if(includeUpstream){
    const backAdj = {}; state.edges.forEach(e=>{ (backAdj[e.to] ||= []).push(e.from); });
    const stack = Array.from(targets);
    while(stack.length){ const u = stack.pop(); if(!u || keep.has(u)) continue; keep.add(u); (backAdj[u]||[]).forEach(v=> stack.push(v)); }
  } else {
    targets.forEach(id=> keep.add(id));
  }
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
    "            rows.append((__name, type(__val).__name__, repr(__val)[:200]))",
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
  const lines = [...header]; const varOf = {}; const ctx = {
    srcVar: (node)=> varOf[upstreamOf(node)?.id],
    srcVars: (node)=> upstreamsOf(node).map(n=> varOf[n?.id]).filter(Boolean),
    varOfId: (id)=> varOf[id],
    setLastPlotNode: (id)=> state.lastPlotNodeId=id,
    incomingCount: (node)=> incomingCount(node)
  };
  order.forEach(n=>{ if(!keep.has(n.id)) return; const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; const srcName = varOf[upstreamOf(n)?.id]; lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); if(def && typeof def.code==='function'){ const seg = def.code(n, ctx) || []; seg.forEach(s=> lines.push(s)); }
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
    "        return re.sub(r'\\$\\{([A-Za-z_][A-ZaZ0-9_]*)\\}', _rep, s)",
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
  const lines = [...header]; const varOf = {}; const ctx = {
    srcVar: (node)=> varOf[upstreamOf(node)?.id],
    srcVars: (node)=> upstreamsOf(node).map(n=> varOf[n?.id]).filter(Boolean),
    varOfId: (id)=> varOf[id],
    setLastPlotNode: (id)=> state.lastPlotNodeId=id,
    incomingCount: (node)=> incomingCount(node)
  };
  order.forEach(n=>{ if(!keep.has(n.id)) return; const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; const srcName = varOf[upstreamOf(n)?.id]; lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); if(def && typeof def.code==='function'){ const seg = def.code(n, ctx) || []; seg.forEach(s=> lines.push(s)); }
    const allowPreview = (pmode==='all');
    if(allowPreview){
      lines.push('try:'); lines.push(`    _fp_preview(${v}, '${n.id}')`); lines.push('except Exception:'); if(srcName){ lines.push('    try:'); lines.push(`        _fp_preview(${srcName}, '${n.id}')`); lines.push('    except Exception:'); lines.push('        pass'); } else { lines.push('    pass'); }
    }
    lines.push(`print("[[NODE:${n.id}:END]]")`); });
  return lines.join('\n');
}

export async function loadPackages(){
  try{
    const res = await fetch('/api/packages');
    const list = await res.json();
    registry.packages = Array.isArray(list) ? list.map(x=> ({name:x.name, label:x.label, entry:x.entry})) : [];
    for(const p of (Array.isArray(list)? list: [])){
      try{
        const mod = await import(`/pkg/${p.name}/${p.entry}`);
        if(mod && typeof mod.register==='function'){
          const reg = { node(def){ if(!def || !def.id) return; registry.nodes.set(def.id, def); const pkgName = p.name; if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []); registry.byPackage.get(pkgName).push(def.id); } };
          mod.register(reg);
        }
      }catch(e){ /* swallow */ }
    }
    state.activePkg = registry.packages[0]?.name || null;
  }catch{
    registry.packages = [];
    state.activePkg = null;
  }
}
