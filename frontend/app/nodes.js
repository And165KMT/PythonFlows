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
// Track which JS packages have already executed their register() to avoid duplicate node lists
const __loadedPackages = new Set();
// Some packages are meant to be provided via Autogen only (no static /pkg files).
// To avoid 404s from dynamic import, skip importing these and let Autogen populate nodes on demand.
// Users may override by defining window.__PF_AUTOGEN_ONLY = ['pandas','sklearn', ...] before runtime.js loads.
const AUTOGEN_ONLY = new Set(
  (Array.isArray(window.__PF_AUTOGEN_ONLY) ? window.__PF_AUTOGEN_ONLY : ['pandas','sklearn']).map(x=> String(x))
);

// Register built-in control nodes that are always available (no static package file)
// - python.ForEach: iterate over a list or DataFrame rows and execute a selected subsystem (group) per item
try{
  (function registerBuiltinControlNodes(){
    // ensure logical package bucket exists
    const pkgName = 'python';
    if(!registry.packages.some(p=> p.name===pkgName)) registry.packages.push({ name: pkgName, label: 'Python', entry: '' });
    if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []);

    // Minimal Exec node
    if(!registry.nodes.has('python.Exec')){
      const defExec = {
        id: 'python.Exec',
        title: 'Exec',
        category: 'Python',
        inputType: 'Any',
        outputType: 'Any',
        origin: 'builtin',
        defaultParams: { code: '' },
        form(node){
          const v = node.params || (node.params = { code: '' });
          return `<div class="pf-field"><div class="pf-label"><span>Code</span></div><textarea name="code" placeholder="例: return len(src)\n# または\n# out = process(src)\n# （戻り値なしなら None）">${(v.code||'')}</textarea></div>`;
        },
        code(node, ctx){
          const vout = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
          const p = node.params||{}; const code = String(p.code||'');
          const src = (ctx && typeof ctx.srcVar==='function') ? (ctx.srcVar(node) || 'None') : 'None';
          const seg = [];
          seg.push(`__in = _fp_as_scalar(${src})`);
          // expose convenient alias
          seg.push(`src = __in`);
          if(code.trim().startsWith('return ')){
            const expr = code.trim().slice('return '.length);
            seg.push(`try:\n ${vout} = eval(r'''${expr.replace(/'/g, "'\''")}''', _fp_env(), globals())\nexcept Exception:\n ${vout} = None`);
          } else if(code.trim().startsWith('=')){
            const expr = code.trim().slice(1);
            seg.push(`try:\n ${vout} = eval(r'''${expr.replace(/'/g, "'\''")}''', _fp_env(), globals())\nexcept Exception:\n ${vout} = None`);
          } else {
            seg.push(`try:\n exec(r'''${code.replace(/'/g, "'\''")}''', globals(), globals())\nexcept Exception as __e:\n print('[Exec] error:', __e)`);
            seg.push(`${vout} = globals().get('out', None)`);
          }
          seg.push(`print(${vout})`);
          return seg;
        }
      };
      registry.nodes.set(defExec.id, defExec);
      const arr0 = registry.byPackage.get(pkgName);
      if(!arr0.includes(defExec.id)) arr0.push(defExec.id);
    }

    const defForEach = {
      id: 'python.ForEach',
      title: 'ForEach (Subsystem)',
      category: 'Python',
      inputType: 'Any',
      outputType: 'Any',
      origin: 'builtin',
      defaultParams: { group: '', iterVar: 'row', collect: '', limit: '', mode: 'list', dfKeyOrder: '' },
      form(node){
        const v = node.params || (node.params = JSON.parse(JSON.stringify(this.defaultParams)));
        const groups = (state.groups||[]);
        const opts = groups.map(g=> `<option value="${g.id}" ${v.group===g.id?'selected':''}>${(g.name||g.id)}</option>`).join('');
        return [
          `<div class="desc">選択したサブシステム（グループ）を入力データの各要素（またはDataFrameの各行）に対して繰り返し実行します。グループ内のノードは通常実行から除外され、ここでのみ実行されます。グループ内では ${v.iterVar||'row'} が現在の要素（dict/行/値）として利用できます。</div>`,
          `<div class="pf-field"><div class="pf-label"><span>Subsystem (Group)</span></div><select name="group"><option value="">（未選択）</option>${opts}</select></div>`,
          `<div class="pf-field"><div class="pf-label"><span>Iter var name</span></div><input name="iterVar" value="${v.iterVar||'row'}"></div>`,
          `<div class="pf-field"><div class="pf-label"><span>Collect expression (optional)</span></div><input name="collect" placeholder="例: ${ (v.iterVar||'row') }['id'] または calc" value="${v.collect||''}"></div>`,
          `<div class="pf-field"><div class="pf-label"><span>Collect mode</span></div><select name="mode"><option value="list" ${v.mode==='list'?'selected':''}>list</option><option value="dict" ${v.mode==='dict'?'selected':''}>dict (key=idx)</option><option value="df" ${v.mode==='df'?'selected':''}>DataFrame concat</option></select></div>`,
          `<div class="pf-field"><div class="pf-label"><span>DF columns (optional)</span></div><input name="dfKeyOrder" placeholder="comma-separated keys" value="${v.dfKeyOrder||''}"></div>`,
          `<div class="pf-field"><div class="pf-label"><span>Limit (optional)</span></div><input name="limit" type="number" step="1" min="0" placeholder="0=制限なし" value="${v.limit||''}"></div>`
        ].join('\n');
      },
      code(node, ctx){
        const vout = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
        const p = node.params||{};
        const gid = String(p.group||'');
        const iterVar = String(p.iterVar||'row');
        const collectExpr = String(p.collect||'');
        const mode = String(p.mode||'list');
        const dfKeyOrder = String(p.dfKeyOrder||'');
        const limit = parseInt(p.limit||'0')||0;
        // fetch subsystem definition
        let ids = [];
        try{ const g = getGroup(gid); if(g && Array.isArray(g.nodeIds)) ids = g.nodeIds.slice(); }catch(e){}
        // Generate Python code for the subsystem; include only the group's own nodes
        let subCode = '';
        try{ subCode = genCodeForNodes(ids, false); }catch(e){}
        // Base64 encode to embed safely into Python
        let b64 = '';
        try{ b64 = (typeof btoa==='function')? btoa(unescape(encodeURIComponent(subCode||''))) : ''; }catch(e){ b64=''; }
        const src = (ctx && typeof ctx.srcVar==='function') ? (ctx.srcVar(node) || 'None') : 'None';
        const seg = [];
        seg.push(`# ForEach(SubSystem): group=${gid||'-'} iterVar=${iterVar}`);
        seg.push(`import base64 as _b64`);
        seg.push(`__pf_sub_b64 = r'''${b64}'''`);
        seg.push(`__pf_sub_code = ''`);
        seg.push(`try:\n __pf_sub_code = _b64.b64decode(__pf_sub_b64.encode('utf-8')).decode('utf-8')\nexcept Exception:\n __pf_sub_code = ''`);
        seg.push(`__src = _fp_as_scalar(${src})`);
        seg.push(`__iter = None`);
        seg.push(`try:\n import pandas as _pd\n if hasattr(__src, 'to_dict'):\n  __iter = __src.to_dict('records')\nexcept Exception:\n pass`);
        seg.push(`if __iter is None:\n try:\n  __iter = list(__src)\n except Exception:\n  __iter = []`);
  seg.push(`__acc = []`);
  seg.push(`__dict_acc = {}`);
  seg.push(`__df_acc = None`);
        seg.push(`__i = 0`);
        seg.push(`for __item in __iter:`);
        seg.push(`  if ${limit} and __i>=${limit}: break`);
        seg.push(`  globals()[r'''${iterVar}'''] = __item`);
        seg.push(`  try:\n   exec(__pf_sub_code, globals(), globals())\n  except Exception as __e:\n   print('[ForEach] error:', __e)`);
  seg.push(`  __val = None`);
  seg.push(`  __ce = r'''${collectExpr.replace(/'/g, "'\''")}'''`);
        seg.push(`  if __ce:\n   try:\n    __val = eval(__ce, _fp_env(), globals())\n   except Exception:\n    __val = None`);
  seg.push(`  if __ce:\n   __v = __val\n  else:\n   __v = __item`);
  seg.push(`  if r'''${mode}''' == 'dict':\n   __dict_acc[__i] = __v\n  elif r'''${mode}''' == 'df':\n   try:\n    import pandas as _pd\n    if isinstance(__v, dict):\n     __row = __v\n    elif hasattr(__v, 'to_dict'):\n     __row = __v.to_dict()\n    else:\n     __row = {'value': __v}\n    __cols = [c.strip() for c in r'''${dfKeyOrder}'''.split(',') if c.strip()]\n    if not __cols:\n     __cols = list(__row.keys())\n    __df = _pd.DataFrame([{k: __row.get(k) for k in __cols}])\n    __df_acc = (__df if __df_acc is None else _pd.concat([__df_acc, __df], ignore_index=True))\n   except Exception as __e:\n    print('[ForEach:df] error:', __e)\n  else:\n   __acc.append(__v)`);
        seg.push(`  __i += 1`);
  seg.push(`if r'''${mode}''' == 'dict':\n ${vout} = __dict_acc\nelif r'''${mode}''' == 'df':\n ${vout} = __df_acc\nelse:\n ${vout} = __acc`);
        return seg;
      }
    };
    registry.nodes.set(defForEach.id, defForEach);
    const arr = registry.byPackage.get(pkgName);
    if(!arr.includes(defForEach.id)) arr.push(defForEach.id);
  })();
}catch(e){}

// Build a dynamic node definition from an autogen spec
// Spec shape: { id, title, category, inputType, outputType, params:[{name, default, ui, hidden?, advanced?, when?}],
//   pkg, call:{ target, kind:'function'|'constructor'|'method', receiver:string|null, dfParam:string|null, returnsSelf?:boolean } }
export function makeAutogenDef(spec){
  // Heuristics to enrich param UI
  function isBoolName(n){
    const s = String(n||'').toLowerCase();
    return s.endsWith('able') || s==='inplace' || s==='copy' || s==='ascending' || s==='drop' || s==='sorted' || s==='normalize' || s.startsWith('return_') || s==='shuffle';
  }
    function getEnumForParam(name, target){
      const n = String(name||'').toLowerCase();
      const tgt = String(target||'');
    if(n==='how') return ['left','right','inner','outer','cross'];
    if(n==='axis') return [0,1,'index','columns'];
    if(n==='aggfunc' || n==='agg' || n==='func') return ['mean','sum','max','min','count','median','std','var'];
    if(n==='method' && /fillna|interpolate/.test(tgt)) return ['ffill','bfill','pad','backfill'];
    if(n==='strategy' && tgt.includes('sklearn')) return ['mean','median','most_frequent','constant'];
    if(n==='solver' && tgt.includes('sklearn')) return ['lbfgs','liblinear','newton-cg','newton-cholesky','sag','saga'];
    if(n==='criterion' && tgt.includes('sklearn.tree')) return ['gini','entropy','log_loss'];
    if(n==='penalty' && tgt.includes('sklearn.linear_model')) return ['l1','l2','elasticnet','none'];
    return null;
  }
  function inferParams(rawParams, target){
    const out = [];
    for(const p of (rawParams||[])){
      const cp = { ...p };
      const defv = cp.default;
      // Bool by default value or name
      if(typeof defv==='boolean' || isBoolName(cp.name)) cp.ui = 'bool';
      // Number by default type or common numeric names
      if(cp.ui!=='bool' && (typeof defv==='number' || /^(n_|num|count|seed|random_state|alpha|gamma|eta|lr|rate|bins?|limit|topk|k)$/.test(String(cp.name||'').toLowerCase()))) cp.ui = 'number';
      // Enums by common params
      const en = getEnumForParam(cp.name, target);
      if(en){ cp.ui = 'select'; cp.enum = en; }
      out.push(cp);
    }
    return out;
  }
  const call = spec?.call || {};
  const enrichedParams = inferParams(spec?.params||[], call.target||'');
  const enrichedSpec = { ...spec, params: enrichedParams };
  const id = spec.id || `autogen.${Math.random().toString(36).slice(2,8)}`;
  const def = {
    id,
    title: enrichedSpec.title || id,
    category: enrichedSpec.category || 'Auto',
    inputType: enrichedSpec.inputType || 'Any',
    outputType: enrichedSpec.outputType || 'Any',
    // mark as autogen for UI優先度
    origin: 'autogen',
    defaultParams: Object.fromEntries((enrichedSpec.params||[]).map(p=> [p.name, p.default])),
  form(node, ctx){
      const v = node.params || (node.params = this.defaultParams ? JSON.parse(JSON.stringify(this.defaultParams)) : {});
      const fields = (enrichedSpec.params||[]).filter(p=> !p.hidden);
      function shown(p){ if(!p.when) return true; const m = String(p.when).split('='); if(m.length!==2) return true; const [k,val] = m; return String(v[k]||'')===String(val); }
      function inputFor(p){
        const name = p.name; const label = p.label||name; const val = v[name] ?? p.default ?? '';
        const ui = String(p.ui||'string').toLowerCase();
        const head = `<div class="pf-label"><span class="param-port" data-param="${name}" title="Connect input to ${name}"></span><span>${label}</span></div>`;
        const bound = v['__bound__'+name] || '';
        // column suggestions via datalist from upstream
  const cols = (ctx && typeof ctx.getUpstreamColumns==='function') ? (ctx.getUpstreamColumns()||[]) : [];
  const nm = String(name||'').toLowerCase();
  const isColLike = nm==='column' || nm==='by' || nm==='x' || nm==='y' || nm==='on' || nm==='left_on' || nm==='right_on' || nm==='columns' || nm==='subset' || nm==='index' || nm==='values' || nm==='id_vars' || nm==='value_vars' || nm.includes('col');
  const hasColSuggest = isColLike && cols.length>0;
        const listId = `cols_${node.id}_${name}`;
        if(ui==='select' && Array.isArray(p.enum)){
          const opts = p.enum.map(x=> `<option value="${x}" ${String(val)===String(x)?'selected':''}>${x}</option>`).join('');
          return `<div class="pf-field ${bound?'bound':''}" data-param="${name}" data-bound="${bound}">${head}<select name="${name}">${opts}</select></div>`;
        }
        if(ui==='bool'){
          const on = String(val)==='true' || val===true; return `<div class="pf-field ${bound?'bound':''}" data-param="${name}" data-bound="${bound}">${head}<select name="${name}"><option value="false" ${!on?'selected':''}>false</option><option value="true" ${on?'selected':''}>true</option></select></div>`;
        }
        if(ui==='number'){
          const num = (val===null||val===undefined)? '' : String(val); return `<div class="pf-field ${bound?'bound':''}" data-param="${name}" data-bound="${bound}">${head}<input type="number" step="any" name="${name}" value="${num}"></div>`;
        }
        if(ui==='textarea'){
          return `<div class="pf-field ${bound?'bound':''}" data-param="${name}" data-bound="${bound}">${head}<textarea name="${name}">${val||''}</textarea></div>`;
        }
        if(ui==='upload'){
          return `<div class="pf-field ${bound?'bound':''}" data-param="${name}" data-bound="${bound}">${head}<div style="display:flex; gap:6px"><input name="${name}" value="${val||''}" placeholder="Uploaded filename" style="flex:1" readonly><button class="upload-file" title="upload file">Upload...</button></div></div>`;
        }
        // default string with column datalist suggestions if available
        if(hasColSuggest){
          const opts = cols.map(c=> `<option value="${String(c).replace(/["<>]/g,'')}"></option>`).join('');
          return `<div class="pf-field ${bound?'bound':''}" data-param="${name}" data-bound="${bound}">${head}<input list="${listId}" name="${name}" value="${val||''}"><datalist id="${listId}">${opts}</datalist></div>`;
        }
        return `<div class="pf-field ${bound?'bound':''}" data-param="${name}" data-bound="${bound}">${head}<input name="${name}" value="${val||''}"></div>`;
      }
      const basic = fields.filter(p=> !p.advanced && shown(p)).map(inputFor).join('\n');
      const adv = fields.filter(p=> p.advanced && shown(p)).map(inputFor).join('\n');
  const desc = enrichedSpec.desc ? `<div class="desc">${String(enrichedSpec.desc).replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]))}</div>` : '';
  // ForEach integration: offer an optional toggle to use current iterVar as source when no upstream is connected
  const foreachAssist = `<div class="pf-field"><div class="pf-label"><span>Use ForEach iter as source</span></div><select name="__use_iter_src"><option value="">auto (off)</option><option value="1" ${(v['__use_iter_src']?'selected':'')}>on</option></select></div>`;
  return `${desc}${basic}${adv? `<details style=\"margin-top:8px\"><summary style=\"cursor:pointer; user-select:none\">Advanced</summary>${adv}</details>`:''}${foreachAssist}`;
    },
  code(node, ctx){
      const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
      const p = node.params||{};
      const call = enrichedSpec.call || {};
      const paramsSpec = Array.isArray(enrichedSpec.params)? enrichedSpec.params : [];
      // Helper: convert JS values (often strings from form inputs) to Python literals safely
      const toPy = (val)=>{
        // Treat explicit sentinel strings as Python literals
        if(val===null || val===undefined || String(val)==='None') return 'None';
        const s = String(val);
        if(s==='True' || val===true) return 'True';
        if(s==='False' || val===false) return 'False';
        // Leave numbers as-is when provided as numbers
        if(typeof val==='number' && Number.isFinite(val)) return String(val);
        // Fallback: JSON string encoding
        return JSON.stringify(val);
      };
      let src = (ctx && typeof ctx.srcVar==='function') ? (ctx.srcVar(node) || null) : null;
      const srcs0 = (ctx && typeof ctx.srcVars==='function') ? (ctx.srcVars(node) || []) : (src? [src] : []);
      let srcs = srcs0;
      // ForEach: allow auto-binding current iter value when no upstream
      try{
        if((!src || srcs.length===0) && node && node.params && node.params.__use_iter_src){
          src = '__pf_iter_val';
          srcs = [src];
        }
      }catch(e){}
      const provided = new Set();
      const activeParams = paramsSpec.filter(x=> (!x.when || String(p[String(x.when).split('=')[0]]||'')===String(String(x.when).split('=')[1]||'')));
      const kwargs = [];
      for(const x of activeParams){
        if(['mode','inline','path','upload','dir'].includes(x.name)) continue;
        const bound = p['__bound__'+x.name];
        if(bound!=null){ kwargs.push(`${x.name}=_fp_as_scalar(${bound})`); provided.add(x.name); continue; }
        if(p[x.name]!==undefined){ kwargs.push(`${x.name}=${toPy(p[x.name])}`); provided.add(x.name); }
      }
      const addKw = (k, expr)=>{ const key = String(k||''); if(key && !provided.has(key)) { kwargs.unshift(`${key}=${expr}`); provided.add(key); } };
      const target = call.target || 'None';
      const parts = String(target).split('.');
      const root = parts[0] || '';
      const modPath = parts.slice(0, -1).join('.');
      const kind = call.kind || 'function';
      const dfParam = call.dfParam || null;
      const srcParams = Array.isArray(call.srcParams) ? call.srcParams : (dfParam? [dfParam] : []);
      const seg = [];
      if(root){ seg.push(`import ${root}`); seg.push(`_fp_register_import('${root}')`); }
      if(modPath && modPath.includes('.')){ seg.push(`import importlib; importlib.import_module(r'''${modPath}''')`); }
      if(kind==='method'){
        const meth = parts[parts.length-1] || '';
        const recv = srcs[0] || src || 'None';
        const dataVar = srcs.length>=2 ? srcs[srcs.length-1] : (srcs[0] || null);
        const argz = [];
        if(dfParam && dfParam!=='self' && dataVar) argz.push(`${dfParam}=_fp_as_scalar(${dataVar})`);
        if(kwargs.length) argz.push(...kwargs);
        const joined = argz.join(', ');
        if(dfParam==='self') seg.push(`${v} = ${recv}.${meth}(${kwargs.join(', ')})`);
        else seg.push(`${v} = ${recv}.${meth}(${joined})`);
        if(call.returnsSelf) seg.push(`${v} = ${recv}`);
      } else if(kind==='constructor'){
        seg.push(`${v} = ${target}(${kwargs.join(', ')})`);
  } else { // function
        if(srcs && srcs.length){
          for(let i=0; i<Math.min(srcs.length, srcParams.length); i++){
            const k = srcParams[i]; const s = srcs[i]; if(k){ addKw(k, `_fp_as_scalar(${String(s)})`); }
          }
          if(dfParam && !srcParams.includes(dfParam) && srcs[0]) addKw(dfParam, `_fp_as_scalar(${String(srcs[0])})`);
        } else if(src && dfParam){
          addKw(dfParam, `_fp_as_scalar(${String(src)})`);
        } else if(src){
          const names = activeParams.map(x=> x.name); const want = names[0] || null; if(want && !provided.has(want)) addKw(want, String(src));
        }
        seg.push(`${v} = ${target}(${kwargs.join(', ')})`);
      }
      // Always print the object for logging
      seg.push(`print(${v})`);
      // Special preview for HTTP responses (requests.Response)
      try{
        const tgt = String(call.target||'');
        if(tgt.startsWith('requests.')){
          seg.push(`\ntry:\n import requests as _rq\n if isinstance(${v}, _rq.Response):\n  _fp_preview(str(${v}.status_code) + ' ' + (${v}.headers.get('content-type','') or ''), r'''${node.id}''')\n  try:\n   _fp_preview((${v}.text or '')[:2000], r'''${node.id}''')\n  except Exception:\n   pass\nexcept Exception:\n pass`);
        }
      }catch(e){}
      return seg;
    }
  };
  return def;
}

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
    // sklearn synthetic data: known columns
    if(cur.type==='sklearn.MakeBlobs'){
      return ['x1','x2','label'];
    }
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
    if(cur.type==='sklearn.KMeans'){
      const cols = walk(up); return uniq([...cols, 'cluster']);
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
  }catch(e){}
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
      state.groups = Array.isArray(obj.groups) ? obj.groups.map(g=> ({
        id: g.id,
        name: g.name || 'Subsystem',
        nodeIds: Array.isArray(g.nodeIds) ? g.nodeIds : [],
        collapsed: !!g.collapsed,
        frame: (g.frame && typeof g.frame==='object') ? g.frame : null
      })) : [];
      state.selection = new Set();
      return true;
    }
  }catch(e){}
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
  state.groups.push({ id, name: name||('Subsystem '+(state.groups.length+1)), nodeIds, collapsed: false, frame: null });
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
  ['pandas.SelectColumns','pandas.FilterRows','pandas.SortValues','pandas.GroupByAggregate','pandas.ValueCounts','pandas.PivotTable','pandas.Melt','pandas.AddColumn','pandas.DropNA','pandas.FillNA','pandas.RenameColumns','pandas.HeadTail','pandas.Merge','pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','pandas.CorrHeatmap','python.Exec','python.ForEach','python.FileWriteCSV','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
  } else if(t==='numpy.RandomNormal'){
  ['pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','sklearn.StandardScaler','sklearn.KMeans','sklearn.ClusterPlot','python.Exec','python.ForEach','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
  } else if(t.startsWith('sklearn.')){
  const extras = ['sklearn.KMeans','sklearn.ClusterPlot','pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','sklearn.StandardScaler','sklearn.TrainTestSplit','python.Exec','python.ForEach','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'];
    if(t==='sklearn.TrainTestSplit') extras.unshift('sklearn.SplitSelect');
    extras.forEach(x=> has(x)&&acc.push(x));
  } else {
  ['pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','pandas.FilterRows','pandas.SelectColumns','python.Exec','python.ForEach','python.FileReadText','python.Math','python.SetGlobal','python.ListVariables','python.GetGlobal'].forEach(x=> has(x)&&acc.push(x));
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
  // Exclude nodes that are part of subsystems referenced by python.ForEach nodes
  const excluded = new Set();
  try{
    const foreachNodes = state.nodes.filter(n=> n.type==='python.ForEach');
    foreachNodes.forEach(n=>{
      const gid = String((n.params&&n.params.group)||'');
      const g = getGroup(gid);
      if(g && Array.isArray(g.nodeIds)) g.nodeIds.forEach(id=> excluded.add(id));
    });
  }catch(e){}
  // Build minimal header; add pandas/matplotlib only if needed by nodes
  const header = [ 'import io', 'import glob', 'import importlib',
    '# --- FlowPython helpers (shared) ---',
    '__pf_imports = globals().get("__pf_imports", {})',
    'def _fp_register_import(mod, alias=None):',
    '    _im = importlib',
    '    try:',
    '        m = _im.import_module(mod)',
    "        ver = getattr(m, '__version__', None)",
    '    except Exception:',
    '        ver = None',
    '    d = globals().get("__pf_imports", {})',
    '    d[str(mod)] = {"alias": alias, "version": (str(ver) if ver is not None else None)}',
    '    globals()["__pf_imports"] = d',
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
  '                exec(__name + " = (" + __expr + ")", globals())',
    '            except Exception:',
    '                pass',
    '    rows = []',
    '    for __ln in lines:',
    '        __name, __eq, __expr = __ln.partition("=")',
    '        __name = __name.strip()',
    '        if not __name: continue',
    '        try:',
    '            __val = globals().get(__name, None)',
    "            rows.append({'name': __name, 'type': type(__val).__name__, 'repr': repr(__val)[:200]})",
    '        except Exception:',
    "            rows.append({'name': __name, 'type': 'unknown', 'repr': '<unrepr>'})",
    '    return rows',
    '',
    'def _fp_preview(x, nid):',
    '    def _esc(s):',
    "        try: return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')",
    '        except Exception: return str(s)',
    '    try:',
    '        import pandas as _pd',
    '        if isinstance(x, _pd.DataFrame):',
    '            try:',
    '                print(f"[[PREVIEW:{nid}:HEAD]]" + x.head().to_string())',
    '                print(f"[[PREVIEW:{nid}:HEADHTML]]" + x.head().to_html())',
    '            except Exception: pass',
    '            try:',
    '                print(f"[[PREVIEW:{nid}:DESC]]" + x.describe().to_string())',
    '                print(f"[[PREVIEW:{nid}:DESCHTML]]" + x.describe().to_html())',
    '            except Exception:',
    '                print(f"[[PREVIEW:{nid}:DESC]]N/A")',
    '            return',
    '    except Exception:',
    '        pass',
    '    # Fallbacks for built-in Python types',
    '    try:',
    '        import itertools as _it',
    '        if isinstance(x, list) and (len(x)==0 or isinstance(x[0], dict)):',
    '            # list-of-dicts table',
    '            head = list(x[:5])',
    '            cols = []',
    '            for r in head:',
    '                for k in (r.keys() if isinstance(r, dict) else []):',
    '                    if k not in cols: cols.append(k)',
  "            txt_rows = [\"\\t\".join([str(c) for c in cols])]",
  '            for r in head:',
  "                row = [str((r.get(c, \"\")) if isinstance(r, dict) else \"\") for c in cols]",
  "                txt_rows.append(\"\\t\".join(row))",
  "            print(f\"[[PREVIEW:{nid}:HEAD]]\" + \"\\n\".join(txt_rows))",
  "            html = [\"<table><thead><tr>\" + \"\".join([f\"<th>{_esc(c)}</th>\" for c in cols]) + \"</tr></thead><tbody>\"]",
  '            for r in head:',
  "                html.append(\"<tr>\" + \"\".join([\"<td>\"+_esc(r.get(c, ''))+\"</td>\" for c in cols]) + \"</tr>\")",
  "            html.append(\"</tbody></table>\")",
  "            print(f\"[[PREVIEW:{nid}:HEADHTML]]\" + \"\".join(html))",
  "            print(f\"[[PREVIEW:{nid}:DESC]]type=list(len={len(x)})\")",
  "            print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>list len={len(x)}</pre>\")",
    '            return',
    '        if isinstance(x, dict):',
  "            items = list(x.items())[:10]",
  "            txt = \"\\n\".join([f\"{k}: {v}\" for k,v in items])",
  "            print(f\"[[PREVIEW:{nid}:HEAD]]\" + txt)",
  "            html = \"<table><tbody>\" + \"\".join([f\"<tr><th>{_esc(k)}</th><td>{_esc(v)}</td></tr>\" for k,v in items]) + \"</tbody></table>\"",
  "            print(f\"[[PREVIEW:{nid}:HEADHTML]]\" + html)",
  "            print(f\"[[PREVIEW:{nid}:DESC]]type=dict(size={len(x)})\")",
  "            print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>dict size={len(x)}</pre>\")",
    '            return',
    '        if isinstance(x, (list, tuple, set)):',
    '            head = list(_it.islice(x, 5))',
  "            txt = \"\\n\".join([repr(i) for i in head])",
  "            print(f\"[[PREVIEW:{nid}:HEAD]]\" + txt)",
  "            html = \"<pre>\" + _esc(txt) + \"</pre>\"",
  "            print(f\"[[PREVIEW:{nid}:HEADHTML]]\" + html)",
  '            try:',
  '                ln = len(list(x)) if not isinstance(x, set) else len(x)',
  '            except Exception:',
  '                ln = len(head)',
  "            print(f\"[[PREVIEW:{nid}:DESC]]type={type(x).__name__}(len={ln})\")",
  "            print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>{_esc(type(x).__name__)} len={ln}</pre>\")",
    '            return',
    '        # default scalar/string',
  "        s = str(x)",
  "        print(f\"[[PREVIEW:{nid}:HEAD]]\" + s)",
  "        print(f\"[[PREVIEW:{nid}:HEADHTML]]<pre>\" + _esc(s) + \"</pre>\")",
  "        print(f\"[[PREVIEW:{nid}:DESC]]type={type(x).__name__}\")",
  "        print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>{_esc(type(x).__name__)}</pre>\")",
    '    except Exception:',
    '        pass',
    '',
  '__pf_hash = globals().get("__pf_hash", {})',
    'def _fp_should_run(nid, h):',
  '    # Skip 機能をマスク：常に実行する',
  '    try:',
  '        d = globals().get("__pf_hash", {})',
  '        d[nid] = h',
  '        globals()["__pf_hash"] = d',
  '    except Exception:',
  '        pass',
  '    return True',
  '',
  'def _fp_as_scalar(x):',
  '    try:',
  '        # pandas DataFrame -> scalar extraction (optional)',
  '        try:',
  '            import pandas as _pd',
  '            if isinstance(x, _pd.DataFrame):',
  '                try:',
  '                    if getattr(x, "size", 0) == 1:',
  '                        return x.values.tolist()[0][0]',
  '                    if "text" in x.columns and len(x)==1:',
  '                        return str(x["text"].iloc[0])',
  '                except Exception: pass',
  '                try:',
  '                    return str(x.iloc[0,0])',
  '                except Exception:',
  '                    return str(x)',
  '        except Exception:',
  '            pass',
  '        # numpy scalar (optional)',
  '        try:',
  '            import numpy as _np',
  '            if isinstance(x, _np.ndarray):',
  '                try: return x.item()',
  '                except Exception: pass',
  '        except Exception:',
  '            pass',
  '        # list/dict common extraction patterns',
  '        if isinstance(x, list):',
  '            if len(x)==1:',
  '                e = x[0]',
  '                if isinstance(e, dict):',
  "                    if 'text' in e: return e.get('text')",
  "                    if len(e)==1: return next(iter(e.values()))",
  '                return e',
  '        if isinstance(x, dict):',
  "            if 'text' in x and isinstance(x['text'], (str, bytes)): return x['text']",
  '            if len(x)==1: return next(iter(x.values()))',
  '        if isinstance(x, (list, tuple)) and len(x)==1:',
  '            return x[0]',
  '        return x',
  '    except Exception:',
  '        return x'
  ];
  // Determine required optional imports from node types in this run
  const types = order.map(n=> n?.type||'');
  const needsPandasFromPkgs = types.some(t=> t.startsWith('pandas.') || t.startsWith('sklearn.'));
  const pandasPythonNodes = new Set([
  // 可能な限りpandasに依存しないように縮小（現状ゼロ）
  ]);
  const needsPandasFromPython = types.some(t=> pandasPythonNodes.has(t));
  const needsPandas = needsPandasFromPkgs || needsPandasFromPython;
  const plottingNodes = new Set(['pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','pandas.CorrHeatmap','sklearn.ClusterPlot']);
  const needsMatplotlib = types.some(t=> plottingNodes.has(t));
  const lines = [...header];
  if(needsPandas){ lines.push('import pandas as pd'); lines.push("_fp_register_import('pandas','pd')"); }
  if(needsMatplotlib){ lines.push('import matplotlib.pyplot as plt'); lines.push('plt.close("all")'); lines.push("_fp_register_import('matplotlib.pyplot','plt')"); }
  const varOf = {}; const ctx = {
    srcVar: (node)=> varOf[upstreamOf(node)?.id],
    srcVars: (node)=> upstreamsOf(node).map(n=> varOf[n?.id]).filter(Boolean),
    varOfId: (id)=> varOf[id],
    setLastPlotNode: (id)=> state.lastPlotNodeId=id,
    incomingCount: (node)=> incomingCount(node)
  };
  order.forEach(n=>{ if(excluded.has(n.id)) return; const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; const srcName = varOf[upstreamOf(n)?.id]; const phash = (()=>{ try{ const base = JSON.stringify(n.params||{}); const hasIn = incomingCount(n)>0; return (hasIn? (Date.now().toString(36)+base) : base); }catch(e){return 'na';} })(); lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); lines.push(`_run = _fp_should_run('${n.id}', '${btoa(unescape(encodeURIComponent(''+phash))).slice(0,24)}')`); if(def && typeof def.code==='function'){ const raw = (def.code(n, ctx) || []); const seg = []; for(const s of raw){ const parts = String(s).split('\n'); for(const p of parts){ seg.push('  ' + p); } } lines.push('if _run:'); seg.forEach(s=> lines.push(s)); }
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
  // Build minimal header; add pandas/matplotlib only if needed by kept nodes
  const header = [ 'import io', 'import glob', 'import importlib',
    '# --- FlowPython helpers (shared) ---',
    '__pf_imports = globals().get("__pf_imports", {})',
    'def _fp_register_import(mod, alias=None):',
    '    _im = importlib',
    '    try:',
    '        m = _im.import_module(mod)',
    "        ver = getattr(m, '__version__', None)",
    '    except Exception:',
    '        ver = None',
    '    d = globals().get("__pf_imports", {})',
    '    d[str(mod)] = {"alias": alias, "version": (str(ver) if ver is not None else None)}',
    '    globals()["__pf_imports"] = d',
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
  '                exec(__name + " = (" + __expr + ")", globals())',
  '            except Exception:',
  '                pass',
  '    rows = []',
  '    for __ln in lines:',
  '        __name, __eq, __expr = __ln.partition("=")',
  '        __name = __name.strip()',
  '        if not __name: continue',
  '        try:',
  '            __val = globals().get(__name, None)',
  "            rows.append({'name': __name, 'type': type(__val).__name__, 'repr': repr(__val)[:200]})",
  '        except Exception:',
  "            rows.append({'name': __name, 'type': 'unknown', 'repr': '<unrepr>'})",
  '    return rows',
  '',
  'def _fp_preview(x, nid):',
  '    def _esc(s):',
  "        try: return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')",
  '        except Exception: return str(s)',
  '    try:',
  '        import pandas as _pd',
  '        if isinstance(x, _pd.DataFrame):',
  '            try:',
  '                print(f"[[PREVIEW:{nid}:HEAD]]" + x.head().to_string())',
  '                print(f"[[PREVIEW:{nid}:HEADHTML]]" + x.head().to_html())',
  '            except Exception: pass',
  '            try:',
  '                print(f"[[PREVIEW:{nid}:DESC]]" + x.describe().to_string())',
  '                print(f"[[PREVIEW:{nid}:DESCHTML]]" + x.describe().to_html())',
  '            except Exception:',
  '                print(f"[[PREVIEW:{nid}:DESC]]N/A")',
  '            return',
  '    except Exception:',
  '        pass',
  '    # Fallbacks for built-in Python types',
  '    try:',
  '        import itertools as _it',
  '        if isinstance(x, list) and (len(x)==0 or isinstance(x[0], dict)):',
  '            # list-of-dicts table',
  '            head = list(x[:5])',
  '            cols = []',
  '            for r in head:',
  '                for k in (r.keys() if isinstance(r, dict) else []):',
  '                    if k not in cols: cols.append(k)',
  "            txt_rows = [\"\\t\".join([str(c) for c in cols])]",
  '            for r in head:',
  "                row = [str((r.get(c, \"\")) if isinstance(r, dict) else \"\") for c in cols]",
  "                txt_rows.append(\"\\t\".join(row))",
  "            print(f\"[[PREVIEW:{nid}:HEAD]]\" + \"\\n\".join(txt_rows))",
  "            html = [\"<table><thead><tr>\" + \"\".join([f\"<th>{_esc(c)}</th>\" for c in cols]) + \"</tr></thead><tbody>\"]",
  '            for r in head:',
  "                html.append(\"<tr>\" + \"\".join([\"<td>\"+_esc(r.get(c, ''))+\"</td>\" for c in cols]) + \"</tr>\")",
  "            html.append(\"</tbody></table>\")",
  "            print(f\"[[PREVIEW:{nid}:HEADHTML]]\" + \"\".join(html))",
  "            print(f\"[[PREVIEW:{nid}:DESC]]type=list(len={len(x)})\")",
  "            print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>list len={len(x)}</pre>\")",
  '            return',
  '        if isinstance(x, dict):',
  "            items = list(x.items())[:10]",
  "            txt = \"\\n\".join([f\"{k}: {v}\" for k,v in items])",
  "            print(f\"[[PREVIEW:{nid}:HEAD]]\" + txt)",
  "            html = \"<table><tbody>\" + \"\".join([f\"<tr><th>{_esc(k)}</th><td>{_esc(v)}</td></tr>\" for k,v in items]) + \"</tbody></table>\"",
  "            print(f\"[[PREVIEW:{nid}:HEADHTML]]\" + html)",
  "            print(f\"[[PREVIEW:{nid}:DESC]]type=dict(size={len(x)})\")",
  "            print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>dict size={len(x)}</pre>\")",
  '            return',
  '        if isinstance(x, (list, tuple, set)):',
  '            head = list(_it.islice(x, 5))',
  "            txt = \"\\n\".join([repr(i) for i in head])",
  "            print(f\"[[PREVIEW:{nid}:HEAD]]\" + txt)",
  "            html = \"<pre>\" + _esc(txt) + \"</pre>\"",
  "            print(f\"[[PREVIEW:{nid}:HEADHTML]]\" + html)",
  '            try:',
  '                ln = len(list(x)) if not isinstance(x, set) else len(x)',
  '            except Exception:',
  '                ln = len(head)',
  "            print(f\"[[PREVIEW:{nid}:DESC]]type={type(x).__name__}(len={ln})\")",
  "            print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>{_esc(type(x).__name__)} len={ln}</pre>\")",
  '            return',
  '        # default scalar/string',
  "        s = str(x)",
  "        print(f\"[[PREVIEW:{nid}:HEAD]]\" + s)",
  "        print(f\"[[PREVIEW:{nid}:HEADHTML]]<pre>\" + _esc(s) + \"</pre>\")",
  "        print(f\"[[PREVIEW:{nid}:DESC]]type={type(x).__name__}\")",
  "        print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>{_esc(type(x).__name__)}</pre>\")",
  '    except Exception:',
  '        pass',
  '',
  '__pf_hash = globals().get("__pf_hash", {})',
    'def _fp_should_run(nid, h):',
  '    # Skip 機能をマスク：常に実行する',
  '    try:',
  '        d = globals().get("__pf_hash", {})',
  '        d[nid] = h',
  '        globals()["__pf_hash"] = d',
  '    except Exception:',
  '        pass',
  '    return True',
  '',
  'def _fp_as_scalar(x):',
  '    try:',
  '        # pandas DataFrame -> scalar extraction (optional)',
  '        try:',
  '            import pandas as _pd',
  '            if isinstance(x, _pd.DataFrame):',
  '                try:',
  '                    if getattr(x, "size", 0) == 1:',
  '                        return x.values.tolist()[0][0]',
  '                    if "text" in x.columns and len(x)==1:',
  '                        return str(x["text"].iloc[0])',
  '                except Exception: pass',
  '                try:',
  '                    return str(x.iloc[0,0])',
  '                except Exception:',
  '                    return str(x)',
  '        except Exception:',
  '            pass',
  '        # numpy scalar (optional)',
  '        try:',
  '            import numpy as _np',
  '            if isinstance(x, _np.ndarray):',
  '                try: return x.item()',
  '                except Exception: pass',
  '        except Exception:',
  '            pass',
  '        # list/dict common extraction patterns',
  '        if isinstance(x, list):',
  '            if len(x)==1:',
  '                e = x[0]',
  '                if isinstance(e, dict):',
  "                    if 'text' in e: return e.get('text')",
  "                    if len(e)==1: return next(iter(e.values()))",
  '                return e',
  '        if isinstance(x, dict):',
  "            if 'text' in x and isinstance(x['text'], (str, bytes)): return x['text']",
  '            if len(x)==1: return next(iter(x.values()))',
  '        if isinstance(x, (list, tuple)) and len(x)==1:',
  '            return x[0]',
  '        return x',
  '    except Exception:',
  '        return x'
  ];
  // Determine required optional imports from node types participating in this run
  const keptOrder = order.filter(n=> keep.has(n.id));
  const types = keptOrder.map(n=> n?.type||'');
  const needsPandasFromPkgs = types.some(t=> t.startsWith('pandas.') || t.startsWith('sklearn.'));
  const pandasPythonNodes = new Set([
    
  ]);
  const needsPandasFromPython = types.some(t=> pandasPythonNodes.has(t));
  const needsPandas = needsPandasFromPkgs || needsPandasFromPython;
  const plottingNodes = new Set(['pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','pandas.CorrHeatmap','sklearn.ClusterPlot']);
  const needsMatplotlib = types.some(t=> plottingNodes.has(t));
  const lines = [...header];
  if(needsPandas){ lines.push('import pandas as pd'); lines.push("_fp_register_import('pandas','pd')"); }
  if(needsMatplotlib){ lines.push('import matplotlib.pyplot as plt'); lines.push('plt.close("all")'); lines.push("_fp_register_import('matplotlib.pyplot','plt')"); }
  const varOf = {}; const ctx = {
    srcVar: (node)=> varOf[upstreamOf(node)?.id],
    srcVars: (node)=> upstreamsOf(node).map(n=> varOf[n?.id]).filter(Boolean),
    varOfId: (id)=> varOf[id],
    setLastPlotNode: (id)=> state.lastPlotNodeId=id,
    incomingCount: (node)=> incomingCount(node)
  };
  order.forEach(n=>{ if(!keep.has(n.id)) return; const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; const srcName = varOf[upstreamOf(n)?.id]; const phash = (()=>{ try{ const base = JSON.stringify(n.params||{}); const hasIn = incomingCount(n)>0; return (hasIn? (Date.now().toString(36)+base) : base); }catch(e){return 'na';} })(); lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); lines.push(`_run = _fp_should_run('${n.id}', '${btoa(unescape(encodeURIComponent(''+phash))).slice(0,24)}')`); if(def && typeof def.code==='function'){ const raw = (def.code(n, ctx) || []); const seg = []; for(const s of raw){ const parts = String(s).split('\n'); for(const p of parts){ seg.push('  ' + p); } } lines.push('if _run:'); seg.forEach(s=> lines.push(s)); }
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
  // Build minimal header; add pandas/matplotlib only if needed by kept nodes
  const header = [ 'import io', 'import glob', 'import importlib',
    '# --- FlowPython helpers (shared) ---',
    '__pf_imports = globals().get("__pf_imports", {})',
    'def _fp_register_import(mod, alias=None):',
    '    _im = importlib',
    '    try:',
    '        m = _im.import_module(mod)',
    "        ver = getattr(m, '__version__', None)",
    '    except Exception:',
    '        ver = None',
    '    d = globals().get("__pf_imports", {})',
    '    d[str(mod)] = {"alias": alias, "version": (str(ver) if ver is not None else None)}',
    '    globals()["__pf_imports"] = d',
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
  '                exec(__name + " = (" + __expr + ")", globals())',
  '            except Exception:',
  '                pass',
  '    rows = []',
  '    for __ln in lines:',
  '        __name, __eq, __expr = __ln.partition("=")',
  '        __name = __name.strip()',
  '        if not __name: continue',
  '        try:',
  '            __val = globals().get(__name, None)',
  "            rows.append({'name': __name, 'type': type(__val).__name__, 'repr': repr(__val)[:200]})",
  '        except Exception:',
  "            rows.append({'name': __name, 'type': 'unknown', 'repr': '<unrepr>'})",
  '    return rows',
  '',
  'def _fp_preview(x, nid):',
  '    def _esc(s):',
  "        try: return str(s).replace('&','&amp;').replace('<','&lt;').replace('>','&gt;')",
  '        except Exception: return str(s)',
  '    try:',
  '        import pandas as _pd',
  '        if isinstance(x, _pd.DataFrame):',
  '            try:',
  '                print(f"[[PREVIEW:{nid}:HEAD]]" + x.head().to_string())',
  '                print(f"[[PREVIEW:{nid}:HEADHTML]]" + x.head().to_html())',
  '            except Exception: pass',
  '            try:',
  '                print(f"[[PREVIEW:{nid}:DESC]]" + x.describe().to_string())',
  '                print(f"[[PREVIEW:{nid}:DESCHTML]]" + x.describe().to_html())',
  '            except Exception:',
  '                print(f"[[PREVIEW:{nid}:DESC]]N/A")',
  '            return',
  '    except Exception:',
  '        pass',
  '    # Fallbacks for built-in Python types',
  '    try:',
  '        import itertools as _it',
  '        if isinstance(x, list) and (len(x)==0 or isinstance(x[0], dict)):',
  '            # list-of-dicts table',
  '            head = list(x[:5])',
  '            cols = []',
  '            for r in head:',
  '                for k in (r.keys() if isinstance(r, dict) else []):',
  '                    if k not in cols: cols.append(k)',
  "            txt_rows = [\"\\t\".join([str(c) for c in cols])]",
  '            for r in head:',
  "                row = [str((r.get(c, \"\")) if isinstance(r, dict) else \"\") for c in cols]",
  "                txt_rows.append(\"\\t\".join(row))",
  "            print(f\"[[PREVIEW:{nid}:HEAD]]\" + \"\\n\".join(txt_rows))",
  "            html = [\"<table><thead><tr>\" + \"\".join([f\"<th>{_esc(c)}</th>\" for c in cols]) + \"</tr></thead><tbody>\"]",
  '            for r in head:',
  "                html.append(\"<tr>\" + \"\".join([\"<td>\"+_esc(r.get(c, ''))+\"</td>\" for c in cols]) + \"</tr>\")",
  "            html.append(\"</tbody></table>\")",
  "            print(f\"[[PREVIEW:{nid}:HEADHTML]]\" + \"\".join(html))",
  "            print(f\"[[PREVIEW:{nid}:DESC]]type=list(len={len(x)})\")",
  "            print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>list len={len(x)}</pre>\")",
  '            return',
  '        if isinstance(x, dict):',
  "            items = list(x.items())[:10]",
  "            txt = \"\\n\".join([f\"{k}: {v}\" for k,v in items])",
  "            print(f\"[[PREVIEW:{nid}:HEAD]]\" + txt)",
  "            html = \"<table><tbody>\" + \"\".join([f\"<tr><th>{_esc(k)}</th><td>{_esc(v)}</td></tr>\" for k,v in items]) + \"</tbody></table>\"",
  "            print(f\"[[PREVIEW:{nid}:HEADHTML]]\" + html)",
  "            print(f\"[[PREVIEW:{nid}:DESC]]type=dict(size={len(x)})\")",
  "            print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>dict size={len(x)}</pre>\")",
  '            return',
  '        if isinstance(x, (list, tuple, set)):',
  '            head = list(_it.islice(x, 5))',
  "            txt = \"\\n\".join([repr(i) for i in head])",
  "            print(f\"[[PREVIEW:{nid}:HEAD]]\" + txt)",
  "            html = \"<pre>\" + _esc(txt) + \"</pre>\"",
  "            print(f\"[[PREVIEW:{nid}:HEADHTML]]\" + html)",
  '            try:',
  '                ln = len(list(x)) if not isinstance(x, set) else len(x)',
  '            except Exception:',
  '                ln = len(head)',
  "            print(f\"[[PREVIEW:{nid}:DESC]]type={type(x).__name__}(len={ln})\")",
  "            print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>{_esc(type(x).__name__)} len={ln}</pre>\")",
  '            return',
  '        # default scalar/string',
  "        s = str(x)",
  "        print(f\"[[PREVIEW:{nid}:HEAD]]\" + s)",
  "        print(f\"[[PREVIEW:{nid}:HEADHTML]]<pre>\" + _esc(s) + \"</pre>\")",
  "        print(f\"[[PREVIEW:{nid}:DESC]]type={type(x).__name__}\")",
  "        print(f\"[[PREVIEW:{nid}:DESCHTML]]<pre>{_esc(type(x).__name__)}</pre>\")",
  '    except Exception:',
  '        pass',
  '',
  '__pf_hash = globals().get("__pf_hash", {})',
  'def _fp_should_run(nid, h):',
  "    # Skip 機能をマスク：常に実行する",
  "    try:",
  "        d = globals().get('__pf_hash', {})",
  "        d[nid] = h",
  "        globals()['__pf_hash'] = d",
  "    except Exception:",
  "        pass",
  "    return True",
  '',
  'def _fp_as_scalar(x):',
  '    try:',
  '        # pandas DataFrame -> scalar extraction (optional)',
  '        try:',
  '            import pandas as _pd',
  '            if isinstance(x, _pd.DataFrame):',
  '                try:',
  '                    if getattr(x, "size", 0) == 1:',
  '                        return x.values.tolist()[0][0]',
  '                    if "text" in x.columns and len(x)==1:',
  '                        return str(x["text"].iloc[0])',
  '                except Exception: pass',
  '                try:',
  '                    return str(x.iloc[0,0])',
  '                except Exception:',
  '                    return str(x)',
  '        except Exception:',
  '            pass',
  '        # numpy scalar (optional)',
  '        try:',
  '            import numpy as _np',
  '            if isinstance(x, _np.ndarray):',
  '                try: return x.item()',
  '                except Exception: pass',
  '        except Exception:',
  '            pass',
  '        # list/dict common extraction patterns',
  '        if isinstance(x, list):',
  '            if len(x)==1:',
  '                e = x[0]',
  '                if isinstance(e, dict):',
  "                    if 'text' in e: return e.get('text')",
  "                    if len(e)==1: return next(iter(e.values()))",
  '                return e',
  '        if isinstance(x, dict):',
  "            if 'text' in x and isinstance(x['text'], (str, bytes)): return x['text']",
  '            if len(x)==1: return next(iter(x.values()))',
  '        if isinstance(x, (list, tuple)) and len(x)==1:',
  '            return x[0]',
  '        return x',
  '    except Exception:',
  '        return x'
  ];
  // Determine required optional imports from node types participating in this run
  const keptOrder = order.filter(n=> keep.has(n.id));
  const types = keptOrder.map(n=> n?.type||'');
  const needsPandasFromPkgs = types.some(t=> t.startsWith('pandas.') || t.startsWith('sklearn.'));
  const pandasPythonNodes = new Set([
    
  ]);
  const needsPandasFromPython = types.some(t=> pandasPythonNodes.has(t));
  const needsPandas = needsPandasFromPkgs || needsPandasFromPython;
  const plottingNodes = new Set(['pandas.XYPlot','pandas.BarPlot','pandas.DistributionPlot','pandas.CorrHeatmap','sklearn.ClusterPlot']);
  const needsMatplotlib = types.some(t=> plottingNodes.has(t));
  const lines = [...header];
  if(needsPandas){ lines.push('import pandas as pd'); lines.push("_fp_register_import('pandas','pd')"); }
  if(needsMatplotlib){ lines.push('import matplotlib.pyplot as plt'); lines.push('plt.close("all")'); lines.push("_fp_register_import('matplotlib.pyplot','plt')"); }
  const varOf = {}; const ctx = {
    srcVar: (node)=> varOf[upstreamOf(node)?.id],
    srcVars: (node)=> upstreamsOf(node).map(n=> varOf[n?.id]).filter(Boolean),
    varOfId: (id)=> varOf[id],
    setLastPlotNode: (id)=> state.lastPlotNodeId=id,
    incomingCount: (node)=> incomingCount(node)
  };
  order.forEach(n=>{ if(!keep.has(n.id)) return; const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; const srcName = varOf[upstreamOf(n)?.id]; const phash = (()=>{ try{ const base = JSON.stringify(n.params||{}); const hasIn = incomingCount(n)>0; return (hasIn? (Date.now().toString(36)+base) : base); }catch(e){return 'na';} })(); lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); lines.push(`_run = _fp_should_run('${n.id}', '${btoa(unescape(encodeURIComponent(''+phash))).slice(0,24)}')`); if(def && typeof def.code==='function'){ const raw = (def.code(n, ctx) || []); const seg = []; for(const s of raw){ const parts = String(s).split('\n'); for(const p of parts){ seg.push('  ' + p); } } lines.push('if _run:'); seg.forEach(s=> lines.push(s)); }
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
    const serverPkgs = Array.isArray(list) ? list.map(x=> ({name:x.name, label:x.label, entry:x.entry})) : [];
    // Preserve any pre-registered dynamic packages (e.g., autogen) by merging extras
    const extras = (registry.packages||[]).filter(p=> !serverPkgs.some(sp=> sp.name===p.name));
    registry.packages = [...serverPkgs, ...extras];
    for(const p of (Array.isArray(list)? list: [])){
      try{
        // Avoid re-running register() for the same package; it would duplicate left-pane nodes
        if(__loadedPackages.has(p.name)) continue;
        // If no entry file is provided, this is an Autogen-only logical package.
        // Skip static import and create an empty bucket so UI can toggle and introspect on demand.
        if(!p.entry){
          if(!registry.byPackage.has(p.name)) registry.byPackage.set(p.name, []);
          __loadedPackages.add(p.name);
          continue;
        }
        // Skip static import for Autogen-only packages (no /pkg files on disk)
        if(AUTOGEN_ONLY.has(p.name)){
          if(!registry.byPackage.has(p.name)) registry.byPackage.set(p.name, []);
          __loadedPackages.add(p.name);
          continue;
        }
        const mod = await import(`/pkg/${p.name}/${p.entry}`);
        if(mod && typeof mod.register==='function'){
          const reg = { 
            node(def){
              if(!def || !def.id) return;
              // upsert the node definition
              registry.nodes.set(def.id, def);
              const pkgName = p.name;
              if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []);
              const arr = registry.byPackage.get(pkgName);
              if(!arr.includes(def.id)) arr.push(def.id);
            }
          };
          mod.register(reg);
          __loadedPackages.add(p.name);
        } // silently ignore modules without register()
      }catch(e){
        // Swallow import errors quietly to avoid console noise when files are intentionally absent
        // Ensure package bucket exists so UI can still show toggles for Autogen
        if(!registry.byPackage.has(p.name)) registry.byPackage.set(p.name, []);
        __loadedPackages.add(p.name);
      }
    }
    // Ensure byPackage lists remain unique
    try{ registry.byPackage.forEach((arr, k)=>{ const uniq = Array.from(new Set(arr)); registry.byPackage.set(k, uniq); }); }catch(e3){}
    state.activePkg = registry.packages[0]?.name || null;
  }catch(e){
    registry.packages = [];
    state.activePkg = null;
  }
}
