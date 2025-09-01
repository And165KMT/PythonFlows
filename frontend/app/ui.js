// UI and rendering for FlowPython
import { state, registry, getNode, addNode, selectNode, clearSelection, deleteNodeById, uid, computeUpstreamColumns, suggestionsForNode, genCode, genCodeUpTo, loadPackages, setPreviewModeProvider, upstreamOf, setSelection, addToSelection, removeFromSelection, isSelected, saveToLocal, restoreFromLocal, makeSubgraph, pasteSubgraph, deleteNodes, createGroup, getGroup, genCodeForNodes, makeAutogenDef } from './nodes.js';
import { injectBaseStyles, styleTableHtml as styleTableHtmlUtil, escapeHtml as escapeHtmlUtil } from './utils.js';
import { appendLog, clearLog } from './logger.js';
import { sanitizePython } from './python.js';
import { ensureRunBar as ensureRunBarMod, ensureActionsArea as ensureActionsAreaMod } from './actions.js';
import { drawEdges as drawEdgesMod, syncEdgesViewport as syncEdgesViewportMod, setGhost as setGhostMod, clearGhost as clearGhostMod } from './edges.js';
import { updateNodePreview as updateNodePreviewMod, isFigureNode as isFigureNodeMod, getPreviewMode as getPreviewModeMod } from './preview.js';
import { bindForm as bindFormMod } from './forms.js';
import { initWS } from './ws.js';
import { openContextMenu, closeContextMenu } from './contextmenu.js';
import { initInteractions as initInteractionsMod } from './interactions.js';
import { openQuickAdd as openQuickAddMod, closeQuickAdd as closeQuickAddMod } from './quickadd.js';

// ——— Lightweight adapters/wrappers for clarity and local usage ———
// Treat any node whose outputType is 'Figure' as a plot node (delegated)
function isFigureNode(n){ return isFigureNodeMod(n, registry); }
// Preview mode helper bound to the UI select element
function getPreviewMode(){ try{ return getPreviewModeMod(previewModeEl); }catch(e){ return 'plots'; } }
// Unify helper names used throughout this file
const escapeHtml = (s)=> escapeHtmlUtil(s);
const styleTableHtml = (html)=> styleTableHtmlUtil(html);

// ——— DOM refs and runtime flags ———
const toolbarEl = document.getElementById('toolbar');
const statusEl = document.getElementById('status');
const previewModeEl = document.getElementById('previewMode');
const genCodeEl = document.getElementById('genCode');
const rightCode = document.getElementById('rightCode');
const rightVars = document.getElementById('rightVars');
const rightPkgs = document.getElementById('rightPkgs');
const varsWrap = document.getElementById('varsWrap');
const pkgsWrap = document.getElementById('pkgsWrap');
const tabCode = document.getElementById('tabCode');
const tabVars = document.getElementById('tabVars');
const tabPkgs = document.getElementById('tabPkgs');
const rightPanel = document.getElementById('right');
const rightResizer = document.getElementById('rightResizer');
// Canvas / nodes / edges DOM refs (were missing)
const canvasWrap = document.getElementById('canvasWrap');
const nodesEl = document.getElementById('nodes');
const edgesSvg = document.getElementById('edges');
let subsystemsEl = null;
let groupsLayer = null;
let lastMouseWorld = { x: 100, y: 100 };
let authRequired = false;
let authToken = null;
let kernelDisabled = false;
let runningLock = false;
let globalRunBtn = null;
let wsCtl = null;
// Installed/imported packages tracking to avoid duplicate installs and keep toggles in sync
const importedByUser = new Set();
const installedPkgs = new Set();
// Names of packages that are Autogen-only (no static JS entry) as advertised by the server
const autogenOnlyNames = new Set();
async function ensureAutogenOnlyNames(){
  if(autogenOnlyNames.size>0) return;
  try{
    const res = await apiFetch('/api/packages');
    const js = await res.json().catch(()=>[]);
    (Array.isArray(js)? js: []).forEach(p=>{ if(p && !p.entry) autogenOnlyNames.add(String(p.name||'')); });
  }catch(e){}
}
function isAutogenOnlyName(name){ try{ return autogenOnlyNames.has(String(name||'')); }catch(e){ return false; } }
// Autogen test results per module: { ok, ng, items }
const autogenTestResults = new Map();
let showAutogenNgOnly = false;
// Clipboard buffer for copy/cut/paste of subgraphs
let clipboardGraph = null;

// ——— helpers: auth token + fetch wrapper ———
function getStoredToken(){ try{ return sessionStorage.getItem('pf_token') || null; }catch(e){ return null; } }
function setStoredToken(v){ try{ if(v) sessionStorage.setItem('pf_token', v); else sessionStorage.removeItem('pf_token'); authToken = v || null; }catch(e){} }
async function apiFetch(url, opts){
  const headers = Object.assign({}, (opts && opts.headers) || {});
  if(authRequired && authToken){ headers['Authorization'] = 'Bearer ' + authToken; }
  const next = Object.assign({}, opts || {}, { headers });
  return fetch(url, next);
}

// ——— run buttons state management ———
async function openPackagePickerModal(){
  return new Promise(async (resolve)=>{
    const overlay = document.createElement('div'); overlay.className='modal-overlay';
    const modal = document.createElement('div'); modal.className='modal'; modal.style.maxWidth='900px';
    const head = document.createElement('div'); head.className='modal-head'; head.innerHTML = '<div class="title">Package Explorer</div>';
    const body = document.createElement('div'); body.className='modal-body';
    body.innerHTML = `
      <div class="tabs" style="display:flex; gap:8px; margin-bottom:10px">
        <button id="pf_tab_inst" class="active">Installed</button>
        <button id="pf_tab_pypi">PyPI</button>
      </div>
      <div id="pf_view_inst">
        <div style="display:flex; gap:8px; margin-bottom:10px">
          <input id="pf_pkg_q" class="input" placeholder="Search installed packages (optional)" style="flex:1"/>
          <button id="pf_pkg_search" class="secondary">Search</button>
        </div>
        <div id="pf_pkg_list" style="max-height:360px; overflow:auto; border:1px solid #111824"></div>
        <div id="pf_pkg_pager" style="margin-top:8px; display:flex; align-items:center; gap:8px">
          <button id="pf_pkg_prev" class="secondary">Prev</button>
          <button id="pf_pkg_next" class="secondary">Next</button>
          <div id="pf_pkg_pageinfo" style="color:#9ba3af; margin-left:auto"></div>
        </div>
      </div>
      <div id="pf_view_pypi" style="display:none">
        <div style="display:flex; gap:8px; margin-bottom:10px">
          <input id="pf_pypi_q" class="input" placeholder="Search PyPI (package name or summary)" style="flex:1"/>
          <button id="pf_pypi_search" class="secondary">Search</button>
        </div>
        <div id="pf_pypi_list" style="max-height:360px; overflow:auto; border:1px solid #111824"></div>
        <div id="pf_pypi_pager" style="margin-top:8px; display:flex; align-items:center; gap:8px">
          <button id="pf_pypi_prev" class="secondary">Prev</button>
          <button id="pf_pypi_next" class="secondary">Next</button>
          <div id="pf_pypi_pageinfo" style="color:#9ba3af; margin-left:auto"></div>
        </div>
      </div>`;
    const foot = document.createElement('div'); foot.className='modal-foot';
    const cancelBtn = document.createElement('button'); cancelBtn.className='secondary'; cancelBtn.textContent='Close';
    const installBtn = document.createElement('button'); installBtn.textContent='Install custom...';
    foot.appendChild(cancelBtn); foot.appendChild(installBtn);
    modal.appendChild(head); modal.appendChild(body); modal.appendChild(foot); overlay.appendChild(modal);
    document.body.appendChild(overlay);

    // Tabs
    const tabInst = body.querySelector('#pf_tab_inst');
    const tabPyPI = body.querySelector('#pf_tab_pypi');
    const viewInst = body.querySelector('#pf_view_inst');
    const viewPyPI = body.querySelector('#pf_view_pypi');
    function setTab(mode){ const inst = mode==='inst'; tabInst.classList.toggle('active', inst); tabPyPI.classList.toggle('active', !inst); viewInst.style.display = inst? '' : 'none'; viewPyPI.style.display = inst? 'none' : ''; }
    tabInst.onclick = ()=> setTab('inst');
    tabPyPI.onclick = ()=> setTab('pypi');

    // Installed list controls
    const listEl = body.querySelector('#pf_pkg_list');
    const qEl = body.querySelector('#pf_pkg_q');
    const searchBtn = body.querySelector('#pf_pkg_search');
    const prevBtn = body.querySelector('#pf_pkg_prev');
    const nextBtn = body.querySelector('#pf_pkg_next');
    const pageInfo = body.querySelector('#pf_pkg_pageinfo');
    let offset = 0; const limit = 50; let total = 0;
    const renderInstalled = (items)=>{
      const rows = (items||[]).map(x=> {
        const name = String(x.name||'');
        const nodesCount = (registry.byPackage && registry.byPackage.get(name) ? registry.byPackage.get(name).length : 0) || 0;
        return `<tr data-name="${escapeHtmlUtil(name)}">
          <td style="padding:6px 8px; border-bottom:1px solid #111824">${escapeHtmlUtil(name)}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #111824; color:#cbd5e1">${escapeHtmlUtil(x.version||'-')}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #111824; color:#9ba3af">${nodesCount}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #111824"><button class="mini">Install</button></td>
        </tr>`;
      }).join('');
      listEl.innerHTML = `<table style="width:100%; border-collapse:collapse; font-size:12px"><thead><tr>
        <th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Package</th>
        <th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Version</th>
        <th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Nodes</th>
        <th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Action</th>
      </tr></thead><tbody>${rows}</tbody></table>`;
      listEl.querySelectorAll('button.mini').forEach(btn=>{
        btn.addEventListener('click', ()=>{ const tr = btn.closest('tr'); const name = tr && tr.getAttribute('data-name'); close({ name }); });
      });
      const page = Math.floor(offset/limit)+1; const pages = Math.max(1, Math.ceil(total/limit));
      pageInfo.textContent = `${(total||0).toLocaleString()} items • Page ${page}/${pages}`;
      prevBtn.disabled = offset<=0; nextBtn.disabled = offset+limit>=total;
    };
    const loadInstalled = async ()=>{
      const q = String(qEl.value||'').trim(); const usp = new URLSearchParams(); if(q) usp.set('q', q); usp.set('limit', String(limit)); usp.set('offset', String(offset));
      const js = await (await apiFetch('/api/modules/installed?'+usp.toString())).json().catch(()=>({items:[], total:0}));
      total = Number(js.total||0); renderInstalled(js.items||[]);
    };
    searchBtn.onclick = ()=>{ offset=0; loadInstalled(); };
    prevBtn.onclick = ()=>{ if(offset<=0) return; offset = Math.max(0, offset - limit); loadInstalled(); };
    nextBtn.onclick = ()=>{ if(offset+limit>=total) return; offset += limit; loadInstalled(); };
    qEl.addEventListener('keydown', (e)=>{ if(e.key==='Enter'){ offset=0; loadInstalled(); } });

    // PyPI controls
    const pypiListEl = body.querySelector('#pf_pypi_list');
    const pypiQEl = body.querySelector('#pf_pypi_q');
    const pypiSearchBtn = body.querySelector('#pf_pypi_search');
    const pypiPrevBtn = body.querySelector('#pf_pypi_prev');
    const pypiNextBtn = body.querySelector('#pf_pypi_next');
    const pypiPageInfo = body.querySelector('#pf_pypi_pageinfo');
    let pypiOffset = 0; const pypiLimit = 25; let pypiTotal = 0;
    const renderPyPI = (items)=>{
      const rows = (items||[]).map(x=> {
        const name = String(x.name||''); const summary = String(x.summary||''); const ver = String(x.version||'');
        return `<tr data-name="${escapeHtmlUtil(name)}">
          <td style="padding:6px 8px; border-bottom:1px solid #111824">${escapeHtmlUtil(name)}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #111824; color:#cbd5e1">${escapeHtmlUtil(ver||'-')}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #111824; color:#9ba3af">${escapeHtmlUtil(summary||'')}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #111824"><button class="mini">Install</button></td>
        </tr>`;
      }).join('');
      pypiListEl.innerHTML = `<table style="width:100%; border-collapse:collapse; font-size:12px"><thead><tr>
        <th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Package</th>
        <th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Latest</th>
        <th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Summary</th>
        <th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Action</th>
      </tr></thead><tbody>${rows}</tbody></table>`;
      pypiListEl.querySelectorAll('button.mini').forEach(btn=>{
        btn.addEventListener('click', ()=>{ const tr = btn.closest('tr'); const name = tr && tr.getAttribute('data-name'); close({ name, source:'pypi' }); });
      });
      const page = Math.floor(pypiOffset/pypiLimit)+1; const pages = Math.max(1, Math.ceil(pypiTotal/pypiLimit));
      pypiPageInfo.textContent = `${(pypiTotal||0).toLocaleString()} items • Page ${page}/${pages}`;
      pypiPrevBtn.disabled = pypiOffset<=0; pypiNextBtn.disabled = pypiOffset+pypiLimit>=pypiTotal;
    };
    const loadPyPI = async ()=>{
      const q = String(pypiQEl.value||'').trim(); if(!q){ pypiTotal=0; renderPyPI([]); return; }
      const usp = new URLSearchParams(); usp.set('q', q); usp.set('limit', String(pypiLimit)); usp.set('offset', String(pypiOffset));
      const js = await (await apiFetch('/api/pypi/search?'+usp.toString())).json().catch(()=>({items:[], total:0, error:'request failed'}));
      pypiTotal = Number(js.total||0);
      if((!js.items || js.items.length===0) && js.error){
        pypiListEl.innerHTML = `<div style="color:#fca5a5; padding:6px">検索に失敗しました（${String(js.error)}）。キーワードを変えるか、右上メニューの「Install from PyPI」で名前指定インストールをお試しください。</div>`;
        pypiPageInfo.textContent = '0 items • Page 1/1';
        return;
      }
      renderPyPI(js.items||[]);
    };
    pypiSearchBtn.onclick = ()=>{ pypiOffset=0; loadPyPI(); };
    pypiPrevBtn.onclick = ()=>{ if(pypiOffset<=0) return; pypiOffset = Math.max(0, pypiOffset - pypiLimit); loadPyPI(); };
    pypiNextBtn.onclick = ()=>{ if(pypiOffset+pypiLimit>=pypiTotal) return; pypiOffset += pypiLimit; loadPyPI(); };
    pypiQEl.addEventListener('keydown', (e)=>{ if(e.key==='Enter'){ pypiOffset=0; loadPyPI(); } });

    await loadInstalled();

    function close(v){ overlay.remove(); resolve(v); }
    cancelBtn.onclick = ()=> close(undefined);
    installBtn.onclick = ()=> close({ custom:true });
    overlay.addEventListener('click', (e)=>{ if(e.target===overlay) cancelBtn.click(); });
  });
}

function openInputModal(title, placeholder){
  return new Promise((resolve)=>{
    const overlay = document.createElement('div'); overlay.className='modal-overlay';
    const modal = document.createElement('div'); modal.className='modal'; modal.style.maxWidth='560px';
    modal.innerHTML = `
      <div class="modal-head"><div class="title">${escapeHtml(title||'Input')}</div></div>
      <div class="modal-body"><input id="pf_input_val" class="input" placeholder="${escapeHtml(placeholder||'value')}" style="width:100%"></div>
      <div class="modal-foot"><button class="secondary">Cancel</button><button>OK</button></div>`;
    overlay.appendChild(modal); document.body.appendChild(overlay);
    const inp = modal.querySelector('#pf_input_val'); inp.focus();
    const close = (v)=>{ overlay.remove(); resolve(v); };
    modal.querySelector('.secondary').onclick = ()=> close(undefined);
    modal.querySelector('button:not(.secondary)').onclick = ()=> close(inp.value);
    overlay.addEventListener('click', (e)=>{ if(e.target===overlay) close(undefined); });
    inp.addEventListener('keydown', (e)=>{ if(e.key==='Enter'){ e.preventDefault(); close(inp.value); } });
  });
}

async function pipInstallAndImport(name, version){
  // Guard: stdlib or autogen-only should not be pip installed
  try{ await ensureAutogenOnlyNames(); }catch(e){}
  if(isAutogenOnlyName(name)){
    appendLog(`[pip] skip stdlib/autogen-only: ${name} → introspect only`);
    await introspectAndRegister(name);
    try{ importedByUser.add(name); }catch(e){}
  try{ installedPkgs.add(name); }catch(e){}
    return true;
  }
  try{
    appendLog(`[pip] installing ${name}${version? '=='+version: ''}...`);
    statusEl.textContent='installing...';
    const res = await apiFetch('/api/pip/install', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ name, version }) });
    const js = await res.json().catch(()=>({}));
    if(js && js.ok){
      appendLog(`[pip] installed ${name} ${js.version||''}`);
      await introspectAndRegister(name);
      statusEl.textContent='idle';
  // mark toggle as done if present
  try{ const el = pkgsWrap && pkgsWrap.querySelector(`.pkg-toggle[data-name="${CSS.escape(name)}"] input[type="checkbox"]`); if(el){ el.checked=true; el.disabled=true; } }catch(e){}
      return true;
    } else {
  appendLog('[pip] failed ' + JSON.stringify(js));
      statusEl.textContent='idle';
      return false;
    }
  }catch(e){ appendLog('[pip] error'); statusEl.textContent='idle'; return false; }
}

async function introspectAndRegister(mod){
  try{
    const res = await apiFetch('/api/introspect_module?module=' + encodeURIComponent(mod));
    const js = await res.json().catch(()=>({}));
    const arr = Array.isArray(js.nodes) ? js.nodes : [];
    if(!arr.length){ appendLog('[import] no callables found in ' + mod); return; }
    for(const spec of arr){
      const def = makeAutogenDef(spec);
      const id = def.id;
      if(!id) continue;
      // upsert
      registry.nodes.set(id, def);
      const pkgName = spec.pkg || (spec.call?.target?.split('.')?.[0] || 'autogen');
      if(!registry.packages.some(p=> p.name===pkgName)) registry.packages.push({ name: pkgName, label: pkgName.charAt(0).toUpperCase()+pkgName.slice(1), entry:'' });
      if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []);
      const plist = registry.byPackage.get(pkgName);
      if(!plist.includes(id)) plist.push(id);
    }
    renderToolbar();
  try{ renderPackagesList(); }catch(e){}
    appendLog('[import] added ' + arr.length + ' node(s) from ' + mod);
    // Quality check newly generated nodes
    try{
      const testRes = await apiFetch('/api/autogen/test', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ module: mod, maxTests: 8 }) });
      const testJs = await testRes.json().catch(()=>({items:[]}));
      const items = Array.isArray(testJs.items)? testJs.items: [];
      const ok = items.filter(x=> x && x.ok).length; const ng = items.length - ok;
      appendLog(`[autogen:test] ${ok} OK / ${ng} NG for ${mod}`);
      items.forEach(it=>{ if(!it.ok) appendLog(`[autogen:test:NG] ${it.id}: ${it.error||'error'}`); });
  try{ autogenTestResults.set(mod, { ok, ng, items }); renderPackagesList(); }catch(e){}
  }catch(e){}
  }catch(e){ appendLog('[import] failed'); }
}

// Introspect a dotted target (e.g., pandas.DataFrame.merge) and register nodes
async function introspectTarget(target){
  try{
    const res = await apiFetch('/api/introspect?target=' + encodeURIComponent(target));
    const js = await res.json().catch(()=>({}));
    const arr = Array.isArray(js.nodes) ? js.nodes : [];
    if(!arr.length){ appendLog('[autogen] no nodes for ' + target); return; }
    for(const spec of arr){
      const def = makeAutogenDef(spec);
      const id = def.id; if(!id) continue;
      registry.nodes.set(id, def);
      const pkgName = spec.pkg || (spec.call?.target?.split('.')?.[0] || 'autogen');
      if(!registry.packages.some(p=> p.name===pkgName)) registry.packages.push({ name: pkgName, label: pkgName.charAt(0).toUpperCase()+pkgName.slice(1), entry:'' });
      if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []);
      const arrList = registry.byPackage.get(pkgName);
      if(!arrList.includes(id)) arrList.push(id);
    }
    renderToolbar();
  try{ renderPackagesList(); }catch(e){}
    appendLog('[autogen] added ' + arr.length + ' node(s) from ' + target);
    // Quality check for dotted target's root module
    try{
      const root = String(target||'').split('.')[0] || '';
      if(root){
        const testRes = await apiFetch('/api/autogen/test', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ module: root, maxTests: 6 }) });
        const testJs = await testRes.json().catch(()=>({items:[]}));
        const items = Array.isArray(testJs.items)? testJs.items: [];
        const ok = items.filter(x=> x && x.ok).length; const ng = items.length - ok;
        appendLog(`[autogen:test] ${ok} OK / ${ng} NG for ${root}`);
        items.forEach(it=>{ if(!it.ok) appendLog(`[autogen:test:NG] ${it.id}: ${it.error||'error'}`); });
  try{ autogenTestResults.set(root, { ok, ng, items }); renderPackagesList(); }catch(e){}
      }
  }catch(e){}
  }catch(e){ appendLog('[autogen] failed for ' + target); }
}
// Basic preview dock placeholder (used by WS adapter)
function updatePreviewDock(){}
// Geometry helpers used across UI
function centerOf(el){ const r = el.getBoundingClientRect(); const p = edgesSvg.getBoundingClientRect(); return { x: r.left - p.left + r.width/2, y: r.top - p.top + r.height/2 }; }
function getScale(){ return state.view?.scale || 1; }
function getTx(){ return state.view?.tx || 0; }
function getTy(){ return state.view?.ty || 0; }
function screenToWorldPoint(clientX, clientY){ const rect = canvasWrap.getBoundingClientRect(); const x = clientX - rect.left; const y = clientY - rect.top; const s = getScale(); return { x: (x - getTx())/s, y: (y - getTy())/s }; }
function applyViewTransform(){ const s=getScale(), tx=getTx(), ty=getTy(); nodesEl.style.transformOrigin='0 0'; nodesEl.style.transform = `translate(${tx}px, ${ty}px) scale(${s})`; }
// Ensure form state is synced before codegen (inputs are already bound by forms.js; keep as lightweight guard)
function syncFormsToState(){ try{ /* forms are live-bound; nothing extra needed */ }catch(e){} }
function ensureWS(){
  if(!wsCtl){
    const buildUrl = ()=>{ const proto=(location.protocol==='https:'?'wss://':'ws://'); const tokenQs = (authRequired && authToken) ? ('?token='+encodeURIComponent(authToken)) : ''; return proto + location.host + '/ws' + tokenQs; };
    wsCtl = initWS({
      buildUrl,
      appendLog,
      updateRunButtonsState,
      onKernelDisabled: ()=>{ kernelDisabled = true; statusEl.textContent='kernel disabled'; },
      onIdle: ()=>{ runningLock=false; statusEl.textContent='idle'; updateRunButtonsState(); },
      onBusy: ()=>{ runningLock=true; updateRunButtonsState(); },
      refreshVariables,
      state,
      registry,
      getNode,
      getPreviewMode,
      isFigureNode,
      updateNodePreview: (id)=> updateNodePreview(id),
      apiFetch,
      renderToolbar,
      updatePreviewDock
    });
  }
  wsCtl.ensureWS();
}
// Global Run All button setup (create once)
try{
  globalRunBtn = document.createElement('button');
  globalRunBtn.textContent='▶ Run All';
  Object.assign(globalRunBtn.style, { padding:'6px 10px', background:'#1f6feb', color:'#fff', border:'0', borderRadius:'6px', cursor:'pointer' });
  globalRunBtn.addEventListener('click', async ()=>{
    await runWithBusy(async ()=>{
      ensureWS(); clearLog(); statusEl.textContent='running...';
      syncFormsToState();
      let code = genCode(); code = sanitizePython(code); genCodeEl.textContent = code;
  try{ if(wsCtl && wsCtl.setPendingVarsRefresh) wsCtl.setPendingVarsRefresh(true); }catch(e){}
      const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) });
  let js={}; try{ js=await res.json(); }catch(e){}
      appendLog('Sent exec: ' + JSON.stringify(js));
    }, globalRunBtn, 'Running...');
  });
}catch(e){}
// Run state helpers
function updateRunButtonsState(){
  try{
    const disabled = !!runningLock || !!kernelDisabled;
    document.querySelectorAll('.node .node-run').forEach(b=>{ b.disabled = disabled; });
    if(globalRunBtn) globalRunBtn.disabled = disabled;
  }catch(e){}
}
async function runWithBusy(fn, btn, busyText='Working...'){
  const old = btn && btn.textContent;
  try{
    runningLock = true; updateRunButtonsState(); if(btn){ btn.disabled=true; if(busyText) btn.textContent = busyText; }
    await fn();
  }finally{
    runningLock = false; updateRunButtonsState(); if(btn){ btn.disabled=false; if(old!=null) btn.textContent = old; }
  }
}

// 選択ハイライトをDOMへ反映
function refreshSelectionHighlight(){
  const S = new Set(state.selection||[]);
  document.querySelectorAll('.node').forEach(el=>{
    const id = el.getAttribute('data-node-id');
    if(!id) return;
    if(S.has(id)) el.classList.add('selected'); else el.classList.remove('selected');
  });
}

// Packages board: Left = importable toggles (installed/declared), Right = PyPI results to drag for pip install
async function renderPackagesList(){
  if(!pkgsWrap) return;
  try{
    // Left column: declared/installed packages with toggle to import (deduped)
  await loadPackages();
  await ensureAutogenOnlyNames();
    const declared = Array.isArray(registry.packages) ? registry.packages : [];
    const names = declared.map(p=> p.name);
    const versions = new Map();
    installedPkgs.clear();
    if(names.length){
      try{
        const js = await (await apiFetch('/api/modules/versions?names=' + encodeURIComponent(names.join(',')))).json();
        (js.items||[]).forEach(x=> { versions.set(x.name, x.version||''); if(x.version) installedPkgs.add(x.name); });
  }catch(e){}
    }
    const togglesHtml = declared
      .filter(p=>{
        if(!showAutogenNgOnly) return true;
        const r = autogenTestResults.get(p.name);
        return r && r.ng > 0;
      })
      .map(p=>{
  const ver = versions.get(p.name) || '';
      const nodeCount = (registry.byPackage.get(p.name)||[]).length;
      const test = autogenTestResults.get(p.name);
      const ngBadge = test && test.ng>0 ? `<span class="badge badge-ng" title="Autogen test NG">NG:${test.ng}</span>` : '';
      const okBadge = test && test.ok>0 ? `<span class="badge badge-ok" title="Autogen test OK">OK:${test.ok}</span>` : '';
  const autoBadge = isAutogenOnlyName(p.name) ? `<span class="badge" title="Autogen only (stdlib or no UI)">Autogen</span>` : '';
      // 初期状態は常にOFF（未インポート）。ただしこのセッションでユーザーがONにしたものはON+disabledで表示。
      const isUserImported = importedByUser.has(p.name);
      const alreadyInstalled = installedPkgs.has(p.name);
      return `<div class="pkg-toggle" data-name="${escapeHtmlUtil(p.name)}">
        <div>
          <div style="display:flex; gap:6px; align-items:center;">
    <div>${escapeHtmlUtil(p.label||p.name)}</div>
    ${autoBadge} ${ngBadge} ${okBadge}
          </div>
          <div class="meta">${escapeHtmlUtil(ver||'-')} • ${nodeCount} nodes</div>
        </div>
        <label class="switch"><input type="checkbox" ${isUserImported? 'checked disabled': ''} ${alreadyInstalled && !isUserImported? '': ''}><span class="slider"></span></label>
      </div>`;
    }).join('');

    // Right column: PyPI results, draggable to left to install
    const rightHtml = `<div style="display:flex; gap:8px; margin-bottom:8px"><input id="pypi_q2" class="input" placeholder="Search PyPI" style="flex:1"><button id="pypi_go2" class="secondary">Search</button></div><div id="pypi_list2" class="pkg-list"></div>`;
    pkgsWrap.innerHTML = `
      <div class="pkg-col" id="colLeft">
        <h3 style="display:flex; align-items:center; gap:8px;">
          インポート可能（重複防止）
          <label style="display:flex; align-items:center; gap:6px; font-weight:normal; font-size:12px;">
            <input id="pf_autogen_ng_only" type="checkbox" ${showAutogenNgOnly? 'checked':''}>
            <span>Autogen NGのみ</span>
          </label>
        </h3>
        <div id="left_list" class="pkg-list">${togglesHtml || '<div style="color:#9ba3af; padding:6px">（なし）</div>'}</div>
      </div>
      <div class="pkg-col" id="colRight"><h3>PyPI 検索結果（ドラッグで左にPIP + インポート）</h3>${rightHtml}</div>`;
    // Filter toggle wiring
    const ngOnly = document.getElementById('pf_autogen_ng_only');
    if(ngOnly){
      ngOnly.addEventListener('change', ()=>{ showAutogenNgOnly = !!ngOnly.checked; renderPackagesList(); });
    }

    // Wire toggles: on check = introspectAndRegister if not already
    pkgsWrap.querySelectorAll('.pkg-toggle input[type="checkbox"]').forEach(chk=>{
      chk.addEventListener('change', async ()=>{
        const name = chk.closest('.pkg-toggle')?.getAttribute('data-name'); if(!name) return;
  // ONにしたらインポート実行（既に登録済みでも内部で重複防止）
        if(chk.checked){
          chk.disabled = true;
          const ok = await importPackageFlow(name);
          if(ok){ try{ importedByUser.add(name); }catch(e){} }
          await renderPackagesList();
          renderToolbar();
        }
      });
    });

    // Right: search and render results
    async function loadPyPIList(q){
      const listEl = document.getElementById('pypi_list2'); listEl.innerHTML = '<div style="color:#9ba3af; padding:6px">Loading…</div>';
  try{
        const js = await (await apiFetch('/api/pypi/search?q=' + encodeURIComponent(q||'') + '&limit=50')).json();
        if((!js.items || js.items.length===0) && js.error){
          listEl.innerHTML = `<div style="color:#fca5a5; padding:6px">検索に失敗しました（${String(js.error)}）。右上メニューの「Install from PyPI」で名前指定インストールも可能です。</div>`;
          return;
        }
        const items = Array.isArray(js.items)? js.items: [];
        const rows = items.map(x=> {
          const nm = String(x.name||'');
          const disabled = installedPkgs.has(nm) ? 'disabled' : '';
          const label = installedPkgs.has(nm) ? 'Installed' : 'Install';
          return `<div class=\"pkg-item\" draggable=\"true\" data-name=\"${escapeHtmlUtil(nm)}\"><div style=\"display:flex; gap:8px; align-items:flex-start; justify-content:space-between;\"><div><div>${escapeHtmlUtil(nm)}</div><div class=\"meta\">${escapeHtmlUtil(x.version||'-')} • ${escapeHtmlUtil(x.summary||'')}</div></div><div><button class=\"mini act-install\" ${disabled}>${label}</button></div></div></div>`;
        }).join('');
        listEl.innerHTML = rows || '<div style="color:#9ba3af; padding:6px">No results</div>';
        // draggable
        listEl.querySelectorAll('.pkg-item').forEach(el=>{
          el.addEventListener('dragstart', (e)=>{ const name = el.getAttribute('data-name')||''; try{ e.dataTransfer.setData('text/plain', name); }catch(e2){} e.dataTransfer.effectAllowed='copy'; el.classList.add('dragging'); });
          el.addEventListener('dragend', ()=> el.classList.remove('dragging'));
        });
        // clickable install
        listEl.querySelectorAll('.act-install').forEach(btn=>{
          btn.addEventListener('click', async (e)=>{
            const root = e.target.closest('.pkg-item'); const name = root && root.getAttribute('data-name'); if(!name) return;
            if(installedPkgs.has(name)) return; // guard
            await ensureAutogenOnlyNames();
            const ok = isAutogenOnlyName(name) ? (await introspectAndRegister(name), true) : await pipInstallAndImport(name, '');
            if(ok){ try{ importedByUser.add(name); installedPkgs.add(name); }catch(e2){} }
            await renderPackagesList(); renderToolbar();
          });
        });
  }catch(e){
        document.getElementById('pypi_list2').innerHTML = '<div style="color:#fca5a5; padding:6px">検索に失敗しました</div>';
      }
    }
  document.getElementById('pypi_go2').onclick = ()=> loadPyPIList(document.getElementById('pypi_q2').value);
  document.getElementById('pypi_q2').addEventListener('keydown', (e)=>{ if(e.key==='Enter') loadPyPIList(e.target.value); });
  // 初期ロードは行わない（開いた瞬間にPandas検索しない）
  const listEl0 = document.getElementById('pypi_list2');
  if(listEl0) listEl0.innerHTML = '<div style="color:#9ba3af; padding:6px">Type to search PyPI…</div>';

    // Enable drop on left column: triggers pip + introspect
    const leftCol = document.getElementById('colLeft');
    function allowDrop(ev){ ev.preventDefault(); ev.dataTransfer.dropEffect='copy'; }
    function onDragOver(ev){ allowDrop(ev); leftCol.classList.add('dragover'); }
    function onDragLeave(){ leftCol.classList.remove('dragover'); }
    leftCol.addEventListener('dragover', onDragOver); leftCol.addEventListener('dragleave', onDragLeave);
    leftCol.addEventListener('drop', async (ev)=>{
      ev.preventDefault(); leftCol.classList.remove('dragover');
      const name = ev.dataTransfer.getData('text/plain'); if(!name) return;
      // install then introspect
      await ensureAutogenOnlyNames();
      if(isAutogenOnlyName(name)){
        await introspectAndRegister(name);
      } else {
        await pipInstallAndImport(name, '');
      }
      await renderPackagesList(); renderToolbar();
    });
  }catch(e){ pkgsWrap.innerHTML = '<div style="color:#9ba3af">一覧の描画に失敗しました</div>'; }
}

// Robust import flow: try import; if fails, pip install; then introspect/register
async function importPackageFlow(name){
  try{
    await ensureAutogenOnlyNames();
    // Already registered?
    if(registry.byPackage && registry.byPackage.get(name) && registry.byPackage.get(name).length){
      appendLog(`[import] ${name} already registered`);
      return true;
    }
    // Try introspect first (module must be installed)
    let res = await apiFetch('/api/introspect_module?module=' + encodeURIComponent(name));
    let js = await res.json().catch(()=>({}));
    let arr = Array.isArray(js.nodes) ? js.nodes : [];
    if(!arr.length){
      // fallback: attempt pip install then introspect (unless autogen-only/stdlib)
      if(isAutogenOnlyName(name)){
        appendLog(`[import] ${name} is autogen-only; skipping pip and trying introspect again`);
        await introspectAndRegister(name);
        return true;
      }
      const installed = await pipInstallAndImport(name, '');
      if(!installed) return false;
      res = await apiFetch('/api/introspect_module?module=' + encodeURIComponent(name));
      js = await res.json().catch(()=>({}));
      arr = Array.isArray(js.nodes) ? js.nodes : [];
    }
    if(!arr.length){ appendLog('[import] no callables found in ' + name); return false; }
    let added = 0;
    for(const spec of arr){
      const def = makeAutogenDef(spec);
      const id = def.id;
      if(!id) continue;
      if(!registry.nodes.has(id)) added++;
      registry.nodes.set(id, def);
      const pkgName = spec.pkg || (spec.call?.target?.split('.')?.[0] || 'autogen');
      if(!registry.packages.some(p=> p.name===pkgName)) registry.packages.push({ name: pkgName, label: pkgName.charAt(0).toUpperCase()+pkgName.slice(1), entry:'' });
      if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []);
      const arrList = registry.byPackage.get(pkgName);
      if(!arrList.includes(id)) arrList.push(id);
    }
    renderToolbar();
  try{ renderPackagesList(); }catch(e){}
    appendLog('[import] added ' + added + ' node(s) from ' + name);
    return true;
  }catch(e){ appendLog('[import] failed'); return false; }
}

// Variables
function filterVars(arr){ try{ return (arr||[]).filter(v=>{ const t = String(v.type||'').toLowerCase(); const n = String(v.name||'').toLowerCase(); if(n==='exit' || n==='quit') return false; if(n==='in' || n==='out') return false; if(n.startsWith('_')) return false; if(t.includes('module')) return false; if(t.includes('function')) return false; if(t.includes('method')) return false; if(t.includes('autocall')) return false; if(t.includes('zmqexitautocall')) return false; return true; }); }catch(e){ return arr||[]; } }
async function refreshVariables(){
  if(!rightVars || rightVars.style.display==='none') return;
  try{
    const res = await apiFetch('/api/variables');
    const js = await res.json();
    const arrRaw = Array.isArray(js.variables) ? js.variables : [];
    const arr = filterVars(arrRaw);
    const rows = arr.map(v=>{
      const name = escapeHtml(v.name);
      const type = escapeHtml(v.type);
      const nameCell = `<span class="var-item" draggable="true" data-var="${name}" title="ドラッグ＆ドロップでノードの入力に上書き"><svg class="drag-handle" viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M4 12h16M4 17h16" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg><span class="var-label">${name}</span></span>`;
      const tLower = String(v.type).toLowerCase();
      if(tLower==='dataframe' && v.html){
        const dims = (typeof v.rows==='number' && typeof v.cols==='number') ? `<div style="color:var(--sub); font-size:11px; margin-top:4px">${v.rows.toLocaleString()} rows × ${v.cols.toLocaleString()} cols</div>` : '';
        const csvBtn = `<button class="btn btn-ghost var-csv" data-var="${name}" title="Download CSV"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3v10m0 0l-3.5-3.5M12 13l3.5-3.5M5 21h14" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Export CSV</button>`;
        const menuBtn = `<button class="btn btn-icon var-menu var-float-menu" data-var="${name}" data-type="${type}" title="Actions">⋯</button>`;
        const actions = `<div class="var-actions">${csvBtn}</div>`;
        // wrap DataFrame HTML in a horizontally scrollable container so全列閲覧可
        return `<tr data-var="${name}" data-type="${type}"><td>${nameCell}</td><td>${type}</td><td><div class="var-cell"><div style="max-width:100%; overflow:auto">${styleTableHtml(v.html)}</div>${dims}${actions}${menuBtn}</div></td></tr>`;
      }
      if(tLower==='ndarray'){
        const val = (v.repr!=null? String(v.repr): '');
        const shp = Array.isArray(v.shape) ? v.shape.join('×') : (v.shape!=null? String(v.shape): '');
        const csvBtn = `<button class="btn btn-ghost var-csv" data-var="${name}" title="Download CSV"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3v10m0 0l-3.5-3.5M12 13l3.5-3.5M5 21h14" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Export CSV</button>`;
        const menuBtn = `<button class="btn btn-icon var-menu var-float-menu" data-var="${name}" data-type="${type}" title="Actions">⋯</button>`;
        const actions = `<div class="var-actions">${csvBtn}</div>`;
        return `<tr data-var="${name}" data-type="${type}"><td>${nameCell}</td><td>${type}</td><td><div class="var-cell">${escapeHtml(val).slice(0,200)} <span style=\"color:var(--sub); font-size:11px;\">${shp}</span>${actions}${menuBtn}</div></td></tr>`;
      }
      const val = (v.repr!=null? String(v.repr): (v.value!=null? String(v.value): ''));
      const menuBtn = `<button class="btn btn-icon var-menu var-float-menu" data-var="${name}" data-type="${type}" title="Actions">⋯</button>`;
      return `<tr data-var="${name}" data-type="${type}"><td>${nameCell}</td><td>${type}</td><td><div class="var-cell">${escapeHtml(val).slice(0,200)}${menuBtn}</div></td></tr>`;
    }).join('');
    // 横スクロールを可能にするため、テーブルのcol幅固定を外し、全体をoverflow:autoで包む
    varsWrap.innerHTML = `<div style="width:100%; overflow:auto"><table style="min-width:480px; border-collapse:collapse; font-size:12px;"><thead><tr><th style=\"text-align:left; border-bottom:1px solid #263041; padding:4px 6px;\">名前</th><th style=\"text-align:left; border-bottom:1px solid #263041; padding:4px 6px;\">型</th><th style=\"text-align:left; border-bottom:1px solid #263041; padding:4px 6px;\">値</th></tr></thead><tbody style="word-break:break-word;">${rows || '<tr><td colspan=\"3\" style=\"padding:6px; color:#9ba3af;\">変数がありません</td></tr>'}</tbody></table></div>`;
    // Imports section appended
    try{
      const parent = varsWrap.parentElement;
      if(parent){
        const old = parent.querySelector('#importsWrap')?.parentElement;
        if(old) old.remove();
        const importsBox = await renderImportsList();
        parent.appendChild(importsBox);
      }
  }catch(e){}
    // Make variables draggable
    varsWrap.querySelectorAll('.var-item').forEach(el=>{
      el.addEventListener('dragstart', (e)=>{
        const name = el.getAttribute('data-var') || el.textContent || '';
  try{ e.dataTransfer.setData('text/plain', name); }catch(e2){}
        e.dataTransfer.effectAllowed = 'copy';
        el.classList.add('dragging');
      });
      el.addEventListener('dragend', ()=> el.classList.remove('dragging'));
    });
    // CSV export buttons
    varsWrap.querySelectorAll('.var-csv').forEach(btn=>{
      btn.addEventListener('click', async ()=>{
        const name = btn.getAttribute('data-var'); if(!name) return;
        try{
          const url = `/api/variables/${encodeURIComponent(name)}/export?format=csv`;
          await downloadFrom(url, `${name}.csv`);
        }catch(e){ appendLog('[export] failed'); }
      });
    });
    // Actions menu buttons
    varsWrap.querySelectorAll('.var-menu').forEach(btn=>{
      btn.addEventListener('click', (e)=>{
        const name = btn.getAttribute('data-var'); const type = btn.getAttribute('data-type')||'';
        const r = btn.getBoundingClientRect();
        openVarActionsMenu(name, type, r.left, r.bottom+2);
      });
    });
    // Row context menu
    varsWrap.querySelectorAll('tr[data-var]').forEach(tr=>{
      tr.addEventListener('contextmenu', (e)=>{
        e.preventDefault(); const name = tr.getAttribute('data-var'); const type = tr.getAttribute('data-type')||''; openVarActionsMenu(name, type, e.clientX, e.clientY);
      });
    });
    // Double-click preview for DataFrame
    varsWrap.querySelectorAll('tr[data-type]').forEach(tr=>{
      const t = (tr.getAttribute('data-type')||'').toLowerCase();
      if(t==='dataframe'){
        tr.addEventListener('dblclick', ()=>{
          const name = tr.getAttribute('data-var'); if(name) openVarPreview(name);
        });
      }
    });
  }catch(e){
    varsWrap.innerHTML = '<div style="color:#9ba3af">変数の取得に失敗しました</div>';
  }
}

async function openVarActionsMenu(name, type, x, y){
  const tLower = String(type||'').toLowerCase();
  const items = [];
  function add(label, fn, disabled){ items.push({ key: label, label, disabled: !!disabled, onClick: fn }); }
  function sep(){ items.push({ key:'sep'+Math.random(), label:'-', separator:true }); }
  async function openHead(){ const res = await apiFetch(`/api/variables/${encodeURIComponent(name)}/head?rows=50`); const js = await res.json().catch(()=>({})); if(js && js.columns && js.data){ openGridModal(`${name} • head`, js.columns, js.data); } else openMessageModal('head', JSON.stringify(js)); }
  async function openTail(){ const r = window.prompt('rows (default 50):','50'); if(r==null) return; const res = await apiFetch(`/api/variables/${encodeURIComponent(name)}/tail?rows=${encodeURIComponent(r)}`); const js = await res.json().catch(()=>({})); if(js && js.columns && js.data){ openGridModal(`${name} • tail`, js.columns, js.data); } else openMessageModal('tail', JSON.stringify(js)); }
  async function openSample(){ const r = window.prompt('rows (default 50):','50'); if(r==null) return; const res = await apiFetch(`/api/variables/${encodeURIComponent(name)}/sample?rows=${encodeURIComponent(r)}`); const js = await res.json().catch(()=>({})); if(js && js.columns && js.data){ openGridModal(`${name} • sample`, js.columns, js.data); } else openMessageModal('sample', JSON.stringify(js)); }
  async function openRows(){ const off = window.prompt('offset:','0'); if(off==null) return; const lim = window.prompt('limit:','100'); if(lim==null) return; const res=await apiFetch(`/api/variables/${encodeURIComponent(name)}/rows?offset=${encodeURIComponent(off)}&limit=${encodeURIComponent(lim)}`); const js=await res.json().catch(()=>({})); if(js && js.columns && js.data){ openGridModal(`${name} • rows ${off}-${Number(off)+Number(lim)-1}`, js.columns, js.data); } else openMessageModal('rows', JSON.stringify(js)); }
  async function openCols(){ const pat = window.prompt('column regex (optional):',''); const url = `/api/variables/${encodeURIComponent(name)}/columns${pat? ('?pattern='+encodeURIComponent(pat)) : ''}`; const js = await (await apiFetch(url)).json().catch(()=>({})); if(js && js.columns){ const rows = js.columns.map(c=> [c, (js.dtypes && js.dtypes[c]) || '']); openGridModal(`${name} • columns`, ['column','dtype'], rows); } else openMessageModal('columns', JSON.stringify(js)); }
  async function openDescribe(){ const cols = window.prompt('columns (comma, optional):',''); const url = `/api/variables/${encodeURIComponent(name)}/describe${cols? ('?columns='+encodeURIComponent(cols)) : ''}`; const js = await (await apiFetch(url)).json().catch(()=>({})); if(js && js.describe){ // flatten describe dict-of-dict
      const metrics = Object.keys(js.describe||{}); const colSet = new Set(); metrics.forEach(m=> Object.keys(js.describe[m]||{}).forEach(c=> colSet.add(c)));
      const columns = ['metric', ...Array.from(colSet)]; const rows = metrics.map(m=> [m, ...Array.from(colSet).map(c=> (js.describe[m]&&js.describe[m][c]!=null)? js.describe[m][c] : '')]);
      openGridModal(`${name} • describe`, columns, rows);
    } else openMessageModal('describe', JSON.stringify(js)); }
  async function openCorr(){ const cols = window.prompt('columns (comma, optional):',''); const url = `/api/variables/${encodeURIComponent(name)}/corr${cols? ('?columns='+encodeURIComponent(cols)) : ''}`; const js = await (await apiFetch(url)).json().catch(()=>({})); if(js && js.matrix){ const columns = js.columns||[]; const rows = (js.matrix||[]).map((r,i)=> [columns[i], ...r]); openGridModal(`${name} • corr`, [''] .concat(columns), rows); } else openMessageModal('corr', JSON.stringify(js)); }
  async function openVCounts(){ const col = window.prompt('column:'); if(!col) return; const lim = window.prompt('limit:','20'); if(lim==null) return; const url = `/api/variables/${encodeURIComponent(name)}/value_counts?column=${encodeURIComponent(col)}&limit=${encodeURIComponent(lim)}`; const js = await (await apiFetch(url)).json().catch(()=>({})); if(js && js.items){ openGridModal(`${name} • value_counts(${col})`, ['value','count'], js.items); } else openMessageModal('value_counts', JSON.stringify(js)); }
  async function openUnique(){ const col = window.prompt('column:'); if(!col) return; const lim = window.prompt('limit:','100'); if(lim==null) return; const url = `/api/variables/${encodeURIComponent(name)}/unique?column=${encodeURIComponent(col)}&limit=${encodeURIComponent(lim)}`; const js = await (await apiFetch(url)).json().catch(()=>({})); if(js && js.values){ openGridModal(`${name} • unique(${col})`, ['value'], js.values.map(v=> [v])); } else openMessageModal('unique', JSON.stringify(js)); }
  async function openHist(){ const col = window.prompt('numeric column:'); if(!col) return; const bins = window.prompt('bins:','20'); if(bins==null) return; const url = `/api/variables/${encodeURIComponent(name)}/histogram?column=${encodeURIComponent(col)}&bins=${encodeURIComponent(bins)}`; const js = await (await apiFetch(url)).json().catch(()=>({})); if(js && js.counts && js.edges){ const cols=['bin_from','bin_to','count']; const rows=[]; for(let i=0;i<js.counts.length;i++){ rows.push([js.edges[i], js.edges[i+1], js.counts[i]]); } openGridModal(`${name} • hist(${col})`, cols, rows); } else openMessageModal('hist', JSON.stringify(js)); }
  async function openFilter(){ try{ const tmpl = '{"filters":[{"column":"species","op":"eq","value":"setosa"}],"limit":50,"sort_by":"","descending":false}'; const raw = window.prompt('Filter JSON (example shown):', tmpl); if(!raw) return; const body = JSON.parse(raw); const res = await apiFetch(`/api/variables/${encodeURIComponent(name)}/filter`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body)}); const js = await res.json().catch(()=>({})); if(js && js.columns && js.data){ openGridModal(`${name} • filter`, js.columns, js.data); } else openMessageModal('filter', JSON.stringify(js)); }catch(e){ openMessageModal('filter', 'invalid JSON'); } }
  async function doExport(fmt){ const ext = (fmt==='jsonl')? 'jsonl' : (fmt==='pickle')? 'pkl' : fmt; await downloadFrom(`/api/variables/${encodeURIComponent(name)}/export?format=${encodeURIComponent(fmt)}`, `${name}.${ext}`); }
  async function doRename(){ const to = window.prompt('new variable name:'); if(!to) return; const js = await (await apiFetch(`/api/variables/${encodeURIComponent(name)}/rename`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ to }) })).json().catch(()=>({})); if(js && js.ok){ appendLog(`[var] renamed to ${to}`); refreshVariables(); } else openMessageModal('rename', JSON.stringify(js)); }
  async function doDelete(){ if(!window.confirm(`Delete variable ${name}?`)) return; const js = await (await apiFetch(`/api/variables/${encodeURIComponent(name)}`, { method:'DELETE' })).json().catch(()=>({})); if(js && js.ok){ appendLog(`[var] deleted ${name}`); refreshVariables(); } else openMessageModal('delete', JSON.stringify(js)); }
  // DataFrame-rich items
  if(tLower==='dataframe'){
    add('Head', openHead); add('Tail…', openTail); add('Sample…', openSample); add('Rows (paging)…', openRows); sep(); add('Columns…', openCols); add('Describe…', openDescribe); add('Correlation…', openCorr); sep(); add('Value Counts…', openVCounts); add('Unique…', openUnique); add('Histogram…', openHist); add('Filter (JSON)…', openFilter); sep();
  } else {
    // generic preview
    add('Head', openHead);
  }
  // Detect JSON/XML capabilities dynamically for non-DataFrame
  const addJsonXml = async ()=>{
    try{
      const js = await (await apiFetch(`/api/variables/${encodeURIComponent(name)}/detect_format`)).json().catch(()=>({}));
      if(js && js.format==='json'){
        add('JSON Preview…', async ()=>{ const path = window.prompt('path (e.g., a.b[0])',''); const url = `/api/variables/${encodeURIComponent(name)}/json/preview${path? ('?path='+encodeURIComponent(path)) : ''}`; const r = await (await apiFetch(url)).json().catch(()=>({})); if(r && r.columns && r.data){ openGridModal(`${name} • json preview`, r.columns, r.data); } else openMessageModal('json preview', JSON.stringify(r)); });
        add('JSON Schema…', async ()=>{ const path = window.prompt('path (optional)',''); const url = `/api/variables/${encodeURIComponent(name)}/json/schema${path? ('?path='+encodeURIComponent(path)) : ''}`; const r = await (await apiFetch(url)).json().catch(()=>({})); openMessageModal('json schema', JSON.stringify(r, null, 2)); });
      } else if(js && js.format==='xml'){
        add('XML Preview…', async ()=>{ const xp = window.prompt('xpath (e.g., .//item)', './/'); const url = `/api/variables/${encodeURIComponent(name)}/xml/preview${xp? ('?xpath='+encodeURIComponent(xp)) : ''}`; const r = await (await apiFetch(url)).json().catch(()=>({})); if(r && r.columns && r.data){ openGridModal(`${name} • xml preview`, r.columns, r.data); } else openMessageModal('xml preview', JSON.stringify(r)); });
        add('XML Tags…', async ()=>{ const r = await (await apiFetch(`/api/variables/${encodeURIComponent(name)}/xml/tags`)).json().catch(()=>({})); if(r && r.tags){ openGridModal(`${name} • xml tags`, ['tag','count'], r.tags); } else openMessageModal('xml tags', JSON.stringify(r)); });
      }
  }catch(e){}
    // open after async population
    openContextMenu(items, x, y);
  };
  // Common actions
  sep();
  const exportSubmenu = [
    { key:'csv', label:'CSV', onClick: ()=> doExport('csv') },
    { key:'json', label:'JSON', onClick: ()=> doExport('json') },
    { key:'jsonl', label:'JSON Lines', onClick: ()=> doExport('jsonl') },
    { key:'parquet', label:'Parquet', onClick: ()=> doExport('parquet') },
    { key:'pickle', label:'Pickle', onClick: ()=> doExport('pickle') },
    { key:'npy', label:'NumPy .npy', onClick: ()=> doExport('npy') },
  ];
  items.push({ key:'export', label:'Export ▸', children: exportSubmenu });
  add('Rename…', doRename); add('Delete…', doDelete);
  // If not dataframe, detect json/xml asynchronously; otherwise just open now
  if(tLower!=='dataframe') await addJsonXml(); else openContextMenu(items, x, y);
}

function activateTab(which){
  // reset
  rightCode.style.display='none'; rightVars.style.display='none'; rightPkgs && (rightPkgs.style.display='none');
  // ensure scrollability – guard against stale CSS
  [rightCode, rightVars, rightPkgs].forEach(el=>{ if(el){ el.style.overflow='auto'; el.style.minHeight='0'; } });
  tabCode?.classList.remove('active'); tabVars?.classList.remove('active'); tabPkgs?.classList.remove('active');
  tabCode?.setAttribute('aria-selected','false'); tabVars?.setAttribute('aria-selected','false'); tabPkgs?.setAttribute('aria-selected','false');
  if(which==='code'){
    rightCode.style.display='block'; tabCode?.classList.add('active'); tabCode?.setAttribute('aria-selected','true');
  } else if(which==='vars'){
    rightVars.style.display='block'; tabVars?.classList.add('active'); tabVars?.setAttribute('aria-selected','true'); refreshVariables();
  } else {
    rightPkgs && (rightPkgs.style.display='block'); tabPkgs?.classList.add('active'); tabPkgs?.setAttribute('aria-selected','true'); renderPackagesList();
  }
}
tabCode?.addEventListener('click', ()=> activateTab('code'));
tabVars?.addEventListener('click', ()=> activateTab('vars'));
tabPkgs?.addEventListener('click', ()=> activateTab('pkgs'));
previewModeEl?.addEventListener('change', ()=>{ render(); const figs = state.nodes.filter(isFigureNode); if(figs.length){ state.lastPlotNodeId = figs[figs.length-1].id; } });

function syncEdgesViewport(){ syncEdgesViewportMod(canvasWrap, edgesSvg); }
function drawEdges(){
  drawEdgesMod(state, edgesSvg, nodesEl, canvasWrap);
}
function getPortCenter(nodeId, selector){ const nodeEl = document.querySelector(`[data-node-id="${nodeId}"]`); if(!nodeEl) return null; const port = nodeEl.querySelector(selector); if(!port) return null; return centerOf(port); }
function setGhost(toX, toY){ setGhostMod(state, edgesSvg, toX, toY); }
function clearGhost(){ clearGhostMod(); }

// Quick Add (module)
const openQuickAdd = (x, y, fromId)=> openQuickAddMod(x, y, fromId, suggestionsForNode, addAndConnect);
const closeQuickAdd = ()=> closeQuickAddMod();
function addAndConnect(type, fromId){ const srcEl = document.querySelector(`[data-node-id="${fromId}"]`); const x = (parseInt(srcEl?.style.left||'0')||0) + 280; const y = (parseInt(srcEl?.style.top||'0')||0); const n = addNode(type, x, y); state.edges = state.edges.filter(e=> !(e.from===fromId && e.to===n.id)); state.edges.push({ from: fromId, to: n.id }); selectNode(n.id); drawEdges(); refreshForms(); closeQuickAdd(); state.pendingSrc = null; render(); }

// form binding is handled by forms.bindForm (imported)

function createNodeEl(node){
  const def = registry.nodes.get(node.type);
  const el = document.createElement('div');
  el.className='node';
  el.dataset.nodeId = node.id;
  el.style.left = (node.x||80) + 'px';
  el.style.top = (node.y||80) + 'px';
  el.style.width = Math.max(160, node.w || 220) + 'px';
  if(isSelected(node.id)) el.classList.add('selected');
  const pmode = getPreviewMode();
  const wantPreview = (pmode==='all') || (pmode==='plots' && isFigureNode(node));
  const title = def?.title || node.type.split('.').slice(-1)[0];
  const label = title; const typeLabel = node.type;
  const previewH = Math.max(80, Math.min(500, node.prevH || 140));
  el.innerHTML = `
  <div class=\"head\">
    <div class=\"title\">${label}</div>
    <div class=\"type\">${typeLabel}</div>
  </div>
  <div class=\"ports\">
    <div class=\"port in\"></div>
    <div class=\"port out\"></div>
  </div>
  <div class=\"body\">${(def && typeof def.form==='function')? def.form(node, { getUpstreamColumns: ()=> computeUpstreamColumns(node), getUpstreamNode: ()=> upstreamOf(node) }): ''}</div>
  <div class=\"preview\" style=\"${wantPreview ? '' : 'display:none;'}max-height:${previewH}px\"> 
    <div id=\"prev-${node.id}\"></div> 
    <div class=\"node-resize-v\" title=\"Drag to resize preview height\"></div> 
  </div>
  <div class=\"actions\">
    <button class=\"node-run btn-primary\">Run</button>
    <button class=\"node-del btn-icon danger\" title=\"Delete\" aria-label=\"Delete\">
      <svg viewBox=\"0 0 24 24\" fill=\"none\" stroke=\"currentColor\" stroke-width=\"2\" stroke-linecap=\"round\" stroke-linejoin=\"round\">
        <polyline points=\"3 6 5 6 21 6\"></polyline>
        <path d=\"M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2\"></path>
        <line x1=\"10\" y1=\"11\" x2=\"10\" y2=\"17\"></line>
        <line x1=\"14\" y1=\"11\" x2=\"14\" y2=\"17\"></line>
      </svg>
    </button>
  </div>
  <div class=\"node-resize-h\" title=\"Drag to resize width\"></div>`;
  el.querySelector('.node-del').addEventListener('click', ()=>{ deleteNodeById(node.id); render(); saveToLocal(); });
    // Run this node (exec upstream + this)
    const runBtn = el.querySelector('.node-run');
    if(runBtn){
      runBtn.addEventListener('click', async ()=>{
        await runWithBusy(async()=>{
          ensureWS(); statusEl.textContent='running...';
          syncFormsToState();
          let code = genCodeUpTo(node.id); code = sanitizePython(code); genCodeEl.textContent = code;
          try{ if(wsCtl && wsCtl.setPendingVarsRefresh) wsCtl.setPendingVarsRefresh(true); }catch(e){}
          const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) });
          let js={}; try{ js=await res.json(); }catch(e){}
          appendLog('Sent exec (node '+node.id+'): ' + JSON.stringify(js));
        }, runBtn, 'Running...');
      });
    }
  // 追加: ノード右クリックメニュー
  el.addEventListener('contextmenu', (e)=>{ e.preventDefault(); e.stopPropagation(); openNodeContextMenu(node.id, e.clientX, e.clientY); });
  // 左クリック: 単一選択（フォーム/ポート/リサイズは除外）
  el.addEventListener('mousedown', (e)=>{
    if(e.button!==0) return;
    if(e.target.closest('input, textarea, select, button, a, .port, .node-resize-h, .node-resize-v')) return;
    if(e.shiftKey){
      if(isSelected(node.id)) removeFromSelection(node.id); else addToSelection(node.id);
    } else {
      setSelection([node.id]);
    }
    refreshSelectionHighlight();
    saveToLocal();
  });
  // ポート: 左クリック接続（Out -> In）とサジェスト表示
  const outPort = el.querySelector('.port.out');
  const inPort = el.querySelector('.port.in');
  function clearPendingConnectionUI(){
    state.pendingSrc = null;
  try{ clearGhost(); }catch(e){}
  try{ closeQuickAdd(); }catch(e){}
    document.querySelectorAll('.port.out.selected').forEach(p=> p.classList.remove('selected'));
  }
  if(outPort){
    outPort.addEventListener('mousedown', (e)=>{
      if(e.button!==0) return;
      e.preventDefault(); e.stopPropagation();
      // トグル
      if(state.pendingSrc===node.id){ clearPendingConnectionUI(); return; }
      document.querySelectorAll('.port.out.selected').forEach(p=> p.classList.remove('selected'));
      state.pendingSrc = node.id;
      outPort.classList.add('selected');
  // Quick Add は空白クリック時に表示する（ここでは表示しない）
    });
  }
  if(inPort){
    const finish = (e)=>{
      if(e.button!==0) return;
      if(!state.pendingSrc) return;
      e.preventDefault(); e.stopPropagation();
      const from = state.pendingSrc; const to = node.id;
      if(from && from!==to){
        const exists = state.edges.some(ed=> ed.from===from && ed.to===to);
        if(!exists){ state.edges.push({ from, to }); drawEdges(); saveToLocal(); }
      }
      clearPendingConnectionUI();
      render();
    };
    inPort.addEventListener('mouseup', finish);
    inPort.addEventListener('click', finish);
  }
  // Param ports: click while an out connection is armed to bind to that specific parameter
  el.querySelectorAll('.param-port').forEach(pp=>{
    pp.addEventListener('click', (e)=>{
      const param = pp.getAttribute('data-param'); if(!param) return;
      if(!state.pendingSrc){ return; }
      e.preventDefault(); e.stopPropagation();
      const srcId = state.pendingSrc;
      // record param binding to upstream variable name
      try {
        // ask nodes.js to compute source variable name for srcId
        const upNode = getNode(srcId);
        const varName = upNode ? ('v_'+upNode.id.replace(/[^a-zA-Z0-9_]/g,'')) : null;
        const def = registry.nodes.get(node.type);
        const v = node.params || (node.params = def?.defaultParams ? JSON.parse(JSON.stringify(def.defaultParams)) : {});
        if(varName){ v['__bound__'+param] = varName; }
        // ensure a graph edge exists from srcId -> node.id
        const exists = state.edges.some(ed=> ed.from===srcId && ed.to===node.id);
        if(!exists){ state.edges.push({ from: srcId, to: node.id }); drawEdges(); }
        // mark field as bound for styling and better edge target
        const field = el.querySelector(`.pf-field[data-param="${param}"]`);
        if(field){ field.classList.add('bound'); field.setAttribute('data-bound', varName||''); }
        clearPendingConnectionUI(); render(); saveToLocal();
  }catch(e){}
    });
  });
  // Drag and drop from Variables panel: bind variable to parameter
  try{
    const bodyEl = el.querySelector('.body');
    if(bodyEl){
      bodyEl.addEventListener('dragover', (ev)=>{
        const src = ev.dataTransfer && ev.dataTransfer.types && ev.dataTransfer.types.includes('text/plain');
        if(!src) return;
        const fromVars = ev.dataTransfer.getData('text/plain');
        if(!fromVars) return;
        ev.preventDefault();
        bodyEl.classList.add('dragover');
      });
      bodyEl.addEventListener('dragleave', ()=> bodyEl.classList.remove('dragover'));
      bodyEl.addEventListener('drop', (ev)=>{
        ev.preventDefault(); bodyEl.classList.remove('dragover');
        const name = ev.dataTransfer.getData('text/plain'); if(!name) return;
        // Find the nearest pf-field under pointer
        const tgt = ev.target.closest('.pf-field');
        if(!tgt) return;
        const param = tgt.getAttribute('data-param'); if(!param) return;
        try{
          const def = registry.nodes.get(node.type);
          const v = node.params || (node.params = def?.defaultParams ? JSON.parse(JSON.stringify(def.defaultParams)) : {});
          // name could be a variable name; we bind to upstream node variable if available; otherwise treat as Python identifier
          const pyVar = /^[A-Za-z_][A-Za-z0-9_]*$/.test(name) ? name : JSON.stringify(name);
          v['__bound__'+param] = pyVar;
          tgt.classList.add('bound');
          tgt.setAttribute('data-bound', String(pyVar));
          render(); saveToLocal();
  }catch(e){}
      });
    }
  }catch(e){}
  return el; }

function renderToolbar(){
  toolbarEl.innerHTML = '';
  // パッケージ順序: AUTOGENノードを含むパッケージを優先
  let pkgs = (registry.packages || []).slice();
  pkgs.sort((a,b)=>{
    const typesA = (registry.byPackage.get(a.name)||[]);
    const typesB = (registry.byPackage.get(b.name)||[]);
    const autA = typesA.some(t=> (registry.nodes.get(t)||{}).origin==='autogen') ? 1 : 0;
    const autB = typesB.some(t=> (registry.nodes.get(t)||{}).origin==='autogen') ? 1 : 0;
    if(autA!==autB) return autB - autA; // autogen含む=先頭
    return a.name.localeCompare(b.name);
  });
  // 未インポート（ノード未登録）のパッケージは左ペインに出さない
  pkgs = pkgs.filter(p=> (registry.byPackage.get(p.name)||[]).length > 0);
  if(pkgs.length===0){
    const empty = document.createElement('div');
    empty.style.color = '#9ba3af';
    empty.style.padding = '8px 10px';
    empty.textContent = 'パッケージはまだありません。右上のPackagesから追加してください。';
    toolbarEl.appendChild(empty);
    return;
  }
  if(!state.activePkg && pkgs[0]) state.activePkg = pkgs[0].name;
  pkgs.forEach(p=>{
    const details = document.createElement('details');
    details.className='pkg-section';
    details.open = (state.activePkg === p.name);
    const summary = document.createElement('summary');
    summary.textContent = p.label || p.name;
    Object.assign(summary.style, { cursor:'pointer', userSelect:'none', padding:'8px 10px' });
    details.appendChild(summary);

    // Build category -> [node types] map for this package
    // AUTOGEN優先で並べ替え
    const types = (registry.byPackage.get(p.name) || [])
      .filter(t=> !(registry.nodes.get(t)?.hidden))
      .slice()
      .sort((t1,t2)=>{
        const d1 = registry.nodes.get(t1)||{}; const d2 = registry.nodes.get(t2)||{};
        const a1 = d1.origin==='autogen'?1:0; const a2 = d2.origin==='autogen'?1:0;
        if(a1!==a2) return a2 - a1; // autogen first
        const c1 = String(d1.category||''); const c2 = String(d2.category||'');
        if(c1!==c2) return c1.localeCompare(c2);
        const n1 = String(d1.title||t1); const n2 = String(d2.title||t2);
        return n1.localeCompare(n2);
      });
    const byCat = new Map();
    for(const t of types){
      const def = registry.nodes.get(t) || {};
      const cat = (def.category || 'General');
      if(!byCat.has(cat)) byCat.set(cat, []);
      byCat.get(cat).push(t);
    }
    // Sort categories: General first, then alphabetically
    const cats = Array.from(byCat.keys()).sort((a,b)=>{
      if(a==='General' && b!=='General') return -1; if(b==='General' && a!=='General') return 1; return a.localeCompare(b);
    });

    // Render each category as its own collapsible section
    const wrap = document.createElement('div');
    wrap.style.padding='6px 6px 8px';
    cats.forEach(cat=>{
      const catDetails = document.createElement('details');
      catDetails.className='cat-section';
      catDetails.open = true;
      const catSummary = document.createElement('summary');
      catSummary.textContent = cat;
      Object.assign(catSummary.style, { cursor:'pointer', userSelect:'none', padding:'6px 8px', fontWeight:'600' });
      catDetails.appendChild(catSummary);
      const listWrap = document.createElement('div');
      listWrap.style.padding='6px 8px';
      (byCat.get(cat)||[]).forEach(type=>{
        const def = registry.nodes.get(type) || {};
        const btn = document.createElement('button');
        const label = (def.origin==='autogen' ? '⭐ ' : '➕ ') + (def.title || type);
        btn.textContent = label;
        btn.style.width = '100%';
        btn.style.marginBottom = '6px';
        btn.dataset.type = type;
        btn.addEventListener('click', ()=>{ addNode(type, 80+Math.random()*200, 80+Math.random()*200); render(); });
        listWrap.appendChild(btn);
      });
      catDetails.appendChild(listWrap);
      wrap.appendChild(catDetails);
    });
    details.appendChild(wrap);

    details.addEventListener('toggle', ()=>{ if(details.open){ state.activePkg = p.name; document.querySelectorAll('#toolbar details.pkg-section').forEach(el=>{ if(el!==details) el.open=false; }); } });
    toolbarEl.appendChild(details);
  });
}

function renderSubsystems(){
  if(!subsystemsEl){ subsystemsEl = document.createElement('div'); subsystemsEl.id='subsystems'; subsystemsEl.style.marginTop = '12px'; toolbarEl.appendChild(subsystemsEl); }
  const items = state.groups || [];
  subsystemsEl.innerHTML = `
    <div style="padding:8px 10px; border-top:1px solid #1f2329; font-weight:600;">Subsystems</div>
    <div style="padding:8px 10px; display:flex; flex-direction:column; gap:6px;">
      ${items.length? items.map(g=>`<div class="sub-item" data-gid="${g.id}" style="display:flex; gap:6px; align-items:center; justify-content:space-between;"><span style="white-space:nowrap; overflow:hidden; text-overflow:ellipsis; max-width:180px;" title="${g.name}">${g.name}</span><span style="font-size:11px; color:#9ba3af">${g.nodeIds.length} nodes</span><span style="margin-left:auto"></span><button class="run-sub" data-gid="${g.id}">Run</button><button class="del-sub" data-gid="${g.id}">Delete</button></div>`).join('') : '<div style="padding:6px; color:#9ba3af;">No subsystems</div>'}
    </div>`;
  subsystemsEl.querySelectorAll('.run-sub').forEach(btn=>{
    btn.addEventListener('click', async ()=>{
      const gid = btn.getAttribute('data-gid'); const g = getGroup(gid); if(!g) return;
      await runWithBusy(async ()=>{
        ensureWS(); statusEl.textContent='running...';
  syncFormsToState();
  let code = genCodeForNodes(g.nodeIds, true); code = sanitizePython(code); genCodeEl.textContent = code;
  try{ if(wsCtl && wsCtl.setPendingVarsRefresh) wsCtl.setPendingVarsRefresh(true); }catch(e){}
  const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) }); let js={}; try{ js=await res.json(); }catch(e){}
  appendLog('Sent exec (group): ' + JSON.stringify(js));
      }, btn, 'Running...');
    });
  });
  subsystemsEl.querySelectorAll('.del-sub').forEach(btn=>{
    btn.addEventListener('click', ()=>{ const gid = btn.getAttribute('data-gid'); state.groups = state.groups.filter(x=> x.id!==gid); renderSubsystems(); saveToLocal(); });
  });
}

function ensureGroupsLayer(){
  if(!groupsLayer){
    groupsLayer = document.createElement('div');
    groupsLayer.id='groupsLayer';
    groupsLayer.style.position='absolute';
    groupsLayer.style.inset='0';
  // Important: don't intercept canvas/nodes interactions in empty areas
  groupsLayer.style.pointerEvents = 'none';
    nodesEl.appendChild(groupsLayer);
  }
  groupsLayer.innerHTML='';
}
function renderGroups(){ ensureGroupsLayer(); if(!Array.isArray(state.groups)) return; const scale=getScale();
  state.groups.forEach(g=>{
    // 存在しないノードIDを除去（枠は残す）
    g.nodeIds = (g.nodeIds||[]).filter(id=> !!getNode(id));
    // バウンディングボックス計算（折りたたみ時は最後のframeを利用）
    let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
    const nodeEls = (g.nodeIds||[]).map(id=> document.querySelector(`[data-node-id="${id}"]`)).filter(Boolean);
    if(nodeEls.length){
      nodeEls.forEach(el=>{ const id=el.dataset.nodeId; const n=getNode(id); const r=el.getBoundingClientRect(); const w=r.width/scale, h=r.height/scale; const x=n?.x||0, y=n?.y||0; minX=Math.min(minX,x); minY=Math.min(minY,y); maxX=Math.max(maxX,x+w); maxY=Math.max(maxY,y+h); });
    } else if(g.frame){
      // ノードがすべて削除されても枠は維持（前回保存のframe）
      const f=g.frame; minX=f.x; minY=f.y; maxX=f.x+f.w; maxY=f.y+f.h;
    } else {
      // 初期サイズ（空の枠）
      minX=80; minY=60; maxX=260; maxY=180;
    }
    const margin=16; const left=(minX-margin), top=(minY-margin), width=(maxX-minX+margin*2), height=(maxY-minY+margin*2);
    g.frame = { x:left, y:top, w:width, h:height };
  const frame=document.createElement('div');
  frame.className='group-frame';
  frame.style.left=left+'px';
  frame.style.top=top+'px';
  frame.style.width=width+'px';
  frame.style.height=height+'px';
  // Do not intercept events on the frame area itself
  frame.style.pointerEvents = 'none';
    if(g.collapsed) frame.classList.add('collapsed');
  const title=document.createElement('div'); title.className='title'; title.textContent=g.name||'Subsystem'; title.style.pointerEvents='auto'; frame.appendChild(title);
  const actions=document.createElement('div'); actions.className='actions'; actions.style.pointerEvents='auto'; actions.innerHTML=`<button class="toggle">${g.collapsed?'Expand':'Collapse'}</button><button class="run">RUN</button><button class="copy">Copy</button><button class="del">Delete</button>`; frame.appendChild(actions);
    // タイトルドラッグでグループ移動
    let dragging=false,start=null,starts=null;
    title.addEventListener('mousedown',(e)=>{ if(e.button!==0) return; dragging=true; document.body.style.userSelect='none'; start=screenToWorldPoint(e.clientX,e.clientY); starts=(g.nodeIds||[]).map(id=>{ const n=getNode(id); return {id,x:n?.x||0,y:n?.y||0}; }); e.stopPropagation(); });
    const onMove=(e)=>{ if(!dragging) return; const p=screenToWorldPoint(e.clientX,e.clientY); const dx=p.x-start.x, dy=p.y-start.y; (starts||[]).forEach(s=>{ const n=getNode(s.id); if(!n) return; n.x=s.x+dx; n.y=s.y+dy; const el=document.querySelector(`[data-node-id="${s.id}"]`); if(el){ el.style.left=n.x+'px'; el.style.top=n.y+'px'; } }); drawEdges(); frame.style.left=(left+dx)+'px'; frame.style.top=(top+dy)+'px'; };
    const onUp=()=>{ if(!dragging) return; dragging=false; document.body.style.userSelect=''; g.frame = { x: parseFloat(frame.style.left)||left, y: parseFloat(frame.style.top)||top, w: width, h: height }; renderGroups(); saveToLocal(); };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp, { once:true });

    actions.querySelector('.toggle').addEventListener('click', (e)=>{ e.stopPropagation(); g.collapsed = !g.collapsed; render(); saveToLocal(); });
    actions.querySelector('.run').addEventListener('click', async (e)=>{
      e.stopPropagation(); await runWithBusy(async ()=>{
  ensureWS(); statusEl.textContent='running...'; syncFormsToState(); let code = genCodeForNodes(g.nodeIds, true); code = sanitizePython(code); genCodeEl.textContent = code; try{ if(wsCtl && wsCtl.setPendingVarsRefresh) wsCtl.setPendingVarsRefresh(true); }catch(e){} const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) }); let js={}; try{ js=await res.json(); }catch(e){} appendLog('Sent exec (group-frame): ' + JSON.stringify(js));
      }, actions.querySelector('.run'));
    });
  actions.querySelector('.copy').addEventListener('click', (e)=>{ e.stopPropagation(); try{ const data = makeSubgraph(g.nodeIds); const wpt = screenToWorldPoint(left+width+20, top+height/2); const newIds = pasteSubgraph(data, { x: (wpt.x||0), y: (wpt.y||0) }); if(newIds && newIds.length){ createGroup((g.name||'Subsystem')+' Copy', newIds); } render(); saveToLocal(); }catch(e){} });
    actions.querySelector('.del').addEventListener('click', (e)=>{ e.stopPropagation(); state.groups = state.groups.filter(x=> x!==g); render(); });
    groupsLayer.appendChild(frame);

    // 折りたたみ時はノードを簡易的に非表示（実データは保持）
    if(g.collapsed){
      (g.nodeIds||[]).forEach(id=>{ const el=document.querySelector(`[data-node-id="${id}"]`); if(el){ el.style.display='none'; } });
    } else {
      (g.nodeIds||[]).forEach(id=>{ const el=document.querySelector(`[data-node-id="${id}"]`); if(el){ el.style.display=''; } });
    }
  });
}
function render(){ nodesEl.innerHTML=''; state.nodes.forEach(n=> nodesEl.appendChild(createNodeEl(n)) ); applyViewTransform(); drawEdges(); const code = genCode(); genCodeEl.textContent = code; refreshForms(); renderSubsystems(); renderGroups(); updateRunButtonsState(); saveToLocal(); }
function refreshForms(){ state.nodes.forEach(n=>{ const el = document.querySelector(`[data-node-id="${n.id}"]`); if(!el) return; const body = el.querySelector('.body'); if(!body) return; const def = registry.nodes.get(n.type); const html = (def && typeof def.form==='function') ? def.form(n, { getUpstreamColumns: ()=> computeUpstreamColumns(n), getUpstreamNode: ()=> upstreamOf(n) }) : ''; if(typeof html === 'string' && html !== '' && body.innerHTML !== html){ body.innerHTML = html; bindFormMod(el, n, refreshForms); } }); }

// WebSocket handling moved to ws.js; ensureWS() above delegates to it

function updateNodePreview(id){ return updateNodePreviewMod(state, registry, id); }

  // Import / Install: guard setup if a button exists (moved proper wiring to boot())
  async function __pf_init(){
    // Guard: if a Packages button already exists, wire it (legacy support)
    const importBtn = document.getElementById('packagesBtn') || document.getElementById('importBtn');
    if(importBtn){
      importBtn.addEventListener('click', async ()=>{
        const pick = await openPackagePickerModal();
        if(!pick) return;
        if(pick.custom){
          const name = await openInputModal('Install from PyPI', 'package name (e.g., polars)');
          if(!name) return;
          await pipInstallAndImport(String(name).trim(), '');
          return;
        }
        const name = String(pick.name||'').trim();
        if(!name) return;
        if(pick.source==='pypi'){
          await pipInstallAndImport(name, '');
        } else {
          await introspectAndRegister(name);
        }
      });
    }
  

  // Buttons (optional in this layout)
  const sampleBtnEl = document.getElementById('sampleBtn');
  if(sampleBtnEl) sampleBtnEl.addEventListener('click', ()=>{
    state.nodes=[]; state.edges=[]; state.nextId=1; state.groups=[];
    const n1 = addNode('pandas.ReadCSV', 60, 60);
    const n2 = addNode('pandas.FilterRows', 340, 80);
    const n3 = addNode('pandas.XYPlot', 620, 100);
    state.edges.push({from:n1.id,to:n2.id},{from:n2.id,to:n3.id});
    setSelection([]);
    render();
  });

  const installBtnEl = document.getElementById('installBtn');
  if(installBtnEl) installBtnEl.addEventListener('click', async ()=>{
    statusEl.textContent='installing...';
    appendLog('Installing requirements...');
    const res = await apiFetch('/bootstrap', { method:'POST' });
    const js = await res.json().catch(()=>({}));
    appendLog(js.output ? js.output : JSON.stringify(js));
    statusEl.textContent='idle';
  });

  const restartBtnEl = document.getElementById('restartBtn');
  if(restartBtnEl) restartBtnEl.addEventListener('click', async ()=>{
    if(kernelDisabled){ appendLog('[kernel] feature disabled'); return; }
    appendLog('[kernel] restarting...');
    statusEl.textContent='restarting...';
    try{
      const res = await apiFetch('/restart', { method:'POST' });
      const js = await res.json().catch(()=>({}));
      appendLog('[kernel] restarted ' + JSON.stringify(js));
    }catch(e){
      appendLog('[kernel] restart error');
    }
    statusEl.textContent='idle';
  try{ const _ws = wsCtl && wsCtl.getWS ? wsCtl.getWS() : null; if(_ws) _ws.close(); }catch(e){}
    ensureWS();
  });

  // Initial kernel availability check + auth
  try{
    const res = await apiFetch('/health');
    const js = await res.json();
    if(js && js.auth === 'required'){
      authRequired = true;
      const tok = getStoredToken();
      if(tok){ authToken = tok; }
      else {
        const t = window.prompt('API token required. Enter token:');
        if(t && String(t).trim()){ setStoredToken(String(t).trim()); }
      }
    }
    if(js && js.kernel === 'disabled'){
      kernelDisabled = true;
      statusEl.textContent = 'kernel disabled';
      appendLog('[kernel] feature disabled');
    }
  }catch(e){}

  // always reset kernel variables on page load
  if(!kernelDisabled){
    try{
      await apiFetch('/restart', { method:'POST' });
      appendLog('[kernel] restarted on load');
  }catch(e){}
  }
  ensureWS();
  render();
  // Ensure edges are drawn after first layout pass
  try{ requestAnimationFrame(()=>{ try{ syncEdgesViewport(); drawEdges(); }catch(e){} }); }catch(e){}
  // Window resize: keep edges viewport and transforms in sync
  try{
    window.addEventListener('resize', ()=>{ try{ syncEdgesViewport(); drawEdges(); }catch(e){} });
  }catch(e){}
  // 画像クリックで拡大
  document.addEventListener('click', (e)=>{
    const img = e.target && e.target.tagName==='IMG' ? e.target : null;
    if(img && img.closest('.preview')){
  try{ openZoomOverlay(img.src); }catch(e){}
    }
  });
  // 空白クリックでQuick Add（接続モード中）
  document.addEventListener('pf:openQuickAddAt', (e)=>{
    try{
      const d = e.detail || {}; const x = d.x, y = d.y, fromId = d.fromId;
      if(fromId){ openQuickAdd(x, y, fromId); }
  }catch(e){}
  });
  // グループ更新イベントで再描画
  document.addEventListener('pf:groups:changed', ()=>{ try{ renderSubsystems(); renderGroups(); saveToLocal(); }catch(e){} });
  // マウス座標の追跡（キーボード貼り付け位置用）
  canvasWrap.addEventListener('mousemove', (e)=>{ lastMouseWorld = screenToWorldPoint(e.clientX, e.clientY); });
  // キーボードショートカット（コピー/切り取り/貼り付け/複製/削除）
  window.addEventListener('keydown', (e)=>{
    const t = (e.target && e.target.tagName) ? e.target.tagName.toUpperCase() : '';
    if(t==='INPUT' || t==='TEXTAREA' || t==='SELECT') return;
    const ids = Array.from(state.selection||[]);
    const withCtrl = (e.ctrlKey||e.metaKey);
    if(withCtrl && e.key.toLowerCase()==='c'){
  if(ids.length){ try{ clipboardGraph = makeSubgraph(ids); window.__pf_clipboardGraph = clipboardGraph; }catch(e){ clipboardGraph=null; } }
      e.preventDefault();
    } else if(withCtrl && e.key.toLowerCase()==='x'){
  if(ids.length){ try{ clipboardGraph = makeSubgraph(ids); window.__pf_clipboardGraph = clipboardGraph; deleteNodes(ids); setSelection([]); render(); saveToLocal(); }catch(e){} }
      e.preventDefault();
    } else if(withCtrl && e.key.toLowerCase()==='v'){
      const g = window.__pf_clipboardGraph || clipboardGraph;
  if(g){ try{ const newIds = pasteSubgraph(g, { x:(lastMouseWorld.x||100), y:(lastMouseWorld.y||100) }); setSelection(newIds); render(); saveToLocal(); }catch(e){} }
      e.preventDefault();
    } else if(withCtrl && e.key.toLowerCase()==='d'){
  if(ids.length){ try{ const data = makeSubgraph(ids); const newIds = pasteSubgraph(data, { x:(lastMouseWorld.x||100)+20, y:(lastMouseWorld.y||100)+20 }); setSelection(newIds); render(); saveToLocal(); }catch(e){} }
      e.preventDefault();
    } else if(e.key==='Delete'){
  if(ids.length){ try{ deleteNodes(ids); setSelection([]); render(); saveToLocal(); }catch(e){} }
      e.preventDefault();
    } else if(e.key==='Escape'){
  if(state.pendingSrc){ try{ clearGhost(); }catch(e){} try{ closeQuickAdd(); }catch(e){} state.pendingSrc=null; document.querySelectorAll('.port.out.selected').forEach(p=> p.classList.remove('selected')); e.preventDefault(); }
    }
  });
  // 接続モード中に外側をクリックしたらキャンセル
  document.addEventListener('mousedown', (e)=>{
    try{
      if(state.pendingSrc && !e.target.closest('.port') && !e.target.closest('#quickAdd')){
        // キャンバスの空白をクリックしたら Quick Add をその位置で開く
        const onCanvas = !!e.target.closest('#canvasWrap');
        if(onCanvas){
          const x = e.clientX; const y = e.clientY;
          try{ openQuickAdd(x, y, state.pendingSrc); }catch(e){}
          e.preventDefault();
          return; // 接続モードは継続
        }
        // それ以外（UI等）をクリックしたらキャンセル
        state.pendingSrc = null; clearGhost(); closeQuickAdd(); document.querySelectorAll('.port.out.selected').forEach(p=> p.classList.remove('selected'));
      }
  }catch(e){}
  });

  // —— 右ペインのリサイズ ——
  try{
    // 初期幅の復元
    const savedW = Number(localStorage.getItem('pf_right_w')||'');
    if(savedW && savedW>=240 && savedW<=1600){
      document.documentElement.style.setProperty('--right-w', savedW + 'px');
    }
  }catch(e){}
  try{
    if(rightResizer){
      let dragging = false;
      const onMove = (ev)=>{
        if(!dragging) return;
        try{
          const x = ev.clientX || 0;
          // 右列の幅 = 画面幅 - 仕切り位置
          const vw = window.innerWidth || (document.documentElement.clientWidth||0);
          let w = Math.max(240, Math.min(1600, vw - x));
          document.documentElement.style.setProperty('--right-w', w + 'px');
          try{ localStorage.setItem('pf_right_w', String(Math.round(w))); }catch(e){}
        }catch(e){}
      };
      const onUp = ()=>{
        if(!dragging) return;
        dragging = false;
        document.body.style.cursor = '';
        window.removeEventListener('mousemove', onMove);
        window.removeEventListener('mouseup', onUp);
      };
      rightResizer.addEventListener('mousedown', (ev)=>{
        dragging = true;
        document.body.style.cursor = 'col-resize';
        window.addEventListener('mousemove', onMove);
        window.addEventListener('mouseup', onUp);
        ev.preventDefault();
      });
      // ダブルクリックで幅リセット
      rightResizer.addEventListener('dblclick', ()=>{
        document.documentElement.style.setProperty('--right-w', '380px');
        try{ localStorage.removeItem('pf_right_w'); }catch(e){}
      });
    }
  }catch(e){}
  // 追加: インタラクション初期化（最後に呼ぶ）
  window.__pf_clipboardGraph = clipboardGraph;
  initInteractionsMod({
    state,
    canvasWrap,
    nodesEl,
    edgesSvg,
    getScale,
    getTx,
    getTy,
    screenToWorldPoint,
    applyViewTransform,
    drawEdges,
    saveToLocal
  });
  }

// Imports list (Variables panel)
async function renderImportsList(){
  const box = document.createElement('div'); box.style.marginTop='8px';
  box.innerHTML = `<div style="padding:6px 8px; font-weight:600; border-top:1px solid #1f2329;">Imports</div><div id="importsWrap" style="padding:8px 10px; font-size:12px; color:#cbd5e1"></div>`;
  const wrap = box.querySelector('#importsWrap');
  try{
    const res = await apiFetch('/api/imports');
    const js = await res.json().catch(()=>({items:[]}));
    const items = Array.isArray(js.items)? js.items: [];
    if(!items.length){ wrap.innerHTML = '<div style="color:#9ba3af">（インポートなし）</div>'; }
    else {
      wrap.innerHTML = items.map(it=> `<div class="imp-item" draggable="true" data-name="${escapeHtml(it.name)}" data-alias="${escapeHtml(it.alias||'')}"><span style="min-width:180px">${escapeHtml(it.name)}</span><span style="color:#9ba3af">as ${escapeHtml(it.alias||'')}</span><span class="chip">${escapeHtml(it.version||'-')}</span></div>`).join('');
      wrap.querySelectorAll('.imp-item').forEach(el=>{
        el.addEventListener('dragstart', (e)=>{
          const name = el.getAttribute('data-name')||'';
          const alias = el.getAttribute('data-alias')||'';
          const text = alias || name;
          try{ e.dataTransfer.setData('text/plain', text); }catch(e){}
          e.dataTransfer.effectAllowed = 'copy';
          el.classList.add('dragging');
        });
        el.addEventListener('dragend', ()=> el.classList.remove('dragging'));
      });
    }
  }catch(e){ wrap.innerHTML = '<div style="color:#9ba3af">取得失敗</div>'; }
  return box;
}

// expose boot for runtime
async function boot(){
  try{ ensureActionsAreaMod(); }catch(e){}
  try{ ensureRunBarMod(globalRunBtn); }catch(e){}
  // Ensure Packages button exists and wire it
  try{
    const actions = document.getElementById('actions');
    if(actions && !document.getElementById('packagesBtn')){
      const b = document.createElement('button'); b.id='packagesBtn'; b.textContent='Packages'; b.className='secondary'; actions.appendChild(b);
      b.addEventListener('click', async ()=>{
        const pick = await openPackagePickerModal();
        if(!pick) return;
        if(pick.custom){
          const name = await openInputModal('Install from PyPI', 'package name (e.g., polars)');
          if(!name) return; await pipInstallAndImport(String(name).trim(), ''); return;
        }
        const name = String(pick.name||'').trim(); if(!name) return;
        if(pick.source==='pypi') await pipInstallAndImport(name, ''); else await introspectAndRegister(name);
      });
    }
  }catch(e){}
  try{ await __pf_init(); }catch(e){}
}
try{ window.__PF_boot = boot; }catch(e){}

 
