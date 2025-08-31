// UI and rendering for FlowPython
import { state, registry, getNode, addNode, selectNode, clearSelection, deleteNodeById, uid, computeUpstreamColumns, suggestionsForNode, genCode, genCodeUpTo, loadPackages, setPreviewModeProvider, upstreamOf, setSelection, addToSelection, removeFromSelection, isSelected, saveToLocal, restoreFromLocal, makeSubgraph, pasteSubgraph, deleteNodes, createGroup, getGroup, genCodeForNodes } from './nodes.js';
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
function getPreviewMode(){ try{ return getPreviewModeMod(previewModeEl); }catch{ return 'plots'; } }
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
let subsystemsEl = null;
let groupsLayer = null;
let lastMouseWorld = { x: 100, y: 100 };
let authRequired = false;
let authToken = null;
let kernelDisabled = false;
let runningLock = false;
let globalRunBtn = null;
let wsCtl = null;

// ——— helpers: auth token + fetch wrapper ———
function getStoredToken(){ try{ return sessionStorage.getItem('pf_token') || null; }catch{ return null; } }
function setStoredToken(v){ try{ if(v) sessionStorage.setItem('pf_token', v); else sessionStorage.removeItem('pf_token'); authToken = v || null; }catch{} }
async function apiFetch(url, opts){
  const headers = Object.assign({}, (opts && opts.headers) || {});
  if(authRequired && authToken){ headers['Authorization'] = 'Bearer ' + authToken; }
  const next = Object.assign({}, opts || {}, { headers });
  return fetch(url, next);
}

// ——— run buttons state management ———
function canRun(){
  if(kernelDisabled) return false;
  if(runningLock) return false;
  try{ const ws = wsCtl && wsCtl.getWS ? wsCtl.getWS() : null; return !!(ws && ws.readyState===1); }catch{ return false; }
}
function updateRunButtonsState(){
  const ok = canRun();
  try{ if(globalRunBtn) globalRunBtn.disabled = !ok; }catch{}
  try{ document.querySelectorAll('.node-run').forEach(b=>{ b.disabled = !ok; }); }catch{}
}
async function runWithBusy(fn, btn, busyLabel='Running...'){
  if(kernelDisabled){ appendLog('[kernel] feature disabled'); return; }
  runningLock = true; updateRunButtonsState();
  const oldTxt = btn ? btn.textContent : '';
  if(btn){ btn.disabled = true; btn.textContent = busyLabel; }
  try{ if(wsCtl && wsCtl.setPendingVarsRefresh) wsCtl.setPendingVarsRefresh(true); await fn(); }
  finally { runningLock = false; updateRunButtonsState(); if(btn){ btn.disabled = false; btn.textContent = oldTxt; } }
}

// ——— small wrappers for modularized helpers ———
function ensureRunBar(){ try{ ensureRunBarMod(globalRunBtn); }catch{} }
function ensureActionsArea(){ try{ ensureActionsAreaMod(); }catch{} }

// Minimal UI helpers used in this file
function openMessageModal(title, message){
  try{
    const overlay = document.createElement('div'); overlay.className='modal-overlay';
    const modal = document.createElement('div'); modal.className='modal'; modal.style.maxWidth='720px';
    modal.innerHTML = `<div class="modal-head"><div class="title">${escapeHtml(title||'Message')}</div></div>
      <div class="modal-body"><pre style="margin:0; white-space:pre-wrap">${escapeHtml(String(typeof message==='string'? message: JSON.stringify(message, null, 2)))}</pre></div>
      <div class="modal-foot"><button class="secondary">Close</button></div>`;
    overlay.appendChild(modal); document.body.appendChild(overlay);
    modal.querySelector('button').addEventListener('click', ()=> overlay.remove());
    overlay.addEventListener('click', (e)=>{ if(e.target===overlay) overlay.remove(); });
  }catch{}
}
function openGridModal(title, columns, rows){
  try{
    const overlay = document.createElement('div'); overlay.className='modal-overlay';
    const modal = document.createElement('div'); modal.className='modal'; modal.style.maxWidth='960px'; modal.style.maxHeight='80vh'; modal.style.overflow='auto';
    const thead = `<thead><tr>${(columns||[]).map(c=> `<th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">${escapeHtml(String(c))}</th>`).join('')}</tr></thead>`;
    const tbody = `<tbody>${(rows||[]).map(r=> `<tr>${(r||[]).map(v=> `<td style=\"padding:6px 8px; border-bottom:1px solid #111824\">${escapeHtml(String(v))}</td>`).join('')}</tr>`).join('')}</tbody>`;
    modal.innerHTML = `<div class="modal-head"><div class="title">${escapeHtml(title||'Grid')}</div></div><div class="modal-body"><div style="overflow:auto"><table style="width:100%; border-collapse:collapse; font-size:12px">${thead}${tbody}</table></div></div><div class="modal-foot"><button class="secondary">Close</button></div>`;
    overlay.appendChild(modal); document.body.appendChild(overlay);
    modal.querySelector('button').addEventListener('click', ()=> overlay.remove());
    overlay.addEventListener('click', (e)=>{ if(e.target===overlay) overlay.remove(); });
  }catch{}
}
async function openVarPreview(name){
  try{
    const res = await apiFetch(`/api/variables/${encodeURIComponent(name)}/head?rows=50`);
    const js = await res.json().catch(()=>({}));
    if(js && js.columns && js.data){ openGridModal(`${name} • head`, js.columns, js.data); }
    else openMessageModal('preview', JSON.stringify(js));
  }catch{ openMessageModal('preview', 'failed'); }
}
async function downloadFrom(url, filename){
  try{
    const res = await apiFetch(url);
    const blob = await res.blob();
    const a = document.createElement('a'); a.href = URL.createObjectURL(blob); a.download = filename || 'download'; a.click(); URL.revokeObjectURL(a.href);
  }catch{ appendLog('[download] failed'); }
}
function openZoomOverlay(src){
  try{
    const overlay = document.createElement('div'); overlay.className='modal-overlay';
    const img = document.createElement('img'); img.src = src; img.style.maxWidth='90vw'; img.style.maxHeight='90vh'; img.style.display='block'; img.style.margin='auto';
    const wrap = document.createElement('div'); wrap.className='modal-body'; wrap.style.background='transparent'; wrap.style.boxShadow='none'; wrap.appendChild(img);
    overlay.appendChild(wrap); document.body.appendChild(overlay);
    overlay.addEventListener('click', ()=> overlay.remove());
  }catch{}
}
function syncFormsToState(){ /* inputs are bound live in forms.js; nothing to do */ }

const canvasWrap = document.getElementById('canvasWrap');
const nodesEl = document.getElementById('nodes');
const edgesSvg = document.getElementById('edges');
// WebSocket & streaming
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
      const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) });
      let js={}; try{ js=await res.json(); }catch{}
      appendLog('Sent exec: ' + JSON.stringify(js));
    }, globalRunBtn, 'Running...');
  });
}catch{}
async function openPackagePickerModal(){
  return new Promise(async (resolve)=>{
    const overlay = document.createElement('div'); overlay.className='modal-overlay';
    const modal = document.createElement('div'); modal.className='modal'; modal.style.maxWidth='760px';
    const head = document.createElement('div'); head.className='modal-head'; head.innerHTML = '<div class="title">Packages</div>';
    const body = document.createElement('div'); body.className='modal-body';
    body.innerHTML = `
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
      <div style="color:#9ba3af; font-size:12px; margin-top:8px">Tip: 空のまま Install を押すと任意のパッケージ名を入力できます</div>`;
    const foot = document.createElement('div'); foot.className='modal-foot';
    const cancelBtn = document.createElement('button'); cancelBtn.className='secondary'; cancelBtn.textContent='Close';
    const installBtn = document.createElement('button'); installBtn.textContent='Install custom...';
    foot.appendChild(cancelBtn); foot.appendChild(installBtn);
    modal.appendChild(head); modal.appendChild(body); modal.appendChild(foot); overlay.appendChild(modal);
    document.body.appendChild(overlay);

    const listEl = body.querySelector('#pf_pkg_list');
    const qEl = body.querySelector('#pf_pkg_q');
    const searchBtn = body.querySelector('#pf_pkg_search');
    const prevBtn = body.querySelector('#pf_pkg_prev');
    const nextBtn = body.querySelector('#pf_pkg_next');
    const pageInfo = body.querySelector('#pf_pkg_pageinfo');

    let offset = 0; const limit = 50; let total = 0;

    const render = (items)=>{
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
        btn.addEventListener('click', ()=>{
          const tr = btn.closest('tr'); const name = tr && tr.getAttribute('data-name'); close({ name });
        });
      });
      const page = Math.floor(offset/limit)+1; const pages = Math.max(1, Math.ceil(total/limit));
      pageInfo.textContent = `${(total||0).toLocaleString()} items • Page ${page}/${pages}`;
      prevBtn.disabled = offset<=0; nextBtn.disabled = offset+limit>=total;
    };
    const load = async ()=>{
      const q = String(qEl.value||'').trim();
      const usp = new URLSearchParams(); if(q) usp.set('q', q); usp.set('limit', String(limit)); usp.set('offset', String(offset));
      const js = await (await apiFetch('/api/modules/installed?'+usp.toString())).json().catch(()=>({items:[], total:0}));
      total = Number(js.total||0); render(js.items||[]);
    };
    const goPrev = ()=>{ if(offset<=0) return; offset = Math.max(0, offset - limit); load(); };
    const goNext = ()=>{ if(offset+limit>=total) return; offset = offset + limit; load(); };
    searchBtn.onclick = ()=>{ offset=0; load(); };
    prevBtn.onclick = goPrev; nextBtn.onclick = goNext; qEl.addEventListener('keydown', (e)=>{ if(e.key==='Enter'){ offset=0; load(); } });
    await load();

    function close(v){ overlay.remove(); resolve(v); }
    cancelBtn.onclick = ()=> close(undefined);
    installBtn.onclick = ()=> close({});
    overlay.addEventListener('click', (e)=>{ if(e.target===overlay) cancelBtn.click(); });
  });
}

// (removed orphaned install-stream code)

// appendLog, clearLog imported from ./logger.js
function centerOf(el){ const r = el.getBoundingClientRect(); const p = edgesSvg.getBoundingClientRect(); return { x: r.left - p.left + r.width/2, y: r.top - p.top + r.height/2 }; }
// View helpers (screen<->world)
function getScale(){ return state.view?.scale || 1; }
function getTx(){ return state.view?.tx || 0; }
function getTy(){ return state.view?.ty || 0; }
function screenToWorldPoint(clientX, clientY){ const rect = canvasWrap.getBoundingClientRect(); const x = clientX - rect.left; const y = clientY - rect.top; const s = getScale(); return { x: (x - getTx())/s, y: (y - getTy())/s }; }
function applyViewTransform(){ const s=getScale(), tx=getTx(), ty=getTy(); nodesEl.style.transformOrigin='0 0'; nodesEl.style.transform = `translate(${tx}px, ${ty}px) scale(${s})`; }
function updatePreviewDock(){}

// 選択ハイライトをDOMへ反映
function refreshSelectionHighlight(){
  const S = new Set(state.selection||[]);
  document.querySelectorAll('.node').forEach(el=>{
    const id = el.getAttribute('data-node-id');
    if(!id) return;
    if(S.has(id)) el.classList.add('selected'); else el.classList.remove('selected');
  });
}

// Packages list (right panel)
async function renderPackagesList(){
  if(!pkgsWrap) return;
  try{
    const pkgs = Array.isArray(registry.packages) ? registry.packages : [];
    const totalNodes = (name)=> (registry.byPackage.get(name)||[]).length;
    if(pkgs.length===0){ pkgsWrap.innerHTML = '<div style="color:#9ba3af">パッケージが読み込まれていません</div>'; return; }
    // まず表の骨格
    pkgsWrap.innerHTML = `<table style="width:100%; border-collapse:collapse; font-size:12px;"><thead><tr><th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Package</th><th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Version</th><th style="text-align:left; padding:6px 8px; border-bottom:1px solid #263041">Nodes</th></tr></thead><tbody id="pkgsTbody"></tbody></table>`;
    const tbody = pkgsWrap.querySelector('#pkgsTbody');
    const names = pkgs.map(p=> p.name);
    let versions = new Map();
    try{
      const res = await apiFetch('/api/modules/versions?names=' + encodeURIComponent(names.join(',')));
      const js = await res.json().catch(()=>({items:[]}));
      (Array.isArray(js.items)? js.items: []).forEach(x=> versions.set(x.name, x.version||''));
    }catch{}
    const makeRow = (p)=>{
      const n = totalNodes(p.name);
      const ver = versions.get(p.name) || '';
      return `<tr><td style=\"padding:6px 8px; border-bottom:1px solid #111824\">${escapeHtmlUtil(p.label||p.name)}</td><td style=\"padding:6px 8px; border-bottom:1px solid #111824; color:#cbd5e1\">${escapeHtmlUtil(ver||'-')}</td><td style=\"padding:6px 8px; border-bottom:1px solid #111824; color:#9ba3af\">${n} nodes</td></tr>`;
    };
    tbody.innerHTML = pkgs.map(makeRow).join('');
  }catch{ pkgsWrap.innerHTML = '<div style="color:#9ba3af">一覧の描画に失敗しました</div>'; }
}

// 追加: クリップボード（キャンバス貼り付け用に window にも同期）
let clipboardGraph = null;

// ノード右クリックメニュー
function openNodeContextMenu(nodeId, clientX, clientY){
  const ids = (state.selection && state.selection.size && state.selection.has(nodeId))
    ? Array.from(state.selection)
    : [nodeId];
  const doCopy = ()=>{ try{ clipboardGraph = makeSubgraph(ids); }catch(e){ clipboardGraph = null; console.error('Copy failed:', e); } window.__pf_clipboardGraph = clipboardGraph; };
  const doCut = ()=>{ try{ clipboardGraph = makeSubgraph(ids); }catch{ clipboardGraph = null; } window.__pf_clipboardGraph = clipboardGraph; try{ deleteNodes(ids); }catch{} render(); };
  const doDuplicate = ()=>{
    try{
      const g = makeSubgraph(ids);
      const wpt = screenToWorldPoint(clientX, clientY);
      const newIds = pasteSubgraph(g, { x: (wpt.x||0) + 40, y: (wpt.y||0) + 40 }) || [];
      if(newIds && newIds.length) setSelection(newIds);
      render();
    }catch{}
  };
  const items = [
    { key:'copy', label:'Copy', onClick: doCopy },
    { key:'cut', label:'Cut', onClick: doCut },
    { key:'dup', label:'Duplicate', onClick: doDuplicate },
    { key:'paste', label:'Paste', disabled:!clipboardGraph, onClick: ()=>{
        if(!clipboardGraph) return;
        try{
          const wpt = screenToWorldPoint(clientX, clientY);
          const newIds = pasteSubgraph(clipboardGraph, { x: (wpt.x||0) + 40, y: (wpt.y||0) + 40 }) || [];
          if(newIds && newIds.length) setSelection(newIds);
          render();
        }catch{}
      } },
  ];
  openContextMenu(items, clientX, clientY);
}

// Variables
function filterVars(arr){ try{ return (arr||[]).filter(v=>{ const t = String(v.type||'').toLowerCase(); const n = String(v.name||'').toLowerCase(); if(n==='exit' || n==='quit') return false; if(n==='in' || n==='out') return false; if(n.startsWith('_')) return false; if(t.includes('module')) return false; if(t.includes('function')) return false; if(t.includes('method')) return false; if(t.includes('autocall')) return false; if(t.includes('zmqexitautocall')) return false; return true; }); }catch{ return arr||[]; } }
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
        const shp = Array.isArray(v.shape)? `shape=${escapeHtml(String(v.shape))}` : '';
        const val = (v.repr!=null? String(v.repr): '');
        const csvBtn = `<button class="btn btn-ghost var-csv" data-var="${name}" title="Download CSV"><svg viewBox="0 0 24 24" aria-hidden="true"><path d="M12 3v10m0 0l-3.5-3.5M12 13l3.5-3.5M5 21h14" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"/></svg>Export CSV</button>`;
        const menuBtn = `<button class="btn btn-icon var-menu var-float-menu" data-var="${name}" data-type="${type}" title="Actions">⋯</button>`;
        const actions = `<div class="var-actions">${csvBtn}</div>`;
        return `<tr data-var="${name}" data-type="${type}"><td>${nameCell}</td><td>${type}</td><td><div class="var-cell">${escapeHtml(val).slice(0,200)} <span style="color:var(--sub); font-size:11px;">${shp}</span>${actions}${menuBtn}</div></td></tr>`;
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
    }catch{}
    // Make variables draggable
    varsWrap.querySelectorAll('.var-item').forEach(el=>{
      el.addEventListener('dragstart', (e)=>{
        const name = el.getAttribute('data-var') || el.textContent || '';
        try{ e.dataTransfer.setData('text/plain', name); }catch{}
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
  }catch{
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
    }catch{}
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
  <div class=\"body\">${(typeof def.form==='function')? def.form(node, { getUpstreamColumns: ()=> computeUpstreamColumns(node), getUpstreamNode: ()=> upstreamOf(node) }): ''}</div>
  <div class=\"preview\" style=\"${wantPreview ? '' : 'display:none;'}max-height:${previewH}px\"> 
    <div id=\"prev-${node.id}\"></div> 
    <div class=\"node-resize-v\" title=\"Drag to resize preview height\"></div> 
  </div>
  <div class=\"actions\">
    <button class=\"node-run btn-primary\">Run</button>
    <button class=\"node-del btn-icon danger\" title=\"Delete\" aria-label=\"Delete\">${'<svg viewBox=\\"0 0 24 24\\" fill=\\"none\\" stroke=\\"currentColor\\" stroke-width=\\"2\\" stroke-linecap=\\"round\\" stroke-linejoin=\\"round\\"><polyline points=\\"3 6 5 6 21 6\\"></polyline><path d=\\"M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6m3 0V4a2 2 0 0 1 2-2h4a2 2 0 0 1 2 2v2\\"></path><line x1=\\"10\\" y1=\\"11\\" x2=\\"10\\" y2=\\"17\\"></line><line x1=\\"14\\" y1=\\"11\\" x2=\\"14\\" y2=\\"17\\"></line></svg>'}</button>
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
          const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) });
          let js={}; try{ js=await res.json(); }catch{}
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
    try{ clearGhost(); }catch{}
    try{ closeQuickAdd(); }catch{}
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
  return el; }

function renderToolbar(){
  toolbarEl.innerHTML = '';
  if(!state.activePkg && registry.packages[0]) state.activePkg = registry.packages[0].name;
  (registry.packages || []).forEach(p=>{
    const details = document.createElement('details');
    details.className='pkg-section';
    details.open = (state.activePkg === p.name);
    const summary = document.createElement('summary');
    summary.textContent = p.label || p.name;
    Object.assign(summary.style, { cursor:'pointer', userSelect:'none', padding:'8px 10px' });
    details.appendChild(summary);

    // Build category -> [node types] map for this package
    const types = (registry.byPackage.get(p.name) || []).filter(t=> !(registry.nodes.get(t)?.hidden));
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
        btn.textContent = '➕ ' + (def.title || type);
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
  const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) }); let js={}; try{ js=await res.json(); }catch{}
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
  ensureWS(); statusEl.textContent='running...'; syncFormsToState(); let code = genCodeForNodes(g.nodeIds, true); code = sanitizePython(code); genCodeEl.textContent = code; const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) }); let js={}; try{ js=await res.json(); }catch{} appendLog('Sent exec (group-frame): ' + JSON.stringify(js));
      }, actions.querySelector('.run'));
    });
    actions.querySelector('.copy').addEventListener('click', (e)=>{ e.stopPropagation(); try{ const data = makeSubgraph(g.nodeIds); const wpt = screenToWorldPoint(left+width+20, top+height/2); const newIds = pasteSubgraph(data, { x: (wpt.x||0), y: (wpt.y||0) }); if(newIds && newIds.length){ createGroup((g.name||'Subsystem')+' Copy', newIds); } render(); saveToLocal(); }catch{} });
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
function refreshForms(){ state.nodes.forEach(n=>{ const el = document.querySelector(`[data-node-id="${n.id}"]`); if(!el) return; const body = el.querySelector('.body'); if(!body) return; const def = registry.nodes.get(n.type); const html = (typeof def.form==='function') ? def.form(n, { getUpstreamColumns: ()=> computeUpstreamColumns(n), getUpstreamNode: ()=> upstreamOf(n) }) : ''; if(typeof html === 'string' && html !== '' && body.innerHTML !== html){ body.innerHTML = html; bindFormMod(el, n, refreshForms); } }); }

// WebSocket handling moved to ws.js; ensureWS() above delegates to it

function updateNodePreview(id){ return updateNodePreviewMod(state, registry, id); }

async function boot(){
  await loadPackages();
  try{ setPreviewModeProvider(()=> getPreviewMode()); }catch{}
  renderToolbar();
  try{ renderPackagesList(); }catch{}
  // Left pane uses sidebar scroll; no explicit maxHeight on toolbar
  applyViewTransform();
  // Ensure right panel can scroll even if stale CSS is cached
  [rightCode, rightVars, rightPkgs].forEach(el=>{ if(el){ el.style.overflow='auto'; el.style.minHeight='0'; } });
  // Ensure code block scrolls
  try{ if(genCodeEl){ genCodeEl.style.overflow='auto'; genCodeEl.style.minHeight='0'; } }catch{}
  // Ensure run bar exists (rare cases where DOM was reflowed before creation)
  ensureRunBar();

  // Ensure left-bottom actions area exists (Install/Restart/Sample)
  ensureActionsArea();
  // try restore
  try { restoreFromLocal(); } catch {}

  // Add extra actions (auth/flows)
  const actionsEl = document.getElementById('actions');
  const signBtn = document.createElement('button'); signBtn.id='signBtn'; signBtn.className='secondary'; signBtn.textContent='Sign in';
  const saveFlowBtn = document.createElement('button'); saveFlowBtn.id='saveFlowBtn'; saveFlowBtn.className='secondary'; saveFlowBtn.textContent='Save Flow';
  const loadFlowBtn = document.createElement('button'); loadFlowBtn.id='loadFlowBtn'; loadFlowBtn.className='secondary'; loadFlowBtn.textContent='Load Flow';
  const addTargetBtn = document.createElement('button'); addTargetBtn.id='addTargetBtn'; addTargetBtn.className='secondary'; addTargetBtn.textContent='Add Target'; addTargetBtn.title='Create a node from fully-qualified name via introspection';
  const importBtn = document.createElement('button'); importBtn.id='importBtn'; importBtn.className='secondary'; importBtn.textContent='Import Module'; importBtn.title='Introspect a module and add its nodes to the toolbar';
  actionsEl?.appendChild(saveFlowBtn); actionsEl?.appendChild(loadFlowBtn); actionsEl?.appendChild(importBtn); actionsEl?.appendChild(addTargetBtn); actionsEl?.appendChild(signBtn);

  signBtn.addEventListener('click', ()=>{
    const cur = getStoredToken();
    const t = window.prompt('Enter API token (leave blank to sign out):', cur||'');
    if(t!=null){ if(String(t).trim()){ setStoredToken(String(t).trim()); appendLog('[auth] token set'); } else { setStoredToken(null); appendLog('[auth] signed out'); } }
  });

  saveFlowBtn.addEventListener('click', async ()=>{
    const name = window.prompt('Flow name (A-Za-z0-9_- up to 64 chars):', 'flow1'); if(!name) return;
    try{
      const body = { version:1, nodes: state.nodes, edges: state.edges, nextId: state.nextId, groups: state.groups, view: state.view, activePkg: state.activePkg };
      const res = await apiFetch(`/api/flows/${encodeURIComponent(name)}.json`, { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify(body) });
      const js = await res.json().catch(()=>({}));
      if(js && js.ok){ appendLog(`[flow] saved ${name}`); }
      else { appendLog('[flow] save error ' + JSON.stringify(js)); }
    }catch(e){ appendLog('[flow] save failed'); }
  });

  loadFlowBtn.addEventListener('click', async ()=>{
    try{
      const res = await apiFetch('/api/flows');
      const js = await res.json().catch(()=>({items:[]}));
      const names = (Array.isArray(js.items)? js.items: []).map(x=>x.name);
      const pick = window.prompt('Enter flow name to load' + (names.length? ` (available: ${names.join(', ')})` : ''), names[0]||'');
      if(!pick) return;
      const res2 = await apiFetch(`/api/flows/${encodeURIComponent(pick)}.json`);
      const data = await res2.json();
      if(data && Array.isArray(data.nodes) && Array.isArray(data.edges)){
        state.nodes = data.nodes; state.edges = data.edges; state.nextId = data.nextId||1; state.groups = Array.isArray(data.groups)? data.groups: []; state.view = data.view || state.view; state.activePkg = data.activePkg || state.activePkg; setSelection([]); render(); appendLog(`[flow] loaded ${pick}`); saveToLocal();
      } else {
        appendLog('[flow] invalid flow file');
      }
    }catch(e){ appendLog('[flow] load failed'); }
  });

  // Add Target: create node spec from callable via backend introspection
  addTargetBtn.addEventListener('click', async ()=>{
    try{
      const tgt = window.prompt('Enter fully-qualified callable (e.g., pandas.read_csv or sklearn.cluster.KMeans):', 'pandas.read_csv');
      if(!tgt) return;
      const res = await apiFetch('/api/introspect?target=' + encodeURIComponent(tgt));
      const js = await res.json().catch(()=>({}));
      const arr = Array.isArray(js.nodes) ? js.nodes : [];
      if(!arr.length){ appendLog('[introspect] no spec'); return; }
  for(const spec of arr){
        const id = spec.id || ('autogen.' + Math.random().toString(36).slice(2,8));
        const def = {
          id,
          title: spec.title || id,
          category: spec.category || 'Auto',
          inputType: spec.inputType || 'Any',
          outputType: spec.outputType || 'Any',
          defaultParams: Object.fromEntries((spec.params||[]).map(p=> [p.name, p.default])),
          form(node){
            const v = node.params || (node.params = this.defaultParams ? JSON.parse(JSON.stringify(this.defaultParams)) : {});
            const fields = (spec.params||[]).filter(p=> !p.hidden);
            function shown(p){ if(!p.when) return true; const m=String(p.when).split('='); if(m.length!==2) return true; const [k,val]=m; return String(v[k]||'')===String(val); }
            function inputFor(p){ const name=p.name; const label=p.label||name; const val=v[name] ?? p.default ?? ''; const ui=p.ui||'string'; if(ui==='select' && Array.isArray(p.enum)){ const opts=p.enum.map(x=>`<option value="${x}" ${String(val)===String(x)?'selected':''}>${x}</option>`).join(''); return `<label>${label}</label><select name="${name}">${opts}</select>`; } if(ui==='textarea'){ return `<label>${label}</label><textarea name="${name}">${val||''}</textarea>`; } return `<label>${label}</label><input name="${name}" value="${val||''}">`; }
            const basic = fields.filter(p=> !p.advanced && shown(p)).map(inputFor).join('\n');
            const adv = fields.filter(p=> p.advanced && shown(p)).map(inputFor).join('\n');
            return `${basic}${adv? `<details style="margin-top:8px"><summary style="cursor:pointer; user-select:none">Advanced</summary>${adv}</details>`:''}`;
          },
          code(node, ctx){
            const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
            const p = node.params||{};
            const call = spec.call || {};
            const params = (spec.params||[])
              .filter(x=> !x.when || String(p[String(x.when).split('=')[0]]||'')===String(String(x.when).split('=')[1]||''))
              .filter(x=> p[x.name]!==undefined)
              .map(x=> `${x.name}=${JSON.stringify(p[x.name])}`)
              .join(', ');
            const target = call.target || '';
            const parts = target.split('.');
            const root = parts[0] || '';
            const modPath = parts.slice(0, -1).join('.');
            const seg = [];
            if(root){ seg.push(`import ${root}`); seg.push(`_fp_register_import('${root}')`); }
            if(modPath && modPath.includes('.')){ seg.push(`import importlib; importlib.import_module(r'''${modPath}''')`); }
            const srcs = (ctx && typeof ctx.srcVars==='function') ? ctx.srcVars(node) : [];
            const src = srcs[0] || null;
            const dfParam = call.dfParam || null;
            if(call.kind==='function'){
              if(dfParam && src){
                const argz = [];
                argz.push(`${dfParam}=${src}`);
                if(params) argz.push(params);
                seg.push(`${v} = ${target}(${argz.join(', ')})`);
              } else {
                seg.push(`${v} = ${target}(${params})`);
              }
            } else if(call.kind==='constructor'){
              seg.push(`${v} = ${target}(${params})`);
            } else if(call.kind==='method'){
              let recv = srcs[0] || src;
              const dataVar = srcs.length>=2 ? srcs[srcs.length-1] : (srcs[0] || null);
              if(!recv && call.receiver && call.receiver!=='estimator'){
                recv = `globals().get('${call.receiver}', None)`;
              }
              const meth = target.split('.').slice(-1)[0];
              if(recv){
                const argz = [];
                if(dfParam && dfParam!=='self' && dataVar) argz.push(`${dfParam}=${dataVar}`);
                if(params) argz.push(params);
                const joined = argz.join(', ');
                if(dfParam==='self') seg.push(`${v} = ${recv}.${meth}(${params})`);
                else seg.push(`${v} = ${recv}.${meth}(${joined})`);
                if(call.returnsSelf) seg.push(`${v} = ${recv}`);
              } else {
                seg.push(`${v} = ${target}(${params})`);
              }
            } else {
              seg.push(`${v} = ${target}(${params})`);
            }
            seg.push(`print(${v})`);
            return seg;
          }
        };
        registry.nodes.set(id, def);
  const pkgName = spec.pkg || (spec.call?.target?.split('.')?.[0] || 'autogen');
  if(!registry.packages.some(p=> p.name===pkgName)) registry.packages.push({ name: pkgName, label: pkgName.charAt(0).toUpperCase()+pkgName.slice(1), entry:'' });
  if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []);
  registry.byPackage.get(pkgName).push(id);
      }
  renderToolbar();
  try{ renderPackagesList(); }catch{}
  appendLog('[introspect] added ' + arr.length + ' node(s) to Autogen');
    }catch(e){ appendLog('[introspect] failed'); }
  });

  // Import Module: introspect a module and register its auto-generated nodes
  importBtn.addEventListener('click', async ()=>{
    try{
      const mod = window.prompt('Enter module name to import (e.g., pandas, polars, sklearn):', 'pandas');
      if(!mod) return;
      const res = await apiFetch('/api/introspect_module?module=' + encodeURIComponent(mod));
      const js = await res.json().catch(()=>({}));
      const arr = Array.isArray(js.nodes) ? js.nodes : [];
      if(!arr.length){ appendLog('[import] no callables found in ' + mod); return; }
      for(const spec of arr){
        const id = spec.id || ('autogen.' + Math.random().toString(36).slice(2,8));
        if(registry.nodes.has(id)) continue;
        const def = {
          id,
          title: spec.title || id,
          category: spec.category || 'Auto',
          inputType: spec.inputType || 'Any',
          outputType: spec.outputType || 'Any',
          defaultParams: Object.fromEntries((spec.params||[]).map(p=> [p.name, p.default])),
          form(node){
            const v = node.params || (node.params = this.defaultParams ? JSON.parse(JSON.stringify(this.defaultParams)) : {});
            const fields = (spec.params||[]).filter(p=> !p.hidden);
            function shown(p){ if(!p.when) return true; const m=String(p.when).split('='); if(m.length!==2) return true; const [k,val]=m; return String(v[k]||'')===String(val); }
            function inputFor(p){ const name=p.name; const label=p.label||name; const val=v[name] ?? p.default ?? ''; const ui=p.ui||'string'; if(ui==='select' && Array.isArray(p.enum)){ const opts=p.enum.map(x=>`<option value="${x}" ${String(val)===String(x)?'selected':''}>${x}</option>`).join(''); return `<label>${label}</label><select name="${name}">${opts}</select>`; } if(ui==='textarea'){ return `<label>${label}</label><textarea name="${name}">${val||''}</textarea>`; } return `<label>${label}</label><input name="${name}" value="${val||''}">`; }
            const basic = fields.filter(p=> !p.advanced && shown(p)).map(inputFor).join('\n');
            const adv = fields.filter(p=> p.advanced && shown(p)).map(inputFor).join('\n');
            return `${basic}${adv? `<details style="margin-top:8px"><summary style="cursor:pointer; user-select:none">Advanced</summary>${adv}</details>`:''}`;
          },
          code(node, ctx){
            const v = 'v_'+node.id.replace(/[^a-zA-Z0-9_]/g,'');
            const p = node.params||{};
            const call = spec.call || {};
            const params = (spec.params||[])
              .filter(x=> !x.when || String(p[String(x.when).split('=')[0]]||'')===String(String(x.when).split('=')[1]||''))
              .filter(x=> p[x.name]!==undefined)
              .map(x=> `${x.name}=${JSON.stringify(p[x.name])}`)
              .join(', ');
            const target = call.target || '';
            const parts = target.split('.');
            const root = parts[0] || '';
            const modPath = parts.slice(0, -1).join('.');
            const seg = [];
            if(root){ seg.push(`import ${root}`); seg.push(`_fp_register_import('${root}')`); }
            if(modPath && modPath.includes('.')){ seg.push(`import importlib; importlib.import_module(r'''${modPath}''')`); }
            const srcs = (ctx && typeof ctx.srcVars==='function') ? ctx.srcVars(node) : [];
            const src = srcs[0] || null;
            const dfParam = call.dfParam || null;
            if(call.kind==='function'){
              if(dfParam && src){
                const argz = [];
                argz.push(`${dfParam}=${src}`);
                if(params) argz.push(params);
                seg.push(`${v} = ${target}(${argz.join(', ')})`);
              } else {
                seg.push(`${v} = ${target}(${params})`);
              }
            } else if(call.kind==='constructor'){
              seg.push(`${v} = ${target}(${params})`);
            } else if(call.kind==='method'){
              let recv = srcs[0] || src;
              const dataVar = srcs.length>=2 ? srcs[srcs.length-1] : (srcs[0] || null);
              if(!recv && call.receiver && call.receiver!=='estimator'){
                recv = `globals().get('${call.receiver}', None)`;
              }
              const meth = target.split('.').slice(-1)[0];
              if(recv){
                const argz = [];
                if(dfParam && dfParam!=='self' && dataVar) argz.push(`${dfParam}=${dataVar}`);
                if(params) argz.push(params);
                const joined = argz.join(', ');
                if(dfParam==='self') seg.push(`${v} = ${recv}.${meth}(${params})`);
                else seg.push(`${v} = ${recv}.${meth}(${joined})`);
                if(call.returnsSelf) seg.push(`${v} = ${recv}`);
              } else {
                seg.push(`${v} = ${target}(${params})`);
              }
            } else {
              seg.push(`${v} = ${target}(${params})`);
            }
            seg.push(`print(${v})`);
            return seg;
          }
        };
        registry.nodes.set(id, def);
        const pkgName = spec.pkg || (spec.call?.target?.split('.')?.[0] || 'autogen');
        if(!registry.packages.some(p=> p.name===pkgName)) registry.packages.push({ name: pkgName, label: pkgName.charAt(0).toUpperCase()+pkgName.slice(1), entry:'' });
        if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []);
        registry.byPackage.get(pkgName).push(id);
      }
      renderToolbar();
      try{ renderPackagesList(); }catch{}
      appendLog('[import] added ' + arr.length + ' node(s) from ' + mod);
    }catch(e){ appendLog('[import] failed'); }
  });

  // Buttons
  document.getElementById('sampleBtn').addEventListener('click', ()=>{
    state.nodes=[]; state.edges=[]; state.nextId=1; state.groups=[];
    const n1 = addNode('pandas.ReadCSV', 60, 60);
    const n2 = addNode('pandas.FilterRows', 340, 80);
    const n3 = addNode('pandas.XYPlot', 620, 100);
    state.edges.push({from:n1.id,to:n2.id},{from:n2.id,to:n3.id});
    setSelection([]);
    render();
  });

  document.getElementById('installBtn').addEventListener('click', async ()=>{
    statusEl.textContent='installing...';
    appendLog('Installing requirements...');
    const res = await apiFetch('/bootstrap', { method:'POST' });
    const js = await res.json().catch(()=>({}));
    appendLog(js.output ? js.output : JSON.stringify(js));
    statusEl.textContent='idle';
  });

  document.getElementById('restartBtn').addEventListener('click', async ()=>{
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
  try{ const _ws = wsCtl && wsCtl.getWS ? wsCtl.getWS() : null; if(_ws) _ws.close(); }catch{}
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
  }catch{}

  // always reset kernel variables on page load
  if(!kernelDisabled){
    try{
      await apiFetch('/restart', { method:'POST' });
      appendLog('[kernel] restarted on load');
    }catch{}
  }
  ensureWS();
  render();
  // Ensure edges are drawn after first layout pass
  try{ requestAnimationFrame(()=>{ try{ syncEdgesViewport(); drawEdges(); }catch{} }); }catch{}
  // 画像クリックで拡大
  document.addEventListener('click', (e)=>{
    const img = e.target && e.target.tagName==='IMG' ? e.target : null;
    if(img && img.closest('.preview')){
      try{ openZoomOverlay(img.src); }catch{}
    }
  });
  // 空白クリックでQuick Add（接続モード中）
  document.addEventListener('pf:openQuickAddAt', (e)=>{
    try{
      const d = e.detail || {}; const x = d.x, y = d.y, fromId = d.fromId;
      if(fromId){ openQuickAdd(x, y, fromId); }
    }catch{}
  });
  // グループ更新イベントで再描画
  document.addEventListener('pf:groups:changed', ()=>{ try{ renderSubsystems(); renderGroups(); saveToLocal(); }catch{} });
  // マウス座標の追跡（キーボード貼り付け位置用）
  canvasWrap.addEventListener('mousemove', (e)=>{ lastMouseWorld = screenToWorldPoint(e.clientX, e.clientY); });
  // キーボードショートカット（コピー/切り取り/貼り付け/複製/削除）
  window.addEventListener('keydown', (e)=>{
    const t = (e.target && e.target.tagName) ? e.target.tagName.toUpperCase() : '';
    if(t==='INPUT' || t==='TEXTAREA' || t==='SELECT') return;
    const ids = Array.from(state.selection||[]);
    const withCtrl = (e.ctrlKey||e.metaKey);
    if(withCtrl && e.key.toLowerCase()==='c'){
      if(ids.length){ try{ clipboardGraph = makeSubgraph(ids); window.__pf_clipboardGraph = clipboardGraph; }catch{ clipboardGraph=null; } }
      e.preventDefault();
    } else if(withCtrl && e.key.toLowerCase()==='x'){
      if(ids.length){ try{ clipboardGraph = makeSubgraph(ids); window.__pf_clipboardGraph = clipboardGraph; deleteNodes(ids); setSelection([]); render(); saveToLocal(); }catch{} }
      e.preventDefault();
    } else if(withCtrl && e.key.toLowerCase()==='v'){
      const g = window.__pf_clipboardGraph || clipboardGraph;
      if(g){ try{ const newIds = pasteSubgraph(g, { x:(lastMouseWorld.x||100), y:(lastMouseWorld.y||100) }); setSelection(newIds); render(); saveToLocal(); }catch{} }
      e.preventDefault();
    } else if(withCtrl && e.key.toLowerCase()==='d'){
      if(ids.length){ try{ const data = makeSubgraph(ids); const newIds = pasteSubgraph(data, { x:(lastMouseWorld.x||100)+20, y:(lastMouseWorld.y||100)+20 }); setSelection(newIds); render(); saveToLocal(); }catch{} }
      e.preventDefault();
    } else if(e.key==='Delete'){
      if(ids.length){ try{ deleteNodes(ids); setSelection([]); render(); saveToLocal(); }catch{} }
      e.preventDefault();
    } else if(e.key==='Escape'){
      if(state.pendingSrc){ try{ clearGhost(); }catch{} try{ closeQuickAdd(); }catch{} state.pendingSrc=null; document.querySelectorAll('.port.out.selected').forEach(p=> p.classList.remove('selected')); e.preventDefault(); }
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
          try{ openQuickAdd(x, y, state.pendingSrc); }catch{}
          e.preventDefault();
          return; // 接続モードは継続
        }
        // それ以外（UI等）をクリックしたらキャンセル
        state.pendingSrc = null; clearGhost(); closeQuickAdd(); document.querySelectorAll('.port.out.selected').forEach(p=> p.classList.remove('selected'));
      }
    }catch{}
  });
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
          try{ e.dataTransfer.setData('text/plain', text); }catch{}
          e.dataTransfer.effectAllowed = 'copy';
          el.classList.add('dragging');
        });
        el.addEventListener('dragend', ()=> el.classList.remove('dragging'));
      });
    }
  }catch{ wrap.innerHTML = '<div style="color:#9ba3af">取得失敗</div>'; }
  return box;
}

// expose boot for runtime
try{ window.__PF_boot = boot; }catch{}

 
