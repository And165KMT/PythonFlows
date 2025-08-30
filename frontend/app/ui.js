// UI and rendering for FlowPython
import { state, registry, getNode, addNode, selectNode, clearSelection, deleteNodeById, uid, computeUpstreamColumns, suggestionsForNode, genCode, genCodeUpTo, loadPackages, setPreviewModeProvider, upstreamOf, setSelection, addToSelection, removeFromSelection, isSelected, saveToLocal, restoreFromLocal, makeSubgraph, pasteSubgraph, deleteNodes, createGroup, getGroup, genCodeForNodes } from './nodes.js';
import { injectBaseStyles, styleTableHtml as styleTableHtmlUtil, escapeHtml as escapeHtmlUtil } from './utils.js';
import { drawEdges as drawEdgesMod, syncEdgesViewport as syncEdgesViewportMod, setGhost as setGhostMod, clearGhost as clearGhostMod } from './edges.js';
import { updateNodePreview as updateNodePreviewMod, isFigureNode as isFigureNodeMod, getPreviewMode as getPreviewModeMod } from './preview.js';
import { bindForm as bindFormMod } from './forms.js';
import { openContextMenu, closeContextMenu } from './contextmenu.js';
import { initInteractions as initInteractionsMod } from './interactions.js';
import { openQuickAdd as openQuickAddMod, closeQuickAdd as closeQuickAddMod } from './quickadd.js';

// Treat any node whose outputType is 'Figure' as a plot node (delegated)
function isFigureNode(n){ return isFigureNodeMod(n, registry); }

const canvasWrap = document.getElementById('canvasWrap');
const nodesEl = document.getElementById('nodes');
const edgesSvg = document.getElementById('edges');
const log = document.getElementById('log');
const genCodeEl = document.getElementById('genCode');
const statusEl = document.getElementById('status');
const toolbarEl = document.getElementById('toolbar');
const rightCode = document.getElementById('rightCode');
const rightVars = document.getElementById('rightVars');
const tabCode = document.getElementById('tabCode');
const tabVars = document.getElementById('tabVars');
const varsWrap = document.getElementById('varsWrap');
const previewModeEl = document.getElementById('previewMode');
let subsystemsEl; // created when rendering subsystems
// selection rectangle state
let selBoxEl = null; let selecting = false; let selStart = null; let lastSel = [];
let groupsLayer = null; // layer for subsystem frames inside #nodes
let lastMouseWorld = { x: 100, y: 100 };
let isSpaceDown = false; // hold Space for panning
let panning = false; let panStart = null; let panStartView = null;
let kernelDisabled = false; // backend gated kernel feature
let authRequired = false; // backend may require token
let authToken = null; // API token stored in sessionStorage when set

// Keep edges SVG in sync with canvas size using ResizeObserver
try{
  const ro = new ResizeObserver(() => {
    try{ syncEdgesViewport(); drawEdges(); }catch{}
  });
  if(canvasWrap) ro.observe(canvasWrap);
  window.addEventListener('resize', () => { try{ syncEdgesViewport(); drawEdges(); }catch{} });
}catch{}

// Auth helpers
function getStoredToken(){ try{ return sessionStorage.getItem('pf_token'); }catch{ return null; } }
function setStoredToken(tok){ authToken = tok || null; try{ if(authToken) sessionStorage.setItem('pf_token', authToken); else sessionStorage.removeItem('pf_token'); }catch{} }
function apiHeaders(extra){ const h = Object.assign({}, extra||{}); if(authRequired && authToken){ h['Authorization'] = 'Bearer ' + authToken; } return h; }
async function apiFetch(url, opts){ const o = Object.assign({ method:'GET' }, opts||{}); o.headers = apiHeaders(o.headers||{}); return fetch(url, o); }

// inject minimal styles for spinner and group frames
injectBaseStyles();

// ランボタン有効/無効の集中管理
let runningLock = false; // 実行中はtrue
function canRun(){ return !kernelDisabled && ws && ws.readyState===1 && !runningLock; }
function updateRunButtonsState(){
  const enabled = canRun();
    const toggle = (btn) => { if (!btn) return; btn.disabled = !enabled; btn.classList.toggle('btn-busy', !enabled); };
  // グローバル
  toggle(globalRunBtn);
  // ノード
  document.querySelectorAll('.node .node-run').forEach(toggle);
  // サブシステム一覧
  document.querySelectorAll('#subsystems .run-sub').forEach(toggle);
  // サブシステム枠
  document.querySelectorAll('.group-frame .actions .run').forEach(toggle);
}

// Busy-run helper: disable a button, show spinner while async fn runs
async function runWithBusy(fn, btn, runningLabel){
  try{
    // カーネル未接続や実行中は押せない
    if(!canRun()) { updateRunButtonsState(); return; }
    // 実行開始: Variables更新をidleで一度だけ行うためのフラグ
    pendingVarsRefresh = true; 
    runningLock = true; statusEl.textContent='running...'; updateRunButtonsState();
    if(btn && btn.classList.contains('btn-busy') === false){
      const orig = { html: btn.innerHTML, text: btn.textContent };
      btn.disabled = true; btn.classList.add('btn-busy');
      const label = (typeof runningLabel==='string' && runningLabel) ? runningLabel : (orig.text||'Running');
      btn.innerHTML = `<span class="spinner"></span>${label}`;
      try{ await fn(); }
  finally{ btn.disabled = false; btn.classList.remove('btn-busy'); btn.innerHTML = orig.html; /* runningLock is released on WS idle */ updateRunButtonsState(); }
    } else {
      try{ await fn(); }
  finally{ /* runningLock is released on WS idle */ updateRunButtonsState(); }
    }
  }catch(err){ runningLock = false; updateRunButtonsState(); appendLog('[run] error ' + (err && err.message ? err.message : String(err))); }
}

// Simple image zoom overlay
function openZoomOverlay(src){
  try{
    const ov = document.createElement('div');
    Object.assign(ov.style, { position:'fixed', inset:'0', background:'rgba(0,0,0,0.7)', zIndex:5000, display:'flex', alignItems:'center', justifyContent:'center' });
    const img = document.createElement('img'); img.src = src; Object.assign(img.style, { maxWidth:'90%', maxHeight:'90%', boxShadow:'0 10px 30px rgba(0,0,0,0.6)', border:'1px solid #000' });
    ov.appendChild(img);
    const close = ()=>{ ov.remove(); document.removeEventListener('keydown', onKey); };
    const onKey = (e) => { if (e.key === 'Escape') close(); };
    ov.addEventListener('click', close);
    document.addEventListener('keydown', onKey);
    document.body.appendChild(ov);
  }catch{}
}

// Variable preview modal (DataFrame CSV with column filter)
async function openVarPreview(name){
  try{
    const url = `/api/variables/${encodeURIComponent(name)}/export?format=csv`;
    const res = await apiFetch(url);
    const text = await res.text();
    const lines = text.split(/\r?\n/).filter(l=> l.length>0);
    if(lines.length===0) return;
    const parseRow = (row)=> row.split(',');
    const header = parseRow(lines[0]);
    const rows = lines.slice(1, 201).map(parseRow); // limit to 200 rows
    const overlay = document.createElement('div'); overlay.className='modal-overlay';
    const modal = document.createElement('div'); modal.className='modal';
    const head = document.createElement('div'); head.className='modal-head'; head.innerHTML = `<div class="title">${escapeHtml(name)} preview</div><span class="chip">${rows.length.toLocaleString()} rows</span>`;
    const body = document.createElement('div'); body.className='modal-body';
    const foot = document.createElement('div'); foot.className='modal-foot';
    const closeBtn = document.createElement('button'); closeBtn.textContent='Close'; closeBtn.className='secondary';
    const filterInput = document.createElement('input'); filterInput.className='input'; filterInput.placeholder='列名フィルタ（カンマ区切り、空で全列）'; filterInput.style.flex='1';
    head.appendChild(document.createElement('div')).style.marginLeft='auto';
    head.appendChild(filterInput);
    foot.appendChild(closeBtn);
    modal.appendChild(head); modal.appendChild(body); modal.appendChild(foot); overlay.appendChild(modal);
    const renderTable = (cols)=>{
      const idx = cols && cols.length ? cols.map(c=> header.indexOf(c)).filter(i=> i>=0) : header.map((_,i)=> i);
      const th = idx.map(i=> `<th style="text-align:left; border-bottom:1px solid #263041; padding:4px 6px;">${escapeHtml(String(header[i]||''))}</th>`).join('');
      const trs = rows.map(r=> `<tr>${idx.map(i=> `<td style="padding:4px 6px; border-bottom:1px solid #111824;">${escapeHtml(String(r[i]||''))}</td>`).join('')}</tr>`).join('');
      body.innerHTML = `<div style="width:100%; overflow:auto"><table style="min-width:600px; border-collapse:collapse; font-size:12px;"><thead><tr>${th}</tr></thead><tbody>${trs}</tbody></table></div>`;
    };
    renderTable(null);
    const applyFilter = ()=>{
      const raw = String(filterInput.value||'').trim();
      if(!raw){ renderTable(null); return; }
      const want = raw.split(',').map(s=> s.trim()).filter(Boolean);
      renderTable(want);
    };
    filterInput.addEventListener('change', applyFilter);
    filterInput.addEventListener('keyup', (e)=>{ if(e.key==='Enter') applyFilter(); });
    const close = ()=> overlay.remove();
    closeBtn.addEventListener('click', close);
    overlay.addEventListener('click', (e)=>{ if(e.target===overlay) close(); });
    window.addEventListener('keydown', function onk(e){ if(e.key==='Escape'){ close(); window.removeEventListener('keydown', onk); } });
    document.body.appendChild(overlay);
    setTimeout(()=> filterInput.focus(), 30);
  }catch(e){ appendLog('[preview] failed to open'); }
}

function getPreviewMode(){ return getPreviewModeMod(previewModeEl); }
setPreviewModeProvider(getPreviewMode);

const resizer = document.getElementById('rightResizer');
if(resizer){ let dragging=false, startX=0, startW=0; resizer.addEventListener('mousedown', (e)=>{ dragging=true; startX=e.clientX; const cs = getComputedStyle(document.documentElement); const w = cs.getPropertyValue('--right-w').trim(); startW = parseInt(w||'380') || 380; document.body.style.userSelect='none'; }); window.addEventListener('mouseup', ()=>{ if(!dragging) return; dragging=false; document.body.style.userSelect=''; }); window.addEventListener('mousemove', (e)=>{ if(!dragging) return; const dx = startX - e.clientX; const newW = Math.max(260, Math.min(900, startW + dx)); document.documentElement.style.setProperty('--right-w', newW + 'px'); syncEdgesViewport(); }); }

// Toolbar top bar with Run All
const tabsBar = document.createElement('div'); tabsBar.style.display='flex'; tabsBar.style.gap='6px'; tabsBar.style.marginBottom='8px'; toolbarEl.before(tabsBar);
const globalRunBtn = document.createElement('button'); globalRunBtn.textContent='▶ Run All'; Object.assign(globalRunBtn.style, { padding:'6px 10px', background:'#1f6feb', color:'#fff', border:'0', borderRadius:'6px', cursor:'pointer' }); globalRunBtn.addEventListener('click', async ()=>{
  await runWithBusy(async ()=>{
    ensureWS(); clearLog(); statusEl.textContent='running...';
    const code = genCode(); genCodeEl.textContent = code;
    const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) });
    let js={}; try{ js=await res.json(); }catch{}
  appendLog('Sent exec: ' + JSON.stringify(js));
  }, globalRunBtn, 'Running...');
}); tabsBar.appendChild(globalRunBtn);

function appendLog(x, level){
  const line = document.createElement('div');
  line.className = 'log-line ' + (level||'info');
  line.textContent = x;
  log.appendChild(line);
  log.scrollTop = log.scrollHeight;
}
function clearLog(){ log.innerHTML = ''; }
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

// 追加: クリップボード（キャンバス貼り付け用に window にも同期）
let clipboardGraph = null;

// ノード右クリックメニュー
function openNodeContextMenu(nodeId, clientX, clientY){
  const ids = (state.selection && state.selection.size && state.selection.has(nodeId))
    ? Array.from(state.selection)
    : [nodeId];
  const doCopy = ()=>{ try{ clipboardGraph = makeSubgraph(ids); }catch{ clipboardGraph = null; } window.__pf_clipboardGraph = clipboardGraph; };
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
const escapeHtml = (s)=> escapeHtmlUtil(s);
const styleTableHtml = (html)=> styleTableHtmlUtil(html);
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
        const csvBtn = `<button class="var-csv" data-var="${name}" title="Download CSV" style="margin-left:8px; font-size:11px;">CSV</button>`;
        // wrap DataFrame HTML in a horizontally scrollable container so全列閲覧可
        return `<tr data-var="${name}" data-type="${type}"><td>${nameCell}</td><td>${type}</td><td><div style="max-width:100%; overflow:auto">${styleTableHtml(v.html)}</div>${dims}${csvBtn}</td></tr>`;
      }
      if(tLower==='ndarray'){
        const shp = Array.isArray(v.shape)? `shape=${escapeHtml(String(v.shape))}` : '';
        const val = (v.repr!=null? String(v.repr): '');
        const csvBtn = `<button class="var-csv" data-var="${name}" title="Download CSV" style="margin-left:8px; font-size:11px;">CSV</button>`;
        return `<tr data-var="${name}" data-type="${type}"><td>${nameCell}</td><td>${type}</td><td>${escapeHtml(val).slice(0,200)} <span style="color:var(--sub); font-size:11px;">${shp}</span>${csvBtn}</td></tr>`;
      }
      const val = (v.repr!=null? String(v.repr): (v.value!=null? String(v.value): ''));
      return `<tr data-var="${name}" data-type="${type}"><td>${nameCell}</td><td>${type}</td><td>${escapeHtml(val).slice(0,200)}</td></tr>`;
    }).join('');
    // 横スクロールを可能にするため、テーブルのcol幅固定を外し、全体をoverflow:autoで包む
    varsWrap.innerHTML = `<div style="width:100%; overflow:auto"><table style="min-width:480px; border-collapse:collapse; font-size:12px;"><thead><tr><th style=\"text-align:left; border-bottom:1px solid #263041; padding:4px 6px;\">名前</th><th style=\"text-align:left; border-bottom:1px solid #263041; padding:4px 6px;\">型</th><th style=\"text-align:left; border-bottom:1px solid #263041; padding:4px 6px;\">値</th></tr></thead><tbody style="word-break:break-word;">${rows || '<tr><td colspan=\"3\" style=\"padding:6px; color:#9ba3af;\">変数がありません</td></tr>'}</tbody></table></div>`;
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
          const res2 = await apiFetch(url);
          const ct = (res2.headers.get('content-type')||'').toLowerCase();
          if(ct.includes('application/json')){
            const j = await res2.json().catch(()=>({}));
            appendLog('[export] error ' + JSON.stringify(j));
            return;
          }
          const blob = await res2.blob();
          const a = document.createElement('a');
          a.href = URL.createObjectURL(blob);
          a.download = `${name}.csv`;
          document.body.appendChild(a); a.click(); setTimeout(()=>{ URL.revokeObjectURL(a.href); a.remove(); }, 500);
        }catch(e){ appendLog('[export] failed'); }
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

function activateTab(which){ if(which==='code'){ rightCode.style.display='block'; rightVars.style.display='none'; tabCode?.classList.add('active'); tabVars?.classList.remove('active'); tabCode?.setAttribute('aria-selected','true'); tabVars?.setAttribute('aria-selected','false'); } else { rightCode.style.display='none'; rightVars.style.display='block'; tabVars?.classList.add('active'); tabCode?.classList.remove('active'); tabVars?.setAttribute('aria-selected','true'); tabCode?.setAttribute('aria-selected','false'); refreshVariables(); } }
tabCode?.addEventListener('click', ()=> activateTab('code'));
tabVars?.addEventListener('click', ()=> activateTab('vars'));
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
  <div class=\"preview\" ${wantPreview? '':'style=\"display:none\"'} style=\"max-height:${previewH}px\"> <div id=\"prev-${node.id}\"></div> <div class=\"node-resize-v\" title=\"Drag to resize preview height\"></div> </div>
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
          const code = genCodeUpTo(node.id); genCodeEl.textContent = code;
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
        const code = genCodeForNodes(g.nodeIds, true); genCodeEl.textContent = code;
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
  ensureWS(); statusEl.textContent='running...'; const code = genCodeForNodes(g.nodeIds, true); genCodeEl.textContent = code; const res = await apiFetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) }); let js={}; try{ js=await res.json(); }catch{} appendLog('Sent exec (group-frame): ' + JSON.stringify(js));
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

// WebSocket & streaming
let ws; let pendingVarsRefresh = false; function ensureWS(){ if(ws && ws.readyState===1) return; const proto=(location.protocol==='https:'?'wss://':'ws://'); const tokenQs = (authRequired && authToken) ? ('?token='+encodeURIComponent(authToken)) : ''; ws = new WebSocket(proto + location.host + '/ws' + tokenQs); ws.onopen = ()=> { appendLog('[ws] connected'); updateRunButtonsState(); }; ws.onclose = ()=> { appendLog('[ws] closed'); updateRunButtonsState(); }; ws.onmessage = ev => { const data = JSON.parse(ev.data); if(data.type==='error' && data.content && data.content.message==='kernel feature disabled'){ kernelDisabled = true; statusEl.textContent='kernel disabled'; appendLog('[kernel] feature disabled'); try{ ws && ws.close(); }catch{} updateRunButtonsState(); return; } if (data.type === 'stream') { const streamName = (data.content && data.content.name) ? data.content.name : ''; const t = data.content.text || ''; if(streamName==='stderr'){ t.split(/\r?\n/).forEach(ln=>{ if(ln) appendLog(ln, 'stderr'); }); return; } const re = /\[\[PREVIEW:([^:]+):(HEAD|DESC)\]\]([\s\S]*?)(?=(\n\[\[PREVIEW:|$))/g; let m; let rest = t; while((m = re.exec(t))){ const id=m[1], kind=m[2], body=(m[3]||''); if(kind==='HEAD'){ state.preview.head.set(id, body); const tgt = document.getElementById('prev-' + id); if(tgt){ const hasImg = !!tgt.querySelector('img'); if(!hasImg && !state.preview.headHtml.get(id)){ tgt.innerHTML = `<pre style="margin:0; white-space:pre-wrap">${body.replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]))}</pre>`; } } } else { state.preview.desc.set(id, body); } } const reHtml = /\[\[PREVIEW:([^:]+):(HEADHTML|DESCHTML)\]\]([\s\S]*?)(?=(\n\[\[PREVIEW:|$))/g; let mh; while((mh = reHtml.exec(t))){ const id=mh[1], kind=mh[2], body=(mh[3]||''); const n=getNode(id); const pmode=getPreviewMode(); const want = document.getElementById('prev-' + id) && (pmode==='all' || (pmode==='plots' && n && isFigureNode(n))); if(!want) continue; if(n && isFigureNode(n) && pmode!=='all' && pmode!=='plots'){ continue; } if(kind==='HEADHTML'){ state.preview.headHtml.set(id, body); updateNodePreview(id); } else { state.preview.descHtml.set(id, body); updateNodePreview(id); } } rest = rest.replace(re, '').replace(reHtml, ''); const lines = String(rest).split(/\r?\n/); for(const ln of lines){ if(!ln) continue; /* INSERT START: handle [[SKIP:nid]] */ let msK = ln.match(/^\[\[SKIP:([^\]]+)\]\]$/); if(msK){ const nid = msK[1]; const title = document.querySelector(`[data-node-id="${nid}"] .head .title`); if(title){ const old = title.querySelector('.chip'); if(old) old.remove(); const chip = document.createElement('span'); chip.className='chip'; chip.textContent='skip'; title.appendChild(chip); } appendLog(`[node ${nid}] unchanged -> skip`); continue; } /* INSERT END */ let mb = ln.match(/^\[\[NODE:([^:]+):BEGIN\]\]$/); if(mb){ const nid = mb[1]; state.stream.currentNodeId = nid; state.preview.head.delete(nid); state.preview.desc.delete(nid); state.preview.headHtml.delete(nid); state.preview.descHtml.delete(nid); state.stream.buffers.set(nid, ''); state.stream.timings = state.stream.timings || new Map(); state.stream.timings.set(nid, { start: performance.now() }); const tgt = document.getElementById('prev-' + nid); if(tgt){ tgt.innerHTML = '<div class="empty">Running…</div>'; } continue; } let me = ln.match(/^\[\[NODE:([^:]+):END\]\]$/); if(me){ const nid = me[1]; const rec = (state.stream.timings && state.stream.timings.get(nid)) || null; const end = performance.now(); const ms = rec && rec.start ? Math.max(0, Math.round(end - rec.start)) : null; state.stream.currentNodeId = null; if(ms!=null){ const tgt = document.querySelector(`[data-node-id="${nid}"] .head .title`); if(tgt){ const old = tgt.querySelector('.chip'); if(old) old.remove(); const chip = document.createElement('span'); chip.className='chip'; chip.textContent = `${ms} ms`; tgt.appendChild(chip); } } continue; } const cur = state.stream.currentNodeId; if(cur){ const n=getNode(cur); const pmode=getPreviewMode(); const tgt = document.getElementById('prev-' + cur); const allowText = tgt && (pmode==='all' || (pmode==='plots' && n && isFigureNode(n))); if(allowText){ if(!(n && isFigureNode(n))){ const prevTxt = state.stream.buffers.get(cur) || ''; const next = prevTxt + (prevTxt? '\n':'') + ln; state.stream.buffers.set(cur, next); if(tgt && !tgt.querySelector('img') && !state.preview.headHtml.get(cur)){ tgt.innerHTML = `<pre style=\"margin:0; white-space:pre-wrap\">${next.replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]))}</pre>`; } } } } else { appendLog(ln); } } updatePreviewDock(); } else if (data.type === 'display_data' || data.type === 'execute_result') { const d = data.content.data || {}; if(d['image/png']){ let nid = (state.lastPlotNodeId||''); let n = getNode(nid); const pmode=getPreviewMode(); let tgt = document.getElementById('prev-' + nid); if(!(n && isFigureNode(n)) || !tgt){ const figs = state.nodes.filter(isFigureNode); if(figs.length){ nid = figs[figs.length-1].id; n = getNode(nid); tgt = document.getElementById('prev-' + nid); } } const allowPlot = pmode!=='none' && (pmode==='all' || (pmode==='plots' && n && isFigureNode(n))); if(allowPlot && tgt){ const imgHtml = `<img style=\"margin-top:8px\" src=\"data:image/png;base64,${d['image/png']}\">`; if(n && isFigureNode(n)){ tgt.innerHTML = imgHtml; const wrap=document.getElementById('prevwrap-'+nid); if(wrap) wrap.open = true; } else { const existingImg = tgt.querySelector('img'); if(tgt.querySelector('.node-preview-grid')){ if(existingImg) existingImg.remove(); tgt.insertAdjacentHTML('beforeend', imgHtml); } else { tgt.innerHTML = imgHtml; } } } } else if (d['text/plain']) { appendLog(d['text/plain']); } else { appendLog('[output] ' + JSON.stringify(d)); } } else if (data.type === 'error') { appendLog('[error] ' + (data.content.ename + ': ' + data.content.evalue), 'error'); const id = state.stream.currentNodeId; if(id){ const tgt = document.getElementById('prev-' + id); if(tgt){ tgt.innerHTML = `<pre style=\"color:#ff8888; white-space:pre-wrap; margin:0\">${(data.content.evalue||'').toString().replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]))}</pre>`; const wrap=document.getElementById('prevwrap-'+id); if(wrap) wrap.open = true; } } } else if (data.type === 'status') { if(data.content && data.content.execution_state==='idle'){ if(pendingVarsRefresh){ pendingVarsRefresh=false; try{ refreshVariables(); }catch{} } runningLock=false; updateRunButtonsState(); } else { runningLock=true; updateRunButtonsState(); } } }; }

function updateNodePreview(id){ return updateNodePreviewMod(state, registry, id); }

export async function boot(){
  await loadPackages();
  renderToolbar();
  // Left pane scroll
  try{ toolbarEl.style.overflowY='auto'; toolbarEl.style.maxHeight='calc(100vh - 120px)'; }catch{}
  applyViewTransform();
  // try restore
  try { restoreFromLocal(); } catch {}

  // Add extra actions (auth/flows)
  const actionsEl = document.getElementById('actions');
  const signBtn = document.createElement('button'); signBtn.id='signBtn'; signBtn.className='secondary'; signBtn.textContent='Sign in';
  const saveFlowBtn = document.createElement('button'); saveFlowBtn.id='saveFlowBtn'; saveFlowBtn.className='secondary'; saveFlowBtn.textContent='Save Flow';
  const loadFlowBtn = document.createElement('button'); loadFlowBtn.id='loadFlowBtn'; loadFlowBtn.className='secondary'; loadFlowBtn.textContent='Load Flow';
  actionsEl?.appendChild(saveFlowBtn); actionsEl?.appendChild(loadFlowBtn); actionsEl?.appendChild(signBtn);

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
    try{ if(ws) ws.close(); }catch{}
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
