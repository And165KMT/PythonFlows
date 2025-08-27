// UI and rendering for FlowPython
import { state, registry, getNode, addNode, selectNode, clearSelection, deleteNodeById, uid, computeUpstreamColumns, suggestionsForNode, genCode, genCodeUpTo, loadPackages, setPreviewModeProvider, upstreamOf, setSelection, addToSelection, removeFromSelection, isSelected, saveToLocal, restoreFromLocal, makeSubgraph, pasteSubgraph, deleteNodes, createGroup, getGroup, genCodeForNodes } from './nodes.js';

// Treat any node whose outputType is 'Figure' as a plot node
function isFigureNode(n){
  if(!n) return false;
  const def = registry.nodes.get(n.type);
  return String(def?.outputType||'') === 'Figure';
}

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

// inject minimal styles for spinner and group frames
(function injectStyles(){
  try{
    const style = document.createElement('style');
    style.textContent = `@keyframes spin{from{transform:rotate(0)}to{transform:rotate(360deg)}}
    .spinner{display:inline-block;width:14px;height:14px;border:2px solid #1f6feb;border-right-color:transparent;border-radius:50%;animation:spin .8s linear infinite;margin-right:6px;vertical-align:-2px}
    .btn-busy{opacity:.7; pointer-events:none}
    .group-frame{position:absolute; border:2px dashed #385a9a; background:rgba(31,111,235,0.06); border-radius:8px; padding:22px 10px 10px 10px; box-sizing:border-box; pointer-events:none}
    .group-frame .title{position:absolute; top:0; left:8px; transform:translateY(-60%); background:#0b1220; padding:2px 8px; border:1px solid #263041; border-radius:999px; font-size:12px; cursor:move; pointer-events:auto}
    .group-frame .actions{position:absolute; top:2px; right:6px; display:flex; gap:6px; pointer-events:auto}
    .group-frame .actions button{padding:4px 8px; border:1px solid #2a3445; background:#111824; color:var(--text); border-radius:6px; cursor:pointer; font-size:12px}`;
    document.head.appendChild(style);
  }catch{}
})();

// ランボタン有効/無効の集中管理
let runningLock = false; // 実行中はtrue
function canRun(){ return ws && ws.readyState===1 && !runningLock; }
function updateRunButtonsState(){
  const enabled = canRun();
  const toggle = (btn)=>{ if(!btn) return; btn.disabled = !enabled; btn.classList.toggle('btn-busy', !enabled); };
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
    const onKey = (e)=>{ if(e.key==='Escape') close(); };
    ov.addEventListener('click', close);
    document.addEventListener('keydown', onKey);
    document.body.appendChild(ov);
  }catch{}
}

function getPreviewMode(){ const v = previewModeEl?.value || 'plots'; return (v==='all' || v==='plots' || v==='none') ? v : 'plots'; }
setPreviewModeProvider(getPreviewMode);

const resizer = document.getElementById('rightResizer');
if(resizer){ let dragging=false, startX=0, startW=0; resizer.addEventListener('mousedown', (e)=>{ dragging=true; startX=e.clientX; const cs = getComputedStyle(document.documentElement); const w = cs.getPropertyValue('--right-w').trim(); startW = parseInt(w||'380') || 380; document.body.style.userSelect='none'; }); window.addEventListener('mouseup', ()=>{ if(!dragging) return; dragging=false; document.body.style.userSelect=''; }); window.addEventListener('mousemove', (e)=>{ if(!dragging) return; const dx = startX - e.clientX; const newW = Math.max(260, Math.min(900, startW + dx)); document.documentElement.style.setProperty('--right-w', newW + 'px'); syncEdgesViewport(); }); }

// Toolbar top bar with Run All
const tabsBar = document.createElement('div'); tabsBar.style.display='flex'; tabsBar.style.gap='6px'; tabsBar.style.marginBottom='8px'; toolbarEl.before(tabsBar);
const globalRunBtn = document.createElement('button'); globalRunBtn.textContent='▶ Run All'; Object.assign(globalRunBtn.style, { padding:'6px 10px', background:'#1f6feb', color:'#fff', border:'0', borderRadius:'6px', cursor:'pointer' }); globalRunBtn.addEventListener('click', async ()=>{
  await runWithBusy(async ()=>{
    ensureWS(); clearLog(); statusEl.textContent='running...';
    const code = genCode(); genCodeEl.textContent = code;
    const res = await fetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) });
    let js={}; try{ js=await res.json(); }catch{}
  appendLog('Sent exec: ' + JSON.stringify(js));
  }, globalRunBtn, 'Running...');
}); tabsBar.appendChild(globalRunBtn);

function appendLog(x){ log.textContent += x + "\n"; log.scrollTop = log.scrollHeight; }
function clearLog(){ log.textContent = ''; }
function centerOf(el){ const r = el.getBoundingClientRect(); const p = edgesSvg.getBoundingClientRect(); return { x: r.left - p.left + r.width/2, y: r.top - p.top + r.height/2 }; }
// View helpers (screen<->world)
function getScale(){ return state.view?.scale || 1; }
function getTx(){ return state.view?.tx || 0; }
function getTy(){ return state.view?.ty || 0; }
function screenToWorldPoint(clientX, clientY){ const rect = canvasWrap.getBoundingClientRect(); const x = clientX - rect.left; const y = clientY - rect.top; const s = getScale(); return { x: (x - getTx())/s, y: (y - getTy())/s }; }
function applyViewTransform(){ const s=getScale(), tx=getTx(), ty=getTy(); nodesEl.style.transformOrigin='0 0'; nodesEl.style.transform = `translate(${tx}px, ${ty}px) scale(${s})`; }
function updatePreviewDock(){}

// --- Selection Rectangle helpers ---
function ensureSelBox(){ if(selBoxEl) return; selBoxEl = document.createElement('div'); selBoxEl.id = 'selectionBox'; Object.assign(selBoxEl.style, { position:'absolute', border:'1px dashed #1f6feb', background:'rgba(31,111,235,0.1)', pointerEvents:'none', display:'none', zIndex:1500 }); canvasWrap.appendChild(selBoxEl); }
function worldRectFromScreen(a, b){ const p1 = screenToWorldPoint(a.x, a.y); const p2 = screenToWorldPoint(b.x, b.y); const x1 = Math.min(p1.x, p2.x), y1 = Math.min(p1.y, p2.y); const x2 = Math.max(p1.x, p2.x), y2 = Math.max(p1.y, p2.y); return { x:x1, y:y1, w:x2-x1, h:y2-y1 }; }
function rectsIntersect(r1, r2){ return !(r2.x>r1.x+r1.w || r2.x+r2.w<r1.x || r2.y>r1.y+r1.h || r2.y+r2.h<r1.y); }

// Variables
function escapeHtml(s){ return String(s).replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch])); }
function styleTableHtml(html){ try{ const wrapper = document.createElement('div'); wrapper.innerHTML = html; const table = wrapper.querySelector('table'); if(table){ table.style.width='100%'; table.style.borderCollapse='collapse'; table.querySelectorAll('th,td').forEach(cell=>{ cell.style.border='1px solid #263041'; cell.style.padding='4px 6px'; }); table.querySelectorAll('thead').forEach(t=> t.style.background = '#111824'); table.querySelectorAll('tbody tr:nth-child(even)').forEach(tr=> tr.style.background = '#0b1220'); table.style.color = 'var(--text)'; table.style.fontSize = '12px'; return wrapper.innerHTML; } }catch{} return html; }
function filterVars(arr){ try{ return (arr||[]).filter(v=>{ const t = String(v.type||'').toLowerCase(); const n = String(v.name||'').toLowerCase(); if(n==='exit' || n==='quit') return false; if(n==='in' || n==='out') return false; if(n.startsWith('_')) return false; if(t.includes('module')) return false; if(t.includes('function')) return false; if(t.includes('method')) return false; if(t.includes('autocall')) return false; if(t.includes('zmqexitautocall')) return false; return true; }); }catch{ return arr||[]; } }
async function refreshVariables(){
  if(!rightVars || rightVars.style.display==='none') return;
  try{
    const res = await fetch('/api/variables');
    const js = await res.json();
    const arrRaw = Array.isArray(js.variables) ? js.variables : [];
    const arr = filterVars(arrRaw);
    const rows = arr.map(v=>{
      const name = escapeHtml(v.name);
      const type = escapeHtml(v.type);
      const nameCell = `<span class="var-item" draggable="true" data-var="${name}" title="ドラッグ＆ドロップでノードの入力に上書き"><svg class="drag-handle" viewBox="0 0 24 24" aria-hidden="true"><path d="M4 7h16M4 12h16M4 17h16" stroke="currentColor" stroke-width="2" stroke-linecap="round"/></svg><span class="var-label">${name}</span></span>`;
      if(String(v.type).toLowerCase()==='dataframe' && v.html){
        const dims = (typeof v.rows==='number' && typeof v.cols==='number') ? `<div style="color:var(--sub); font-size:11px; margin-top:4px">${v.rows.toLocaleString()} rows × ${v.cols.toLocaleString()} cols</div>` : '';
        return `<tr><td>${nameCell}</td><td>${type}</td><td>${styleTableHtml(v.html)}${dims}</td></tr>`;
      }
      const val = (v.repr!=null? String(v.repr): (v.value!=null? String(v.value): ''));
      return `<tr><td>${nameCell}</td><td>${type}</td><td>${escapeHtml(val).slice(0,200)}</td></tr>`;
    }).join('');
    varsWrap.innerHTML = `<table style="width:100%; border-collapse:collapse; font-size:12px; table-layout:fixed;"><colgroup><col style="width:32%"><col style="width:20%"><col style="width:48%"></colgroup><thead><tr><th style=\"text-align:left; border-bottom:1px solid #263041; padding:4px 6px;\">名前</th><th style=\"text-align:left; border-bottom:1px solid #263041; padding:4px 6px;\">型</th><th style=\"text-align:left; border-bottom:1px solid #263041; padding:4px 6px;\">値</th></tr></thead><tbody style="word-break:break-word;">${rows || '<tr><td colspan=\"3\" style=\"padding:6px; color:#9ba3af;\">変数がありません</td></tr>'}</tbody></table>`;
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
  }catch{
    varsWrap.innerHTML = '<div style="color:#9ba3af">変数の取得に失敗しました</div>';
  }
}

function activateTab(which){ if(which==='code'){ rightCode.style.display='block'; rightVars.style.display='none'; tabCode?.classList.add('active'); tabVars?.classList.remove('active'); tabCode?.setAttribute('aria-selected','true'); tabVars?.setAttribute('aria-selected','false'); } else { rightCode.style.display='none'; rightVars.style.display='block'; tabVars?.classList.add('active'); tabCode?.classList.remove('active'); tabVars?.setAttribute('aria-selected','true'); tabCode?.setAttribute('aria-selected','false'); refreshVariables(); } }
tabCode?.addEventListener('click', ()=> activateTab('code'));
tabVars?.addEventListener('click', ()=> activateTab('vars'));
previewModeEl?.addEventListener('change', ()=>{ render(); const figs = state.nodes.filter(isFigureNode); if(figs.length){ state.lastPlotNodeId = figs[figs.length-1].id; } });

function syncEdgesViewport(){ const w = canvasWrap.clientWidth || canvasWrap.getBoundingClientRect().width; const h = canvasWrap.clientHeight || canvasWrap.getBoundingClientRect().height; if(w && h){ edgesSvg.setAttribute('width', String(Math.floor(w))); edgesSvg.setAttribute('height', String(Math.floor(h))); edgesSvg.setAttribute('viewBox', `0 0 ${Math.floor(w)} ${Math.floor(h)}`); } }
function drawEdges(){
  syncEdgesViewport();
  // ensure group wrapper
  let g = edgesSvg.querySelector('g');
  if(!g){ g = document.createElementNS('http://www.w3.org/2000/svg','g'); edgesSvg.appendChild(g); }
  g.innerHTML='';
  state.edges.forEach(e => {
    const from = document.querySelector(`[data-node-id="${e.from}"]`);
    const to = document.querySelector(`[data-node-id="${e.to}"]`);
    if (!from || !to) return;
    const a = centerOf(from.querySelector('.port.out'));
    const b = centerOf(to.querySelector('.port.in'));
    const path = document.createElementNS('http://www.w3.org/2000/svg','path');
    const dx = Math.abs(b.x - a.x) * 0.5;
    const d = `M ${a.x} ${a.y} C ${a.x+dx} ${a.y}, ${b.x-dx} ${b.y}, ${b.x} ${b.y}`;
    path.setAttribute('d', d);
    path.setAttribute('class','edge');
    g.appendChild(path);
  });
}
function getPortCenter(nodeId, selector){ const nodeEl = document.querySelector(`[data-node-id="${nodeId}"]`); if(!nodeEl) return null; const port = nodeEl.querySelector(selector); if(!port) return null; return centerOf(port); }
function setGhost(toX, toY){
  let g = edgesSvg.querySelector('g');
  if(!g){ g = document.createElementNS('http://www.w3.org/2000/svg','g'); edgesSvg.appendChild(g); }
  let ghost = document.getElementById('ghost-edge');
  if(!ghost){ ghost = document.createElementNS('http://www.w3.org/2000/svg','path'); ghost.id='ghost-edge'; ghost.setAttribute('class','edge'); ghost.setAttribute('stroke-dasharray','5,5'); g.appendChild(ghost); }
  const a = getPortCenter(state.pendingSrc, '.port.out'); if(!a){ ghost.remove(); return; }
  const dx = Math.abs(toX - a.x) * 0.5; const d = `M ${a.x} ${a.y} C ${a.x+dx} ${a.y}, ${toX-dx} ${toY}, ${toX} ${toY}`; ghost.setAttribute('d', d);
}
function clearGhost(){ const ghost = document.getElementById('ghost-edge'); if(ghost) ghost.remove(); }

// Quick Add
const quickAdd = document.createElement('div'); quickAdd.id='quickAdd'; Object.assign(quickAdd.style, { position:'absolute', display:'none', zIndex:'2000', background:'#0b1220', border:'1px solid #263041', borderRadius:'8px', minWidth:'260px', maxWidth:'320px', boxShadow:'0 8px 24px rgba(0,0,0,0.35)' }); quickAdd.innerHTML = `<div style="padding:8px 8px 0 8px; border-bottom:1px solid #263041"><input id="qaSearch" placeholder="Search nodes..." style="width:100%; padding:6px 8px; border:1px solid #2c3b52; border-radius:6px; background:#111824; color:var(--text)"></div><div id="qaSuggestedWrap" style="padding:8px; display:none"><div style="font-size:12px; opacity:0.8; margin-bottom:6px">Suggested</div><div id="qaSuggested" style="display:flex; flex-wrap:wrap; gap:6px"></div></div><div id="qaAll" style="max-height:240px; overflow:auto; padding:6px 0"></div>`; document.body.appendChild(quickAdd);
function itemHtml(type){ const def = registry.nodes.get(type); const label = def?.title || type.split('.').slice(-1)[0] || type; return `<button data-type="${type}" style="display:block; width:100%; text-align:left; padding:8px 10px; background:transparent; color:var(--text); border:0; cursor:pointer">${label} <span style="opacity:0.6; font-size:12px">(${type})</span></button>`; }
function buttonPill(type){ const def = registry.nodes.get(type); const label = def?.title || type.split('.').slice(-1)[0] || type; return `<button data-type="${type}" style="padding:6px 8px; background:#111824; color:var(--text); border:1px solid #263041; border-radius:999px; cursor:pointer">${label}</button>`; }
function openQuickAdd(x, y, fromId){ const sWrap = quickAdd.querySelector('#qaSuggestedWrap'); const s = quickAdd.querySelector('#qaSuggested'); const all = quickAdd.querySelector('#qaAll'); const inp = quickAdd.querySelector('#qaSearch'); quickAdd.style.left = `${x}px`; quickAdd.style.top = `${y}px`; const sugg = suggestionsForNode(fromId); if(sugg.length){ sWrap.style.display='block'; s.innerHTML = sugg.map(buttonPill).join(''); } else { sWrap.style.display='none'; s.innerHTML=''; } const types = Array.from(registry.nodes.keys()).filter(t=> !(registry.nodes.get(t)?.hidden)); all.innerHTML = types.map(itemHtml).join(''); quickAdd.style.display = 'block'; const clickHandler = (ev)=>{ const btn = ev.target.closest('button[data-type]'); if(!btn) return; ev.preventDefault(); ev.stopPropagation(); const type = btn.getAttribute('data-type'); addAndConnect(type, fromId); closeQuickAdd(); }; quickAdd.addEventListener('click', clickHandler, { once: true }); inp.value=''; inp.oninput = ()=>{ const q = inp.value.toLowerCase(); const list = types.filter(t=> t.toLowerCase().includes(q) || (registry.nodes.get(t)?.title||'').toLowerCase().includes(q)); all.innerHTML = list.map(itemHtml).join(''); }; setTimeout(()=>{ const closeOnOutside = (e)=>{ if(!quickAdd.contains(e.target)){ closeQuickAdd(); document.removeEventListener('mousedown', closeOnOutside); } }; document.addEventListener('mousedown', closeOnOutside); }, 0); }
function closeQuickAdd(){ quickAdd.style.display='none'; }
function addAndConnect(type, fromId){ const srcEl = document.querySelector(`[data-node-id="${fromId}"]`); const x = (parseInt(srcEl?.style.left||'0')||0) + 280; const y = (parseInt(srcEl?.style.top||'0')||0); const n = addNode(type, x, y); state.edges = state.edges.filter(e=> !(e.from===fromId && e.to===n.id)); state.edges.push({ from: fromId, to: n.id }); selectNode(n.id); drawEdges(); refreshForms(); closeQuickAdd(); state.pendingSrc = null; render(); }

function bindForm(el, node){
  el.querySelectorAll('input,select,textarea').forEach(inp=>{
    const handler = ()=>{
      node.params = node.params || {};
      node.params[inp.name] = inp.value;
      // Only re-render the body when structural fields change, not on every keystroke
      if(node.type==='pandas.ReadCSV' && inp.name==='mode'){
        el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: ()=> computeUpstreamColumns(node), getUpstreamNode: ()=> upstreamOf(node) });
        bindForm(el, node);
      }
    };
    // Update state on input, but avoid full refresh that would recreate the field and lose caret
    inp.addEventListener('input', handler);
    // On change/blur, refresh dependent UI once
    inp.addEventListener('change', ()=>{ handler(); refreshForms(); });
  });
  // Removed deprecated Load Variables UI. Use the Variables tab with drag-and-drop instead.
  const chooseBtn = el.querySelector('.choose-folder');
  if(chooseBtn){
    chooseBtn.addEventListener('click', async (e)=>{
      e.preventDefault();
      const info = el.querySelector('.folder-info');
      const setInfo = (t)=>{ if(info) info.textContent = t; };
      try{
        if(window.showDirectoryPicker){
          const dir = await window.showDirectoryPicker();
          let firstCsv = null;
          let count=0;
          for await (const [name, handle] of dir.entries()){
            if(handle.kind==='file' && name.toLowerCase().endsWith('.csv')){
              const f = await handle.getFile();
              const text = await f.text();
              if(!firstCsv) firstCsv = { name, text };
              count++;
            }
          }
          setInfo(`${count} CSV files found`);
          if(firstCsv){
            node.params.mode='inline';
            node.params.inline = firstCsv.text;
            el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: ()=> computeUpstreamColumns(node), getUpstreamNode: ()=> upstreamOf(node) });
            bindForm(el, node);
          }
        } else {
          const input = document.createElement('input');
          input.type='file';
          input.multiple=true;
          input.webkitdirectory=true;
          input.style.display='none';
          document.body.appendChild(input);
          input.addEventListener('change', async ()=>{
            const files = Array.from(input.files||[]).filter(f=> f.name.toLowerCase().endsWith('.csv'));
            setInfo(`${files.length} CSV files selected`);
            if(files[0]){
              const text = await files[0].text();
              node.params.mode='inline';
              node.params.inline = text;
              el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: ()=> computeUpstreamColumns(node), getUpstreamNode: ()=> upstreamOf(node) });
              bindForm(el, node);
            }
            input.remove();
          }, { once:true });
          input.click();
        }
      }catch(err){ setInfo('folder selection canceled'); }
    });
  }
  // Operation tabs for python.Math
  const opTabs = el.querySelectorAll('.op-tab');
  if(opTabs && opTabs.length){
    opTabs.forEach(btn=>{
      btn.addEventListener('click', ()=>{
        const op = btn.getAttribute('data-op');
        node.params = node.params || {};
        node.params.op = op;
        const hidden = el.querySelector('input[name="op"]');
        if(hidden) hidden.value = op;
        refreshForms();
      });
    });
  }
  const chooseFile = el.querySelector('.choose-file');
  if(chooseFile){
    chooseFile.addEventListener('click', async (e)=>{
      e.preventDefault();
      try{
        const info = el.querySelector('.folder-info');
        if(window.showOpenFilePicker){
          const [fileHandle] = await window.showOpenFilePicker({ multiple:false });
          const file = await fileHandle.getFile();
          const text = await file.text();
          // Update UI input for visibility
          const pathInput = el.querySelector('input[name="path"]');
          if(pathInput){ pathInput.value = file.name; }
          // Switch to inline mode with loaded content (avoid needing full path)
          node.params = node.params || {};
          node.params.mode = 'inline';
          node.params.inline = text;
          if(info) info.textContent = `Loaded ${file.name} into inline`;
          el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: ()=> computeUpstreamColumns(node), getUpstreamNode: ()=> upstreamOf(node) });
          bindForm(el, node);
        } else {
          const input = document.createElement('input');
          input.type='file';
          input.style.display='none';
          document.body.appendChild(input);
          input.addEventListener('change', async ()=>{
            const f = input.files && input.files[0];
            if(f){
              const text = await f.text();
              const pathInput = el.querySelector('input[name="path"]');
              if(pathInput){ pathInput.value = f.name; }
              node.params = node.params || {};
              node.params.mode = 'inline';
              node.params.inline = text;
              if(info) info.textContent = `Loaded ${f.name} into inline`;
              el.querySelector('.body').innerHTML = registry.nodes.get(node.type).form(node, { getUpstreamColumns: ()=> computeUpstreamColumns(node), getUpstreamNode: ()=> upstreamOf(node) });
              bindForm(el, node);
            }
            input.remove();
          }, { once:true });
          input.click();
        }
      }catch{}
    });
  }
}

function createNodeEl(node){
  const def = registry.nodes.get(node.type);
  const el = document.createElement('div');
  el.className='node';
  el.dataset.nodeId = node.id;
  el.style.left = (node.x||80) + 'px';
  el.style.top = (node.y||80) + 'px';
  el.style.width = Math.max(160, node.w || 220) + 'px';
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
  if(state.selectedNodeId === node.id){ el.classList.add('selected'); el.style.outline='2px solid #1f6feb'; }
  if(state.selection && state.selection.has(node.id)){ el.classList.add('selected'); el.style.outline='2px solid #1f6feb'; }
  // Node dragging (accounts for zoom/pan) with multi-select drag
  let dragging=false, offX=0, offY=0; let multiOffsets=null; const head=el.querySelector('.head');
  head.addEventListener('mousedown', e=>{ if(e.button!==0) return; dragging=true; document.body.style.userSelect='none';
    // selection toggle with Ctrl/Meta, range with Shift
    if(e.ctrlKey||e.metaKey){ if(state.selection.has(node.id)) removeFromSelection(node.id); else addToSelection(node.id); }
    else if(e.shiftKey){ if(!state.selection.has(node.id)) addToSelection(node.id); }
    else if(!state.selection.has(node.id)) { setSelection([node.id]); }
    const p = screenToWorldPoint(e.clientX, e.clientY); offX = p.x - (node.x||0); offY = p.y - (node.y||0);
    const selIds = Array.from(state.selection.size? state.selection : new Set([node.id])); multiOffsets = selIds.map(id=>{ const n=getNode(id); return { id, dx: p.x - (n.x||0), dy: p.y - (n.y||0) }; });
  });
  window.addEventListener('mouseup', ()=>{ if(!dragging) return; dragging=false; document.body.style.userSelect='auto'; drawEdges(); saveToLocal(); });
  window.addEventListener('mousemove', e=>{ if(!dragging) return; const p = screenToWorldPoint(e.clientX, e.clientY); const sel = multiOffsets || [{ id: node.id, dx: offX, dy: offY }]; sel.forEach(s=>{ const n=getNode(s.id); if(!n) return; const nx = Math.max(0, p.x - s.dx); const ny = Math.max(0, p.y - s.dy); const el2 = document.querySelector(`[data-node-id="${s.id}"]`); if(el2){ el2.style.left = nx + 'px'; el2.style.top = ny + 'px'; } n.x=nx; n.y=ny; }); drawEdges(); }); el.addEventListener('mousedown', (e)=>{ if(e.button===0){ if(e.shiftKey){ if(state.selection.has(node.id)) removeFromSelection(node.id); else addToSelection(node.id); } else if(!(e.ctrlKey||e.metaKey)) { setSelection([node.id]); } } e.stopPropagation(); });
  const outPort = el.querySelector('.port.out');
  const inPort = el.querySelector('.port.in');
  outPort.addEventListener('click', (ev)=>{ ev.stopPropagation(); document.querySelectorAll('.port.selected').forEach(p=> p.classList.remove('selected')); state.pendingSrc = node.id; outPort.classList.add('selected'); const rect = canvasWrap.getBoundingClientRect(); openQuickAdd(ev.clientX - rect.left + 10, ev.clientY - rect.top + 10, node.id); });
  inPort.addEventListener('click', (ev)=>{ ev.stopPropagation(); if(state.pendingSrc && state.pendingSrc!==node.id){
      // Simple type validation based on node definition metadata (inputType/outputType)
      const fromId = state.pendingSrc; const toId = node.id;
      const fromNode = getNode(fromId); const toNode = getNode(toId);
      const fromDef = registry.nodes.get(fromNode?.type || '') || {};
      const toDef = registry.nodes.get(toNode?.type || '') || {};
      const outT = String(fromDef.outputType || 'Any');
      const inT  = String(toDef.inputType  || 'Any');
      const normalize = (s)=> String(s||'').trim();
      const expandSyn = (t)=>{
        // basic synonyms/aliases
        if(t==='number' || t==='Number') return ['int','float'];
        if(t==='sequence' || t==='Sequence') return ['list','tuple'];
        if(t==='mapping' || t==='Mapping') return ['dict'];
        return [t];
      };
      const typeSet = (spec)=>{
        const raw = normalize(spec);
        if(!raw || raw==='Any') return new Set(['Any']);
        return new Set(raw.split('|').map(s=>normalize(s)).filter(Boolean).flatMap(expandSyn));
      };
      const S = typeSet(outT); const D = typeSet(inT);
      const assignable = (S, D)=>{
        if(D.has('Any') || S.has('Any')) return true;
        if(D.has('None') || S.has('None')) return false;
        for(const s of S){ if(D.has(s)) return true; }
        return false;
      };
      if(!assignable(S, D)){
        appendLog(`[type] connection blocked: ${fromNode?.type||'?'} (${outT}) -> ${toNode?.type||'?'} (${inT})`);
        inPort.animate([{ outline:'2px solid #ff5555' }, { outline:'0' }], { duration: 500 });
        state.pendingSrc=null; document.querySelectorAll('.port.selected').forEach(p=> p.classList.remove('selected')); clearGhost(); closeQuickAdd(); return;
      }
      // Connection policy: most nodes accept a single incoming edge (replace existing).
      // Special-case pandas.Merge to accept up to two incoming edges (do not delete the first).
      const isMerge = (toNode?.type === 'pandas.Merge');
      if(isMerge){
        const incoming = state.edges.filter(e=> e.to===node.id);
        if(incoming.length >= 2){
          appendLog(`[connect] pandas.Merge already has 2 inputs; ignoring extra connection`);
          inPort.animate([{ outline:'2px solid #ffcc00' }, { outline:'0' }], { duration: 500 });
        } else {
          // prevent duplicate identical edge
          const dup = incoming.some(e=> e.from === state.pendingSrc);
          if(!dup){ state.edges.push({ from: state.pendingSrc, to: node.id }); }
        }
      } else {
        state.edges = state.edges.filter(e=> e.to!==node.id);
        state.edges.push({from: state.pendingSrc, to: node.id});
      }
      state.pendingSrc=null; document.querySelectorAll('.port.selected').forEach(p=> p.classList.remove('selected')); drawEdges(); clearGhost(); refreshForms(); closeQuickAdd(); } });
  // Bind form controls
  bindForm(el, node);

  // Horizontal resize (right edge)
  const hHandle = el.querySelector('.node-resize-h');
  if(hHandle){
    let resizing = false, startX=0, startW=0;
    hHandle.addEventListener('mousedown', (e)=>{
      e.stopPropagation();
      resizing = true; startX = e.clientX; startW = parseInt((el.style.width||'').replace('px','')) || (node.w||220);
      document.body.style.userSelect='none';
    });
    window.addEventListener('mouseup', ()=>{ if(!resizing) return; resizing=false; document.body.style.userSelect=''; });
    window.addEventListener('mousemove', (e)=>{
      if(!resizing) return; const dx = (e.clientX - startX) / getScale(); const w = Math.max(160, Math.min(520, startW + dx)); el.style.width = w + 'px'; node.w = w; drawEdges();
    });
  }

  // Vertical resize for preview area (bottom of .preview)
  const vHandle = el.querySelector('.node-resize-v');
  const prevEl = el.querySelector('.preview');
  if(vHandle && prevEl){
    let resizing = false, startY=0, startH=previewH;
    vHandle.addEventListener('mousedown', (e)=>{
      e.stopPropagation();
      resizing = true; startY = e.clientY; startH = prevEl.clientHeight || previewH; document.body.style.userSelect='none';
    });
    window.addEventListener('mouseup', ()=>{ if(!resizing) return; resizing=false; document.body.style.userSelect=''; });
    window.addEventListener('mousemove', (e)=>{
      if(!resizing) return; const dy = (e.clientY - startY) / getScale(); const h = Math.max(80, Math.min(500, startH + dy)); prevEl.style.maxHeight = h + 'px'; node.prevH = h; });
  }
  const nodeRunBtn = el.querySelector('.node-run');
  nodeRunBtn.addEventListener('click', async ()=>{
    await runWithBusy(async ()=>{
      if(isFigureNode(node)){ state.lastPlotNodeId = node.id; }
      ensureWS(); statusEl.textContent='running...';
      const code = genCodeUpTo(node.id); genCodeEl.textContent = code;
      const res = await fetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) });
    let js = {}; try{ js = await res.json(); }catch{}
  appendLog('Sent exec (node): ' + JSON.stringify(js));
    }, nodeRunBtn, 'Running...');
  });
  const prev = el.querySelector(`#prev-${node.id}`); if(prev){ prev.addEventListener('click', (e)=>{ const img = prev.querySelector('img'); if(!img) return; openZoomOverlay(img.src); }); }
  return el; }

function renderToolbar(){ toolbarEl.innerHTML = ''; if(!state.activePkg && registry.packages[0]) state.activePkg = registry.packages[0].name; (registry.packages || []).forEach(p=>{ const details = document.createElement('details'); details.className='pkg-section'; details.open = (state.activePkg === p.name); const summary = document.createElement('summary'); summary.textContent = p.label || p.name; Object.assign(summary.style, { cursor:'pointer', userSelect:'none', padding:'8px 10px' }); details.appendChild(summary); const listWrap = document.createElement('div'); listWrap.style.padding='8px 10px'; const list = (registry.byPackage.get(p.name) || []).filter(t=> !(registry.nodes.get(t)?.hidden)); list.forEach(type=>{ const def = registry.nodes.get(type); const btn = document.createElement('button'); btn.textContent = '➕ ' + (def.title || type); btn.dataset.type = type; btn.addEventListener('click', ()=>{ addNode(type, 80+Math.random()*200, 80+Math.random()*200); render(); }); listWrap.appendChild(btn); }); details.appendChild(listWrap); details.addEventListener('toggle', ()=>{ if(details.open){ state.activePkg = p.name; document.querySelectorAll('#toolbar details.pkg-section').forEach(el=>{ if(el!==details) el.open=false; }); } }); toolbarEl.appendChild(details); }); }

function renderSubsystems(){
  if(!subsystemsEl){ subsystemsEl = document.createElement('div'); subsystemsEl.id='subsystems'; subsystemsEl.style.marginTop = '12px'; toolbarEl.after(subsystemsEl); }
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
  const res = await fetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) }); let js={}; try{ js=await res.json(); }catch{}
  appendLog('Sent exec (group): ' + JSON.stringify(js));
      }, btn, 'Running...');
    });
  });
  subsystemsEl.querySelectorAll('.del-sub').forEach(btn=>{
    btn.addEventListener('click', ()=>{ const gid = btn.getAttribute('data-gid'); state.groups = state.groups.filter(x=> x.id!==gid); renderSubsystems(); saveToLocal(); });
  });
}

function ensureGroupsLayer(){ if(!groupsLayer){ groupsLayer = document.createElement('div'); groupsLayer.id='groupsLayer'; groupsLayer.style.position='absolute'; groupsLayer.style.inset='0'; nodesEl.appendChild(groupsLayer); } groupsLayer.innerHTML=''; }
function renderGroups(){ ensureGroupsLayer(); if(!Array.isArray(state.groups)) return; const scale=getScale();
  state.groups.forEach(g=>{
    const nodesArr = (g.nodeIds||[]).map(id=> state.nodes.find(n=> n.id===id)).filter(Boolean);
    if(!nodesArr.length) return;
    // DOMサイズから厳密に枠を算出（はみ出し抑止）
    const nodeEls = (g.nodeIds||[]).map(id=> document.querySelector(`[data-node-id="${id}"]`)).filter(Boolean);
    let minX=Infinity,minY=Infinity,maxX=-Infinity,maxY=-Infinity;
    nodeEls.forEach(el=>{ const id=el.dataset.nodeId; const n=getNode(id); const r=el.getBoundingClientRect(); const w=r.width/scale, h=r.height/scale; const x=n?.x||0, y=n?.y||0; minX=Math.min(minX,x); minY=Math.min(minY,y); maxX=Math.max(maxX,x+w); maxY=Math.max(maxY,y+h); });
    if(!isFinite(minX)||!isFinite(minY)||!isFinite(maxX)||!isFinite(maxY)) return;
    const margin=16; const frame=document.createElement('div'); frame.className='group-frame'; frame.style.left=(minX-margin)+'px'; frame.style.top=(minY-margin)+'px'; frame.style.width=(maxX-minX+margin*2)+'px'; frame.style.height=(maxY-minY+margin*2)+'px';
    const title=document.createElement('div'); title.className='title'; title.textContent=g.name||'Subsystem'; frame.appendChild(title);
    const actions=document.createElement('div'); actions.className='actions'; actions.innerHTML=`<button class="run">Run</button><button class="del">Delete</button>`; frame.appendChild(actions);
    // タイトルドラッグでグループ移動
    let dragging=false,start=null,starts=null;
    title.addEventListener('mousedown',(e)=>{ if(e.button!==0) return; dragging=true; document.body.style.userSelect='none'; start=screenToWorldPoint(e.clientX,e.clientY); starts=(g.nodeIds||[]).map(id=>{ const n=getNode(id); return {id,x:n?.x||0,y:n?.y||0}; }); e.stopPropagation(); });
    const onMove=(e)=>{ if(!dragging) return; const p=screenToWorldPoint(e.clientX,e.clientY); const dx=p.x-start.x, dy=p.y-start.y; (starts||[]).forEach(s=>{ const n=getNode(s.id); if(!n) return; n.x=s.x+dx; n.y=s.y+dy; const el=document.querySelector(`[data-node-id="${s.id}"]`); if(el){ el.style.left=n.x+'px'; el.style.top=n.y+'px'; } }); drawEdges(); frame.style.left=(minX-margin+dx)+'px'; frame.style.top=(minY-margin+dy)+'px'; };
    const onUp=()=>{ if(!dragging) return; dragging=false; document.body.style.userSelect=''; renderGroups(); saveToLocal(); };
    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp, { once:true });

    actions.querySelector('.run').addEventListener('click', async (e)=>{
      e.stopPropagation(); await runWithBusy(async ()=>{
  ensureWS(); statusEl.textContent='running...'; const code = genCodeForNodes(g.nodeIds, true); genCodeEl.textContent = code; const res = await fetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) }); let js={}; try{ js=await res.json(); }catch{} appendLog('Sent exec (group-frame): ' + JSON.stringify(js));
      }, actions.querySelector('.run'));
    });
    actions.querySelector('.del').addEventListener('click', (e)=>{ e.stopPropagation(); state.groups = state.groups.filter(x=> x!==g); render(); });
    groupsLayer.appendChild(frame);
  });
}
function render(){ nodesEl.innerHTML=''; state.nodes.forEach(n=> nodesEl.appendChild(createNodeEl(n)) ); applyViewTransform(); drawEdges(); const code = genCode(); genCodeEl.textContent = code; refreshForms(); renderSubsystems(); renderGroups(); updateRunButtonsState(); saveToLocal(); }
function refreshForms(){ state.nodes.forEach(n=>{ const el = document.querySelector(`[data-node-id="${n.id}"]`); if(!el) return; const body = el.querySelector('.body'); if(!body) return; const def = registry.nodes.get(n.type); const html = (typeof def.form==='function') ? def.form(n, { getUpstreamColumns: ()=> computeUpstreamColumns(n), getUpstreamNode: ()=> upstreamOf(n) }) : ''; if(typeof html === 'string' && html !== '' && body.innerHTML !== html){ body.innerHTML = html; bindForm(el, n); } }); }

// WebSocket & streaming
let ws; let pendingVarsRefresh = false; function ensureWS(){ if(ws && ws.readyState===1) return; ws = new WebSocket((location.protocol==='https:'?'wss://':'ws://') + location.host + '/ws'); ws.onopen = ()=> { appendLog('[ws] connected'); updateRunButtonsState(); }; ws.onclose = ()=> { appendLog('[ws] closed'); updateRunButtonsState(); }; ws.onmessage = ev => { const data = JSON.parse(ev.data); if (data.type === 'stream') { const t = data.content.text || ''; const re = /\[\[PREVIEW:([^:]+):(HEAD|DESC)\]\]([\s\S]*?)(?=(\n\[\[PREVIEW:|$))/g; let m; let rest = t; while((m = re.exec(t))){ const id=m[1], kind=m[2], body=(m[3]||''); if(kind==='HEAD'){ state.preview.head.set(id, body); const tgt = document.getElementById('prev-' + id); if(tgt){ const hasImg = !!tgt.querySelector('img'); if(!hasImg && !state.preview.headHtml.get(id)){ tgt.innerHTML = `<pre style="margin:0; white-space:pre-wrap">${body.replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&gt;','>':'&gt;'}[ch]))}</pre>`; } } } else { state.preview.desc.set(id, body); } } const reHtml = /\[\[PREVIEW:([^:]+):(HEADHTML|DESCHTML)\]\]([\s\S]*?)(?=(\n\[\[PREVIEW:|$))/g; let mh; while((mh = reHtml.exec(t))){ const id=mh[1], kind=mh[2], body=(mh[3]||''); const n=getNode(id); const pmode=getPreviewMode(); const want = document.getElementById('prev-' + id) && (pmode==='all' || (pmode==='plots' && n && isFigureNode(n))); if(!want) continue; if(n && isFigureNode(n) && pmode!=='all' && pmode!=='plots'){ continue; } if(kind==='HEADHTML'){ state.preview.headHtml.set(id, body); updateNodePreview(id); } else { state.preview.descHtml.set(id, body); updateNodePreview(id); } } rest = rest.replace(re, '').replace(reHtml, ''); const lines = String(rest).split(/\r?\n/); for(const ln of lines){ if(!ln) continue; let mb = ln.match(/^\[\[NODE:([^:]+):BEGIN\]\]$/); if(mb){ state.stream.currentNodeId = mb[1]; state.preview.head.delete(mb[1]); state.preview.desc.delete(mb[1]); state.preview.headHtml.delete(mb[1]); state.preview.descHtml.delete(mb[1]); state.stream.buffers.set(mb[1], ''); const tgt = document.getElementById('prev-' + mb[1]); if(tgt){ tgt.innerHTML = '<div class="empty">Running…</div>'; } continue; } let me = ln.match(/^\[\[NODE:([^:]+):END\]\]$/); if(me){ state.stream.currentNodeId = null; continue; } const cur = state.stream.currentNodeId; if(cur){ const n=getNode(cur); const pmode=getPreviewMode(); const tgt = document.getElementById('prev-' + cur); const allowText = tgt && (pmode==='all' || (pmode==='plots' && n && isFigureNode(n))); if(allowText){ if(!(n && isFigureNode(n))){ const prevTxt = state.stream.buffers.get(cur) || ''; const next = prevTxt + (prevTxt? '\n':'') + ln; state.stream.buffers.set(cur, next); if(tgt && !tgt.querySelector('img') && !state.preview.headHtml.get(cur)){ tgt.innerHTML = `<pre style=\"margin:0; white-space:pre-wrap\">${next.replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&gt;','>':'&gt;'}[ch]))}</pre>`; } } } } else { appendLog(ln); } } updatePreviewDock(); } else if (data.type === 'display_data' || data.type === 'execute_result') { const d = data.content.data || {}; if(d['image/png']){ let nid = (state.lastPlotNodeId||''); let n = getNode(nid); const pmode=getPreviewMode(); let tgt = document.getElementById('prev-' + nid); if(!(n && isFigureNode(n)) || !tgt){ const figs = state.nodes.filter(isFigureNode); if(figs.length){ nid = figs[figs.length-1].id; n = getNode(nid); tgt = document.getElementById('prev-' + nid); } } const allowPlot = pmode!=='none' && (pmode==='all' || (pmode==='plots' && n && isFigureNode(n))); if(allowPlot && tgt){ const imgHtml = `<img style=\"margin-top:8px\" src=\"data:image/png;base64,${d['image/png']}\">`; if(n && isFigureNode(n)){ tgt.innerHTML = imgHtml; const wrap=document.getElementById('prevwrap-'+nid); if(wrap) wrap.open = true; } else { const existingImg = tgt.querySelector('img'); if(tgt.querySelector('.node-preview-grid')){ if(existingImg) existingImg.remove(); tgt.insertAdjacentHTML('beforeend', imgHtml); } else { tgt.innerHTML = imgHtml; } } } } else if (d['text/plain']) { appendLog(d['text/plain']); } else { appendLog('[output] ' + JSON.stringify(d)); } } else if (data.type === 'error') { appendLog('[error] ' + (data.content.ename + ': ' + data.content.evalue)); const id = state.stream.currentNodeId; if(id){ const tgt = document.getElementById('prev-' + id); if(tgt){ tgt.innerHTML = `<pre style=\"color:#ff8888; white-space:pre-wrap; margin:0\">${(data.content.evalue||'').toString().replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&gt;','>':'&gt;'}[ch]))}</pre>`; const wrap=document.getElementById('prevwrap-'+id); if(wrap) wrap.open = true; } } } else if (data.type === 'status') { if(data.content && data.content.execution_state==='idle'){ if(pendingVarsRefresh){ pendingVarsRefresh=false; try{ refreshVariables(); }catch{} } runningLock=false; updateRunButtonsState(); } else { runningLock=true; updateRunButtonsState(); } } }; }

function updateNodePreview(id){ const tgt = document.getElementById('prev-' + id); if(!tgt) return; const n = getNode(id); if(n && isFigureNode(n)){ return; } const hHtml = state.preview.headHtml.get(id); const dHtml = state.preview.descHtml.get(id); const hTxt = state.preview.head.get(id); const dTxt = state.preview.desc.get(id); const headPart = hHtml ? styleTableHtml(hHtml) : (hTxt ? `<pre style="margin:0; white-space:pre-wrap">${hTxt.replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&lt;','>':'&gt;'}[ch]))}</pre>` : ''); const descPart = dHtml ? styleTableHtml(dHtml) : (dTxt ? `<pre style="margin:0; white-space:pre-wrap">${dTxt.replace(/[&<>]/g, ch=> ({'&':'&amp;','<':'&gt;','>':'&gt;'}[ch]))}</pre>` : ''); if(!headPart && !descPart){ return; } const oldImg = tgt.querySelector('img'); tgt.innerHTML = `<div class="node-preview-grid" style="display:grid; grid-template-columns:1fr 1fr; gap:8px; align-items:start;"><div>${headPart || ''}</div><div>${descPart || ''}</div></div>`; if(oldImg){ tgt.appendChild(oldImg); } }

export async function boot(){ await loadPackages(); renderToolbar(); applyViewTransform();
  // try restore
  try{ restoreFromLocal(); }catch{}
  document.getElementById('sampleBtn').addEventListener('click', ()=>{ state.nodes=[]; state.edges=[]; state.nextId=1; state.groups=[]; const n1 = addNode('pandas.ReadCSV', 60, 60); const n2 = addNode('pandas.FilterRows', 340, 80); const n3 = addNode('pandas.XYPlot', 620, 100); state.edges.push({from:n1.id,to:n2.id},{from:n2.id,to:n3.id}); setSelection([]); render(); });
  document.getElementById('installBtn').addEventListener('click', async ()=>{ statusEl.textContent='installing...'; appendLog('Installing requirements...'); const res = await fetch('/bootstrap', { method:'POST' }); const js = await res.json().catch(()=>({})); appendLog(js.output ? js.output : JSON.stringify(js)); statusEl.textContent='idle'; });
  document.getElementById('restartBtn').addEventListener('click', async ()=>{ appendLog('[kernel] restarting...'); statusEl.textContent='restarting...'; try{ const res = await fetch('/restart', { method:'POST' }); const js = await res.json().catch(()=>({})); appendLog('[kernel] restarted ' + JSON.stringify(js)); }catch(e){ appendLog('[kernel] restart error'); } statusEl.textContent='idle'; try{ if(ws) ws.close(); }catch{} ensureWS(); });

  // selection rectangle on empty canvas
  ensureSelBox();
  canvasWrap.addEventListener('mousedown', (e)=>{ if(e.button!==0) return; if(e.target.closest('.node')) return; selecting = true; selStart = { x:e.clientX, y:e.clientY }; lastSel = Array.from(state.selection||[]); const r = canvasWrap.getBoundingClientRect(); selBoxEl.style.display='block'; selBoxEl.style.left=(selStart.x-r.left)+'px'; selBoxEl.style.top=(selStart.y-r.top)+'px'; selBoxEl.style.width='0px'; selBoxEl.style.height='0px'; setSelection([]); });
  window.addEventListener('mousemove', (e)=>{ if(!selecting) return; const r = canvasWrap.getBoundingClientRect(); const x1=Math.min(selStart.x,e.clientX)-r.left, y1=Math.min(selStart.y,e.clientY)-r.top; const x2=Math.max(selStart.x,e.clientX)-r.left, y2=Math.max(selStart.y,e.clientY)-r.top; selBoxEl.style.left=x1+'px'; selBoxEl.style.top=y1+'px'; selBoxEl.style.width=(x2-x1)+'px'; selBoxEl.style.height=(y2-y1)+'px'; const wrect = worldRectFromScreen({x:selStart.x,y:selStart.y},{x:e.clientX,y:e.clientY}); const selIds=[]; state.nodes.forEach(n=>{ const el=document.querySelector(`[data-node-id="${n.id}"]`); if(!el) return; const scale=getScale(); const w=el.getBoundingClientRect().width/scale; const h=el.getBoundingClientRect().height/scale; const r2={x:n.x||0,y:n.y||0,w,h}; if(rectsIntersect(wrect,r2)) selIds.push(n.id); }); setSelection(selIds); document.querySelectorAll('#nodes .node').forEach(el=>{ const id=el.dataset.nodeId; if(state.selection.has(id)) el.classList.add('selected'); else el.classList.remove('selected'); }); });
  window.addEventListener('mouseup', ()=>{ if(!selecting) return; selecting=false; selBoxEl.style.display='none'; saveToLocal(); });

  // context menu
  const menu = document.createElement('div'); menu.id='ctxMenu'; Object.assign(menu.style,{position:'absolute',background:'#0b1220',border:'1px solid #263041',borderRadius:'6px',boxShadow:'0 8px 24px rgba(0,0,0,0.35)',zIndex:3000,display:'none',minWidth:'160px'}); menu.innerHTML = `
    <button data-act="copy">コピー</button>
    <button data-act="cut">切り取り</button>
    <button data-act="paste">貼り付け</button>
    <button data-act="dup">複製</button>
    <hr style="border:none; border-top:1px solid #263041; margin:6px 0">
    <button data-act="subsys">サブシステム化</button>
    <button data-act="runSel">選択ノードを実行</button>`; document.body.appendChild(menu);
  menu.querySelectorAll('button').forEach(b=> Object.assign(b.style,{display:'block',width:'100%',textAlign:'left',background:'transparent',color:'var(--text)',border:0,padding:'8px 10px',cursor:'pointer'}));
  function openMenu(x,y){ menu.style.left=x+'px'; menu.style.top=y+'px'; menu.style.display='block'; setTimeout(()=>{ const close=(e)=>{ if(!menu.contains(e.target)){ menu.style.display='none'; document.removeEventListener('mousedown', close); } }; document.addEventListener('mousedown', close); },0); }
  function handleCtxAction(act, clientX, clientY, targetNodeId){ const ids = Array.from(state.selection.size? state.selection : (targetNodeId? [targetNodeId]:[]) ); const world = screenToWorldPoint(clientX, clientY); if(act==='copy'){ state.clipboard = makeSubgraph(ids); } else if(act==='cut'){ state.clipboard = makeSubgraph(ids); deleteNodes(ids); setSelection([]); render(); } else if(act==='paste'){ const newIds = pasteSubgraph(state.clipboard, {x:world.x,y:world.y}); setSelection(newIds); render(); } else if(act==='dup'){ const data = makeSubgraph(ids); const newIds = pasteSubgraph(data, {x:world.x,y:world.y}); setSelection(newIds); render(); } else if(act==='subsys'){ if(!ids.length) return; const name = prompt('サブシステム名を入力', 'Subsystem'); if(name!==null){ createGroup(name, ids); render(); } } else if(act==='runSel'){ if(!ids.length) return; if(!canRun()){ updateRunButtonsState(); return; } runWithBusy(async ()=>{ ensureWS(); statusEl.textContent='running...'; const code = genCodeForNodes(ids, true); genCodeEl.textContent = code; const res = await fetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) }); const js = await res.json().catch(()=>({})); appendLog('Sent exec (selection): ' + JSON.stringify(js)); }); } saveToLocal(); }
  nodesEl.addEventListener('contextmenu', (e)=>{ const nodeEl = e.target.closest('.node'); e.preventDefault(); const targetId = nodeEl?.dataset?.nodeId; openMenu(e.clientX, e.clientY); menu.onclick = (ev)=>{ const b = ev.target.closest('button[data-act]'); if(!b) return; ev.preventDefault(); ev.stopPropagation(); menu.style.display='none'; handleCtxAction(b.getAttribute('data-act'), e.clientX, e.clientY, targetId); }; });
  canvasWrap.addEventListener('contextmenu', (e)=>{ if(e.target.closest('.node')) return; e.preventDefault(); openMenu(e.clientX, e.clientY); menu.onclick = (ev)=>{ const b = ev.target.closest('button[data-act]'); if(!b) return; ev.preventDefault(); ev.stopPropagation(); menu.style.display='none'; handleCtxAction(b.getAttribute('data-act'), e.clientX, e.clientY, null); }; });

  // Keyboard shortcuts & paste anchor tracking
  canvasWrap.addEventListener('mousemove', (e)=>{ const p = screenToWorldPoint(e.clientX, e.clientY); lastMouseWorld = p; });
  window.addEventListener('keydown', (e)=>{
    const t = (e.target && e.target.tagName) ? e.target.tagName.toUpperCase() : '';
    if(t==='INPUT' || t==='TEXTAREA' || t==='SELECT') return;
    const ids = Array.from(state.selection||[]);
    if((e.ctrlKey||e.metaKey) && e.key.toLowerCase()==='c'){
      if(ids.length){ state.clipboard = makeSubgraph(ids); }
      e.preventDefault();
    } else if((e.ctrlKey||e.metaKey) && e.key.toLowerCase()==='x'){
      if(ids.length){ state.clipboard = makeSubgraph(ids); deleteNodes(ids); setSelection([]); render(); saveToLocal(); }
      e.preventDefault();
    } else if((e.ctrlKey||e.metaKey) && e.key.toLowerCase()==='v'){
      if(state.clipboard){ const newIds = pasteSubgraph(state.clipboard, { x:lastMouseWorld.x, y:lastMouseWorld.y }); setSelection(newIds); render(); saveToLocal(); }
      e.preventDefault();
    } else if((e.ctrlKey||e.metaKey) && e.key.toLowerCase()==='d'){
      if(ids.length){ const data = makeSubgraph(ids); const newIds = pasteSubgraph(data, { x:(lastMouseWorld.x||100)+20, y:(lastMouseWorld.y||100)+20 }); setSelection(newIds); render(); saveToLocal(); }
      e.preventDefault();
    } else if(e.key==='Delete'){
      if(ids.length){ deleteNodes(ids); setSelection([]); render(); saveToLocal(); }
      e.preventDefault();
    }
  });

  // always reset kernel variables on page load
  try{ await fetch('/restart', { method:'POST' }); appendLog('[kernel] restarted on load'); }catch{}
  ensureWS(); render(); }
