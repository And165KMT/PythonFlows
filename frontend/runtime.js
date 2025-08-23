// FlowPython runtime: registry, canvas, edges, execution

const canvasWrap = document.getElementById('canvasWrap');
const nodesEl = document.getElementById('nodes');
const edgesSvg = document.getElementById('edges');
const log = document.getElementById('log');
const genCodeEl = document.getElementById('genCode');
const statusEl = document.getElementById('status');
const toolbarEl = document.getElementById('toolbar');

// Tabs bar above the toolbar
const tabsBar = document.createElement('div');
tabsBar.style.display = 'flex';
tabsBar.style.gap = '6px';
tabsBar.style.marginBottom = '8px';
toolbarEl.before(tabsBar);

// Global state
const state = { nodes: [], edges: [], nextId: 1, pendingSrc: null, lastPlotNodeId: null, activePkg: null };

// Global Run button
const globalRunBtn = document.createElement('button');
globalRunBtn.textContent = '▶ Run All';
globalRunBtn.style.padding = '6px 10px';
globalRunBtn.style.background = '#1f6feb';
globalRunBtn.style.color = 'white';
globalRunBtn.style.border = '0';
globalRunBtn.style.borderRadius = '6px';
globalRunBtn.style.cursor = 'pointer';
globalRunBtn.addEventListener('click', async ()=>{
  ensureWS(); clearLog(); statusEl.textContent='running...';
  const code = genCode(); genCodeEl.textContent = code;
  const res = await fetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) });
  let js = {}; try{ js = await res.json(); }catch{}
  appendLog('Sent exec: ' + JSON.stringify(js)); statusEl.textContent='idle';
});
tabsBar.appendChild(globalRunBtn);

// Registry
const registry = { packages: [], nodes: new Map(), byPackage: new Map() };

// Helpers
function uid(){ return 'n' + (state.nextId++); }
function appendLog(x){ log.textContent += x + "\n"; log.scrollTop = log.scrollHeight; }
function clearLog(){ log.textContent = ''; }
function getNode(id){ return state.nodes.find(n => n.id === id); }
function centerOf(el){ const r = el.getBoundingClientRect(); const p = edgesSvg.getBoundingClientRect(); return { x: r.left - p.left + r.width/2, y: r.top - p.top + r.height/2 }; }

function syncEdgesViewport(){
  const w = canvasWrap.clientWidth || canvasWrap.getBoundingClientRect().width;
  const h = canvasWrap.clientHeight || canvasWrap.getBoundingClientRect().height;
  if(w && h){ edgesSvg.setAttribute('width', String(Math.floor(w))); edgesSvg.setAttribute('height', String(Math.floor(h))); edgesSvg.setAttribute('viewBox', `0 0 ${Math.floor(w)} ${Math.floor(h)}`); }
}
function drawEdges(){
  syncEdgesViewport(); edgesSvg.innerHTML = '';
  state.edges.forEach(e => {
    const from = document.querySelector(`[data-node-id="${e.from}"]`);
    const to = document.querySelector(`[data-node-id="${e.to}"]`);
    if (!from || !to) return;
    const a = centerOf(from.querySelector('.port.out')); const b = centerOf(to.querySelector('.port.in'));
    const path = document.createElementNS('http://www.w3.org/2000/svg','path');
    const dx = Math.abs(b.x - a.x) * 0.5; const d = `M ${a.x} ${a.y} C ${a.x+dx} ${a.y}, ${b.x-dx} ${b.y}, ${b.x} ${b.y}`;
    path.setAttribute('d', d); path.setAttribute('class','edge'); edgesSvg.appendChild(path);
  });
}
function getPortCenter(nodeId, selector){ const nodeEl = document.querySelector(`[data-node-id="${nodeId}"]`); if(!nodeEl) return null; const port = nodeEl.querySelector(selector); if(!port) return null; return centerOf(port); }
function setGhost(toX, toY){ let ghost = document.getElementById('ghost-edge'); if(!ghost){ ghost = document.createElementNS('http://www.w3.org/2000/svg','path'); ghost.id='ghost-edge'; ghost.setAttribute('class','edge'); ghost.setAttribute('stroke-dasharray','5,5'); edgesSvg.appendChild(ghost); } const a = getPortCenter(state.pendingSrc, '.port.out'); if(!a){ ghost.remove(); return; } const dx = Math.abs(toX - a.x) * 0.5; const d = `M ${a.x} ${a.y} C ${a.x+dx} ${a.y}, ${toX-dx} ${toY}, ${toX} ${toY}`; ghost.setAttribute('d', d); }
function clearGhost(){ const ghost = document.getElementById('ghost-edge'); if(ghost) ghost.remove(); }

// Infer upstream column names statically (best-effort)
function upstreamOf(node){ const e = state.edges.find(x=>x.to===node.id); if(!e) return null; return getNode(e.from); }
function computeUpstreamColumns(n){
  const seen = new Set();
  function walk(cur){
    if(!cur || seen.has(cur.id)) return [];
    seen.add(cur.id);
    if(cur.type==='pandas.SelectColumns'){
      return String(cur.params?.columns||'').split(',').map(s=>s.trim()).filter(Boolean);
    }
    if(cur.type==='pandas.GroupByAggregate'){
      const by = String(cur.params?.by||'group'); const val = String(cur.params?.value||'value');
      return [by, val];
    }
    if(cur.type==='pandas.ReadCSV'){
      if(cur.params?.mode==='inline' && cur.params?.inline){
        const first = String(cur.params.inline).split(/\r?\n/)[0]||''; return first.split(',').map(s=>s.trim()).filter(Boolean);
      }
    }
    if(cur.type==='pandas.FilterRows'){
      const up = upstreamOf(cur); return walk(up);
    }
    const up = upstreamOf(cur); return up ? walk(up) : [];
  }
  const up = upstreamOf(n); return walk(up);
}
function nodeFormHtml(def, node){ if(typeof def.form === 'function') return def.form(node, { getUpstreamColumns: ()=> computeUpstreamColumns(node) }); return ''; }

function refreshPlotForms(){
  state.nodes.filter(n=> n.type==='pandas.Plot').forEach(n=>{
    const el = document.querySelector(`[data-node-id="${n.id}"]`);
    if(!el) return;
    const body = el.querySelector('.body');
    if(body){ body.innerHTML = nodeFormHtml(registry.nodes.get(n.type), n); bindForm(el, n); }
  });
}

function bindForm(el, node){
  el.querySelectorAll('input,select,textarea').forEach(inp=>{
    inp.addEventListener('change', ()=>{
      node.params = node.params || {};
      node.params[inp.name] = inp.value;
      if(node.type==='pandas.ReadCSV' && inp.name==='mode'){
        el.querySelector('.body').innerHTML = nodeFormHtml(registry.nodes.get(node.type), node);
        bindForm(el, node);
      }
    });
  });
  const chooseBtn = el.querySelector('.choose-folder');
  if(chooseBtn){
    chooseBtn.addEventListener('click', async (e)=>{
      e.preventDefault();
      const info = el.querySelector('.folder-info');
      const setInfo = (t)=>{ if(info) info.textContent = t; };
      try{
        if(window.showDirectoryPicker){
          const dir = await window.showDirectoryPicker();
          let firstCsv = null; let count=0;
          for await (const [name, handle] of dir.entries()){
            if(handle.kind==='file' && name.toLowerCase().endsWith('.csv')){
              const f = await handle.getFile(); const text = await f.text();
              if(!firstCsv) firstCsv = { name, text }; count++;
            }
          }
          setInfo(`${count} CSV files found`);
          if(firstCsv){ node.params.mode='inline'; node.params.inline = firstCsv.text; el.querySelector('.body').innerHTML = nodeFormHtml(registry.nodes.get(node.type), node); bindForm(el, node); }
        } else {
          const input = document.createElement('input'); input.type='file'; input.multiple=true; input.webkitdirectory=true; input.style.display='none'; document.body.appendChild(input);
          input.addEventListener('change', async ()=>{
            const files = Array.from(input.files||[]).filter(f=> f.name.toLowerCase().endsWith('.csv'));
            setInfo(`${files.length} CSV files selected`);
            if(files[0]){ const text = await files[0].text(); node.params.mode='inline'; node.params.inline = text; el.querySelector('.body').innerHTML = nodeFormHtml(registry.nodes.get(node.type), node); bindForm(el, node); }
            input.remove();
          }, { once:true }); input.click();
        }
      }catch(err){ setInfo('folder selection canceled'); }
    });
  }
}

function createNodeEl(node){
  const def = registry.nodes.get(node.type);
  const el = document.createElement('div'); el.className = 'node'; el.style.left = node.x+'px'; el.style.top = node.y+'px'; el.dataset.nodeId = node.id;
  el.innerHTML = `
    <div class=\"head\"><span class=\"title\">${def.title || node.type}</span><span class=\"type\">${node.id}</span></div>
    <div class=\"ports\"><div class=\"port in\"></div><div class=\"port out\"></div></div>
    <div class=\"body\">${nodeFormHtml(def, node)}</div>
    <div style=\"display:flex; gap:6px; padding:6px 10px; border-top:1px solid #263041;\">
      <button class=\"node-run\" style=\"flex:1; padding:6px; background:#1f6feb; color:#fff; border:0; border-radius:6px; cursor:pointer\">▶ Run</button>
    </div>
    <div class=\"preview\" id=\"prev-${node.id}\"><div class=\"empty\">No output yet</div></div>
  `;
  // Dragging
  let dragging=false, offX=0, offY=0; const head=el.querySelector('.head');
  head.addEventListener('mousedown', e=>{ dragging=true; document.body.style.userSelect='none'; const rect = canvasWrap.getBoundingClientRect(); const left = parseInt(el.style.left||'0'), top = parseInt(el.style.top||'0'); offX = e.clientX - (rect.left + left); offY = e.clientY - (rect.top + top); });
  window.addEventListener('mouseup', ()=>{ dragging=false; document.body.style.userSelect='auto'; drawEdges(); });
  window.addEventListener('mousemove', e=>{ if(!dragging) return; const rect = canvasWrap.getBoundingClientRect(); const nx = Math.max(0, e.clientX - rect.left - offX); const ny = Math.max(0, e.clientY - rect.top - offY); el.style.left = nx + 'px'; el.style.top = ny + 'px'; node.x=nx; node.y=ny; drawEdges(); });
  // Connect
  const outPort = el.querySelector('.port.out'); const inPort = el.querySelector('.port.in');
  outPort.addEventListener('click', (ev)=>{ ev.stopPropagation(); document.querySelectorAll('.port.selected').forEach(p=> p.classList.remove('selected')); state.pendingSrc = node.id; outPort.classList.add('selected'); });
  inPort.addEventListener('click', (ev)=>{ ev.stopPropagation(); if(state.pendingSrc && state.pendingSrc!==node.id){ state.edges = state.edges.filter(e=> e.to!==node.id); state.edges.push({from: state.pendingSrc, to: node.id}); state.pendingSrc=null; document.querySelectorAll('.port.selected').forEach(p=> p.classList.remove('selected')); drawEdges(); clearGhost(); refreshPlotForms(); } });
  bindForm(el, node);
  // Per-node Run
  el.querySelector('.node-run').addEventListener('click', async ()=>{
    ensureWS(); statusEl.textContent='running...'; const code = genCodeUpTo(node.id); genCodeEl.textContent = code; const res = await fetch('/run', { method:'POST', headers:{'Content-Type':'application/json'}, body: JSON.stringify({ code }) }); let js = {}; try{ js = await res.json(); }catch{} appendLog('Sent exec (node): ' + JSON.stringify(js)); statusEl.textContent='idle';
  });
  return el;
}

function topoSort(){
  const indeg = Object.fromEntries(state.nodes.map(n=>[n.id,0])); state.edges.forEach(e=> indeg[e.to]++ ); const q = state.nodes.filter(n=> indeg[n.id]===0).map(n=>n.id); const out = []; const adj = {}; state.edges.forEach(e=>{ (adj[e.from] ||= []).push(e.to); }); while(q.length){ const u = q.shift(); out.push(u); (adj[u]||[]).forEach(v=>{ if(--indeg[v]===0) q.push(v); }); } return out.map(id=> getNode(id));
}
// upstreamOf is defined above for column inference

function genCode(){
  const order = topoSort();
  const header = [ 'import pandas as pd', 'import matplotlib.pyplot as plt', 'import io', 'import glob', 'plt.close("all")' ];
  const lines = [...header]; const varOf = {}; const ctx = { srcVar: (node)=> varOf[upstreamOf(node)?.id], varOfId: (id)=> varOf[id], setLastPlotNode: (id)=> state.lastPlotNodeId=id };
  order.forEach(n=>{ const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); if(def && typeof def.code==='function'){ const seg = def.code(n, ctx) || []; seg.forEach(s=> lines.push(s)); } lines.push(`print("[[NODE:${n.id}:END]]")`); });
  return lines.join('\n');
}

function genCodeUpTo(targetId){
  const order = topoSort(); const keep = new Set(); const backAdj = {}; state.edges.forEach(e=>{ (backAdj[e.to] ||= []).push(e.from); }); const stack = [targetId]; while(stack.length){ const u = stack.pop(); if(!u || keep.has(u)) continue; keep.add(u); (backAdj[u]||[]).forEach(v=> stack.push(v)); }
  const header = [ 'import pandas as pd', 'import matplotlib.pyplot as plt', 'import io', 'import glob', 'plt.close("all")' ];
  const lines = [...header]; const varOf = {}; const ctx = { srcVar: (node)=> varOf[upstreamOf(node)?.id], varOfId: (id)=> varOf[id], setLastPlotNode: (id)=> state.lastPlotNodeId=id };
  order.forEach(n=>{ if(!keep.has(n.id)) return; const def = registry.nodes.get(n.type); const v = 'v_'+n.id.replace(/[^a-zA-Z0-9_]/g,''); varOf[n.id]=v; lines.push(`print("[[NODE:${n.id}:BEGIN]]")`); if(def && typeof def.code==='function'){ const seg = def.code(n, ctx) || []; seg.forEach(s=> lines.push(s)); } lines.push(`print("[[NODE:${n.id}:END]]")`); });
  return lines.join('\n');
}

function render(){ nodesEl.innerHTML=''; state.nodes.forEach(n=> nodesEl.appendChild(createNodeEl(n)) ); drawEdges(); const code = genCode(); genCodeEl.textContent = code; }
function addNode(type, x=80, y=80){ const def = registry.nodes.get(type); const n = { id: uid(), type, x, y, params: JSON.parse(JSON.stringify(def?.defaultParams||{})) }; state.nodes.push(n); const el = createNodeEl(n); nodesEl.appendChild(el); drawEdges(); return n; }

function renderToolbar(){ toolbarEl.innerHTML = ''; const pkg = state.activePkg || registry.packages[0]?.name; state.activePkg = pkg; const list = registry.byPackage.get(pkg) || []; list.forEach(type=>{ const def = registry.nodes.get(type); const btn = document.createElement('button'); btn.textContent = '➕ ' + (def.title || type); btn.dataset.type = type; btn.addEventListener('click', ()=>{ addNode(type, 80+Math.random()*200, 80+Math.random()*200); render(); }); toolbarEl.appendChild(btn); }); }
function renderTabs(){ tabsBar.innerHTML = ''; registry.packages.forEach(p=>{ const b = document.createElement('button'); b.textContent = p.label || p.name; b.style.flex='1'; b.style.padding='6px'; b.style.background = (state.activePkg===p.name? '#263041':'#111824'); b.style.color='var(--text)'; b.style.border='1px solid #263041'; b.style.borderRadius='6px'; b.style.cursor='pointer'; b.addEventListener('click', ()=>{ state.activePkg=p.name; renderTabs(); renderToolbar(); }); tabsBar.appendChild(b); }); }

// WebSocket & streaming
let ws; function ensureWS(){ if(ws && ws.readyState===1) return; ws = new WebSocket((location.protocol==='https:'?'wss://':'ws://') + location.host + '/ws'); ws.onopen = ()=> appendLog('[ws] connected'); ws.onclose = ()=> appendLog('[ws] closed'); ws.onmessage = ev => { const data = JSON.parse(ev.data); if (data.type === 'stream') { appendLog(data.content.text || ''); } else if (data.type === 'display_data' || data.type === 'execute_result') { const d = data.content.data || {}; if(d['image/png']){ const tgt = document.getElementById('prev-' + (state.lastPlotNodeId||'')); if(tgt){ tgt.innerHTML = `<img src="data:image/png;base64,${d['image/png']}">`; } } else if (d['text/plain']) { appendLog(d['text/plain']); } else { appendLog('[output] ' + JSON.stringify(d)); } } else if (data.type === 'error') { appendLog('[error] ' + (data.content.ename + ': ' + data.content.evalue)); } }; }

// Background events
canvasWrap.addEventListener('click', ()=>{ state.pendingSrc=null; document.querySelectorAll('.port.selected').forEach(p=> p.classList.remove('selected')); clearGhost(); });
window.addEventListener('resize', drawEdges); window.addEventListener('scroll', drawEdges, { passive: true }); canvasWrap.addEventListener('scroll', drawEdges, { passive: true }); window.addEventListener('mousemove', (e)=>{ if(!state.pendingSrc) return; const rect = edgesSvg.getBoundingClientRect(); setGhost(e.clientX - rect.left, e.clientY - rect.top); });

// Sidebar buttons
document.getElementById('installBtn').addEventListener('click', async ()=>{ statusEl.textContent='installing...'; appendLog('Installing requirements...'); const res = await fetch('/bootstrap', { method:'POST' }); const js = await res.json().catch(()=>({})); appendLog(js.output ? js.output : JSON.stringify(js)); statusEl.textContent='idle'; });
document.getElementById('restartBtn').addEventListener('click', async ()=>{ appendLog('[kernel] restarting...'); statusEl.textContent='restarting...'; try{ const res = await fetch('/restart', { method:'POST' }); const js = await res.json().catch(()=>({})); appendLog('[kernel] restarted ' + JSON.stringify(js)); }catch(e){ appendLog('[kernel] restart error'); } statusEl.textContent='idle'; try{ if(ws) ws.close(); }catch{} ensureWS(); });
document.getElementById('sampleBtn').addEventListener('click', ()=>{ state.nodes=[]; state.edges=[]; state.nextId=1; const n1 = addNode('pandas.ReadCSV', 60, 60); const n2 = addNode('pandas.FilterRows', 340, 80); const n3 = addNode('pandas.Plot', 620, 100); state.edges.push({from:n1.id,to:n2.id},{from:n2.id,to:n3.id}); render(); });

// Package loading
async function loadPackages(){ const res = await fetch('/api/packages'); const list = await res.json(); registry.packages = list.map(x=> ({name:x.name, label:x.label})); for(const p of list){ try{ const mod = await import(`/pkg/${p.name}/${p.entry}`); if(mod && typeof mod.register==='function'){ const reg = { node(def){ if(!def || !def.id) return; registry.nodes.set(def.id, def); const pkgName = p.name; if(!registry.byPackage.has(pkgName)) registry.byPackage.set(pkgName, []); registry.byPackage.get(pkgName).push(def.id); } }; mod.register(reg); } }catch(e){ appendLog(`[pkg:${p.name}] load error`); } } state.activePkg = registry.packages[0]?.name || null; renderTabs(); renderToolbar(); document.getElementById('sampleBtn').click(); }
async function callHealth(){ try { const r=await fetch('/health'); appendLog('[health] '+ JSON.stringify(await r.json())); } catch {} }
callHealth(); loadPackages(); syncEdgesViewport();
