import { registry, addNode, selectNode } from './nodes.js';

let quickAdd;
function ensureQuickAdd() {
  if (quickAdd) return quickAdd;
  quickAdd = document.createElement('div');
  quickAdd.id = 'quickAdd';
  Object.assign(quickAdd.style, { position: 'absolute', display: 'none', zIndex: '2000', background: '#0b1220', border: '1px solid #263041', borderRadius: '8px', minWidth: '260px', maxWidth: '320px', boxShadow: '0 8px 24px rgba(0,0,0,0.35)' });
  quickAdd.innerHTML = `<div style="padding:8px 8px 0 8px; border-bottom:1px solid #263041"><input id="qaSearch" placeholder="Search nodes..." style="width:100%; padding:6px 8px; border:1px solid #2c3b52; border-radius:6px; background:#111824; color:var(--text)"></div><div id="qaSuggestedWrap" style="padding:8px; display:none"><div style="font-size:12px; opacity:0.8; margin-bottom:6px">Suggested</div><div id="qaSuggested" style="display:flex; flex-wrap:wrap; gap:6px"></div></div><div id="qaAll" style="max-height:240px; overflow:auto; padding:6px 0"></div>`;
  document.body.appendChild(quickAdd);
  return quickAdd;
}

function itemHtml(type) { const def = registry.nodes.get(type); const label = def?.title || type.split('.').slice(-1)[0] || type; return `<button data-type="${type}" style="display:block; width:100%; text-align:left; padding:8px 10px; background:transparent; color:var(--text); border:0; cursor:pointer">${label} <span style="opacity:0.6; font-size:12px">(${type})</span></button>`; }
function buttonPill(type) { const def = registry.nodes.get(type); const label = def?.title || type.split('.').slice(-1)[0] || type; return `<button data-type="${type}" style="padding:6px 8px; background:#111824; color:var(--text); border:1px solid #263041; border-radius:999px; cursor:pointer">${label}</button>`; }

export function openQuickAdd(x, y, fromId, suggestionsForNode, addAndConnect) {
  const qa = ensureQuickAdd();
  const sWrap = qa.querySelector('#qaSuggestedWrap');
  const s = qa.querySelector('#qaSuggested');
  const all = qa.querySelector('#qaAll');
  const inp = qa.querySelector('#qaSearch');
  qa.style.left = `${x}px`;
  qa.style.top = `${y}px`;
  const sugg = suggestionsForNode(fromId);
  if (sugg.length) { sWrap.style.display = 'block'; s.innerHTML = sugg.map(buttonPill).join(''); }
  else { sWrap.style.display = 'none'; s.innerHTML = ''; }
  const types = Array.from(registry.nodes.keys()).filter(t => !(registry.nodes.get(t)?.hidden));
  // Relevance sort: autogen first, then title match, then type match
  const scored = types.map(t=>{
    const def = registry.nodes.get(t) || {};
    const isAuto = def.origin==='autogen' ? 1 : 0;
    const title = (def.title||'')+'';
    const base = 0 + isAuto*10 + (title ? 1 : 0);
    return { t, score: base, title: title.toLowerCase(), type: String(t).toLowerCase() };
  });
  scored.sort((a,b)=> b.score - a.score || a.title.localeCompare(b.title));
  all.innerHTML = scored.map(x=> itemHtml(x.t)).join('');
  qa.style.display = 'block';
  const clickHandler = (ev) => {
    const btn = ev.target.closest('button[data-type]');
    if (!btn) return;
    ev.preventDefault(); ev.stopPropagation();
    const type = btn.getAttribute('data-type');
    addAndConnect(type, fromId);
    closeQuickAdd();
  };
  qa.addEventListener('click', clickHandler, { once: true });
  inp.value = '';
  inp.oninput = () => {
    const q = (inp.value||'').toLowerCase();
    const list = types
      .map(t=>({ t, def: registry.nodes.get(t)||{} }))
      .filter(x=> x.t.toLowerCase().includes(q) || String(x.def.title||'').toLowerCase().includes(q))
      .map(x=> ({ t: x.t, def: x.def, title: String(x.def.title||'').toLowerCase() }))
      .sort((a,b)=>{
        const aStarts = (a.title.startsWith(q) ? 2 : 0) + (a.t.toLowerCase().startsWith(q) ? 1 : 0);
        const bStarts = (b.title.startsWith(q) ? 2 : 0) + (b.t.toLowerCase().startsWith(q) ? 1 : 0);
        if(aStarts!==bStarts) return bStarts - aStarts;
        const aAuto = (a.def.origin==='autogen')?1:0; const bAuto = (b.def.origin==='autogen')?1:0;
        if(aAuto!==bAuto) return bAuto - aAuto;
        return a.title.localeCompare(b.title);
      })
      .map(x=> x.t);
    all.innerHTML = list.map(itemHtml).join('');
  };
  setTimeout(() => {
    const closeOnOutside = (e) => { if (!qa.contains(e.target)) { closeQuickAdd(); document.removeEventListener('mousedown', closeOnOutside); } };
    document.addEventListener('mousedown', closeOnOutside);
  }, 0);
}

export function closeQuickAdd() { const qa = ensureQuickAdd(); qa.style.display = 'none'; }
