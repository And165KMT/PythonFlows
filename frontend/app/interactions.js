import { pasteSubgraph, setSelection, createGroup } from './nodes.js';
import { setGhost as setGhostMod } from './edges.js';
import { openContextMenu, closeContextMenu } from './contextmenu.js';

// Initialize selection box, panning/zoom, and canvas context menu
export function initInteractions({ state, canvasWrap, nodesEl, edgesSvg, getScale, getTx, getTy, screenToWorldPoint, applyViewTransform, drawEdges, saveToLocal }) {
  // selection rectangle element
  let selBoxEl = null;
  function ensureSelBox() {
    if (selBoxEl) return;
    selBoxEl = document.createElement('div');
    selBoxEl.id = 'selectionBox';
    Object.assign(selBoxEl.style, {
      position: 'absolute', border: '1px dashed #1f6feb', background: 'rgba(31,111,235,0.1)', pointerEvents: 'none', display: 'none', zIndex: 1500
    });
    canvasWrap.appendChild(selBoxEl);
  }
  function worldRectFromScreen(a, b) {
    const p1 = screenToWorldPoint(a.x, a.y);
    const p2 = screenToWorldPoint(b.x, b.y);
    const x1 = Math.min(p1.x, p2.x), y1 = Math.min(p1.y, p2.y);
    const x2 = Math.max(p1.x, p2.x), y2 = Math.max(p1.y, p2.y);
    return { x: x1, y: y1, w: x2 - x1, h: y2 - y1 };
  }
  function rectsIntersect(r1, r2) {
    return !(r2.x > r1.x + r1.w || r2.x + r2.w < r1.x || r2.y > r1.y + r1.h || r2.y + r2.h < r1.y);
  }

  ensureSelBox();

  // selection drag
  let selecting = false;
  let selStartScreen = null;
  let baseSelection = new Set();

  canvasWrap.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return;
    if (e.target.closest('.node')) return;
    // Quick Add: 右側への接続準備中に空白クリックで候補を出す
    try{
      if(state.pendingSrc && !e.target.closest('#quickAdd')){
        const itemsPanel = document.getElementById('quickAdd');
        if(!itemsPanel){
          const evt = new CustomEvent('pf:openQuickAddAt', { detail: { x: e.clientX, y: e.clientY, fromId: state.pendingSrc } });
          document.dispatchEvent(evt);
          e.preventDefault();
          return;
        }
      }
    }catch{}
    selecting = true;
    const rect = canvasWrap.getBoundingClientRect();
    selStartScreen = { x: e.clientX, y: e.clientY };
    baseSelection = new Set(state.selection || new Set());
    selBoxEl.style.display = 'block';
    selBoxEl.style.left = (e.clientX - rect.left) + 'px';
    selBoxEl.style.top = (e.clientY - rect.top) + 'px';
    selBoxEl.style.width = '0px';
    selBoxEl.style.height = '0px';
    document.body.style.userSelect = 'none';
    closeContextMenu();
  });

  window.addEventListener('mousemove', (e) => {
    // ghost edge while connecting
    if (state.pendingSrc) {
      try {
        const r = edgesSvg.getBoundingClientRect();
        setGhostMod(state, edgesSvg, e.clientX - r.left, e.clientY - r.top);
      } catch {}
    }
    if (!selecting) return;
    const rect = canvasWrap.getBoundingClientRect();
    const x1 = selStartScreen.x, y1 = selStartScreen.y;
    const x2 = e.clientX, y2 = e.clientY;
    const left = Math.min(x1, x2) - rect.left;
    const top = Math.min(y1, y2) - rect.top;
    const w = Math.abs(x2 - x1);
    const h = Math.abs(y2 - y1);
    selBoxEl.style.left = left + 'px';
    selBoxEl.style.top = top + 'px';
    selBoxEl.style.width = w + 'px';
    selBoxEl.style.height = h + 'px';

    const selWorld = worldRectFromScreen({ x: x1, y: y1 }, { x: x2, y: y2 });
    const scale = getScale();
    const ids = [];
    state.nodes.forEach(n => {
      const el = document.querySelector(`[data-node-id="${n.id}"]`);
      if (!el) return;
      const r = el.getBoundingClientRect();
      const w0 = r.width / scale, h0 = r.height / scale;
      const nRect = { x: n.x || 0, y: n.y || 0, w: w0, h: h0 };
      if (rectsIntersect(selWorld, nRect)) ids.push(n.id);
    });
    if (e.shiftKey) {
      const merged = new Set([...baseSelection, ...ids]);
      setSelection([...merged]);
    } else {
      setSelection(ids);
    }
    // 反映: 選択ハイライト
    try{
      const S = new Set(state.selection||[]);
      document.querySelectorAll('.node').forEach(el=>{
        const id = el.getAttribute('data-node-id');
        if(!id) return;
        if(S.has(id)) el.classList.add('selected'); else el.classList.remove('selected');
      });
    }catch{}
  });

  window.addEventListener('mouseup', () => {
    if (selecting) {
      selecting = false;
      selBoxEl.style.display = 'none';
      document.body.style.userSelect = '';
      saveToLocal();
    }
  });

  // node left-drag move (supports multi-selection)
  // 委譲で .node 上の左押下からドラッグを開始。フォームやポート等は除外
  nodesEl.addEventListener('mousedown', (e) => {
    if (e.button !== 0) return; // left only
    const nodeEl = e.target.closest('.node');
    if (!nodeEl) return;
    // ignore interactive/port/resizer elements
    if (e.target.closest('input, textarea, select, button, a, .port, .node-resize-h, .node-resize-v')) return;

    const id = nodeEl.getAttribute('data-node-id');
    if (!id) return;

    // Determine drag targets: current selection if includes id, otherwise just this id
    const sel = new Set(state.selection || []);
    const targetIds = sel.has(id) ? Array.from(sel) : [id];

    let startWorld = null;
    const starts = new Map(); // id -> {x,y}

    startWorld = screenToWorldPoint(e.clientX, e.clientY);
    targetIds.forEach(tid => {
      const n = state.nodes.find(n => n.id === tid);
      if (n) starts.set(tid, { x: n.x || 0, y: n.y || 0 });
    });

    let draggingNode = true;
    document.body.style.userSelect = 'none';

    const onMove = (ev) => {
      if (!draggingNode) return;
      const p = screenToWorldPoint(ev.clientX, ev.clientY);
      const dx = p.x - startWorld.x;
      const dy = p.y - startWorld.y;
      starts.forEach((st, tid) => {
        const n = state.nodes.find(n => n.id === tid);
        if (!n) return;
        n.x = st.x + dx;
        n.y = st.y + dy;
        const el = document.querySelector(`[data-node-id="${tid}"]`);
        if (el) {
          el.style.left = n.x + 'px';
          el.style.top = n.y + 'px';
        }
      });
      drawEdges();
    };

    const onUp = () => {
      if (!draggingNode) return;
      draggingNode = false;
      document.body.style.userSelect = '';
      window.removeEventListener('mousemove', onMove);
      window.removeEventListener('mouseup', onUp);
      saveToLocal();
    };

    window.addEventListener('mousemove', onMove);
    window.addEventListener('mouseup', onUp);
  });

  // right-drag panning
  let panning = false; let panStart = null; let panStartView = null;
  canvasWrap.addEventListener('mousedown', (e) => {
    if (e.button !== 2) return;
    if (e.target.closest('.node')) return;
    panning = true;
    panStart = { x: e.clientX, y: e.clientY };
    panStartView = { tx: getTx(), ty: getTy() };
    document.body.style.userSelect = 'none';
    closeContextMenu();
  });
  window.addEventListener('mousemove', (e) => {
    if (!panning) return;
    const dx = e.clientX - panStart.x;
    const dy = e.clientY - panStart.y;
    state.view = state.view || {};
    state.view.tx = panStartView.tx + dx;
    state.view.ty = panStartView.ty + dy;
    applyViewTransform();
    drawEdges();
  });
  window.addEventListener('mouseup', () => {
    if (panning) {
      panning = false; document.body.style.userSelect = '';
    }
  });

  // wheel zoom
  canvasWrap.addEventListener('wheel', (e) => {
    e.preventDefault();
    const delta = Math.sign(e.deltaY);
    const s0 = getScale();
    const factor = (delta > 0) ? 0.9 : 1.1;
    let s1 = Math.max(0.4, Math.min(2.5, s0 * factor));
    const rect = canvasWrap.getBoundingClientRect();
    const px = e.clientX - rect.left; const py = e.clientY - rect.top;
    const tx0 = getTx(), ty0 = getTy();
    const wx = (px - tx0) / s0; const wy = (py - ty0) / s0;
    const tx1 = px - wx * s1; const ty1 = py - wy * s1;
    state.view = state.view || {}; state.view.scale = s1; state.view.tx = tx1; state.view.ty = ty1;
    applyViewTransform();
    drawEdges();
  }, { passive: false });

  // canvas context menu (paste)
  canvasWrap.addEventListener('contextmenu', (e) => {
    if (e.target.closest('.node')) return;
    e.preventDefault();
    const items = [
      { key: 'paste', label: 'Paste', disabled: !window.__pf_clipboardGraph, onClick: () => {
          const g = window.__pf_clipboardGraph; if (!g) return;
          try {
            const wpt = screenToWorldPoint(e.clientX, e.clientY);
            const newIds = pasteSubgraph(g, { x: (wpt.x || 0) + 40, y: (wpt.y || 0) + 40 }) || [];
            if (newIds && newIds.length) setSelection(newIds);
          } catch {}
        }
      },
      { key: 'group', label: 'Group selection', disabled: !(state.selection && state.selection.size>0), onClick: ()=>{
          try{
            const ids = Array.from(state.selection||[]);
            if(ids.length){ createGroup('Subsystem', ids); }
            // UI側に再描画を促す（renderSubsystems/renderGroups呼び出しはui.js内）
            document.dispatchEvent(new CustomEvent('pf:groups:changed'));
          }catch{}
        } }
    ];
    openContextMenu(items, e.clientX, e.clientY);
  });
  window.addEventListener('scroll', closeContextMenu);
}
